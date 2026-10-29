// File role: Owns the accept-to-registration timeout and socket interruption.
package telnet

import (
	"errors"
	"net"
	"sync"
	"time"
)

var errLoginDeadline = errors.New("configuration handoff exceeded login deadline")

type preloginBudgetState uint8

const (
	preloginActive preloginBudgetState = iota
	preloginFinished
	preloginExpired
	preloginShutdown
)

// Each admitted connection owns one worker and timer, bounded by prelogin
// tickets. The mutex orders socket interruption against registry publication.
// stop joins the worker before its ticket can be reused.
type preloginBudget struct {
	mu       sync.Mutex
	state    preloginBudgetState
	deadline time.Time
	stopCh   chan struct{}
	done     chan struct{}
	shutdown <-chan struct{}
}

func newPreloginBudget(conn net.Conn, deadline time.Time, shutdown <-chan struct{}) *preloginBudget {
	b := &preloginBudget{deadline: deadline, stopCh: make(chan struct{}), done: make(chan struct{}), shutdown: shutdown}
	go func() {
		defer close(b.done)
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		var state preloginBudgetState
		select {
		case <-timer.C:
			state = preloginExpired
		case <-shutdown:
			state = preloginShutdown
		case <-b.stopCh:
			return
		}
		b.mu.Lock()
		if b.state != preloginActive {
			b.mu.Unlock()
			return
		}
		b.state = state
		b.mu.Unlock()
		_ = conn.Close()
	}()
	return b
}

// finish is called while holding the registry lock, immediately before a
// successful registration. An expired worker can never close a new owner.
func (b *preloginBudget) finish() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-b.shutdown:
		if b.state == preloginActive {
			b.state = preloginShutdown
			close(b.stopCh)
		}
		return errClientClosed
	default:
	}
	if b.state == preloginExpired {
		return errLoginDeadline
	}
	if b.state == preloginActive && !time.Now().Before(b.deadline) {
		b.state = preloginExpired
		close(b.stopCh)
		return errLoginDeadline
	}
	if b.state != preloginActive {
		return errClientClosed
	}
	b.state = preloginFinished
	close(b.stopCh)
	return nil
}

func (b *preloginBudget) canContinue() bool {
	b.markDeadline()
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == preloginActive
}

// markDeadline handles a read or handoff deadline that fires before the
// worker is scheduled. Other failures do not become timeouts merely because
// their cleanup took long enough to cross the deadline.
func (b *preloginBudget) markDeadline() {
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-b.shutdown:
		if b.state == preloginActive {
			b.state = preloginShutdown
			close(b.stopCh)
		}
		return
	default:
	}
	if b.state == preloginActive && !time.Now().Before(b.deadline) {
		b.state = preloginExpired
		close(b.stopCh)
	}
}

func (b *preloginBudget) expired() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == preloginExpired
}

func (b *preloginBudget) stop() bool {
	b.mu.Lock()
	if b.state == preloginActive {
		b.state = preloginFinished
		close(b.stopCh)
	}
	expired := b.state == preloginExpired
	b.mu.Unlock()
	<-b.done
	return expired
}
