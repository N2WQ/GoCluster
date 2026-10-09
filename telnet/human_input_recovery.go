// File role: Rejects a whole physical human line, with bounded wire/time recovery
// and an explicit terminal outcome when the next command boundary is untrusted.
package telnet

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	humanRecoveryWireBytes = 64 * 1024
	humanRecoveryTimeout   = 5 * time.Second
	ziutekInputBufferBytes = 256 // github.com/ziutek/telnet v0.1.0 NewConn.
)

var errHumanRecoveryDeadline = errors.New("human line recovery deadline exceeded")

// humanInputError is always terminal. Callers must inspect it before unwrapping
// timeout/validation causes; those ordinary retry paths would dispatch a tail.
type humanInputError struct{ Err error }

func (e *humanInputError) Error() string { return e.Err.Error() }
func (e *humanInputError) Unwrap() error { return e.Err }

type humanRecoveryTimer interface{ Stop() bool }

type humanRecoveryHooks struct {
	now       func() time.Time
	afterFunc func(time.Duration, func()) humanRecoveryTimer
}

type humanRecovery struct {
	client   *Client
	deadline time.Time
	now      func() time.Time
	mu       sync.Mutex
	finished bool
	expired  bool
	done     chan struct{}
	timer    humanRecoveryTimer
}

func (r *humanRecovery) expire() {
	defer close(r.done)
	r.mu.Lock()
	interrupt := !r.finished
	if interrupt {
		r.expired = true
	}
	r.mu.Unlock()
	if interrupt {
		r.client.interrupt() // Mandatory socket/done phase never waits on reporting.
	}
}

func (r *humanRecovery) check() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.expired || !r.now().Before(r.deadline) {
		return errHumanRecoveryDeadline
	}
	return nil
}

func (r *humanRecovery) accept() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.expired || !r.now().Before(r.deadline) {
		return errHumanRecoveryDeadline
	}
	r.finished = true
	return nil
}

func (r *humanRecovery) disarm() {
	r.mu.Lock()
	r.finished = true
	r.mu.Unlock()
	if !r.timer.Stop() {
		<-r.done // Join a started callback before any next reception can begin.
	}
}

func (c *Client) terminalHumanInput(err error) (string, error) {
	c.interrupt()
	return "", &humanInputError{Err: err}
}

func (c *Client) rejectHumanInput(err error) (string, error) {
	recoveryErr := c.recoverHumanLineWithHooks(humanRecoveryHooks{
		now: time.Now,
		afterFunc: func(duration time.Duration, callback func()) humanRecoveryTimer {
			return time.AfterFunc(duration, callback)
		},
	})
	if recoveryErr != nil {
		return c.terminalHumanInput(errors.Join(err, recoveryErr))
	}
	return "", err
}

func (c *Client) recoverHumanLineWithHooks(hooks humanRecoveryHooks) error {
	previous := c.readDeadline
	deadline := hooks.now().Add(humanRecoveryTimeout)
	if !previous.IsZero() && previous.Before(deadline) {
		deadline = previous
	}
	if !hooks.now().Before(deadline) {
		return errHumanRecoveryDeadline
	}
	if err := c.setReadDeadline(deadline); err != nil {
		return fmt.Errorf("setting human recovery deadline: %w", err)
	}
	if c.inputWire != nil {
		debt := c.reader.Buffered()
		if c.decodedInput() {
			// Any old machine-body read-ahead may remain in the outer reader.
			// Decoded escaped IAC accounts for two wire bytes. Reserve the entire
			// hidden codec buffer because exact encoded positions are unavailable.
			debt = 2*debt + ziutekInputBufferBytes
		}
		c.inputWire.remaining = humanRecoveryWireBytes - debt
		c.inputWire.recovering = true
		defer func() { c.inputWire.recovering = false }()
	}
	r := &humanRecovery{client: c, deadline: deadline, now: hooks.now, done: make(chan struct{})}
	r.timer = hooks.afterFunc(deadline.Sub(hooks.now()), r.expire)
	defer r.disarm()
	state := byte(0)
	for discarded := 0; discarded < humanRecoveryWireBytes; discarded++ {
		if err := r.check(); err != nil {
			return err
		}
		b, err := c.reader.ReadByte()
		if err != nil {
			return err
		}
		if !c.decodedInput() {
			// Native data CR/LF is a boundary only outside IAC/SB. Every byte,
			// including options and escaped IAC, spends the bounded tail budget.
			if state != 0 || b == IAC {
				switch state {
				case 0:
					state = 1
				case 1:
					switch b {
					case DO, DONT, WILL, WONT:
						state = 2
					case SB:
						state = 3
					default:
						state = 0
					}
				case 2:
					state = 0
				case 3:
					if b == IAC {
						state = 4
					}
				case 4:
					state = 3
					if b == SE {
						state = 0
					}
				}
				continue
			}
		}
		if b == '\r' || b == '\n' {
			if err := r.accept(); err != nil {
				return err
			}
			if err := c.setReadDeadline(previous); err != nil {
				return fmt.Errorf("restoring human read deadline: %w", err)
			}
			c.skipNextEOL = b == '\r'
			return nil
		}
	}
	return errHumanRecoveryWireLimit
}

func (c *Client) decodedInput() bool { return c.server != nil && c.server.useZiutek }
