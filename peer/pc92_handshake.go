package peer

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// An attempt has one winner: pending(0), committed(1), or expired(2).
// The controller commits under manager.mu immediately before registration.
// The caller may expire queued work, but cannot reclassify a committed owner
// because its replay-ready response was delayed. Only one establishment
// request exists per live session, and the bounded mailbox owns stale requests.
type establishmentAttempt struct {
	deadline time.Time
	state    atomic.Uint32
}

func (a *establishmentAttempt) commit() bool {
	if a == nil {
		return true
	}
	if !a.deadline.IsZero() && !time.Now().Before(a.deadline) {
		a.state.CompareAndSwap(0, 2)
		return false
	}
	return a.state.CompareAndSwap(0, 1)
}

func (m *Manager) protocolCall(kind string, s *session) error {
	var deadline time.Time
	if s != nil && (kind == "initial" || kind == "establish") {
		deadline = s.phaseDeadline
	}
	return m.protocolCallBefore(kind, s, deadline)
}

func (m *Manager) protocolCallBefore(kind string, s *session, deadline time.Time) error {
	if m.protocol == nil || m.ctx == nil {
		return fmt.Errorf("peer controller not started")
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	req := protocolRequest{kind: kind, source: s, done: make(chan error, 1), deadline: deadline}
	if kind == "establish" {
		req.attempt = &establishmentAttempt{deadline: deadline}
	}
	ctx := m.ctx
	if s != nil && s.ctx != nil && kind != "closed" {
		ctx = s.ctx
	}
	var expired <-chan time.Time
	if !deadline.IsZero() {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		expired = timer.C
	}
	select {
	case m.protocol.lifecycle <- req:
	case <-expired:
		return context.DeadlineExceeded
	case <-m.ctx.Done():
		return m.ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
	for {
		select {
		case err := <-req.done:
			return err
		case <-expired:
			if req.attempt != nil && !req.attempt.state.CompareAndSwap(0, 2) && req.attempt.state.Load() == 1 {
				// Authority already committed in time. Keep the live reader
				// parked until FIFO replay is ready or its lifetime is canceled.
				expired = nil
				continue
			}
			return context.DeadlineExceeded
		case <-m.ctx.Done():
			return m.ctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *Manager) publishInitial(s *session) error {
	deadline := time.Now().Add(5 * time.Second)
	if !s.phaseDeadline.IsZero() && s.phaseDeadline.Before(deadline) {
		deadline = s.phaseDeadline
	}
	for {
		err := m.protocolCallBefore("initial", s, deadline)
		if !errors.Is(err, ErrTimestampRate) {
			return err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		timer := time.NewTimer(min(10*time.Millisecond, remaining))
		select {
		case <-timer.C:
		case <-s.ctx.Done():
			timer.Stop()
			return s.ctx.Err()
		case <-m.ctx.Done():
			timer.Stop()
			return m.ctx.Err()
		}
	}
}

func (m *Manager) publishPeriodic(s *session, action string) error { return m.protocolCall(action, s) }
func (m *Manager) establishSession(s *session) error               { return m.protocolCall("establish", s) }

// replayPending tells dispatch that the existing request reply now belongs to
// the bounded replay owner. Session.Run cannot start reading live input until
// that reply is delivered. No second input queue is introduced.
var errReplayPending = errors.New("peer: establishment replay pending")

func (p *protocolController) beginReplay(s *session, req protocolRequest) error {
	m := p.manager
	// Registry commitment makes this a publication recipient immediately.
	// Local C/A depends on the complete local snapshot, not on remote staged
	// topology. Reserve its recovery now so a tick during replay cannot send an
	// ordinary delta first or postpone local membership behind a large batch.
	if s.pc9x {
		p.recovering.Set(s, recoveryState{})
	}
	p.dirty = true
	m.mu.Lock()
	c := m.candidates.Value(s)
	if c != nil {
		m.candidates.Delete(s)
		if s.pendingReserved {
			<-m.pendingSlots
			s.pendingReserved = false
		}
	}
	if c != nil && len(c.staged) != 0 {
		c.replayDone = req.done
		s.replayReady = req.done
		p.replays.Set(s, c) // at most64 registered owners; no duplicate begin
		for i := range p.replayOrder {
			if p.replayOrder[i] == nil {
				p.replayOrder[i] = s
				break
			}
		}
	}
	m.mu.Unlock()
	if c == nil || len(c.staged) == 0 {
		if c != nil {
			m.releaseStaged(c)
		}
		return nil
	}
	return errReplayPending
}

// One staged record per service turn bounds monopolization. A
// record's authoritative graph transaction remains indivisible. The original
// batch reservation includes every retained wire until the batch is retired.
func (p *protocolController) serviceReplay() bool {
	for range len(p.replayOrder) {
		i := p.replayCursor
		p.replayCursor = (i + 1) % len(p.replayOrder)
		s := p.replayOrder[i]
		if s == nil {
			continue
		}
		c := p.replays.Value(s)
		if s.ctx != nil && s.ctx.Err() != nil {
			p.finishReplay(s, s.ctx.Err())
			return true
		}
		if c.replayAt < len(c.staged) {
			wire := c.staged[c.replayAt]
			c.replayAt++
			if f, err := ParseFrame(wire); err == nil {
				p.receive(f, s, time.Now())
			}
		}
		if s.ctx != nil && s.ctx.Err() != nil {
			p.finishReplay(s, s.ctx.Err())
		} else if c.replayAt == len(c.staged) {
			p.finishReplay(s, nil)
		}
		return true
	}
	return false
}

func (p *protocolController) finishReplay(s *session, err error) {
	c, ok := p.replays.Get(s)
	if !ok {
		return
	}
	p.replays.Delete(s)
	for i, owner := range p.replayOrder {
		if owner == s {
			p.replayOrder[i] = nil
			break
		}
	}
	done := c.replayDone
	c.replayDone = nil
	p.manager.releaseStaged(c)
	if done != nil {
		done <- err
		close(done) // also releases terminal Run's ownership-retirement wait
	}
}

func (p *protocolController) retireReplays() {
	for s := range p.replays.All() {
		p.finishReplay(s, context.Canceled)
	}
}
