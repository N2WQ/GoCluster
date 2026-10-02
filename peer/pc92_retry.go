package peer

import (
	"context"
	"fmt"
	"time"
)

// retryCoordinator is owned by Manager.mu. Identities acquire a stable ring
// position on their first authoritative refusal and keep it for this manager's
// lifetime. Configured identity admission bounds the occupied prefix by N.
// No record, graph plan, per-record waiter or historical event is retained here.
type retryCoordinator struct {
	slots         [64]retryIdentity
	count, cursor int
	lastGrant     time.Time
}

type retryIdentity struct {
	call                                                  string
	owner                                                 *session
	waitTimer                                             *time.Timer // one reusable backing allocation per identity
	generation                                            uint64
	invalidationGeneration                                uint64
	delay                                                 time.Duration
	due, deadline, healthySince                           time.Time
	active, dialing, ready, granted, failed, invalidating bool
	flushed, established, interrupted                     bool
}

func (m *Manager) retryIdentityLocked(call string) *retryIdentity {
	for i := range m.retry.count {
		if m.retry.slots[i].call == call {
			return &m.retry.slots[i]
		}
	}
	return nil
}

func (m *Manager) retryEventLocked(r *retryIdentity, kind, cause string, at time.Time) {
	event := qualificationAdmissionEvent{Kind: kind, Call: r.call, Cause: cause, Generation: r.generation, At: at}
	if r.owner != nil {
		event.Direction = directionLabel(r.owner.dir)
	} else if r.dialing {
		event.Direction = "outbound"
	}
	m.protocol.qualificationAdmissionEvent(event)
}

// Preserve both entry points: the loader normalizes zero to2s/300s, whereas
// direct constructor nonpositive values historically normalize to1s/1s.
func (m *Manager) retryDelayLocked(r *retryIdentity, now time.Time) {
	base := time.Duration(max(0, m.cfg.Backoff.BaseMS)) * time.Millisecond
	limit := time.Duration(max(0, m.cfg.Backoff.MaxMS)) * time.Millisecond
	if base <= 0 {
		base = time.Second
	}
	if limit < base {
		limit = base
	}
	if r.delay == 0 {
		r.delay = base
	} else if r.delay >= limit-r.delay {
		r.delay = limit
	} else {
		r.delay *= 2
	}
	r.due = now.Add(r.delay)
}

// Called at an actual failure boundary. Keeping the failed owner until terminal
// retirement prevents simultaneous inbound/outbound replacements. Invalidation
// is acknowledged separately by the controller before any startup grant.
func (m *Manager) retryFailLocked(r *retryIdentity, cause string, now time.Time) {
	if r.failed {
		return
	}
	r.failed, r.active, r.invalidating = true, true, true
	r.invalidationGeneration = r.generation
	r.healthySince = time.Time{}
	m.retryDelayLocked(r, now)
	m.blockedPeers.Set(r.call, true)
	m.admissionFailures.Set(r.call, admissionFailure{at: now, generation: r.generation, cause: admissionAuthority})
	m.retryEventLocked(r, "failure", cause, now)
}

func (m *Manager) retryRefused(s *session, cause admissionCause, now time.Time) {
	m.mu.Lock()
	if m.sessions.Value(s.id) != s {
		m.mu.Unlock()
		return
	}
	r := m.retryIdentityLocked(s.remoteCall)
	if r == nil {
		if m.retry.count >= m.cfg.MaxPeers {
			m.mu.Unlock()
			s.close()
			return
		}
		r = &m.retry.slots[m.retry.count]
		m.retry.count++
		r.call = s.remoteCall
	}
	if !r.active {
		m.admissionGeneration++
		*r = retryIdentity{call: r.call, owner: s, waitTimer: r.waitTimer, generation: m.admissionGeneration}
	}
	if r.owner == s && !r.failed {
		m.retryFailLocked(r, cause.String(), now)
		failure := m.admissionFailures.Value(r.call)
		failure.cause = cause
		m.admissionFailures.Set(r.call, failure)
	}
	m.mu.Unlock()
	m.NotifyMembershipChanged()
	s.close()
}

func (m *Manager) retryAttemptLocked(r *retryIdentity, owner *session) {
	m.admissionGeneration++
	r.owner, r.generation = owner, m.admissionGeneration
	r.dialing, r.ready, r.granted, r.failed = owner == nil, false, false, false
	r.flushed, r.established, r.interrupted = false, false, false
	r.deadline, r.healthySince = time.Time{}, time.Time{}
}

// reserveRetryDial is separate from the global startup grant: a failed socket
// dial advances this identity's delay but cannot spend another peer's grant.
func (m *Manager) reserveRetryDial(call string, now time.Time) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.retryIdentityLocked(call)
	if r == nil || !r.active {
		return 0, true
	}
	if m.stopping || m.pc9xGated.Load() || r.owner != nil || r.dialing || r.invalidating || now.Before(r.due) {
		return 0, false
	}
	m.retryAttemptLocked(r, nil)
	return r.generation, true
}

func (m *Manager) finishRetryDial(call string, generation uint64, s *session, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.retryIdentityLocked(call)
	if generation == 0 {
		return r == nil || !r.active
	}
	if r == nil || r.generation != generation || !r.dialing {
		return false
	}
	if s != nil && !r.interrupted && !m.pc9xGated.Load() && !m.stopping {
		r.owner, r.dialing = s, false
		return true
	}
	if s == nil && !r.interrupted && !m.stopping && (m.ctx == nil || m.ctx.Err() == nil) {
		m.retryFailLocked(r, "dial", now)
	}
	m.retryEventLocked(r, "retired", "dial", now)
	r.dialing = false
	return false
}

// Authentication is complete before the inbound caller arrives here. Ordinary
// handshakes retain the128-candidate behavior; only overload histories use the
// one-candidate identity reservation and globally paced startup grant.
func (m *Manager) waitRetryStartup(s *session, deadline time.Time) error {
	m.mu.Lock()
	if m.stopping || (s.preferPC9x && m.pc9xGated.Load()) {
		m.mu.Unlock()
		return fmt.Errorf("PC9x startup gated")
	}
	// Publish negotiation intent through the manager-owned candidate, rather
	// than making global closure race reader-owned handshake fields.
	if candidate := m.candidates.Value(s); candidate != nil && s.preferPC9x {
		candidate.pc9x = true
	}
	r := m.retryIdentityLocked(s.remoteCall)
	if r == nil || !r.active {
		m.mu.Unlock()
		return nil
	}
	if r.owner != s {
		if r.owner != nil || r.dialing {
			m.mu.Unlock()
			return fmt.Errorf("duplicate recovery candidate")
		}
		m.retryAttemptLocked(r, s)
	}
	r.ready, r.deadline = true, deadline
	// Stop may leave a timer reachable as a runtime-heap zombie. Reusing this
	// exact object bounds backing allocations by N through arbitrary retries.
	// Run joins this waiter before another candidate can reuse the timer.
	if r.waitTimer == nil {
		r.waitTimer = time.NewTimer(min(25*time.Millisecond, time.Until(deadline)))
	} else {
		r.waitTimer.Reset(min(25*time.Millisecond, time.Until(deadline)))
	}
	timer := r.waitTimer
	m.mu.Unlock()
	m.NotifyMembershipChanged()
	// One reusable timer bounds both polling and the original deadline. No
	// extra deadline timer or per-attempt goroutine is retained.
	defer timer.Stop()
	for {
		m.mu.RLock()
		valid := r.owner == s && !r.failed && !r.interrupted && !m.stopping && !m.pc9xGated.Load()
		granted := valid && r.granted
		m.mu.RUnlock()
		if !valid {
			return fmt.Errorf("PC9x recovery attempt retired or gated")
		}
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		if err := s.ctx.Err(); err != nil {
			return err
		}
		if granted {
			return nil
		}
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-timer.C:
			timer.Reset(min(25*time.Millisecond, time.Until(deadline)))
		}
	}
}

func (m *Manager) retrySessionAllowedLocked(s *session) bool {
	if m.pc9xGated.Load() {
		return false
	}
	r := m.retryIdentityLocked(s.remoteCall)
	return r == nil || !r.active || (r.owner == s && r.granted && !r.failed && !r.invalidating && !r.interrupted)
}

func (m *Manager) retrySessionAllowed(s *session) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.retrySessionAllowedLocked(s)
}

func (m *Manager) retrySessionEnded(s *session, now time.Time) {
	m.mu.Lock()
	r := m.retryIdentityLocked(s.remoteCall)
	if r != nil && r.active && r.owner == s {
		if r.granted && !r.failed && !r.interrupted && !m.stopping && (m.ctx == nil || m.ctx.Err() == nil) {
			m.retryFailLocked(r, "session", now)
		}
		r.healthySince = time.Time{}
	}
	m.mu.Unlock()
	m.NotifyMembershipChanged()
}

// Retirement follows joined workers and controller unregister/replay cleanup.
// Old callbacks retain only the old pointer and cannot match a replacement.
func (m *Manager) retryRetireLocked(s *session, now time.Time) {
	r := m.retryIdentityLocked(s.remoteCall)
	if r != nil && r.owner == s {
		m.retryEventLocked(r, "retired", "session", now)
		r.owner, r.ready, r.granted = nil, false, false
		r.healthySince = time.Time{}
	}
}

func (m *Manager) retryReadyHealthyLocked(r *retryIdentity, now time.Time) {
	if !r.failed && !r.interrupted && !m.pc9xGated.Load() {
		// Event occurrence, not callback lock acquisition, defines the later
		// endpoint. A fast Flush callback may wait behind establishment.
		r.healthySince = laterAdmissionTime(r.healthySince, now)
	}
}

func (m *Manager) retryEstablished(s *session, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.retryIdentityLocked(s.remoteCall)
	if r != nil && r.active && r.owner == s && s.pc9x && !r.failed && !r.interrupted {
		r.established = true
		m.retryEventLocked(r, "established", "", now)
		m.retryReadyHealthyLocked(r, now)
	}
}

func (m *Manager) retryRecoveryFlushed(s *session, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.retryIdentityLocked(s.remoteCall)
	if r != nil && r.active && r.owner == s && r.granted && !r.flushed && !r.failed && !r.interrupted && s.ctx.Err() == nil && !m.pc9xGated.Load() {
		r.flushed = true
		m.retryEventLocked(r, "recovery_flush", "", now)
		m.retryReadyHealthyLocked(r, now)
	}
}

func (m *Manager) interruptRetriesLocked(now time.Time) {
	for i := range m.retry.count {
		r := &m.retry.slots[i]
		if r.active {
			// Global PC9x closure preserves established legacy service. Its
			// later unrelated failure must not inherit a canceled-attempt flag.
			if current := m.sessions.Value(r.call); current != nil && current == r.owner && !current.pc9x {
				continue
			}
			r.interrupted, r.healthySince = true, time.Time{}
			m.retryEventLocked(r, "global_gate", "", now)
		}
	}
}

// Counts expose current operational phases without retaining event history.
// A candidate waiting for a future due time counts as cooldown until eligible.
func (m *Manager) retryCountsLocked(now time.Time) [6]int {
	var counts [6]int
	for i := range m.retry.count {
		r := &m.retry.slots[i]
		if !r.active {
			continue
		}
		current := m.sessions.Value(r.call)
		legacy := current != nil && current == r.owner && !current.pc9x
		gated := (!legacy && m.pc9xGated.Load()) || (r.interrupted && (r.owner != nil || r.dialing))
		switch {
		case gated:
			counts[5]++
		case r.established && r.flushed && !r.healthySince.IsZero():
			counts[3]++
		case r.dialing || (r.granted && !r.failed):
			counts[2]++
		case now.Before(r.due):
			counts[0]++
		case r.owner != nil || r.invalidating:
			counts[1]++
		default:
			counts[4]++
		}
	}
	return counts
}

// Existing controller service points inspect only bounded scalar state. There
// is no graph decode/prepare, cache scan or refused-payload feasibility test.
func (p *protocolController) serviceAdmissionRecovery(now time.Time) {
	m := p.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.retry.count {
		r := &m.retry.slots[i]
		if r.active && r.established && r.flushed && !r.healthySince.IsZero() && now.Sub(r.healthySince) >= 60*time.Second && r.owner != nil && r.owner.ctx.Err() == nil && !r.failed && !r.interrupted && !m.pc9xGated.Load() {
			m.retryEventLocked(r, "healthy_reset", "", now)
			m.blockedPeers.Delete(r.call)
			p.blocked.Delete(r.call)
			*r = retryIdentity{call: r.call, owner: r.owner, waitTimer: r.waitTimer, generation: r.generation}
		}
	}
	if m.stopping || m.pc9xGated.Load() || now.Sub(m.retry.lastGrant) < time.Second {
		return
	}
	for offset := range m.retry.count {
		i := (m.retry.cursor + offset) % m.retry.count
		r := &m.retry.slots[i]
		if !r.active || r.owner == nil || !r.ready || r.granted || r.failed || r.interrupted || r.invalidating || now.Before(r.due) || !now.Before(r.deadline) || r.owner.ctx.Err() != nil {
			continue
		}
		r.granted = true
		m.retry.lastGrant, m.retry.cursor = now, (i+1)%m.retry.count
		m.retryEventLocked(r, "startup_grant", "", now)
		return
	}
}
