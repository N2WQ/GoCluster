package peer

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPC92V14RetryTimerReusedAcrossAttemptsAndReset(t *testing.T) {
	p, old, _, _, base := recoveryV12Owner(t)
	m := p.manager
	m.retryRefused(old, admissionAuthority, base)
	var timer *time.Timer
	for range 3 {
		next := retryV14Ready(t, p, old, base.Add(time.Second), true)
		err := m.waitRetryStartup(next, time.Now().Add(time.Millisecond))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		r := m.retryIdentityLocked(old.remoteCall)
		if timer == nil {
			timer = r.waitTimer
		}
		if timer == nil || r.waitTimer != timer {
			t.Fatal("retry wait allocated another retained timer generation")
		}
		old = next
	}
	next := retryV14Ready(t, p, old, base.Add(time.Second), true)
	p.serviceAdmissionRecovery(base.Add(time.Second))
	m.retryEstablished(next, base.Add(2*time.Second))
	m.retryRecoveryFlushed(next, base.Add(2*time.Second))
	p.serviceAdmissionRecovery(base.Add(62 * time.Second))
	r := m.retryIdentityLocked(old.remoteCall)
	if r.active || r.waitTimer != timer {
		t.Fatal("healthy reset lost bounded timer ownership")
	}
	m.sessions.Set(next.id, next)
	m.retryRefused(next, admissionAuthority, base.Add(63*time.Second))
	if r.waitTimer != timer {
		t.Fatal("later overload episode allocated a new timer generation")
	}
}
