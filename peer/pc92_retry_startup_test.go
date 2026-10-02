package peer

import (
	"testing"
	"time"
)

func TestPC92V14RetryGrantToStartupGateBarrier(t *testing.T) {
	for _, inbound := range []bool{false, true} {
		p, old, _, _, base := recoveryV12Owner(t)
		m := p.manager
		m.pc18Banner = "GoCluster Version: test"
		m.retryRefused(old, admissionAuthority, base)
		s := retryV14Ready(t, p, old, base.Add(time.Second), true)
		s.nodeVersion, s.phaseDeadline = "5457", time.Now().Add(time.Second)
		s.priorityLineCh = make(chan string, 128)
		p.serviceAdmissionRecovery(base.Add(time.Second))
		if !m.retrySessionAllowed(s) {
			t.Fatal("fixture did not acquire startup grant")
		}
		m.mu.Lock()
		started, done := make(chan struct{}), make(chan error, 1)
		go func() {
			close(started)
			if inbound {
				done <- s.sendInboundStartup()
			} else {
				done <- s.sendOutboundStartup()
			}
		}()
		<-started
		m.pc9xGated.Store(true)
		m.interruptRetriesLocked(base.Add(2 * time.Second))
		m.mu.Unlock()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("previous grant bypassed concurrent global closure")
			}
		case <-time.After(time.Second):
			t.Fatal("gate/startup lock order deadlocked")
		}
		if len(s.priorityLineCh) != 0 {
			t.Fatal("startup queued after global gate acquired authority")
		}
	}
}
