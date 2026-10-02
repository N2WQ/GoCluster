package peer

import (
	"fmt"
	"testing"
	"time"
)

func TestPC92V14RetryFairness(t *testing.T) {
	for _, n := range []int{1, 2, 8, 63, 64} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			p, _, _, _, base := recoveryV12Owner(t)
			m := p.manager
			m.cfg.MaxPeers = n
			m.sessions = newFixedIndex[string, *session](n)
			for i := range n {
				old := retryV14Candidate(t, m, fmt.Sprintf("N%dPEER", i))
				m.sessions.Set(old.id, old)
				m.retryRefused(old, admissionAuthority, base)
				next := retryV14Ready(t, p, old, base.Add(time.Second), true)
				m.sessions.Set(next.id, next)
			}
			// Replacements retain identity rank. Each successful grant is
			// followed by a real session failure and a fresh ready candidate.
			lastGrant := make(map[string]int)
			for grant := range 3 * n {
				at := base.Add(time.Duration(grant+1) * time.Second)
				p.serviceAdmissionRecovery(at)
				var winner *session
				for i := range m.retry.count {
					r := &m.retry.slots[i]
					if r.granted {
						if winner != nil {
							t.Fatal("one service granted multiple startups")
						}
						winner = r.owner
					}
				}
				if winner == nil {
					t.Fatalf("no eligible grant at position %d", grant)
				}
				if previous, exists := lastGrant[winner.remoteCall]; exists && grant-previous != n {
					t.Fatalf("identity %s overtaken %d times, expected at most %d", winner.remoteCall, grant-previous-1, n-1)
				}
				lastGrant[winner.remoteCall] = grant
				m.retrySessionEnded(winner, at)
				next := retryV14Ready(t, p, winner, at, true)
				m.sessions.Set(next.id, next)
			}
			if len(lastGrant) != n {
				t.Fatal("continuously ready identity starved")
			}
		})
	}
}

func TestPC92V14RetryGlobalPacingAndHoles(t *testing.T) {
	p, _, _, _, base := recoveryV12Owner(t)
	m := p.manager
	for i := range 5 {
		old := retryV14Candidate(t, m, fmt.Sprintf("N%dWAIT", i))
		m.sessions.Set(old.id, old)
		m.retryRefused(old, admissionAuthority, base)
		next := retryV14Ready(t, p, old, base, true)
		m.sessions.Set(next.id, next)
	}
	m.retry.slots[0].owner = nil // absent identities do not reserve a turn
	m.retry.slots[1].deadline = base.Add(time.Second)
	m.retry.slots[2].owner.cancel()
	at := base.Add(100 * time.Second) // idle time must not accumulate grant credit
	p.serviceAdmissionRecovery(at)
	if !m.retry.slots[3].granted || m.retry.slots[4].granted {
		t.Fatal("holes reserved a grant or an idle interval permitted a burst")
	}
	for _, offset := range []time.Duration{0, time.Second - time.Nanosecond} {
		p.serviceAdmissionRecovery(at.Add(offset))
		if m.retry.slots[4].granted {
			t.Fatal("global startup spacing below one second")
		}
	}
	p.serviceAdmissionRecovery(at.Add(time.Second))
	if !m.retry.slots[4].granted {
		t.Fatal("eligible identity missed next legal grant")
	}
}
