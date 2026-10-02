package peer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
	"unsafe"

	"dxcluster/config"
)

func retryV14Candidate(t *testing.T, m *Manager, call string) *session {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return &session{id: call, remoteCall: call, localCall: m.localCall, pc9x: true, preferPC9x: true, manager: m, ctx: ctx, cancel: cancel}
}

// The unit fixture enters through the production refusal and retirement paths;
// ready models completed authentication without executing wire I/O. Integration
// fixtures separately verify that authentication and actual startup own these calls.
func retryV14Ready(t *testing.T, p *protocolController, old *session, at time.Time, drain bool) *session {
	t.Helper()
	m := p.manager
	if drain {
		p.drainFailures()
	}
	next := retryV14Candidate(t, m, old.remoteCall)
	m.mu.Lock()
	m.retryRetireLocked(old, at)
	r := m.retryIdentityLocked(old.remoteCall)
	if r == nil {
		m.mu.Unlock()
		t.Fatal("no retry history")
	}
	m.retryAttemptLocked(r, next)
	r.ready, r.deadline = true, at.Add(5*time.Minute)
	m.mu.Unlock()
	return next
}

func TestPC92V14RetryOutcomeOnce(t *testing.T) {
	p, source, _, _, base := recoveryV12Owner(t)
	m := p.manager
	m.cfg.Backoff = config.PeeringBackoff{BaseMS: 2000, MaxMS: 300000}
	m.retryRefused(source, admissionAuthority, base)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() { m.retryRefused(source, admissionMailbox, base.Add(time.Millisecond)) })
		wg.Go(func() { m.retrySessionEnded(source, base.Add(time.Millisecond)) })
	}
	wg.Wait()
	r := m.retryIdentityLocked(source.remoteCall)
	if m.retry.count != 1 || r.delay != 2*time.Second || !r.due.Equal(base.Add(2*time.Second)) || m.admissionFailures.Len() != 1 {
		t.Fatal("duplicate failure reporters advanced retry history more than once")
	}
	p.drainFailures()
	if r.invalidating || p.blocked.Len() != 1 {
		t.Fatal("controller did not acknowledge one invalidation")
	}
}

func TestPC92V14RetryStaleOutcome(t *testing.T) {
	p, old, _, _, base := recoveryV12Owner(t)
	m := p.manager
	m.retryRefused(old, admissionAuthority, base)
	next := retryV14Ready(t, p, old, base.Add(time.Second), true)
	m.sessions.Set(next.id, next)
	p.serviceAdmissionRecovery(base.Add(time.Second))
	m.retryEstablished(next, base.Add(2*time.Second))
	m.retryRecoveryFlushed(next, base.Add(3*time.Second))
	r := m.retryIdentityLocked(old.remoteCall)
	before := *r
	var wg sync.WaitGroup
	wg.Go(func() { m.retryRefused(old, admissionMailbox, base.Add(4*time.Second)) })
	wg.Go(func() { m.retrySessionEnded(old, base.Add(4*time.Second)) })
	wg.Go(func() { m.retryRecoveryFlushed(old, base.Add(4*time.Second)) })
	wg.Go(func() { m.mu.Lock(); m.retryRetireLocked(old, base.Add(4*time.Second)); m.mu.Unlock() })
	wg.Wait()
	if *r != before || r.owner != next {
		t.Fatal("stale generation changed replacement history or owner")
	}
}

func TestPC92V14RetryInvalidationBarrier(t *testing.T) {
	p, source, _, _, base := recoveryV12Owner(t)
	m := p.manager
	p.graph.observe("N2AAA", source.remoteCall, 1, base, true)
	m.retryRefused(source, admissionAuthority, base)
	next := retryV14Ready(t, p, source, base.Add(time.Second), false)
	p.serviceAdmissionRecovery(base.Add(2 * time.Second))
	if m.retrySessionAllowed(next) {
		t.Fatal("startup granted before old ingress invalidation")
	}
	if p.graph.ingress.Len() != 1 {
		t.Fatal("fixture did not retain pending ingress")
	}
	p.drainFailures()
	if p.graph.ingress.Len() != 0 {
		t.Fatal("invalidation acknowledgement preceded graph removal")
	}
	p.serviceAdmissionRecovery(base.Add(2 * time.Second))
	if !m.retrySessionAllowed(next) {
		t.Fatal("acknowledged invalidation did not permit due startup")
	}
}

func TestPC92V14RetryBackoffContract(t *testing.T) {
	for _, tc := range []struct {
		name        string
		base, limit int
		seconds     []int
	}{
		{"configured", 2000, 300000, []int{2, 4, 8, 16, 32, 64, 128, 256, 300, 300}},
		{"zero direct", 0, 0, []int{1, 1, 1}},
		{"negative direct", -5, -20, []int{1, 1, 1}},
		{"base above limit", 5000, 2000, []int{5, 5, 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, source, _, _, at := recoveryV12Owner(t)
			m := p.manager
			m.cfg.Backoff = config.PeeringBackoff{BaseMS: tc.base, MaxMS: tc.limit}
			m.retryRefused(source, admissionAuthority, at)
			r := m.retryIdentityLocked(source.remoteCall)
			for i, seconds := range tc.seconds {
				want := time.Duration(seconds) * time.Second
				if r.delay != want || !r.due.Equal(at.Add(want)) {
					t.Fatalf("failure %d delay=%v due=%v want %v after failure", i, r.delay, r.due, want)
				}
				if i+1 == len(tc.seconds) {
					break
				}
				p.drainFailures()
				m.mu.Lock()
				m.retryRetireLocked(source, at)
				m.mu.Unlock()
				at = at.Add(want)
				generation, ok := m.reserveRetryDial(r.call, at)
				if !ok || generation == 0 {
					t.Fatal("due dial not reserved")
				}
				if m.finishRetryDial(r.call, generation, nil, at) {
					t.Fatal("failed dial accepted")
				}
			}
			if !m.retry.lastGrant.IsZero() {
				t.Fatal("failed dial consumed startup grant")
			}
		})
	}
}

func TestPC92V14RetryGlobalGatePrecedence(t *testing.T) {
	for _, scenario := range []string{"gate only", "failure before gate", "failure after gate"} {
		t.Run(scenario, func(t *testing.T) {
			p, old, _, _, base := recoveryV12Owner(t)
			m := p.manager
			m.cfg.Backoff = config.PeeringBackoff{BaseMS: 2000, MaxMS: 300000}
			m.retryRefused(old, admissionAuthority, base)
			next := retryV14Ready(t, p, old, base.Add(2*time.Second), true)
			m.sessions.Set(next.id, next)
			p.serviceAdmissionRecovery(base.Add(2 * time.Second))
			m.retryEstablished(next, base.Add(3*time.Second))
			m.retryRecoveryFlushed(next, base.Add(3*time.Second))
			if scenario == "failure before gate" {
				m.retryRefused(next, admissionAuthority, base.Add(4*time.Second))
			}
			m.mu.Lock()
			m.pc9xGated.Store(true)
			m.interruptRetriesLocked(base.Add(5 * time.Second))
			m.mu.Unlock()
			if scenario == "failure after gate" {
				m.retryRefused(next, admissionAuthority, base.Add(5*time.Second))
			}
			m.retrySessionEnded(next, base.Add(6*time.Second))
			r := m.retryIdentityLocked(next.remoteCall)
			wantDelay, wantDue := 2*time.Second, base.Add(2*time.Second)
			switch scenario {
			case "failure before gate":
				wantDelay, wantDue = 4*time.Second, base.Add(8*time.Second)
			case "failure after gate":
				wantDelay, wantDue = 4*time.Second, base.Add(9*time.Second)
			}
			if r.delay != wantDelay || !r.due.Equal(wantDue) || !r.healthySince.IsZero() {
				t.Fatal("global gate advanced/restarted history or erased genuine prior failure")
			}
		})
	}
}

func TestPC92V14RetryHealthyReset(t *testing.T) {
	for _, flushFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(flushFirst), func(t *testing.T) {
			p, old, _, _, base := recoveryV12Owner(t)
			m := p.manager
			receiveControllerWire(t, p, old, "PC92^N2AAA^43200^C^5N2AAA^1K1USER^H1^", base)
			m.retryRefused(old, admissionAuthority, base)
			next := retryV14Ready(t, p, old, base.Add(time.Second), true)
			p.serviceAdmissionRecovery(base.Add(time.Second))
			if flushFirst {
				m.retryRecoveryFlushed(next, base.Add(2*time.Second))
			} else {
				m.retryEstablished(next, base.Add(2*time.Second))
			}
			p.serviceAdmissionRecovery(base.Add(100 * time.Second))
			if !m.blockedPeers.Value(old.remoteCall) {
				t.Fatal("one half of successful recovery reset history")
			}
			if flushFirst {
				m.retryEstablished(next, base.Add(101*time.Second))
			} else {
				m.retryRecoveryFlushed(next, base.Add(101*time.Second))
			}
			p.serviceAdmissionRecovery(base.Add(161*time.Second - time.Nanosecond))
			if !m.blockedPeers.Value(old.remoteCall) {
				t.Fatal("healthy reset before full 60 seconds")
			}
			p.serviceAdmissionRecovery(base.Add(161 * time.Second))
			if m.blockedPeers.Value(old.remoteCall) || m.retryIdentityLocked(old.remoteCall).active {
				t.Fatal("quiet current PC9x peer did not reset at 60 seconds")
			}
			if p.graph.nodes.Value("N2AAA").Complete {
				t.Fatal("local recovery reset invented remote membership completeness")
			}
		})
	}
}

func TestPC92V14GlobalGatePreservesLegacyAttempt(t *testing.T) {
	p, old, _, _, base := recoveryV12Owner(t)
	m := p.manager
	m.retryRefused(old, admissionAuthority, base)
	next := retryV14Ready(t, p, old, base.Add(time.Second), true)
	p.serviceAdmissionRecovery(base.Add(time.Second))
	next.pc9x = false // negotiation completed before registry publication
	m.sessions.Set(next.id, next)
	m.mu.Lock()
	m.pc9xGated.Store(true)
	m.interruptRetriesLocked(base.Add(2 * time.Second))
	m.mu.Unlock()
	if next.ctx.Err() != nil || m.retryIdentityLocked(next.remoteCall).interrupted {
		t.Fatal("PC9x-only gate interrupted established legacy attempt")
	}
	if counts := m.retryCountsLocked(base.Add(2 * time.Second)); counts[2] != 1 || counts[5] != 0 {
		t.Fatal("unaffected legacy owner reported globally gated")
	}
	m.retrySessionEnded(next, base.Add(3*time.Second))
	if got := m.retryIdentityLocked(next.remoteCall); !got.failed || got.delay != time.Second {
		t.Fatal("later real legacy failure was suppressed by unrelated global gate")
	}
}

func TestPC92V14RetryRetiredGateReportsEligibility(t *testing.T) {
	p, old, _, _, base := recoveryV12Owner(t)
	m := p.manager
	m.retryRefused(old, admissionAuthority, base)
	p.drainFailures()
	m.mu.Lock()
	m.pc9xGated.Store(true)
	m.interruptRetriesLocked(base)
	m.retryRetireLocked(old, base)
	m.pc9xGated.Store(false)
	counts := m.retryCountsLocked(base.Add(time.Second))
	m.mu.Unlock()
	if counts[4] != 1 || counts[5] != 0 {
		t.Fatal("retired global interruption hid eligible retry history")
	}
}

func TestPC92V14RetryHandshakeDeadline(t *testing.T) {
	p, old, _, _, base := recoveryV12Owner(t)
	m := p.manager
	m.retryRefused(old, admissionAuthority, base)
	next := retryV14Ready(t, p, old, base, true)
	deadline := time.Now().Add(40 * time.Millisecond)
	started := time.Now()
	err := m.waitRetryStartup(next, deadline)
	if !errors.Is(err, context.DeadlineExceeded) || time.Now().Before(deadline) || time.Since(started) > time.Second {
		t.Fatalf("original absolute deadline not honored: %v elapsed=%v", err, time.Since(started))
	}
	p.serviceAdmissionRecovery(deadline.Add(time.Second))
	if m.retrySessionAllowed(next) {
		t.Fatal("expired ready candidate acquired a startup grant")
	}
}

func TestPC92V14RetryFixedAllocationEnvelope(t *testing.T) {
	// Independent size-class oracle includes the pointer-bearing malloc header.
	// No credit is taken for removing refused wires or v12 headroom trackers.
	managerSize := int(unsafe.Sizeof(Manager{}))
	coordinatorSize := int(unsafe.Sizeof(retryCoordinator{}))
	managerGrowth := dedupeOracleAllocation(managerSize+8) - dedupeOracleAllocation(managerSize-coordinatorSize-int(unsafe.Sizeof(int(0)))+8)
	sessionSize := int(unsafe.Sizeof(session{}))
	receiptSize := 3 * int(unsafe.Sizeof(uint64(0)))
	sessionGrowth := dedupeOracleAllocation(sessionSize+8) - dedupeOracleAllocation(sessionSize-receiptSize+8)
	const retainedSessionIdentities = 641
	// Go 1.26.4 amd64 runtime/time.go timeTimer=112 and runtime/chan.go
	// hchan=112. Pointer-bearing time.Time needs a separate24-byte buffer.
	// Include conservative malloc headers for all three backing allocations.
	// One reusable Timer remains owned by each fixed identity for the manager
	// lifetime; Stop alone cannot bound runtime timer-heap zombie generations.
	timerBacking := dedupeOracleAllocation(112+8) + dedupeOracleAllocation(112+8) + dedupeOracleAllocation(int(unsafe.Sizeof(time.Time{}))+8)
	statsSize := int(unsafe.Sizeof(ProtocolStats{}))
	statsGrowth := 2 * (dedupeOracleAllocation(statsSize+8) - dedupeOracleAllocation(statsSize-6*int(unsafe.Sizeof(int(0)))+8))
	added := managerGrowth + retainedSessionIdentities*sessionGrowth + 64*timerBacking + statsGrowth
	if added > 32<<10 {
		t.Fatalf("fixed retry/receipt backing=%d exceeds 32KiB", added)
	}
	t.Logf("manager=%d coordinator=%d manager class delta=%d session=%d receipt class delta=%d x641 timers=%d x64 stats overlap growth=%d added=%d", managerSize, coordinatorSize, managerGrowth, sessionSize, sessionGrowth, timerBacking, statsGrowth, added)
}
