//go:build qualification

package peer

import (
	"context"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
)

// This first necessary-condition gate compares the controlled-retry service
// against V12's recorded baseline. Passing it is NOT the complete v14
// workload or runtime qualification. Fixtures construct a reachable graph and
// normal registered owners; refusal, release and publication use production
// paths. No TTL is shortened and no gated peer supplies further authority.
func TestPC92V14RetryCostGate(t *testing.T) {
	runPC92V12RecoveryCostGate(t, false, false)
}

func TestPC92V14RetrySustainedCostGate(t *testing.T) {
	runPC92V12RecoveryCostGate(t, true, false)
}

func TestPC92V14RetrySustainedBaseline(t *testing.T) {
	runPC92V12RecoveryCostGate(t, true, true)
}

func runPC92V12RecoveryCostGate(t *testing.T, sustained, baseline bool) {
	if os.Getenv("GOCLUSTER_PC92_V14_COST_GATE") != "1" {
		t.Skip("opt-in necessary-condition v14 service gate")
	}
	cfg := completeProtocolTestConfig(config.PeeringConfig{}, "N0LOCAL")
	var calls []string
	for index := range 64 {
		call := qualificationCall("P0", index)
		calls = append(calls, call)
		cfg.Peers = append(cfg.Peers, config.PeeringPeer{Enabled: true, Family: config.PeeringPeerFamilyDXSpider, Direction: config.PeeringPeerDirectionInbound, PreferPC9x: true, RemoteCallsign: call})
	}
	m, err := NewManager(cfg, "N0LOCAL", nil, 600, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	m.ctx, m.cancel = ctx, cancel
	p := m.protocol
	f := schedulerFullGraph(t, p, calls)
	// Keep synthetic detached/message watermarks consistent with their actual
	// admission age. A zero value may reject present UTC traffic near midnight
	// under the receiver's asymmetric ordering rule, masking service behavior.
	for _, population := range []struct {
		prefix string
		count  int
	}{{"M0", 4096}, {"T0", 8192}} {
		for index := range population.count {
			call := qualificationCall(population.prefix, index)
			watermark := p.graph.freshness.Value(call)
			at := watermark.Accepted.UTC()
			watermark.Value = float64(at.Hour()*3600 + at.Minute()*60 + at.Second())
			p.graph.freshness.Set(call, watermark)
		}
	}
	var workers sync.WaitGroup
	var sessions []*session
	t.Cleanup(func() {
		cancel()
		for _, s := range sessions {
			s.close()
		}
		workers.Wait()
		for _, s := range sessions {
			s.discardQueuedOutput()
			m.releaseCandidate(s)
		}
		p.drainQueuedProjections()
	})
	before := &LocalMembership{Revision: 1, Complete: true, RawCount: 1000}
	for index := range 1000 {
		before.Users = append(before.Users, LocalUser{SessionID: uint64(index + 1), Login: qualificationCall("L0", index), IP: "192.0.2.1"})
	}
	var membership atomic.Pointer[LocalMembership]
	membership.Store(before)
	m.SetMembershipProvider(func() LocalMembership { return *membership.Load() })
	for _, call := range calls {
		s := schedulerPipeSession(ctx, m, call, &workers)
		sessions = append(sessions, s)
		if err := m.trackCandidate(s); err != nil {
			t.Fatal(err)
		}
		if err := p.request(protocolRequest{kind: "establish", source: s, done: make(chan error, 1)}); err != nil {
			t.Fatal(err)
		}
	}
	members := f.members(0)
	members[len(members)-1] = PC92Entry{Call: "W1NEW", Flags: 1}
	wire := qualificationFrame(qualificationCall("N0", 0), qualificationStamp(time.Now()), "C", members, 1)
	witness, err := ParseFrame(wire)
	if err != nil {
		t.Fatal(err)
	}
	expectedBlocked := 63
	if baseline {
		expectedBlocked = 0
	}
	for _, s := range sessions[:63] {
		if baseline {
			s.close()
		} else {
			p.receive(witness, s, time.Now())
			if s.ctx.Err() == nil || p.blocked.Value(s.remoteCall).cause != admissionAuthority {
				t.Fatal("new-user C did not cause an ordinary authoritative refusal")
			}
		}
		if err := p.request(protocolRequest{kind: "closed", source: s}); err != nil {
			t.Fatal(err)
		}
	}
	live := sessions[63]
	if p.blocked.Len() != expectedBlocked || m.sessions.Len() != 1 || p.graph.users.Len() != 65536 || p.graph.users.Value("W1NEW") != 0 {
		t.Fatalf("%d-blocked/one-live population is not reachable as specified", expectedBlocked)
	}
	// Complete the established live peer's baseline before the timed stable
	// metadata change. Recovery processing while the graph is still full must
	// not clear any of the63 actual new-authority failures.
	for range 3 {
		p.tick(time.Now())
	}
	if p.recovering.Len() != 0 || live.ctx.Err() != nil || p.blocked.Len() != expectedBlocked {
		t.Fatal("baseline publication did not leave one healthy established peer")
	}
	if sustained {
		runPC92V12RecoverySustained(t, p, f, live, &membership, before, &workers, expectedBlocked)
		return
	}
	releasedUser := qualificationCall("U0", 16000)
	var parents []string
	for call, node := range p.graph.nodes.All() {
		if _, found := node.Members.Get(memberKey{Call: releasedUser}); found {
			parents = append(parents, call)
		}
	}
	if len(parents) != 2 {
		t.Fatalf("withdrawal needs exactly two actual parents, got%d", len(parents))
	}
	for _, origin := range parents {
		frame, err := ParseFrame(qualificationFrame(origin, qualificationStamp(time.Now()), "D", []PC92Entry{{Call: releasedUser, Flags: 1}}, 1))
		if err != nil {
			t.Fatal(err)
		}
		p.receive(frame, live, time.Now())
	}
	if p.graph.users.Len() != 65535 || p.graph.users.Value(releasedUser) != 0 || p.graph.users.Value("W1NEW") != 0 {
		t.Fatal("ordinary withdrawals did not supply exactly one user's headroom")
	}
	// Availability no longer clears retry history. This fixture proves only
	// that an ordinary withdrawal changes the full graph and scalar retry
	// service preserves the healthy peer's membership admission deadline.
	after := &LocalMembership{Revision: 2, Complete: true, RawCount: 1000, Users: append([]LocalUser(nil), before.Users...)}
	after.Users[0].IP = "203.0.113.9"
	events := make(chan QualificationPublication, 4)
	var overflow atomic.Bool
	m.QualificationSetPublicationObserver(func(event QualificationPublication) {
		if event.Peer == live.remoteCall && event.Action == "A" && strings.Contains(event.Wire, "203.0.113.9") {
			select {
			case events <- event:
			default:
				overflow.Store(true)
			}
		}
	})
	// The actual producer change precedes recovery evaluation, exactly as it
	// can occur while an input transaction returns on the authority owner.
	available := time.Now()
	membership.Store(after)
	p.dirty = true
	evaluationStarted := time.Now()
	p.serviceAdmissionRecovery(evaluationStarted)
	evaluationDuration := time.Since(evaluationStarted)
	p.tick(time.Now())
	var admitted time.Time
	select {
	case event := <-events:
		admitted = event.At
	default:
	}
	duration := admitted.Sub(available)
	t.Logf("necessary-condition gate: configured=%d blocked-at-change=63 live=1 nodes=%d users=%d edges=%d ingress=%d freshness=%d graphBytes=%d witnessMembers=%d witnessBytes=%d localUsers=1000 cause=new_authority newUser=W1NEW evaluations=%s membershipQueueAdmission=%s admitted=%v", len(calls), p.graph.nodes.Len(), p.graph.users.Len(), p.graph.edges, p.graph.ingress.Len(), p.graph.freshness.Len(), p.graph.retainedCharge(), len(members), len(wire), evaluationDuration, duration, !admitted.IsZero())
	if overflow.Load() || admitted.IsZero() || duration < 0 || duration > time.Second {
		t.Fatalf("v14 service design failed necessary membership deadline: evaluation=%s admission=%s present=%v overflow=%v; stop before broader qualification", evaluationDuration, duration, !admitted.IsZero(), overflow.Load())
	}
	t.Log("PASS ONLY for the first necessary condition; actual retry waves, continuous mixed load and the remaining v14 obligations are still required")
}
