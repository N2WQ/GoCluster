//go:build qualification

package peer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
)

// This is a bounded controller/service diagnostic, not Q1-Q6 qualification.
// Before actor ownership begins it constructs a reachable full graph and aged
// cache cohorts. Actual queues, net.Pipe writers, staged replay, actor input,
// publication, projection capture, and maintenance then run concurrently.
func schedulerFullGraph(t *testing.T, p *protocolController, calls []string) *QualificationTopology {
	t.Helper()
	f, err := NewQualificationTopology(true, calls)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().Add(-10 * time.Second)
	for node := range f.nodes {
		origin := qualificationCall("N0", node)
		r := graphRecord(t, qualificationFrame(origin, qualificationStamp(at), "C", f.members(node), 1))
		plan, err := p.graph.prepare(r, p.manager.localCall, calls[0], nil)
		if err != nil {
			t.Fatal(err)
		}
		p.graph.commit(plan, at)
		p.graph.commitWatermark(origin, float64(at.UTC().Hour()*3600+at.UTC().Minute()*60+at.UTC().Second()), at, false)
		for _, call := range calls {
			p.graph.observe(origin, call, 1, at, false)
		}
	}
	for i := range 4096 {
		p.graph.commitWatermark(qualificationCall("M0", i), 0, at, true)
	}
	for i := range 8192 {
		p.graph.commitWatermark(qualificationCall("T0", i), 0, at, false)
	}
	if p.graph.nodes.Len() != 4096 || p.graph.users.Len() != 65536 || p.graph.edges != 131072 || p.graph.ingress.Len() != 262144 || p.graph.freshness.Len() != 16384 {
		t.Fatal("full topology fixture did not reach every required population")
	}
	return f
}

func schedulerPipeSession(ctx context.Context, m *Manager, call string, workers *sync.WaitGroup) *session {
	local, remote := net.Pipe()
	sctx, cancel := context.WithCancel(ctx)
	s := &session{id: call, diagnosticLabel: call, remoteCall: call, localCall: m.localCall, manager: m,
		peer: PeerEndpoint{remoteCall: call, family: config.PeeringPeerFamilyDXSpider}, pc9x: true,
		remoteVersion: "5457", remoteBuild: "633", remoteBitmap: 5, ctx: sctx, cancel: cancel,
		conn: local, writer: bufio.NewWriter(local), priorityLineCh: make(chan string, defaultPriorityQueue), writeCh: make(chan string, 128)}
	s.initializeOutputBudgetLocked()
	workers.Add(3)
	go func() { defer workers.Done(); _, _ = io.Copy(io.Discard, remote); _ = remote.Close() }()
	go func() { defer workers.Done(); s.writerLoop() }()
	go func() { defer workers.Done(); <-sctx.Done(); _ = local.Close(); _ = remote.Close() }()
	return s
}

func schedulerSeedCaches(t *testing.T, p *protocolController) []*dedupeCache {
	t.Helper()
	caches := []*dedupeCache{p.manager.dedupe, p.pc92, p.pc93, p.manager.bulletinDedupe}
	for class, cache := range caches {
		size := 128
		switch class {
		case 0:
			size = 373
		case 3:
			size = 256
		}
		at := time.Now()
		for i := range cache.limit {
			if cache.admit(qualificationKey(fmt.Sprintf("scheduler%d", class), i, size), at) != dedupeAccepted {
				t.Fatalf("cache %d did not reach full population", class)
			}
		}
	}
	return caches
}

// Read-only observation does not call contains/prune: doing so could make an
// absent background cleanup appear to meet its own deadline.
func schedulerExpiredCohortsRemain(caches []*dedupeCache) bool {
	for _, cache := range caches {
		cache.mu.Lock()
		old := len(cache.expiry) != 0 && cache.expiry[0].value == 0
		cache.mu.Unlock()
		if old {
			return true
		}
	}
	return false
}

func schedulerInputPressure(ctx context.Context, p *protocolController, s *session, f *QualificationTopology, failure chan<- error, count *atomic.Int64) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var small, large, message TimestampGenerator
	index := 0
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			node, action, members, generator := 1, "A", f.members(1)[:1], &small
			if index%50 == 0 {
				node, action, members, generator = 0, "C", f.members(0), &large
			} else if index%100 >= 1 && index%100 <= 8 {
				action, members = "K", nil
			} else if (index-index/50-8)%2 == 0 {
				action = "D"
			}
			stamp, err := generator.NextAt(now)
			if err != nil {
				select {
				case failure <- err:
				default:
				}
				return
			}
			frame, err := ParseFrame(qualificationFrame(qualificationCall("N0", node), stamp, action, members, 1))
			if err != nil || !p.enqueue(frame, s, now) {
				if err == nil {
					err = errors.New("input queue refused frame")
				}
				select {
				case failure <- fmt.Errorf("PC92 pressure admission: %w", err):
				default:
				}
				return
			}
			count.Add(1)
			if index%60 == 0 {
				stamp, err = message.NextAt(now)
				if err != nil {
					select {
					case failure <- err:
					default:
					}
					return
				}
				frame, err = ParseFrame("PC93^M0AAAA^" + stamp + "^*^W0TEST^^scheduler-pressure^H1^")
				if err != nil || !p.enqueue(frame, s, now) {
					if err == nil {
						err = errors.New("input queue refused frame")
					}
					select {
					case failure <- fmt.Errorf("PC93 pressure admission: %w", err):
					default:
					}
					return
				}
			}
			index++
		}
	}
}

type schedulerPeerEvidence struct {
	baseline, replacement         string
	paired, withdrawn, joined, ip bool
}

func TestPC92SchedulerCombinedMembershipAndMaintenance(t *testing.T) {
	if testing.Short() {
		t.Skip("full occupancy service diagnostic")
	}
	cfg := completeProtocolTestConfig(config.PeeringConfig{}, "N0LOCAL")
	var calls []string
	for i := range 64 {
		call := qualificationCall("P0", i)
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
	// Only graph capture is exercised here; diagnostic SQLite I/O and its native
	// allocation proof remain separate. One queued projection stays reserved.
	m.topology = &topologyStore{}
	m.cfg.Topology.PersistIntervalSeconds = 1
	before := &LocalMembership{Revision: 1, Complete: true, RawCount: 1000}
	for i := range 1000 {
		before.Users = append(before.Users, LocalUser{SessionID: uint64(i + 1), Login: qualificationCall("L0", i), IP: "192.0.2.1"})
	}
	after := &LocalMembership{Revision: 2, Complete: true, RawCount: 1000, Users: append([]LocalUser(nil), before.Users[1:]...)}
	after.Users[0].IP = "203.0.113.9"
	after.Users = append(after.Users, LocalUser{SessionID: 1001, Login: qualificationCall("L0", 1000), IP: "192.0.2.2"})
	var membership atomic.Pointer[LocalMembership]
	membership.Store(before)
	m.SetMembershipProvider(func() LocalMembership { return *membership.Load() })
	var workers sync.WaitGroup
	var sessions []*session
	actorDone := make(chan struct{})
	actorStarted := false
	t.Cleanup(func() {
		cancel()
		if actorStarted {
			<-actorDone
		}
		for _, s := range sessions {
			s.close()
		}
		workers.Wait()
		p.drainQueuedProjections()
		for _, s := range sessions {
			s.discardQueuedOutput()
			m.releaseCandidate(s)
		}
	})
	for i, call := range calls {
		s := schedulerPipeSession(ctx, m, call, &workers)
		sessions = append(sessions, s)
		if err := m.trackCandidate(s); err != nil {
			t.Fatal(err)
		}
		if i == 63 {
			for j := range 256 {
				frame, err := ParseFrame(qualificationFrame(qualificationCall("N0", 3000+j), qualificationStamp(time.Now()), "K", nil, 1))
				if err != nil {
					t.Fatal(err)
				}
				record, err := DecodePC92(frame)
				if err != nil {
					t.Fatal(err)
				}
				if err := m.stagePC92Record(s, frame, record); err != nil {
					t.Fatal(err)
				}
			}
		}
		err := p.request(protocolRequest{kind: "establish", source: s, done: make(chan error, 1)})
		if err != nil && !errors.Is(err, errReplayPending) {
			t.Fatal(err)
		}
	}
	caches := schedulerSeedCaches(t, p)
	events := make(chan QualificationPublication, 4096)
	changed := make(chan time.Time, 1)
	var overflow atomic.Bool
	var changeOnce atomic.Bool
	m.QualificationSetPublicationObserver(func(event QualificationPublication) {
		select {
		case events <- event:
		default:
			overflow.Store(true)
		}
		if event.Action == "C" && changeOnce.CompareAndSwap(false, true) {
			// Prebuilt immutable provider state makes this fault bounded and
			// nonblocking. No controller-owned authority is edited by the hook.
			at := time.Now()
			membership.Store(after)
			m.NotifyMembershipChanged()
			changed <- at
		}
	})
	// Four remaining slots near the UTC boundary exercise catch-up without
	// manufacturing a new timestamp rate. Startup and elapsed timing stay real.
	start := time.Now().Truncate(time.Second).Add(850 * time.Millisecond)
	if !start.After(time.Now().Add(20 * time.Millisecond)) {
		start = start.Add(time.Second)
	}
	if err := qualificationWait(ctx, time.Until(start)); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	for range 96 {
		if _, err := p.timestamps.NextAt(start); err != nil {
			t.Fatal(err)
		}
	}
	for _, cache := range caches {
		cache.epoch = start.Add(-600 * time.Second)
	}
	actorStarted = true
	go func() { defer close(actorDone); p.run(ctx) }()
	failures := make(chan error, 1)
	var inputs atomic.Int64
	workers.Add(1)
	go func() { defer workers.Done(); schedulerInputPressure(ctx, p, sessions[0], f, failures, &inputs) }()
	for worker := range 4 {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for i := worker; ctx.Err() == nil; i += 4 {
				if _, err := m.QualificationSnapshot(ctx); err != nil {
					return
				}
				if err := m.protocolCall("K", sessions[i%64]); err != nil {
					return
				}
				if err := m.protocolCall("C", sessions[i%64]); err != nil {
					return
				}
			}
		}(worker)
	}
	schedulerCheckEvidence(ctx, t, m, calls, events, changed, failures, &overflow, &inputs, caches, start)
}

func schedulerCheckEvidence(ctx context.Context, t *testing.T, m *Manager, calls []string, events <-chan QualificationPublication, changed <-chan time.Time, failures <-chan error, overflow *atomic.Bool, inputs *atomic.Int64, caches []*dedupeCache, start time.Time) {
	t.Helper()
	var change time.Time
	select {
	case change = <-changed:
	case <-time.After(time.Second):
		t.Fatal("recovery C never admitted")
	}
	deadline := change.Add(time.Second)
	states := make(map[string]*schedulerPeerEvidence, 64)
	for _, call := range calls {
		states[call] = &schedulerPeerEvidence{}
	}
	withdrawn := "^1" + qualificationCall("L0", 0) + ":"
	joined := "^1" + qualificationCall("L0", 1000) + ":192.0.2.2^"
	ip := "^1" + qualificationCall("L0", 1) + ":203.0.113.9^"
	cleanup := time.Time{}
	worst := time.Duration(0)
	completed := 0
	for completed < 64 || cleanup.IsZero() {
		if cleanup.IsZero() && !schedulerExpiredCohortsRemain(caches) {
			cleanup = time.Now()
			if cleanup.After(start.Add(time.Second)) {
				t.Fatal("expired cache cohort exceeded one-second cleanup lag")
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("membership deadline: complete recipients=%d/64 cleanup=%v", completed, cleanup)
		}
		select {
		case err := <-failures:
			t.Fatal(err)
		case event := <-events:
			state := states[event.Peer]
			if state == nil {
				t.Fatalf("unrecognized recipient %s", event.Peer)
			}
			wasComplete := state.paired && state.withdrawn && state.joined && state.ip
			payload := strings.SplitN(event.Wire, "^", 5)
			if len(payload) != 5 {
				t.Fatal("malformed observed publication")
			}
			switch event.Action {
			case "C":
				if state.baseline == "" {
					state.baseline = payload[4]
				} else if !state.paired {
					t.Fatalf("%s restarted C before its matching A", event.Peer)
				} else {
					state.replacement = payload[4]
				}
			case "A":
				if !state.paired {
					if state.baseline == "" || payload[4] != state.baseline || !strings.Contains(event.Wire, withdrawn) {
						t.Fatalf("%s lacked immutable C/A before catch-up", event.Peer)
					}
					state.paired = true
				} else {
					// A later complete C/A may supersede the delta. C alone
					// cannot establish IP convergence in the actual receiver.
					if state.replacement == payload[4] && !strings.Contains(event.Wire, withdrawn) {
						state.withdrawn = true
					}
					state.joined = state.joined || strings.Contains(event.Wire, joined)
					state.ip = state.ip || strings.Contains(event.Wire, ip)
				}
			case "D":
				if strings.Contains(event.Wire, "^1"+qualificationCall("L0", 0)+"^") || strings.Contains(event.Wire, withdrawn) {
					if !state.paired {
						t.Fatal("withdrawal overtook recovery A")
					}
					state.withdrawn = true
				}
			}
			if !wasComplete && state.paired && state.withdrawn && state.joined && state.ip {
				if event.At.After(deadline) {
					t.Fatalf("%s late admission: %s", event.Peer, event.At.Sub(change))
				}
				completed++
				worst = max(worst, event.At.Sub(change))
			}
		case <-time.After(time.Millisecond):
		}
	}
	if overflow.Load() {
		t.Fatal("bounded publication observer overflowed")
	}
	if inputs.Load() < 2 {
		t.Fatal("no concurrent PC92 workload was admitted")
	}
	state, err := m.QualificationSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.ClockGated || state.PublicationGated || state.BlockedPeers != 0 || state.PC92Refused != 0 || state.PC93Refused != 0 || state.PC93InputRefused != 0 || state.Established != 64 {
		t.Fatalf("pressure broke healthy peer service: %+v", state)
	}
	t.Logf("SERVICE DIAGNOSTIC ONLY: 64/64 recipients; membership worst=%s; full-cache cleanup=%s; concurrent PC92 inputs=%d; staged_remaining=%d; projection bytes=%d", worst, cleanup.Sub(start), inputs.Load(), state.StagedRecords, state.ProjectionReservedBytes)
}
