package peer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"dxcluster/config"
)

func takeProtocolRequest(t *testing.T, p *protocolController) protocolRequest {
	t.Helper()
	select {
	case req := <-p.lifecycle:
		return req
	case <-time.After(time.Second):
		t.Fatal("handshake did not reach controller mailbox")
	}
	return protocolRequest{}
}

func requireProtocolResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("protocol caller did not return")
	}
	return nil
}

func stageDeadlineRecords(t *testing.T, m *Manager, s *session) {
	t.Helper()
	stageDeadlineRecordsForOrigin(t, m, s, "N2AAA", "K1OLD", "K2NEW")
}

func stageDeadlineRecordsForOrigin(t *testing.T, m *Manager, s *session, origin, oldUser, newUser string) {
	t.Helper()
	second := utcSecond(time.Now())
	for _, suffix := range []string{"^C^5" + origin + "^1" + oldUser + "^H1^", ".01^A^^1" + newUser + "^H1^", ".02^D^^1" + oldUser + "^H1^"} {
		frame, err := ParseFrame(fmt.Sprintf("PC92^%s^%d%s", origin, second, suffix))
		if err != nil {
			t.Fatal(err)
		}
		record, err := DecodePC92(frame)
		if err != nil {
			t.Fatal(err)
		}
		if err = m.stagePC92Record(s, frame, record); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPC92ReplayInterleavesBatchesWithoutReordering(t *testing.T) {
	p, first, _ := initialRetryOwner(t)
	m := p.manager
	local, remote := net.Pipe()
	ep := PeerEndpoint{host: "pipe", remoteCall: "N2REM", family: config.PeeringPeerFamilyDXSpider}
	second := newSession(local, dirOutbound, m, ep, m.sessionSettings(ep))
	second.ctx, second.cancel = context.WithCancel(context.Background())
	second.pc9x = true
	if err := m.trackCandidate(second); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.close(); _ = remote.Close(); m.releaseCandidate(second) })
	t.Cleanup(p.retireReplays)
	stageDeadlineRecordsForOrigin(t, m, first, "N2AAA", "K1OLD", "K2NEW")
	stageDeadlineRecordsForOrigin(t, m, second, "N3BBB", "K3OLD", "K4NEW")
	replies := [2]chan error{make(chan error, 1), make(chan error, 1)}
	for i, owner := range []*session{first, second} {
		if err := p.request(protocolRequest{kind: "establish", source: owner, done: replies[i]}); !errors.Is(err, errReplayPending) {
			t.Fatal(err)
		}
	}
	bytes := m.stagedBytes
	for turn := range 6 {
		if !p.serviceReplay() {
			t.Fatalf("replay stopped at turn %d", turn)
		}
		for i, users := range [][2]string{{"K1OLD", "K2NEW"}, {"K3OLD", "K4NEW"}} {
			applied := (turn + 2 - i) / 2
			wantOld, wantNew := 0, 0
			if applied == 1 || applied == 2 {
				wantOld = 1
			}
			if applied >= 2 {
				wantNew = 1
			}
			if p.graph.users.Value(users[0]) != wantOld || p.graph.users.Value(users[1]) != wantNew {
				t.Fatalf("turn %d owner %d violated FIFO or fair batch progress", turn, i)
			}
			if applied < 3 {
				select {
				case err := <-replies[i]:
					t.Fatalf("turn %d prematurely released owner %d: %v", turn, i, err)
				default:
				}
			}
		}
		if turn < 4 && (m.stagedRecords != 6 || m.stagedBytes != bytes) {
			t.Fatal("interleaved replay lost original batch reservations")
		}
		if turn == 4 && m.stagedRecords != 3 {
			t.Fatal("first complete batch did not retire independently")
		}
	}
	for _, reply := range replies {
		if err := requireProtocolResult(t, reply); err != nil {
			t.Fatal(err)
		}
	}
	if m.stagedRecords != 0 || m.stagedBytes != 0 || p.replays.Len() != 0 {
		t.Fatal("complete interleaved replay retained reservations")
	}
}

func TestPC92HandshakeDeadlineIncludesControllerQueue(t *testing.T) {
	for _, kind := range []string{"initial", "establish"} {
		for _, queued := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/queued=%v", kind, queued), func(t *testing.T) {
				p, s, _ := initialRetryOwner(t)
				m := p.manager
				m.ctx, m.cancel = context.WithCancel(context.Background())
				t.Cleanup(m.cancel)
				stageDeadlineRecords(t, m, s)
				if !queued {
					for range cap(p.lifecycle) {
						p.lifecycle <- protocolRequest{kind: "held"}
					}
				}
				s.phaseDeadline = time.Now().Add(40 * time.Millisecond)
				result := make(chan error, 1)
				go func() { result <- m.protocolCall(kind, s) }()
				var req protocolRequest
				if queued {
					req = takeProtocolRequest(t, p)
				}
				if err := requireProtocolResult(t, result); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("deadline result=%v", err)
				}
				if queued {
					if err := p.request(req); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("stale request result=%v", err)
					}
					if req.attempt != nil && req.attempt.state.Load() != 2 {
						t.Fatal("expiry did not win establishment outcome")
					}
				} else if len(p.lifecycle) != cap(p.lifecycle) {
					t.Fatal("expired blocked enqueue entered mailbox")
				}
				if m.sessions.Len() != 0 || p.graph.nodes.Len() != 0 || p.graph.freshness.Len() != 0 || len(s.priorityLineCh) != 0 {
					t.Fatal("expired work acquired authority or published")
				}
				if entries, _, _ := p.pc92.occupancy(); entries != 0 {
					t.Fatal("expired establishment replayed cached authority")
				}
				if m.stagedRecords != 3 || m.stagedBytes == 0 {
					t.Fatal("deadline prematurely released candidate-owned staging")
				}
				m.releaseCandidate(s)
				if m.stagedRecords != 0 || m.stagedBytes != 0 || m.candidates.Len() != 0 {
					t.Fatal("expired candidate cleanup retained staging")
				}
			})
		}
	}
}

func TestPC92EstablishmentCommitSurvivesLateReplayReply(t *testing.T) {
	p, s, _ := initialRetryOwner(t)
	m := p.manager
	m.ctx, m.cancel = context.WithCancel(context.Background())
	t.Cleanup(m.cancel)
	t.Cleanup(p.retireReplays)
	stageDeadlineRecords(t, m, s)
	bytes, records := m.stagedBytes, m.stagedRecords
	s.phaseDeadline = time.Now().Add(80 * time.Millisecond)
	result := make(chan error, 1)
	go func() { result <- m.establishSession(s) }()
	req := takeProtocolRequest(t, p)
	if err := p.request(req); !errors.Is(err, errReplayPending) {
		t.Fatalf("establish=%v", err)
	}
	if req.attempt.state.Load() != 1 || m.sessions.Value(s.id) != s {
		t.Fatal("authority did not commit before deadline")
	}
	select {
	case err := <-result:
		t.Fatalf("reader released before replay: %v", err)
	case <-time.After(time.Until(s.phaseDeadline) + 20*time.Millisecond):
	}
	for i := range 3 {
		if m.stagedBytes != bytes || m.stagedRecords != records {
			t.Fatal("active replay lost original batch reservation")
		}
		if !p.serviceReplay() {
			t.Fatal("staged service failed to progress")
		}
		if i < 2 {
			select {
			case err := <-result:
				t.Fatalf("partial replay released reader: %v", err)
			default:
			}
		}
		if i == 0 && (p.graph.users.Value("K1OLD") != 1 || p.graph.users.Value("K2NEW") != 0) {
			t.Fatal("first replay turn did not apply exactly first C")
		}
		if i == 1 && (p.graph.users.Value("K1OLD") != 1 || p.graph.users.Value("K2NEW") != 1) {
			t.Fatal("second replay turn did not preserve FIFO A")
		}
	}
	if err := requireProtocolResult(t, result); err != nil {
		t.Fatalf("timely commitment became late failure: %v", err)
	}
	if p.graph.users.Value("K1OLD") != 0 || p.graph.users.Value("K2NEW") != 1 || m.stagedBytes != 0 || m.stagedRecords != 0 || p.replays.Len() != 0 {
		t.Fatal("final FIFO state or replay retirement incorrect")
	}
	p.finishReplay(s, context.Canceled)
	if m.stagedBytes != 0 || m.stagedRecords != 0 {
		t.Fatal("second retirement double-released reservations")
	}
}

func TestPC92ReplayCancellationReleasesBatchExactlyOnce(t *testing.T) {
	for cycle := range 16 {
		t.Run(fmt.Sprint(cycle), func(t *testing.T) {
			p, s, _ := initialRetryOwner(t)
			m := p.manager
			stageDeadlineRecords(t, m, s)
			reply := make(chan error, 1)
			if err := p.request(protocolRequest{kind: "establish", source: s, done: reply}); !errors.Is(err, errReplayPending) {
				t.Fatal(err)
			}
			if !p.serviceReplay() || m.stagedRecords != 3 || m.stagedBytes == 0 {
				t.Fatal("partial replay lost reservation")
			}
			s.cancel()
			p.serviceReplay()
			if err := requireProtocolResult(t, reply); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel result=%v", err)
			}
			p.retireReplays()
			m.releaseCandidate(s)
			if m.stagedRecords != 0 || m.stagedBytes != 0 || p.replays.Len() != 0 || len(m.ownerSlots) != 0 || len(m.pendingSlots) != 0 || m.ownedRuns.Len() != 0 {
				t.Fatal("canceled replay retained or double-released ownership")
			}
			if p.graph.users.Value("K2NEW") != 0 {
				t.Fatal("canceled replay applied later staged input")
			}
		})
	}
}

func TestPC92EstablishedReaderWaitsForReplayReady(t *testing.T) {
	cfg := completeProtocolTestConfig(config.PeeringConfig{WriteQueueSize: 128, MaxLineLength: 4096, PC92MaxBytes: 4096}, "N0CALL")
	m, err := NewManager(cfg, "N0CALL", nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	local, remote := net.Pipe()
	ep := PeerEndpoint{host: "pipe", remoteCall: "N1REM", family: config.PeeringPeerFamilyDXSpider, preferPC9x: true}
	settings := m.sessionSettings(ep)
	settings.loginTimeout, settings.initTimeout = time.Second, time.Second
	s := newSession(local, dirOutbound, m, ep, settings)
	done := make(chan error, 1)
	go func() { done <- s.Run(m.ctx) }()
	t.Cleanup(func() {
		m.cancel()
		s.close()
		_ = remote.Close()
		m.protocol.retireReplays()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("Run did not retire after replay test")
		}
	})
	reader := bufio.NewReader(remote)
	if login := readSessionWire(t, reader, remote); login != "N0CALL" {
		t.Fatalf("login=%q", login)
	}
	writeSessionWire(t, remote, "PC18^DXSpider Version: 1.57 Build: 633 pc9x^5457^")
	initial := takeProtocolRequest(t, m.protocol)
	initial.done <- m.protocol.request(initial)
	for _, action := range []string{"A", "K"} {
		if line := readSessionWire(t, reader, remote); !pc92TypeLine(action).match(line) {
			t.Fatalf("initial %s=%q", action, line)
		}
	}
	if line := readSessionWire(t, reader, remote); line != "PC20^" {
		t.Fatalf("init end=%q", line)
	}
	second := utcSecond(time.Now())
	for _, suffix := range []string{"^C^5N2AAA^1K1OLD^H1^", ".01^A^^1K2NEW^H1^", ".02^D^^1K1OLD^H1^"} {
		writeSessionWire(t, remote, fmt.Sprintf("PC92^N2AAA^%d%s", second, suffix))
	}
	writeSessionWire(t, remote, "PC22^")
	establish := takeProtocolRequest(t, m.protocol)
	if err = m.protocol.request(establish); !errors.Is(err, errReplayPending) {
		t.Fatalf("establishment=%v", err)
	}
	liveWrite := make(chan error, 1)
	if err = remote.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	go func() {
		_, err := fmt.Fprintf(remote, "PC92^N2AAA^%d.03^C^5N2AAA^1K3LIVE^H1^\r\n", second)
		liveWrite <- err
	}()
	for turn := range 3 {
		select {
		case err := <-liveWrite:
			t.Fatalf("live read preceded replay turn%d: %v", turn, err)
		case <-time.After(10 * time.Millisecond):
		}
		if !m.protocol.serviceReplay() {
			t.Fatal("staged turn missing")
		}
	}
	if err = requireProtocolResult(t, liveWrite); err != nil {
		t.Fatal(err)
	}
	if p := m.protocol; p.graph.users.Value("K1OLD") != 0 || p.graph.users.Value("K2NEW") != 1 || p.graph.users.Value("K3LIVE") != 0 {
		t.Fatal("live input overtook staged FIFO authority")
	}
	select {
	case input := <-m.protocol.input:
		m.protocol.consumeInput(input)
	case <-time.After(time.Second):
		t.Fatal("ready reader did not admit live input")
	}
	if m.protocol.graph.users.Value("K2NEW") != 0 || m.protocol.graph.users.Value("K3LIVE") != 1 {
		t.Fatal("live C failed after complete replay")
	}
}

func TestPC92StopJoinsParkedReplayAndReleasesOwnership(t *testing.T) {
	cfg := completeProtocolTestConfig(config.PeeringConfig{WriteQueueSize: 128, MaxLineLength: 4096, PC92MaxBytes: 4096}, "N0CALL")
	m, err := NewManager(cfg, "N0CALL", nil, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	m.ctx, m.cancel = context.WithCancel(context.Background())
	local, remote := net.Pipe()
	ep := PeerEndpoint{host: "pipe", remoteCall: "N1REM", family: config.PeeringPeerFamilyDXSpider, preferPC9x: true}
	settings := m.sessionSettings(ep)
	settings.loginTimeout, settings.initTimeout = time.Second, time.Second
	s := newSession(local, dirOutbound, m, ep, settings)
	done := make(chan error, 1)
	go func() { done <- s.Run(m.ctx) }()
	joined := false
	t.Cleanup(func() {
		m.cancel()
		s.close()
		_ = remote.Close()
		if !joined {
			m.protocol.retireReplays()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("failed replay fixture did not retire Run")
			}
		}
	})
	reader := bufio.NewReader(remote)
	if login := readSessionWire(t, reader, remote); login != "N0CALL" {
		t.Fatalf("login=%q", login)
	}
	writeSessionWire(t, remote, "PC18^DXSpider Version: 1.57 Build: 633 pc9x^5457^")
	initial := takeProtocolRequest(t, m.protocol)
	initial.done <- m.protocol.request(initial)
	for range 3 {
		readSessionWire(t, reader, remote)
	}
	second := utcSecond(time.Now())
	for _, suffix := range []string{"^C^5N2AAA^1K1OLD^H1^", ".01^A^^1K2NEW^H1^", ".02^D^^1K1OLD^H1^"} {
		writeSessionWire(t, remote, fmt.Sprintf("PC92^N2AAA^%d%s", second, suffix))
	}
	writeSessionWire(t, remote, "PC22^")
	establish := takeProtocolRequest(t, m.protocol)
	if err = m.protocol.request(establish); !errors.Is(err, errReplayPending) {
		t.Fatal(err)
	}
	if !m.protocol.serviceReplay() || m.stagedRecords != 3 || m.stagedBytes == 0 {
		t.Fatal("partial replay did not retain complete batch reservation")
	}
	// Start the actual actor only after cancellation has won. Its production
	// deferred replay retirement must release Run's ownership fence while Stop
	// joins both owners. No test-side release is involved in the success path.
	m.cancel()
	m.wg.Add(1)
	go func() { defer m.wg.Done(); m.protocol.run(m.ctx) }()
	stopped := make(chan struct{})
	go func() { m.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not join canceled replay and parked Run")
	}
	select {
	case <-done:
		joined = true
	case <-time.After(time.Second):
		t.Fatal("Run did not return after Stop joined owners")
	}
	m.Stop()
	if m.stagedRecords != 0 || m.stagedBytes != 0 || m.candidates.Len() != 0 || m.ownedRuns.Len() != 0 || m.protocol.replays.Len() != 0 || len(m.ownerSlots) != 0 || len(m.pendingSlots) != 0 || m.sessions.Len() != 0 {
		t.Fatal("Stop retained staged reservations or transport ownership")
	}
	if m.protocol.graph.users.Value("K2NEW") != 0 {
		t.Fatal("shutdown replay applied staged input after cancellation")
	}
}
