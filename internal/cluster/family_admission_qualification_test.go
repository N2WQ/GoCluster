//go:build qualification

// File role: Uses the existing synchronous qualification boundary to pause a
// real broadcast offer while the other output consumer enters. No new runtime
// hook or network writer delay stands in for admission ownership.
package cluster

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/internal/correctionflow"
	"dxcluster/internal/qualificationstage"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"
)

func TestTelnetFamilyConcurrentAdmissionQualification(t *testing.T) {
	for _, firstStrong := range []bool{true, false} {
		for _, firstRelease := range []bool{false, true} {
			for _, refused := range []bool{false, true} {
				t.Run(fmt.Sprintf("strong_first_%t/release_first_%t/refused_%t", firstStrong, firstRelease, refused), func(t *testing.T) {
					familyQualifiedConcurrentAdmission(t, firstStrong, firstRelease, refused)
				})
			}
		}
	}
}

func TestTelnetFamilyQueueRefusalNoSELFFallbackQualification(t *testing.T) {
	for _, release := range []bool{false, true} {
		t.Run(fmt.Sprintf("stabilizer_release_%t", release), func(t *testing.T) {
			familyQualifiedRefusedSELF(t, release)
		})
	}
}

func familyQualifiedRefusedSELF(t *testing.T, release bool) {
	t.Helper()
	t.Chdir(t.TempDir())
	at := time.Now().UTC().Add(-time.Second)
	r, _ := familyQualifiedResolver(t, at, "N2WQ/KP4", "N2WQ")
	cfg := config.CallCorrectionConfig{Enabled: true, RecencySeconds: 120, FrequencyToleranceHz: 400,
		DistanceModelCW: "morse", DistanceModelRTTY: "baudot",
		FamilyPolicy: config.CallCorrectionFamilyPolicyConfig{
			TelnetSuppression: config.CallCorrectionTelnetFamilySuppressionConfig{Enabled: true, WindowSeconds: 60, MaxEntries: 8},
		}}
	port := runtimeQualificationPort(t)
	srv := telnet.NewServer(telnet.ServerOptions{Port: port, LoginPrompt: "login: ", LoginGreeting: "ACCOUNTING_READY\n",
		MaxConnections: 2, BroadcastWorkers: 1, BroadcastQueue: 4, WorkerQueue: 4, ClientBuffer: 8,
		BroadcastBatchIntervalSet: true}, nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	// The connection closes before this join, and every live client retires
	// before restoring the directory containing the fixture's local user files.
	t.Cleanup(func() {
		deadline := time.Now().Add(3 * time.Second)
		for len(srv.ListClientCallsigns()) != 0 {
			if time.Now().After(deadline) {
				t.Error("SELF refusal client did not retire before restoring its fixture directory")
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	conn, reader, err := runtimeQualificationLogin(t.Context(), fmt.Sprintf("127.0.0.1:%d", port), "", "N2WQ")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	familyQualifiedAwaitLine(t, conn, reader, "ACCOUNTING_READY")
	if _, err := io.WriteString(conn, "PASS SELF\r\n"); err != nil {
		t.Fatal(err)
	}
	familyQualifiedAwaitLine(t, conn, reader, "Own-call spots enabled")
	tracker := stats.NewTracker()
	var lastOutput atomic.Int64
	p := newOutputPipeline(dedup.NewDeduplicator(0, false, 8), nil, nil, nil, nil, nil,
		nil, srv, nil, tracker, r, cfg, func() *cty.CTYDatabase { return nil },
		nil, nil, config.HarmonicConfig{}, nil, config.SpotPolicy{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, false, nil, &lastOutput, nil, nil, nil, nil)
	if p.familySuppressor == nil || p.telnetStabilizer != nil {
		t.Fatal("SELF refusal fixture must use the production family owner without a scheduler")
	}
	entered, completed, resume := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var resumeOnce sync.Once
	const gateComment = "SELF refusal consumer gate"
	registration, installed := qualificationstage.Install(func(event qualificationstage.Event) {
		if event.Stage == qualificationstage.BroadcastReceived && event.Comment == gateComment {
			defer close(completed)
			close(entered)
			<-resume
		}
	})
	if !installed {
		t.Fatal("another qualification stage observer owns the process")
	}
	gateOffered := false
	t.Cleanup(func() {
		resumeOnce.Do(func() { close(resume) })
		if gateOffered {
			select {
			case <-completed:
			case <-time.After(5 * time.Second):
				t.Error("SELF refusal consumer did not join; observer remains owned")
				return
			}
		}
		if !qualificationstage.Remove(registration) {
			t.Error("SELF refusal observer lost its registration")
		}
	})
	gate := spot.NewSpot("K1GATE", "N0AAA", 14100, "CW")
	gate.Comment = gateComment
	// No lane is allowed and K1GATE does not match the live client. Once its
	// receive callback blocks, the single consumer cannot drain any later slot.
	srv.BroadcastSpotOwned(gate.SealForAsync(), false, false, false)
	gateOffered = true
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("live broadcast consumer did not reach the controlled gate")
	}
	for i := 0; i < 4; i++ {
		srv.BroadcastSpotOwned(spot.NewSpot("K1FILL", "N0AAA", 14100, "CW").SealForAsync(), false, false, false)
	}
	if drops, _, _ := srv.BroadcastMetricSnapshot(); drops != 0 {
		t.Fatal("live queue prefill unexpectedly refused a filler")
	}
	s := familyQualifiedSpot("N2WQ", at)
	if release {
		s.EnsureNormalized()
		p.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: s})
	} else {
		p.processSpot(s, nil)
	}
	if drops, clientDrops, senders := srv.BroadcastMetricSnapshot(); drops != 1 || clientDrops != 0 || senders != 0 {
		t.Fatalf("live SELF source did not encounter exactly one ordinary queue refusal: %d/%d/%d", drops, clientDrops, senders)
	}
	if p.familySuppressor.totalEntries != 0 || lastOutput.Load() == 0 {
		t.Error("refused ordinary offer must retain attempted-output health and teach no family witness")
	}
	for i := 0; i < 4; i++ {
		if got, ok := familyQualifiedReadQueue(srv); !ok || got != (familyQualifiedPayload{"K1FILL", false, false, false}) {
			t.Fatalf("paused consumer queue slot %d=%+v available=%t, want its original filler", i, got, ok)
		}
	}
	if _, ok := familyQualifiedReadQueue(srv); ok {
		t.Fatal("refused ordinary SELF offer entered the broadcast queue")
	}
	// Admit a positive FIFO barrier while the consumer is still paused. Any
	// erroneous direct SELF retry is already ahead of this barrier in the live
	// client's spot queue, so observing K1BAR proves absence without quiet time.
	p.processSpot(familyQualifiedSpot("K1BAR", at), nil)
	resumeOnce.Do(func() { close(resume) })
	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("live consumer did not leave its controlled gate")
	}
	var calls []string
	for i := 0; i < 4; i++ {
		line := familyQualifiedAwaitLine(t, conn, reader, "DX de ")
		line = line[strings.Index(line, "DX de "):]
		if len(line) < 78 {
			t.Fatalf("short SELF refusal wire record: %q", line)
		}
		call := strings.TrimSpace(line[27:39])
		calls = append(calls, call)
		if call == "K1BAR" {
			break
		}
	}
	if !reflect.DeepEqual(calls, []string{"K1BAR"}) {
		t.Errorf("live SELF sequence after refused ordinary offer=%q want [K1BAR]; refusal must not trigger direct SELF retry", calls)
	}
	if drops, clientDrops, senders := srv.BroadcastMetricSnapshot(); drops != 1 || clientDrops != 0 || senders != 0 {
		t.Errorf("positive SELF barrier introduced unrelated drops: %d/%d/%d", drops, clientDrops, senders)
	}
	wantReleased := uint64(0)
	if release {
		wantReleased = 1
	}
	if tracker.CallCorrections() != 0 || tracker.StabilizerHeld() != 0 || tracker.StabilizerReleasedDelayed() != wantReleased {
		t.Errorf("SELF refusal changed identity/hold/release ownership: %d/%d/%d", tracker.CallCorrections(), tracker.StabilizerHeld(), tracker.StabilizerReleasedDelayed())
	}
}

func familyQualifiedAwaitLine(t *testing.T, conn net.Conn, reader *bufio.Reader, marker string) string {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("positive %s receipt: %v", marker, err)
		}
		if len(line) > 65538 {
			t.Fatal("SELF fixture line exceeded its wire bound")
		}
		if strings.Contains(line, marker) {
			return line
		}
	}
	t.Fatalf("positive %s receipt exceeded the fixture line bound", marker)
	return ""
}

func familyQualifiedConcurrentAdmission(t *testing.T, firstStrong, firstRelease, refused bool) {
	t.Helper()
	cfg := config.CallCorrectionConfig{Enabled: true, RecencySeconds: 120, FrequencyToleranceHz: 400,
		DistanceModelCW: "morse", DistanceModelRTTY: "baudot",
		FamilyPolicy: config.CallCorrectionFamilyPolicyConfig{
			TelnetSuppression: config.CallCorrectionTelnetFamilySuppressionConfig{Enabled: true, WindowSeconds: 60, MaxEntries: 8},
		}}
	at := time.Now().UTC().Add(-time.Second)
	const strong, weak = "N2WQ/KP4", "N2WQ"
	r, key := familyQualifiedResolver(t, at, strong, weak)
	selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, cfg, weak, time.Now().UTC())
	if !selection.SnapshotOK || selection.Snapshot.State != spot.ResolverStateConfident || selection.NeighborhoodSplit ||
		selection.Snapshot.EvidenceLoss || selection.Snapshot.TotalReporters != 4 ||
		correctionflow.ResolverSupportForCall(selection.Snapshot, strong) != 3 || correctionflow.ResolverSupportForCall(selection.Snapshot, weak) != 1 {
		t.Fatalf("controlled concurrent offer lacks healthy independent 3/1 support: %+v", selection)
	}
	srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 4}, nil)
	t.Cleanup(srv.Stop)
	if refused {
		// No server worker is running: all four slots remain owned by these
		// fillers until both competing producers have completed their offers.
		for i := 0; i < 4; i++ {
			srv.BroadcastSpotOwned(spot.NewSpot("K1FILL", "N0AAA", 14100, "CW").SealForAsync(), true, true, true)
		}
		if drops, _, _ := srv.BroadcastMetricSnapshot(); drops != 0 {
			t.Fatal("deterministic queue fill was refused")
		}
	}
	p := newOutputPipeline(dedup.NewDeduplicator(0, false, 8), nil, nil, nil, nil, nil,
		nil, srv, nil, stats.NewTracker(), r, cfg, func() *cty.CTYDatabase { return nil },
		nil, nil, config.HarmonicConfig{}, nil, config.SpotPolicy{}, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, false, nil, nil, nil, nil, nil, nil)
	if p.familySuppressor == nil || p.stabilizerEnabled || p.telnetStabilizer != nil {
		t.Fatal("production fixture must have one family owner and no scheduler")
	}
	first, second := strong, weak
	if !firstStrong {
		first, second = weak, strong
	}
	firstReady := make(chan bool, 1)
	secondEntered := make(chan struct{})
	resume := make(chan struct{})
	var resumeOnce, secondOnce sync.Once
	var workers sync.WaitGroup
	p.gridUpdate = func(call, _ string) {
		if call == second {
			secondOnce.Do(func() { close(secondEntered) })
		}
	}
	registration, installed := qualificationstage.Install(func(event qualificationstage.Event) {
		if event.Stage != qualificationstage.BroadcastReady || event.Comment != "offer "+first {
			return
		}
		held := !p.familySuppressor.mu.TryLock()
		if !held {
			p.familySuppressor.mu.Unlock()
		}
		firstReady <- held
		<-resume
	})
	if !installed {
		t.Fatal("another qualification stage observer owns the process")
	}
	// Remove does not join loaded callbacks. Always release the gate and join
	// both producer lifetimes before retiring their observer or captured state.
	t.Cleanup(func() {
		resumeOnce.Do(func() { close(resume) })
		joined := make(chan struct{})
		go func() { workers.Wait(); close(joined) }()
		select {
		case <-joined:
			if !qualificationstage.Remove(registration) {
				t.Error("controlled qualification observer lost its registration")
			}
		case <-time.After(5 * time.Second):
			t.Error("qualification producers did not join; observer remains owned")
		}
	})
	launch := func(call string, release bool) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			s := familyQualifiedSpot(call, at)
			if release {
				s.EnsureNormalized()
				p.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: s})
			} else {
				p.processSpot(s, nil)
			}
		}()
	}
	launch(first, firstRelease)
	select {
	case held := <-firstReady:
		if !held {
			t.Fatal("family ownership ended before the actual nonblocking queue offer")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first consumer did not reach its actual broadcast offer")
	}
	launch(second, !firstRelease)
	select {
	case <-secondEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("other consumer did not enter final fanout while the first offer was paused")
	}
	wantQueued := 0
	if refused {
		wantQueued = 4
	}
	if got := reflect.ValueOf(srv).Elem().FieldByName("broadcast").Len(); got != wantQueued {
		t.Fatalf("queue ownership changed before completing the paused offer: got=%d want %d", got, wantQueued)
	}
	if p.familySuppressor.totalEntries != 0 {
		t.Fatal("an offer taught family history before successful admission")
	}
	resumeOnce.Do(func() { close(resume) })
	joined := make(chan struct{})
	go func() { workers.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("serialized consumers failed to complete after releasing their offer")
	}
	want := []familyQualifiedPayload{{strong, true, true, true}}
	if !firstStrong {
		want = []familyQualifiedPayload{{weak, true, true, true}, {strong, true, true, true}}
	}
	wantWitnesses, wantDrops := len(want), uint64(0)
	if refused {
		want = []familyQualifiedPayload{
			{"K1FILL", true, true, true}, {"K1FILL", true, true, true},
			{"K1FILL", true, true, true}, {"K1FILL", true, true, true},
		}
		wantWitnesses, wantDrops = 0, 2
	}
	var got []familyQualifiedPayload
	for {
		payload, ok := familyQualifiedReadQueue(srv)
		if !ok {
			break
		}
		got = append(got, payload)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("controlled consumer admission order=%+v want %+v", got, want)
	}
	if drops, clientDrops, senders := srv.BroadcastMetricSnapshot(); drops != wantDrops || clientDrops != 0 || senders != 0 {
		t.Errorf("completed concurrent offers dropped=%d/%d/%d want %d/0/0", drops, clientDrops, senders, wantDrops)
	}
	p.familySuppressor.mu.Lock()
	defer p.familySuppressor.mu.Unlock()
	if p.familySuppressor.totalEntries != wantWitnesses {
		t.Errorf("admitted witness count=%d want %d; a stronger admission cannot erase prior witnesses",
			p.familySuppressor.totalEntries, wantWitnesses)
	}
	for _, calls := range p.familySuppressor.buckets {
		for _, entry := range calls {
			for lane, witness := range entry.lanes {
				if witness.call != entry.key || witness.at.IsZero() || witness.freqKHz != 14030 {
					t.Errorf("admitted literal witness lane=%d call=%q key=%q time=%s frequency=%f", lane, witness.call, entry.key, witness.at, witness.freqKHz)
				}
			}
		}
	}
	if p.tracker.CallCorrections() != 0 || p.tracker.StabilizerReleasedDelayed() != 1 {
		t.Errorf("consumer boundaries changed identity or release count: corrections=%d released=%d",
			p.tracker.CallCorrections(), p.tracker.StabilizerReleasedDelayed())
	}
}

func familyQualifiedSpot(call string, at time.Time) *spot.Spot {
	s := spot.NewSpot(call, call, 14030, "CW")
	s.SourceType, s.Comment, s.Time = spot.SourceManual, "offer "+call, at
	s.DEMetadata.ADIF, s.DEMetadata.CQZone, s.DEMetadata.Grid = 291, 5, "FN31"
	return s
}

func familyQualifiedResolver(t *testing.T, at time.Time, strong, weak string) (*spot.SignalResolver, spot.ResolverSignalKey) {
	t.Helper()
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 8, MaxActiveKeys: 2, MaxCandidatesPerKey: 4, MaxReportersPerCand: 4,
		InactiveTTL: time.Minute, EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond, HysteresisWindows: 1,
		FreqGuardMinSeparationKHz: 0.3, FreqGuardRunnerUpRatio: 0.6, MaxEditDistance: 6,
		DistanceModelCW: "morse", DistanceModelRTTY: "baudot"})
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(14030, "20m", "CW", 400)
	for i, receiver := range []string{"N0AAA", "N0BBB", "N0CCC", "N0DDD"} {
		call := strong
		if i == 3 {
			call = weak
		}
		if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: call, Spotter: receiver, FrequencyKHz: 14030, RecencyWindow: 2 * time.Minute}) {
			t.Fatal("qualification receiver input was not admitted")
		}
	}
	if got := driver.Drain(); got != 4 {
		t.Fatalf("qualification receiver reports=%d want 4", got)
	}
	driver.Sweep(at.Add(2 * time.Nanosecond))
	return r, key
}

type familyQualifiedPayload struct {
	call            string
	fast, med, slow bool
}

func familyQualifiedReadQueue(srv *telnet.Server) (familyQualifiedPayload, bool) {
	field := reflect.ValueOf(srv).Elem().FieldByName("broadcast")
	queue := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
	value, ok := queue.TryRecv()
	if !ok {
		return familyQualifiedPayload{}, false
	}
	payload := value.Elem()
	return familyQualifiedPayload{payload.FieldByName("spot").Elem().FieldByName("DXCallNorm").String(),
		payload.FieldByName("allowFast").Bool(), payload.FieldByName("allowMed").Bool(), payload.FieldByName("allowSlow").Bool()}, true
}
