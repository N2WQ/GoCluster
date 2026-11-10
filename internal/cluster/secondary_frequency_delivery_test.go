//go:build !qualification

package cluster

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/dedup"
	"dxcluster/peer"
	"dxcluster/spot"
	"dxcluster/telnet"
)

func secondaryDeliveryOptions(state string) dedup.FrequencyChangeOptions {
	return dedup.FrequencyChangeOptions{State: state, CWRTTYToleranceHz: 500,
		FTToleranceHz: 3000, OtherSkimmerToleranceHz: 1000, HumanToleranceHz: 2000, MaxChanges: 2}
}

func secondaryDeliveryEngine(t *testing.T, state string, window time.Duration, key dedup.SecondaryKeyMode) *dedup.SecondaryDeduper {
	t.Helper()
	engine, err := dedup.NewSecondaryDeduperWithFrequencyChanges(window, false, key, secondaryDeliveryOptions(state))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Stop)
	return engine
}

func secondaryDeliverySpot(call string, frequency float64, mode string, at time.Time, source spot.SourceType) *spot.Spot {
	s := newDeliveryTestSpot(call, "N0AAA", at)
	s.Frequency, s.Mode, s.ModeNorm = frequency, mode, mode
	s.SourceType, s.Confidence = source, "V"
	spot.ApplySourceHumanFlag(s)
	return s
}

type secondaryDeliveryTrace struct {
	lanes   [3][]string
	self    []string
	peer    []string
	ring    []string
	archive []string
}

// Actual TCP lane/SELF receipts and a positive peer barrier distinguish
// suppression from an asynchronous consumer that has not processed its queue.
func TestSecondaryFrequencyShadowPreservesConsumerOutput(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Minute).Add(-8 * time.Minute)
	var baseline secondaryDeliveryTrace
	for _, state := range []string{"off", "shadow"} {
		t.Run(state, func(t *testing.T) {
			got := secondaryConsumerTrace(t, state, at)
			if state == "off" {
				baseline = got
			} else if !reflect.DeepEqual(got, baseline) {
				t.Fatalf("shadow changed actual consumers:\ngot=%+v\noff=%+v", got, baseline)
			}
		})
	}
}

func secondaryConsumerInputs(at time.Time) []*spot.Spot {
	inputs := []*spot.Spot{
		secondaryDeliverySpot("K1ABC", 14020, "CW", at, spot.SourceRBN),
		secondaryDeliverySpot("K1ABC", 14021, "CW", at.Add(10*time.Second), spot.SourceRBN),
		secondaryDeliverySpot("K1ABC", 14074, "FT8", at.Add(20*time.Second), spot.SourceRBN),
		secondaryDeliverySpot("K1ABC", 14021, "CW", at.Add(30*time.Second), spot.SourceRBN),
		secondaryDeliverySpot("K1ABC", 14021, "CW", at.Add(20*time.Second), spot.SourceRBN),
		secondaryDeliverySpot("K1ABC", 14021, "CW", at.Add(220*time.Second), spot.SourceRBN),
		secondaryDeliverySpot("K1ABC", 14021, "CW", at.Add(400*time.Second), spot.SourceRBN),
		secondaryDeliverySpot("K2HUM", 14020, "CW", at.Add(410*time.Second), spot.SourceManual),
		secondaryDeliverySpot("K2HUM", 14023, "CW", at.Add(420*time.Second), spot.SourceManual),
		secondaryDeliverySpot("K1BAR", 14020, "CW", at.Add(430*time.Second), spot.SourceManual),
	}
	for _, i := range []int{3, 4, 5, 6} {
		inputs[i].HasReport, inputs[i].Report = true, -10
	}
	return inputs
}

func secondaryConsumerTrace(t *testing.T, state string, at time.Time) secondaryDeliveryTrace {
	t.Helper()
	temporalDeliveryDirectory(t)
	ring := buffer.NewRingBuffer(16)
	writer := newDeliveryTestArchiveWriter(t)
	port := runtimeQualificationPort(t)
	srv := telnet.NewServer(telnet.ServerOptions{Port: port, LoginPrompt: "login: ", LoginGreeting: "ACCOUNTING_READY\n",
		MaxConnections: 4, BroadcastWorkers: 1, BroadcastQueue: 32, WorkerQueue: 32, ClientBuffer: 32,
		DefaultDedupePolicy: "FAST", DedupeFastEnabled: true, DedupeMedEnabled: true, DedupeSlowEnabled: true,
		BroadcastBatchIntervalSet: true}, nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	familyAdmissionJoinClients(t, srv)
	clients := []familyAdmissionClient{
		familyAdmissionLogin(t, port, "DL1ABC", "OFF", "FAST"),
		familyAdmissionLogin(t, port, "DL2ABC", "OFF", "MED"),
		familyAdmissionLogin(t, port, "DL3ABC", "OFF", "SLOW"),
	}
	self := familyAdmissionLogin(t, port, "K1ABC", "DEDUPE", "FAST")
	familyAdmissionCommand(t, self, "PASS SELF", "Own-call spots enabled")
	manager, recipient := archiveFanoutPeer(t)
	pipeline := newDeliveryTestPipeline(ring, writer, srv)
	pipeline.peerManager, pipeline.secondaryActive = manager, true
	pipeline.secondaryFast = secondaryDeliveryEngine(t, state, 179*time.Second, dedup.SecondaryKeyGrid2)
	pipeline.secondaryMed = secondaryDeliveryEngine(t, state, 359*time.Second, dedup.SecondaryKeyGrid2)
	pipeline.secondarySlow = secondaryDeliveryEngine(t, state, 479*time.Second, dedup.SecondaryKeyCQZone)
	inputs := secondaryConsumerInputs(at)
	var got secondaryDeliveryTrace
	for i, s := range inputs {
		pipeline.deliverSpot(&outputSpotContext{spot: s, peerOriginal: captureOriginalPeerPublication(s)})
		if s.SecondaryDedupe != ([3]spot.SecondaryDedupeEvidence{}) {
			t.Fatalf("%s leaked candidate diagnostics into output at input %d", state, i)
		}
		// SELF receives every own-call input. Consume each before submitting
		// the next, so direct SELF cannot overtake a previous broadcast here.
		if i < 8 || i == 9 {
			got.self = append(got.self, correctionEvidenceReadDX(t, self.conn, self.reader))
		}
	}
	wantLaneCounts := [3]int{6, 5, 4}
	for i, client := range clients {
		got.lanes[i] = secondaryReadThroughBarrier(t, client, len(inputs))
		if len(got.lanes[i]) != wantLaneCounts[i] {
			t.Fatalf("%s lane receipts=%d want %d: %q", client.lane, len(got.lanes[i]), wantLaneCounts[i], got.lanes[i])
		}
	}
	got.peer = secondaryPeerThroughBarrier(t, recipient)
	for _, s := range ring.GetRecent(16) {
		got.ring = append(got.ring, s.FormatDXCluster())
	}
	for s, ok := tryReadArchiveQueuedSpot(writer); ok; s, ok = tryReadArchiveQueuedSpot(writer) {
		got.archive = append(got.archive, s.FormatDXCluster())
	}
	if len(got.self) != 9 || len(got.ring) != 6 || len(got.archive) != 5 || len(got.peer) != 2 {
		t.Fatalf("consumer counts SELF/ring/archive/peer=%d/%d/%d/%d want9/6/5/2", len(got.self), len(got.ring), len(got.archive), len(got.peer))
	}
	return got
}

func secondaryReadThroughBarrier(t *testing.T, client familyAdmissionClient, limit int) []string {
	t.Helper()
	var lines []string
	for range limit {
		line := correctionEvidenceReadDX(t, client.conn, client.reader)
		lines = append(lines, line)
		if len(line) >= 39 && strings.TrimSpace(line[27:39]) == "K1BAR" {
			return lines
		}
	}
	t.Fatal("positive telnet barrier missing")
	return nil
}

func secondaryPeerThroughBarrier(t *testing.T, recipient *peerAdmissionSocket) []string {
	t.Helper()
	var lines []string
	for _, call := range []string{"K2HUM", "K1BAR"} {
		line := peerAdmissionReadSpot(t, recipient)
		frame, err := peer.ParseFrame(line)
		if err != nil || frame.Type != "PC61" || len(frame.Fields) != 8 || frame.Fields[1] != call || frame.Fields[0] != "14020" {
			t.Fatalf("actual human/barrier peer receipt=%q err=%v want call=%s freq14020", line, err, call)
		}
		lines = append(lines, line)
	}
	return lines
}

func TestSecondaryFrequencyOnFeedsFinalConsumersAndCapturesFallback(t *testing.T) {
	temporalDeliveryDirectory(t)
	ring := buffer.NewRingBuffer(8)
	writer := newDeliveryTestArchiveWriter(t)
	srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 8}, nil)
	pipeline := newDeliveryTestPipeline(ring, writer, srv)
	pipeline.secondaryMed = secondaryDeliveryEngine(t, "on", time.Minute, dedup.SecondaryKeyGrid2)
	pipeline.secondaryActive = true
	manager, recipient := archiveFanoutPeer(t)
	pipeline.peerManager = manager
	at := time.Now().UTC().Truncate(time.Second)
	baseline := dedup.NewSecondaryDeduper(time.Minute, false)
	t.Cleanup(baseline.Stop)
	first := secondaryDeliverySpot("K1CW", 14020, "CW", at, spot.SourceRBN)
	changed := secondaryDeliverySpot("K1CW", 14021, "CW", at.Add(time.Second), spot.SourceRBN)
	if !baseline.ShouldForward(first.Clone()) || baseline.ShouldForward(changed.Clone()) {
		t.Fatal("control must establish that the historical engine suppresses the same-key 1kHz move")
	}
	pipeline.deliverSpot(&outputSpotContext{spot: first})
	// Exercise the distinct stabilizer-release caller against the same cache.
	pipeline.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: changed})
	for _, want := range []*spot.Spot{first, changed} {
		if archived := readArchiveQueuedSpot(t, writer); archived != want {
			t.Fatal("archive did not receive the final sealed candidate snapshot")
		}
		if broadcast := readTelnetBroadcastSpot(t, srv); broadcast != want {
			t.Fatal("broadcast did not receive the same candidate snapshot")
		}
	}
	mode, _ := spot.SecondaryModeIdentity("CW")
	want := spot.SecondaryDedupeEvidence{Enabled: true, HasAnchor: true, AnchorHz: 14020000, ModeID: mode}
	if mode == 0 || changed.SecondaryDedupe != ([3]spot.SecondaryDedupeEvidence{want, want, want}) {
		t.Fatalf("disabled FAST/SLOW must copy the real MED decision evidence: %+v", changed.SecondaryDedupe)
	}
	if rows := ring.GetRecent(8); len(rows) != 2 || rows[0] != changed || rows[0].SnapshotForAsync() != changed {
		t.Fatal("changed CW report missing as the immutable ring snapshot")
	}
	repeat := secondaryDeliverySpot("K1CW", 14021, "CW", at.Add(2*time.Second), spot.SourceRBN)
	pipeline.deliverSpot(&outputSpotContext{spot: repeat})
	if ring.GetCount() != 2 || changed.SecondaryDedupe != ([3]spot.SecondaryDedupeEvidence{want, want, want}) {
		t.Fatal("same-frequency repeat escaped or rewrote earlier captured evidence")
	}
	if _, ok := tryReadArchiveQueuedSpot(writer); ok {
		t.Fatal("same-frequency repeat entered the archive")
	}
	if _, ok := tryReadTelnetBroadcastSpot(srv); ok {
		t.Fatal("same-frequency repeat entered broadcast")
	}
	// Skimmers are not peer-publishable. A human move over the human tolerance
	// proves that additional MED admission also reaches the actual peer socket.
	for i, frequency := range []float64{14020, 14023} {
		s := secondaryDeliverySpot("K2HUM", frequency, "CW", at.Add(time.Duration(i+3)*time.Second), spot.SourceManual)
		if baseline.ShouldForward(s.Clone()) != (i == 0) {
			t.Fatal("historical human control did not suppress the changed frequency")
		}
		pipeline.deliverSpot(&outputSpotContext{spot: s, peerOriginal: captureOriginalPeerPublication(s)})
		frame, err := peer.ParseFrame(peerAdmissionReadSpot(t, recipient))
		wantFrequency := []string{"14020", "14023"}[i]
		if err != nil || len(frame.Fields) != 8 || frame.Fields[0] != wantFrequency || frame.Fields[1] != "K2HUM" {
			t.Fatalf("additional human peer publication=%+v err=%v", frame, err)
		}
		if readArchiveQueuedSpot(t, writer) != s || readTelnetBroadcastSpot(t, srv) != s {
			t.Fatal("human frequency change did not reach archive and broadcast")
		}
	}
	if ring.GetCount() != 4 {
		t.Fatalf("candidate final ring count=%d want4", ring.GetCount())
	}
}

func TestSecondaryFrequencyStartupConsumesPolicySettings(t *testing.T) {
	cfg := &config.Config{Dedup: config.DedupConfig{SecondaryMedWindowSeconds: 2, SecondarySlowWindowSeconds: 3,
		SecondarySlowPreferStrong: true, SecondaryFrequencyChanges: config.SecondaryFrequencyChangesConfig{
			State: "on", CWRTTYToleranceHz: 1500, FTToleranceHz: 4000, OtherSkimmerToleranceHz: 1200,
			HumanToleranceHz: 2500, MaxChanges: 1}}}
	r := newClusterRuntime(BuildInfo{}, cfg, "", config.LoadDiagnostics{})
	if !r.initializeSecondaryDedupers() {
		t.Fatal(r.startupErr)
	}
	t.Cleanup(r.stopSecondaryDedupers)
	if r.secondaryFast != nil || r.secondaryMed == nil || r.secondarySlow == nil {
		t.Fatal("startup did not preserve disabled/enabled policy construction")
	}
	at := time.Now().UTC()
	inputs := []struct {
		frequency float64
		offset    time.Duration
		med, slow bool
	}{
		{14020, 0, true, true}, {14021, 100 * time.Millisecond, false, false},
		{14022, 200 * time.Millisecond, true, true}, {14024, 300 * time.Millisecond, false, false},
		{14024, 2500 * time.Millisecond, true, false}, {14024, 2600 * time.Millisecond, false, true},
	}
	for i, input := range inputs {
		s := secondaryDeliverySpot("K1WIRE", input.frequency, "CW", at.Add(input.offset), spot.SourceRBN)
		s.HasReport, s.Report = true, -10
		if i == len(inputs)-1 {
			s.Report = -5
		}
		if med, slow := r.secondaryMed.ShouldForward(s.Clone()), r.secondarySlow.ShouldForward(s.Clone()); med != input.med || slow != input.slow {
			t.Fatalf("input%d construction/tolerance/budget/window/preference wiring gotMED/SLOW=%t/%t want%t/%t", i, med, slow, input.med, input.slow)
		}
	}
	r.stopSecondaryDedupers()
	r.stopSecondaryDedupers() // Runtime retirement is idempotent, including cleanup.
}
