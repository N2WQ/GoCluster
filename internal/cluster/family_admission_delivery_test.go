//go:build !qualification

// File role: Reproduces refused broadcast admission through the existing API,
// then checks actual TCP output and independently persisted local/peer output.
// Queue ownership is deterministic before Start; positive FIFO barriers avoid
// treating socket silence as proof of family hiding.
package cluster

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/dedup"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"
)

func TestTelnetFamilyQueueRefusalDelivered(t *testing.T) {
	for _, release := range []bool{false, true} {
		t.Run(fmt.Sprintf("stabilizer_release_%t", release), func(t *testing.T) {
			testTelnetFamilyQueueRefusalDelivered(t, release)
		})
	}
}

func testTelnetFamilyQueueRefusalDelivered(t *testing.T, release bool) {
	t.Helper()
	source, err := filepath.Abs(filepath.Join("..", "..", "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	temporalDeliveryDirectory(t)
	cfg := familyDeliveryLoadedConfig(t, source, false, 15, 8, stabilizerTimeoutRelease)
	if !cfg.CallCorrection.Enabled || !cfg.CallCorrection.FamilyPolicy.TelnetSuppression.Enabled {
		t.Fatal("loaded fixture omitted the enabled correction/family policy")
	}
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	strong := familyDeliveryInput{"N2WQ/KP4", "N2WQ/KP4", "refused family seed", "V", at}
	weak := familyDeliveryInput{"N2WQ", "N2WQ", "visible family input", "V", at.Add(time.Second)}
	barrier := familyDeliveryInput{"K1BAR", "K1BAR", "admission barrier", "V", at.Add(2 * time.Second)}
	r := familyAdmissionResolver(t, cfg.CallCorrection.FrequencyToleranceHz, at, strong.call, weak.call)
	key := spot.NewResolverSignalKey(14030, "20m", "CW", cfg.CallCorrection.FrequencyToleranceHz)
	selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, cfg.CallCorrection, weak.call, time.Now().UTC())
	if !selection.SnapshotOK || selection.NeighborhoodSplit || selection.Snapshot.EvidenceLoss ||
		selection.Snapshot.State == spot.ResolverStateSplit || selection.Snapshot.State == spot.ResolverStateUncertain ||
		selection.Snapshot.TotalReporters != 4 || selection.Snapshot.TotalWeightedSupportMilli != 4000 ||
		correctionflow.ResolverSupportForCall(selection.Snapshot, strong.call) != 3 ||
		correctionflow.ResolverSupportForCall(selection.Snapshot, weak.call) != 1 {
		t.Fatalf("fresh family fixture lacks healthy independent 3/1 receiver evidence: %+v", selection)
	}
	for _, check := range []struct {
		call string
		want int
	}{{strong.call, 75}, {weak.call, 25}} {
		if got, ok := correctionflow.ResolverCallConfidencePercent(selection.Snapshot, check.call); !ok || got != check.want {
			t.Fatalf("fresh family support percentage for %s=%d available=%t want %d", check.call, got, ok, check.want)
		}
	}
	f := familyDeliveryFixture{ambiguityDeliveryFixture: ambiguityDeliveryFixture{
		ring: buffer.NewRingBuffer(8), archiveCfg: config.ArchiveConfig{
			DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 8, BatchSize: 1, BatchIntervalMS: 1,
		},
	}}
	f.writer, err = archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	f.writer.Start()
	t.Cleanup(f.writer.Stop)
	port := runtimeQualificationPort(t)
	srv := telnet.NewServer(telnet.ServerOptions{Port: port, LoginPrompt: "login: ", LoginGreeting: "ACCOUNTING_READY\n",
		MaxConnections: 4, BroadcastWorkers: 1, BroadcastQueue: 4, WorkerQueue: 4, ClientBuffer: 8,
		BroadcastBatchIntervalSet: true}, nil)
	t.Cleanup(srv.Stop)
	manager, recipient := correctionEvidencePeer(t)
	f.peer = recipient
	f.recent = spot.NewRecentBandStore(time.Duration(cfg.CallCorrection.RecentBandWindowSeconds) * time.Second)
	var lastOutput atomic.Int64
	f.pipeline = newOutputPipeline(dedup.NewDeduplicator(0, false, 8), nil, nil, nil, nil, nil,
		f.ring, srv, manager, stats.NewTracker(), r, cfg.CallCorrection, func() *cty.CTYDatabase { return nil },
		nil, nil, cfg.Harmonics, nil, cfg.SpotPolicy, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		f.recent, nil, nil, false, f.writer, &lastOutput, nil, nil, nil, nil)
	if f.pipeline.familySuppressor == nil || !f.pipeline.stabilizerEnabled || f.pipeline.telnetStabilizer == nil {
		t.Fatal("loaded production constructor omitted the family or stabilizer owner")
	}
	t.Cleanup(f.pipeline.telnetStabilizer.Stop)

	// Start has not run, so no consumer can free a broadcast slot. The legacy
	// void API is intentional: this fixture must compile on the defective head.
	fillers := make([]*spot.Spot, 4)
	for i := range fillers {
		fillers[i] = spot.NewSpot("K1FILL", "N0AAA", 14100, "CW").SealForAsync()
		srv.BroadcastSpotOwned(fillers[i], true, true, true)
	}
	if queue, clients, senders := srv.BroadcastMetricSnapshot(); queue != 0 || clients != 0 || senders != 0 {
		t.Fatalf("deterministic queue prefill unexpectedly dropped: %d/%d/%d", queue, clients, senders)
	}
	if release {
		s := strong.spot()
		ctx, ok := f.pipeline.prepareSpotContext(s)
		if !ok || !f.pipeline.finalizeSpotForMetrics(&ctx) {
			t.Fatal("scheduler-owned input must pass normalization and reporter preparation")
		}
		f.pipeline.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: s, peerOriginal: captureOriginalPeerPublication(s)})
		if f.pipeline.tracker.StabilizerReleasedDelayed() != 1 {
			t.Error("release consumer must resolve the refused source exactly once")
		}
	} else {
		f.pipeline.processSpot(strong.spot(), nil)
	}
	if queue, clients, senders := srv.BroadcastMetricSnapshot(); queue != 1 || clients != 0 || senders != 0 {
		t.Fatalf("strong family input did not encounter exactly one real broadcast refusal: %d/%d/%d", queue, clients, senders)
	}
	for i, want := range fillers {
		if got, ok := tryReadTelnetBroadcastSpot(srv); !ok || got != want {
			t.Fatalf("queue slot %d changed despite refusal: got=%p available=%t want=%p", i, got, ok, want)
		}
	}
	if _, ok := tryReadTelnetBroadcastSpot(srv); ok {
		t.Fatal("refused family input unexpectedly entered the broadcast queue")
	}
	if f.ring.GetCount() != 1 || f.pipeline.tracker.StabilizerHeld() != 0 || f.pipeline.tracker.CallCorrections() != 0 {
		t.Fatal("authoritative self V must keep local fanout without acquiring a hold or changing identity")
	}
	if lastOutput.Load() == 0 {
		t.Error("refused ordinary broadcast must retain attempted-output health semantics")
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deadline := time.Now().Add(3 * time.Second)
		for len(srv.ListClientCallsigns()) != 0 {
			if time.Now().After(deadline) {
				t.Error("family admission clients did not retire before restoring the fixture directory")
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	f.normalConn, f.normalReader = correctionEvidenceLogin(t, port, "DL1ABC", "OFF")
	f.diagConn, f.diagReader = correctionEvidenceLogin(t, port, "DL2ABC", "CONF")
	f.pipeline.processSpot(weak.spot(), nil)
	f.pipeline.processSpot(barrier.spot(), nil)
	for _, dst := range []struct {
		name   string
		reader *bufio.Reader
	}{{"normal", f.normalReader}, {"DIAG", f.diagReader}} {
		conn := f.normalConn
		if dst.name == "DIAG" {
			conn = f.diagConn
		}
		var got []customSCPConsumerReceipt
		for i := 0; i < 4; i++ {
			line := correctionEvidenceReadDX(t, conn, dst.reader)
			if len(line) < 78 {
				t.Fatalf("short family admission %s line: %q", dst.name, line)
			}
			receipt := customSCPConsumerReceipt{strings.TrimSpace(line[27:39]), line[71:72]}
			got = append(got, receipt)
			if receipt.call == barrier.call {
				break
			}
		}
		want := []customSCPConsumerReceipt{{weak.call, "V"}, {barrier.call, "V"}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s actual TCP after refused family admission=%+v want %+v; refused publication cannot teach hiding", dst.name, got, want)
		}
	}
	if queue, clients, senders := srv.BroadcastMetricSnapshot(); queue != 1 || clients != 0 || senders != 0 {
		t.Errorf("follow-up/barrier must add no unrelated delivery drops: %d/%d/%d", queue, clients, senders)
	}
	f.assertPeerReceipts(t, []familyDeliveryInput{strong, weak, barrier}, barrier.call)
	f.assertStored(t, []familyDeliveryInput{strong, weak, barrier})
}

func TestTelnetFamilyRefusedAttemptAccounting(t *testing.T) {
	srv := telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 1}, nil)
	srv.BroadcastSpotOwned(spot.NewSpot("K1FILL", "N0AAA", 14100, "CW").SealForAsync(), true, true, true)
	tracker := stats.NewTracker()
	var lastOutput atomic.Int64
	p := &outputPipeline{telnet: srv, tracker: tracker, lastOutput: &lastOutput,
		correctionCfg:    config.CallCorrectionConfig{Enabled: true, RecencySeconds: 120, FrequencyToleranceHz: 400},
		familySuppressor: newTelnetFamilySuppressor(time.Minute, 8, spot.CorrectionFamilyPolicy{}, 400)}
	result := &runtimeTemporalResult{tracker: tracker}
	ctx := outputSpotContext{spot: spot.NewSpot("N2WQ", "N2WQ", 14030, "CW"), temporalResult: result}
	plan := outputDeliveryPlan{allowFast: true, allowMed: true, allowSlow: true, telnetDeliverNow: true}
	// This request has only the telnet consumer, so a prior ring/archive/peer
	// handoff cannot conceal removal of refused-broadcast accounting.
	p.emitSpot(&ctx, plan)
	p.emitSpot(&ctx, plan)
	if queue, _, _ := srv.BroadcastMetricSnapshot(); queue != 2 || tracker.TemporalHandoffAttempts() != 1 || lastOutput.Load() == 0 {
		t.Errorf("refused retry accounting: queue drops=%d handoff attempts=%d output attempt=%d; want 2/1/nonzero",
			queue, tracker.TemporalHandoffAttempts(), lastOutput.Load())
	}
	if p.familySuppressor.totalEntries != 0 {
		t.Error("refused attempts must create no family witnesses")
	}
}

func TestTelnetFamilyPartialLaneDeliveredSELF(t *testing.T) {
	source, err := filepath.Abs(filepath.Join("..", "..", "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	for _, protected := range []string{"FAST", "MED", "SLOW"} {
		for _, self := range []bool{false, true} {
			t.Run(fmt.Sprintf("protected_%s/self_%t", protected, self), func(t *testing.T) {
				testTelnetFamilyPartialLane(t, source, protected, self)
			})
		}
	}
}

func TestTelnetFamilyRankedDelivered(t *testing.T) {
	source, err := filepath.Abs(filepath.Join("..", "..", "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range []struct {
		name, strong, weak string
		strongPercent      int
		weakPercent        int
	}{
		{"stronger_base", "N2WQ", "N2WQ/KP4", 75, 25},
		{"stronger_shorter", "K1ABC", "K1ABCD", 75, 25},
		{"slash_whole_tie_33", "N2WQ/KP4", "N2WQ", 33, 33},
	} {
		for _, weakFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/weak_first_%t", pair.name, weakFirst), func(t *testing.T) {
				temporalDeliveryDirectory(t)
				cfg := familyDeliveryLoadedConfig(t, source, false, 15, 8, stabilizerTimeoutRelease)
				// The unrelated winner in the 33% vector is deliberately retained
				// by exact selection. A synthesized uncertain population would
				// make a displayed tie unavailable rather than test its policy.
				cfg.CallCorrection.ResolverNeighborhoodEnabled = false
				at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
				strong := familyDeliveryInput{pair.strong, pair.strong, "ranked family strong", "V", at}
				weak := familyDeliveryInput{pair.weak, pair.weak, "ranked family weak", "V", at.Add(time.Second)}
				barrier := familyDeliveryInput{"K1BAR", "K1BAR", "ranked family barrier", "V", at.Add(2 * time.Second)}
				observations := []familyAdmissionObservation{
					{pair.strong, "N0AAA"}, {pair.strong, "N0BBB"}, {pair.strong, "N0CCC"}, {pair.weak, "N0DDD"},
				}
				wantReceivers, wantWinner, winnerPercent := 4, pair.strong, 75
				if pair.strongPercent == 33 {
					// Both family candidates use the same two receivers. Four
					// independent receivers support the unrelated winner: receiver
					// union=6, shares=2/6, 2/6, 4/6. The whole tie is real while
					// the selected population remains confidently usable.
					observations = []familyAdmissionObservation{
						{pair.strong, "N0AAA"}, {pair.strong, "N0BBB"},
						{pair.weak, "N0AAA"}, {pair.weak, "N0BBB"},
						{"DL9ZZZ", "N0CCC"}, {"DL9ZZZ", "N0DDD"}, {"DL9ZZZ", "N0EEE"}, {"DL9ZZZ", "N0FFF"},
					}
					wantReceivers, wantWinner, winnerPercent = 6, "DL9ZZZ", 66
				}
				r := familyAdmissionPopulation(t, cfg.CallCorrection.FrequencyToleranceHz, at, observations)
				key := spot.NewResolverSignalKey(14030, "20m", "CW", cfg.CallCorrection.FrequencyToleranceHz)
				selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, cfg.CallCorrection, weak.call, time.Now().UTC())
				if !selection.SnapshotOK || selection.Snapshot.State != spot.ResolverStateConfident || selection.NeighborhoodSplit ||
					selection.Snapshot.EvidenceLoss || selection.Snapshot.TotalReporters != wantReceivers ||
					selection.Snapshot.TotalWeightedSupportMilli != wantReceivers*1000 || selection.Snapshot.Winner != wantWinner {
					t.Fatalf("ranked fixture lacks its independent healthy receiver population: %+v", selection)
				}
				for _, check := range []struct {
					call string
					want int
				}{{strong.call, pair.strongPercent}, {weak.call, pair.weakPercent}, {wantWinner, winnerPercent}} {
					if got, ok := correctionflow.ResolverCallConfidencePercent(selection.Snapshot, check.call); !ok || got != check.want {
						t.Fatalf("ranked whole support for %s=%d available=%t want %d", check.call, got, ok, check.want)
					}
				}
				f := familyAdmissionRankedFixture(t, cfg, r)
				inputs, expected := []familyDeliveryInput{strong, weak, barrier}, []familyDeliveryInput{strong, barrier}
				if weakFirst {
					inputs, expected = []familyDeliveryInput{weak, strong, barrier}, []familyDeliveryInput{weak, strong, barrier}
				}
				for _, input := range inputs {
					f.pipeline.processSpot(input.spot(), nil)
				}
				familyAdmissionAssertTCP(t, familyAdmissionClient{"DL1ABC", "normal", f.normalConn, f.normalReader}, expected, barrier.call)
				familyAdmissionAssertTCP(t, familyAdmissionClient{"DL2ABC", "DIAG", f.diagConn, f.diagReader}, expected, barrier.call)
				if queue, clients, senders := f.pipeline.telnet.BroadcastMetricSnapshot(); queue != 0 || clients != 0 || senders != 0 {
					t.Errorf("ranked family delivery added unrelated drops: %d/%d/%d", queue, clients, senders)
				}
				if f.pipeline.tracker.CallCorrections() != 0 || f.pipeline.tracker.StabilizerHeld() != 0 {
					t.Error("authoritative self input changed identity or acquired a hold")
				}
				f.assertPeerReceipts(t, inputs, barrier.call)
				f.assertStored(t, inputs)
			})
		}
	}
}

func familyAdmissionRankedFixture(t *testing.T, cfg *config.Config, resolver *spot.SignalResolver) familyDeliveryFixture {
	t.Helper()
	f := familyDeliveryFixture{ambiguityDeliveryFixture: ambiguityDeliveryFixture{
		ring: buffer.NewRingBuffer(8), archiveCfg: config.ArchiveConfig{
			DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 8, BatchSize: 1, BatchIntervalMS: 1,
		},
	}}
	var err error
	f.writer, err = archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	f.writer.Start()
	t.Cleanup(f.writer.Stop)
	srv := f.startTelnet(t, false)
	manager, recipient := correctionEvidencePeer(t)
	f.peer = recipient
	f.recent = spot.NewRecentBandStore(time.Duration(cfg.CallCorrection.RecentBandWindowSeconds) * time.Second)
	f.pipeline = newOutputPipeline(dedup.NewDeduplicator(0, false, 8), nil, nil, nil, nil, nil,
		f.ring, srv, manager, stats.NewTracker(), resolver, cfg.CallCorrection, func() *cty.CTYDatabase { return nil },
		nil, nil, cfg.Harmonics, nil, cfg.SpotPolicy, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		f.recent, nil, nil, false, f.writer, nil, nil, nil, nil, nil)
	if f.pipeline.familySuppressor == nil || f.pipeline.telnetStabilizer == nil {
		t.Fatal("ranked production constructor omitted family/stabilizer ownership")
	}
	t.Cleanup(f.pipeline.telnetStabilizer.Stop)
	return f
}

func testTelnetFamilyPartialLane(t *testing.T, source, protected string, self bool) {
	t.Helper()
	temporalDeliveryDirectory(t)
	cfg := familyDeliveryLoadedConfig(t, source, false, 15, 8, stabilizerTimeoutRelease)
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	strong := familyDeliveryInput{"N2WQ/KP4", "N2WQ/KP4", "single lane family seed", "V", at}
	weak := familyDeliveryInput{"N2WQ", "N2WQ", "partial lane family input", "V", at.Add(time.Second)}
	barrier := familyDeliveryInput{"K1BAR", "K1BAR", "partial lane barrier", "V", at.Add(2 * time.Second)}
	r := familyAdmissionResolver(t, cfg.CallCorrection.FrequencyToleranceHz, at, strong.call, weak.call)
	f := familyDeliveryFixture{ambiguityDeliveryFixture: ambiguityDeliveryFixture{
		ring: buffer.NewRingBuffer(8), archiveCfg: config.ArchiveConfig{
			DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 8, BatchSize: 1, BatchIntervalMS: 1,
		},
	}}
	var err error
	f.writer, err = archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	f.writer.Start()
	t.Cleanup(f.writer.Stop)
	port := runtimeQualificationPort(t)
	srv := telnet.NewServer(telnet.ServerOptions{Port: port, LoginPrompt: "login: ", LoginGreeting: "ACCOUNTING_READY\n",
		MaxConnections: 6, BroadcastWorkers: 1, BroadcastQueue: 8, WorkerQueue: 8, ClientBuffer: 8,
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
		familyAdmissionLogin(t, port, "DL4ABC", "CONF", protected),
		familyAdmissionLogin(t, port, "N2WQ", "OFF", protected),
	}
	selfCommand, selfAck := "REJECT SELF", "Self spots disabled"
	if self {
		selfCommand, selfAck = "PASS SELF", "Self spots enabled"
	}
	familyAdmissionCommand(t, clients[4], selfCommand, selfAck)
	manager, recipient := correctionEvidencePeer(t)
	f.peer = recipient
	secondaries := []*dedup.SecondaryDeduper{
		dedup.NewSecondaryDeduper(time.Minute, false), dedup.NewSecondaryDeduper(time.Minute, false), dedup.NewSecondaryDeduper(time.Minute, false),
	}
	for i, lane := range []string{"FAST", "MED", "SLOW"} {
		if lane == protected {
			continue
		}
		seed := strong.spot()
		seed.EnsureNormalized()
		spot.ApplySourceHumanFlag(seed)
		if !secondaries[i].ShouldForward(seed) {
			t.Fatal("independent secondary prefill must accept its first observation")
		}
	}
	f.recent = spot.NewRecentBandStore(time.Duration(cfg.CallCorrection.RecentBandWindowSeconds) * time.Second)
	f.pipeline = newOutputPipeline(dedup.NewDeduplicator(0, false, 8), secondaries[0], secondaries[1], secondaries[2], nil, nil,
		f.ring, srv, manager, stats.NewTracker(), r, cfg.CallCorrection, func() *cty.CTYDatabase { return nil },
		nil, nil, cfg.Harmonics, nil, cfg.SpotPolicy, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		f.recent, nil, nil, false, f.writer, nil, nil, nil, nil, nil)
	if f.pipeline.familySuppressor == nil || f.pipeline.telnetStabilizer == nil {
		t.Fatal("production construction omitted enabled family/stabilizer ownership")
	}
	t.Cleanup(f.pipeline.telnetStabilizer.Stop)
	f.pipeline.processSpot(strong.spot(), nil)
	f.pipeline.processSpot(weak.spot(), nil)
	f.pipeline.processSpot(barrier.spot(), nil)
	for i, client := range clients {
		want := []familyDeliveryInput{weak, barrier}
		if client.lane == protected {
			want = []familyDeliveryInput{strong, barrier}
		}
		if i == 4 && self {
			want = []familyDeliveryInput{strong, weak, barrier}
		}
		familyAdmissionAssertTCP(t, client, want, barrier.call)
	}
	if queue, drops, failures := srv.BroadcastMetricSnapshot(); queue != 0 || drops != 0 || failures != 0 {
		t.Errorf("partial-lane delivery added unrelated drops: %d/%d/%d", queue, drops, failures)
	}
	if f.pipeline.tracker.StabilizerHeld() != 0 || f.pipeline.tracker.CallCorrections() != 0 {
		t.Error("manual self input acquired a hold or changed identity")
	}
	for i, lane := range []string{"FAST", "MED", "SLOW"} {
		processed, duplicates, _ := secondaries[i].GetStats()
		wantProcessed, wantDuplicates := uint64(4), uint64(1)
		if lane == protected {
			wantProcessed, wantDuplicates = 3, 0
		}
		if processed != wantProcessed || duplicates != wantDuplicates {
			t.Errorf("%s real secondary admission=%d/%d want %d/%d", lane, processed, duplicates, wantProcessed, wantDuplicates)
		}
	}
	archiveInputs := []familyDeliveryInput{weak, barrier}
	if protected == "MED" {
		archiveInputs = []familyDeliveryInput{strong, weak, barrier}
	}
	f.assertPeerReceipts(t, archiveInputs, barrier.call)
	f.assertRows(t, "partial-lane sealed ring", f.ring.GetRecent(8), []familyDeliveryInput{strong, weak, barrier}, true)
	f.writer.Stop()
	familyDeliveryAssertArchiveBytes(t, f.archiveCfg.DBPath, archiveInputs)
	reopened, err := archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err := reopened.Recent(8)
	if err != nil {
		t.Fatal(err)
	}
	f.assertRows(t, "partial-lane reopened archive", rows, archiveInputs, false)
}

type familyAdmissionClient struct {
	call, lane string
	conn       net.Conn
	reader     *bufio.Reader
}

func familyAdmissionLogin(t *testing.T, port int, call, diag, lane string) familyAdmissionClient {
	t.Helper()
	conn, reader := correctionEvidenceLogin(t, port, call, diag)
	client := familyAdmissionClient{call, lane, conn, reader}
	familyAdmissionCommand(t, client, "SET DEDUPE "+lane, "Dedupe policy set to "+lane)
	return client
}

func familyAdmissionCommand(t *testing.T, client familyAdmissionClient, command, acknowledgement string) {
	t.Helper()
	if _, err := io.WriteString(client.conn, command+"\r\n"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 16; i++ {
		if strings.Contains(customSCPDeliveryReadLine(t, client.reader), acknowledgement) {
			return
		}
	}
	t.Fatalf("%s did not acknowledge %s", client.call, command)
}

func familyAdmissionAssertTCP(t *testing.T, client familyAdmissionClient, inputs []familyDeliveryInput, barrier string) {
	t.Helper()
	var got []customSCPConsumerReceipt
	for i := 0; i < 6; i++ {
		line := correctionEvidenceReadDX(t, client.conn, client.reader)
		if len(line) < 78 {
			t.Fatalf("short %s/%s TCP line: %q", client.call, client.lane, line)
		}
		receipt := customSCPConsumerReceipt{strings.TrimSpace(line[27:39]), line[71:72]}
		got = append(got, receipt)
		if receipt.call == barrier {
			break
		}
	}
	want := make([]customSCPConsumerReceipt, 0, len(inputs))
	for _, input := range inputs {
		want = append(want, customSCPConsumerReceipt{input.call, input.glyph})
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("actual %s/%s TCP sequence=%+v want %+v", client.call, client.lane, got, want)
	}
}

func familyAdmissionJoinClients(t *testing.T, srv *telnet.Server) {
	t.Helper()
	t.Cleanup(func() {
		deadline := time.Now().Add(3 * time.Second)
		for len(srv.ListClientCallsigns()) != 0 {
			if time.Now().After(deadline) {
				t.Error("family admission clients did not retire before restoring the fixture directory")
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
}

func familyAdmissionResolver(t *testing.T, bucketWidthHz float64, at time.Time, strong, weak string) *spot.SignalResolver {
	t.Helper()
	return familyAdmissionPopulation(t, bucketWidthHz, at, []familyAdmissionObservation{
		{strong, "N0AAA"}, {strong, "N0BBB"}, {strong, "N0CCC"}, {weak, "N0DDD"},
	})
}

type familyAdmissionObservation struct{ call, receiver string }

func familyAdmissionPopulation(t *testing.T, bucketWidthHz float64, at time.Time, observations []familyAdmissionObservation) *spot.SignalResolver {
	t.Helper()
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 8, MaxActiveKeys: 2, MaxCandidatesPerKey: 4, MaxReportersPerCand: 4,
		InactiveTTL: 5 * time.Minute, EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
		HysteresisWindows: 1, FreqGuardMinSeparationKHz: 0.3, FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance: 6, DistanceModelCW: "morse", DistanceModelRTTY: "baudot",
	})
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(14030, "20m", "CW", bucketWidthHz)
	for _, observation := range observations {
		if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: observation.call, Spotter: observation.receiver,
			FrequencyKHz: 14030, RecencyWindow: 2 * time.Minute}) {
			t.Fatal("bounded fresh family receiver observation was refused")
		}
	}
	if got := driver.Drain(); got != len(observations) {
		t.Fatalf("fresh family population processed %d receiver reports, want %d", got, len(observations))
	}
	driver.Sweep(at.Add(2 * time.Nanosecond))
	return r
}
