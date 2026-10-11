//go:build !qualification

// File role: Proves that terminal temporal eviction returns key capacity to
// actual delivery, with independent TCP, original peer and stored-byte oracles.
package cluster

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"

	"github.com/cockroachdb/pebble"
)

func TestRuntimeTemporalReclaimDelivered(t *testing.T) {
	temporalDeliveryDirectory(t)
	cfg := temporalDeliveryConfig()
	cfg.MinConsensusReports, cfg.MinAdvantage, cfg.MinConfidencePercent = 3, 1, 45
	cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 1, 5
	cfg.TemporalDecoder.MinMarginScore = 80
	cfg.TemporalDecoder.MaxEventsPerKey, cfg.TemporalDecoder.MaxActiveKeys = 1, 1
	cfg.TemporalDecoder.MaxPending, cfg.TemporalDecoder.OverflowAction = 8, "abstain"
	at := time.Now().UTC().Truncate(time.Second)
	controller := newRuntimeTemporalController(cfg)
	temporalReclaimStrandController(t, controller, at.Add(-10*time.Second))
	resolver, _, key := temporalDeliveryPopulation(t, at, 2*time.Minute, "K1ABC")
	published, ok := resolver.Lookup(key)
	if !ok || published.Winner != "K1ABC" || published.WinnerSupport != 3 || published.RunnerSupport != 1 || published.TotalReporters != 4 {
		t.Fatalf("fresh literal B3/A1 control was not published: %+v", published)
	}
	f := newAmbiguityDeliveryFixture(t, resolver, cfg)
	p := f.pipeline
	p.temporal, p.recentBandStore, p.customSCPStore = controller, nil, nil
	incoming := newDeliveryTestSpot("K1ABD", "N0AAA", at)
	incoming.SourceType = spot.SourceManual
	p.processSpot(incoming, nil)
	held := len(controller.pending) == 1
	if !held {
		// Continue through the actual recipients on the defective baseline:
		// capacity refusal emits A/P/25 immediately rather than holding it.
		t.Errorf("fresh key was not held after final eviction: pending=%d heap=%d decoder=%d abstain=%d call=%s glyph=%s",
			len(controller.pending), len(controller.queue), controller.decoder.PendingCount(), p.tracker.TemporalAbstainLowMargin(), incoming.DXCall, incoming.Confidence)
	} else {
		if len(controller.queue) != 1 || controller.decoder.PendingCount() != 1 || p.tracker.TemporalPending() != 1 ||
			f.ring.GetCount() != 0 || p.tracker.CallCorrections() != 0 || p.tracker.TemporalHandoffAttempts() != 0 {
			t.Fatal("actual admission did not retain sole ownership before delivery")
		}
		// The resolver driver remains idle: the newly enqueued N0AAA report
		// must not change the independently seeded three-versus-one population.
		if resolver.MetricsSnapshot().QueueDepth != 1 {
			t.Fatal("incoming evidence was not left queued during the hold")
		}
		due, exists := controller.NextDue()
		if !exists {
			t.Fatal("held request has no actual release deadline")
		}
		peerOriginalDeliveryBarrier(t, f, at)
		p.releaseDueTemporal(due.Add(-time.Nanosecond), false)
		if len(controller.pending) != 1 || f.ring.GetCount() != 1 || p.tracker.CallCorrections() != 0 {
			t.Fatal("request escaped its exclusive pre-deadline hold")
		}
		p.releaseDueTemporal(due, false)
		p.releaseDueTemporal(due, false) // A retry cannot redeliver or reconfirm.
	}
	if len(controller.pending) != 0 || len(controller.queue) != 0 || controller.decoder.PendingCount() != 0 || p.tracker.TemporalPending() != 0 {
		t.Error("final delivery retained temporal ownership")
	}
	temporalDeliveryAssertAccounting(t, p, 1, 1, 1)
	if p.tracker.TemporalCommitted() != 1 || p.tracker.TemporalPathSwitches() != 0 || p.tracker.CallCorrections() != 1 || p.tracker.TemporalAbstainLowMargin() != 0 {
		t.Errorf("fresh admitted mutation accounting: proposed=%d committed=%d switched=%d corrections=%d abstain=%d",
			p.tracker.TemporalProposed(), p.tracker.TemporalCommitted(), p.tracker.TemporalPathSwitches(), p.tracker.CallCorrections(), p.tracker.TemporalAbstainLowMargin())
	}
	temporalReclaimAssertDelivery(t, f, at, held)
}

func temporalReclaimStrandController(t *testing.T, c *runtimeTemporalController, at time.Time) {
	t.Helper()
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	observe := func(observed time.Time, winnerWeight, runnerWeight int) uint64 {
		s := spot.NewSpotNormalized("K1ABC", "N0AAA", 7010, "CW")
		s.Time = observed
		accepted, id, reason := c.Observe(observed, key, "K1ABC", runtimeTemporalPending{
			spot: s, selection: runtimeTemporalTestSelection(key, "K1ABC", winnerWeight, "K1ABD", runnerWeight),
		})
		if !accepted {
			t.Fatalf("capacity setup request refused: %s", reason)
		}
		return id
	}
	older := observe(at, 2500, 2500)
	if releases := c.Drain(at.Add(time.Second), false); len(releases) != 0 {
		t.Fatal("tied older request did not defer at lag")
	}
	if due, ok := c.NextDue(); !ok || !due.Equal(at.Add(5*time.Second)) {
		t.Fatal("older request did not requeue to max wait")
	}
	newer := observe(at.Add(time.Second), 4500, 500)
	releases := c.Drain(at.Add(2*time.Second), false)
	if len(releases) != 1 || releases[0].pending.id != newer || releases[0].decision.Action != correctionflow.TemporalDecisionActionApply ||
		len(c.pending) != 1 || len(c.queue) != 1 || c.decoder.PendingCount() != 1 {
		t.Fatalf("newer request did not complete while preserving the evicted older request: %+v", releases)
	}
	releases = c.Drain(at.Add(5*time.Second), false)
	if len(releases) != 1 || releases[0].pending.id != older || releases[0].decision.Action != correctionflow.TemporalDecisionActionAbstain ||
		releases[0].decision.Reason != "event_evicted" || len(c.pending) != 0 || len(c.queue) != 0 || c.decoder.PendingCount() != 0 {
		t.Fatalf("last evicted request did not retire all request/heap ownership: %+v", releases)
	}
	// These setup releases are capacity witnesses, not delivered mutations.
	if history := c.decoder.ConfirmedHistoryMetrics(); history.Keys != 0 || history.Evictions != 0 {
		t.Fatalf("capacity setup manufactured confirmation history: %+v", history)
	}
}

func temporalReclaimAssertDelivery(t *testing.T, f ambiguityDeliveryFixture, at time.Time, barrier bool) {
	t.Helper()
	for _, dst := range []struct {
		name, comment string
		read          func() string
	}{
		{"normal TCP", "CW", func() string { return correctionEvidenceReadDX(t, f.normalConn, f.normalReader) }},
		{"DIAG TCP", "CW 75%", func() string { return correctionEvidenceReadDX(t, f.diagConn, f.diagReader) }},
	} {
		if got, want := dst.read(), correctionEvidenceWire("K1ABC", "C", dst.comment, at); got != want {
			t.Errorf("%s delivered %q want %q", dst.name, got, want)
		}
	}
	correctionEvidenceAssertOriginalPeer(t, f.peer, "K1ABD", at)
	want := correctionEvidenceDeliveryCase{wantCall: "K1ABC", wantGlyph: "C", wantPercent: 75}
	rows := f.ring.GetRecent(4)
	wantRows := 1
	if barrier {
		wantRows++
	}
	if len(rows) != wantRows {
		t.Errorf("ring deliveries=%d want %d", len(rows), wantRows)
	}
	targets := 0
	for _, row := range rows {
		if row.DXCall == "K1XYZ" {
			continue
		}
		targets++
		correctionEvidenceAssertSpot(t, "sealed ring", row, want, at, true)
		if row.SnapshotForAsync() != row {
			t.Error("ring record is not sealed")
		}
	}
	if targets != 1 {
		t.Errorf("target ring deliveries=%d want 1", targets)
	}
	f.writer.Stop()
	temporalReclaimAssertArchiveBytes(t, f.archiveCfg.DBPath, at, wantRows)
	reopened, err := archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err = reopened.Recent(4)
	if err != nil || len(rows) != wantRows {
		t.Fatalf("reopened deliveries=%d want %d err=%v", len(rows), wantRows, err)
	}
	targets = 0
	for _, row := range rows {
		if row.DXCall == "K1XYZ" {
			continue
		}
		targets++
		correctionEvidenceAssertSpot(t, "reopened archive", row, want, at, false)
	}
	if targets != 1 {
		t.Errorf("target reopened deliveries=%d want 1", targets)
	}
}

// Inspect raw version-six records before archive decoding can normalize calls.
// The independently assembled target bytes use literal inputs and format
// offsets; neither the emitted Spot nor the production encoder is the oracle.
func temporalReclaimAssertArchiveBytes(t *testing.T, path string, at time.Time, wantRows int) {
	t.Helper()
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	}()
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte("s|"), UpperBound: []byte("s}")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := iter.Close(); err != nil {
			t.Error(err)
		}
	}()
	rows, targets := 0, 0
	for valid := iter.First(); valid; valid = iter.Next() {
		rows++
		key, raw := iter.Key(), iter.Value()
		if len(key) != 14 || int64(binary.BigEndian.Uint64(key[2:10])) != at.UnixNano() || len(raw) < 74 || raw[0] != 6 {
			t.Errorf("raw persisted key/header changed: key=%x raw=%x", key, raw)
			continue
		}
		dxLength := int(binary.BigEndian.Uint16(raw[36:38]))
		if 74+dxLength > len(raw) {
			t.Error("raw DX length exceeds record")
			continue
		}
		if string(raw[74:74+dxLength]) == "K1XYZ" {
			continue
		}
		targets++
		if expected := temporalReclaimExpectedArchiveBytes(); !bytes.Equal(raw, expected) {
			t.Errorf("persisted target bytes before decoding:\n got %x\nwant %x", raw, expected)
		}
	}
	if err := iter.Error(); err != nil {
		t.Error(err)
	}
	if rows != wantRows || targets != 1 {
		t.Errorf("raw stored rows=%d targets=%d want %d/1", rows, targets, wantRows)
	}
}

func temporalReclaimExpectedArchiveBytes() []byte {
	fields := []string{"K1ABC", "N0AAA", "N0AAA", "CW", "", "MANUAL", "", "C", "20m",
		"", "FN31", "", "", "", "UNKNOWN", "", "", "", ""}
	raw := make([]byte, 74)
	raw[0], raw[1], raw[2] = 6, 2, 5
	binary.BigEndian.PutUint64(raw[4:12], math.Float64bits(14020))
	binary.BigEndian.PutUint16(raw[18:20], 5)
	binary.BigEndian.PutUint32(raw[24:28], 291)
	binary.BigEndian.PutUint64(raw[28:36], math.Float64bits(14020))
	for i, field := range fields {
		binary.BigEndian.PutUint16(raw[36+i*2:38+i*2], uint16(len(field)))
	}
	for _, field := range fields {
		raw = append(raw, field...)
	}
	return raw
}
