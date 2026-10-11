//go:build !qualification

// File role: Makes actual resolver cap discards observable in local delivery,
// original peer publication, and reopened history without inventing RF truth.
package cluster

import (
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"

	"github.com/cockroachdb/pebble"
)

func correctionLossConfig() config.CallCorrectionConfig {
	return config.CallCorrectionConfig{
		Enabled: true, RecencySeconds: 120, FrequencyToleranceHz: 500,
		MaxEditDistance: 1, MinConsensusReports: 2, MinAdvantage: 1, MinConfidencePercent: 60,
		DistanceModelCW: "plain", DistanceModelRTTY: "plain", InvalidAction: "broadcast",
	}
}

func correctionLossResolver(t *testing.T, candidateCap, receiverCap int) (*spot.SignalResolver, *spot.SignalResolverDriver, spot.ResolverSignalKey) {
	t.Helper()
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 32, MaxActiveKeys: 4, MaxCandidatesPerKey: candidateCap, MaxReportersPerCand: receiverCap,
		InactiveTTL: 5 * time.Minute, EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
		HysteresisWindows: 1, MaxEditDistance: 1, DistanceModelCW: "plain", DistanceModelRTTY: "plain",
		// A zero frequency-guard width isolates cap loss from narrow split policy.
		FreqGuardMinSeparationKHz: 0,
	})
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	return r, driver, spot.NewResolverSignalKey(14020, "20m", "CW", 500)
}

func correctionLossAdmit(t *testing.T, r *spot.SignalResolver, d *spot.SignalResolverDriver, key spot.ResolverSignalKey, call string, frequency float64, at time.Time, window time.Duration, receivers ...string) {
	t.Helper()
	for _, receiver := range receivers {
		if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: call,
			Spotter: receiver, FrequencyKHz: frequency, RecencyWindow: window}) {
			t.Fatal("bounded fixture refused a receiver before owner processing")
		}
	}
	if got := d.Drain(); got != len(receivers) {
		t.Fatalf("owner processing barrier consumed %d reports, want %d", got, len(receivers))
	}
	// Several batches share a literal observation timestamp. Advance evaluation
	// beyond earlier sweeps so throttling cannot hide the final population.
	d.Sweep(at.Add(time.Duration(r.MetricsSnapshot().Processed+1) * time.Nanosecond))
}

func correctionLossPopulation(t *testing.T, r *spot.SignalResolver, key spot.ResolverSignalKey, total int, counts map[string]int) spot.ResolverSnapshot {
	t.Helper()
	snapshot, ok := r.Lookup(key)
	if !ok || snapshot.TotalReporters != total || snapshot.TotalWeightedSupportMilli != total*1000 || snapshot.ReceiverEvidence() == nil || snapshot.ReceiverEvidence().Len() != total {
		t.Fatalf("literal retained population must precede delivery: available=%t snapshot=%+v want %d unit-weight receivers", ok, snapshot, total)
	}
	for call, count := range counts {
		if got := correctionflow.ResolverSupportForCall(snapshot, call); got != count {
			t.Fatalf("retained support for %s=%d, want %d: %+v", call, got, count, snapshot)
		}
	}
	return snapshot
}

func correctionLossEvictions(t *testing.T, r *spot.SignalResolver, candidates, receivers uint64) {
	t.Helper()
	m := r.MetricsSnapshot()
	if m.EvictedCandidates != candidates || m.EvictedReporters != receivers || m.CapPressureCandidates != candidates || m.CapPressureReporters != receivers ||
		m.DropQueueFull != 0 || m.DropMaxKeys != 0 || m.DropMaxCandidates != 0 || m.DropMaxReporters != 0 || m.DropReliability != 0 || m.QueueDepth != 0 {
		t.Fatalf("fixture must prove only its declared actual cap discard: metrics=%+v want candidate/receiver evictions=%d/%d", m, candidates, receivers)
	}
}

func correctionLossFixture(t *testing.T, r *spot.SignalResolver, cfg config.CallCorrectionConfig) ambiguityDeliveryFixture {
	t.Helper()
	f := newAmbiguityDeliveryFixture(t, r, cfg)
	// The shared delivery fixture installs a history store. Remove it so no
	// independent S floor or bonus can conceal unavailable current consensus.
	f.pipeline.recentBandStore, f.pipeline.customSCPStore = nil, nil
	return f
}

func TestCorrectionLossDeliveredActualCaps(t *testing.T) {
	for _, cause := range []string{"candidate", "receiver"} {
		t.Run(cause, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			at := time.Now().UTC().Truncate(time.Second)
			observed := at.Add(-3 * time.Second)
			candidateCap, receiverCap := 2, 8
			if cause == "receiver" {
				candidateCap, receiverCap = 4, 2
			}
			r, d, key := correctionLossResolver(t, candidateCap, receiverCap)
			correctionLossAdmit(t, r, d, key, "K1ABC", 14020, observed, 2*time.Minute, "N0AAA")
			correctionLossAdmit(t, r, d, key, "K1ABD", 14020, observed, 2*time.Minute, "N0BBB", "N0CCC")
			if cause == "candidate" {
				// A1/B2/C1 originally gives B 50%; discarding A changes it to 66%.
				correctionLossAdmit(t, r, d, key, "K1ABE", 14020, observed, 2*time.Minute, "N0DDD")
				correctionLossEvictions(t, r, 1, 0)
				correctionLossPopulation(t, r, key, 3, map[string]int{"K1ABC": 0, "K1ABD": 2, "K1ABE": 1})
			} else {
				// Discarding B's own support lowers its share from 75% to 66%.
				// Abstention here establishes the selected conservative loss rule.
				correctionLossAdmit(t, r, d, key, "K1ABD", 14020, observed, 2*time.Minute, "N0DDD")
				correctionLossEvictions(t, r, 0, 1)
				correctionLossPopulation(t, r, key, 3, map[string]int{"K1ABC": 1, "K1ABD": 2})
			}
			f := correctionLossFixture(t, r, correctionLossConfig())
			s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
			s.SourceType = spot.SourceManual
			f.pipeline.processSpot(s, nil)
			temporalDeliveryCheckWithoutPercent(t, f, correctionEvidenceDeliveryCase{
				subject: "K1ABC", wantCall: "K1ABC", wantGlyph: "?",
			}, at, true)
		})
	}
}

func TestCorrectionLossDeliveredGenuineExpiryRecovery(t *testing.T) {
	temporalDeliveryDirectory(t)
	at := time.Now().UTC().Truncate(time.Second)
	old, fresh := at.Add(-3*time.Minute), at.Add(-time.Second)
	r, d, key := correctionLossResolver(t, 2, 8)
	for _, row := range []struct {
		call      string
		receivers []string
	}{
		{"K1ABC", []string{"N0AAA"}}, {"K1ABD", []string{"N0BBB", "N0CCC"}}, {"K1ABE", []string{"N0DDD"}},
	} {
		correctionLossAdmit(t, r, d, key, row.call, 14020, old, time.Minute, row.receivers...)
	}
	correctionLossEvictions(t, r, 1, 0)
	correctionLossPopulation(t, r, key, 3, map[string]int{"K1ABC": 0, "K1ABD": 2, "K1ABE": 1})
	// Advance observation/query time past the lost population's unchanged
	// one-minute horizon. Fresh accepted traffic arrives immediately in process
	// time, so this control cannot rely on ten-minute idle retirement.
	correctionLossAdmit(t, r, d, key, "K1ABD", 14020, fresh, time.Minute, "N0BBB", "N0CCC", "N0EEE")
	correctionLossAdmit(t, r, d, key, "K1ABC", 14020, fresh, time.Minute, "N0AAA")
	correctionLossEvictions(t, r, 1, 0) // Recovery must not cause another discard.
	snapshot := correctionLossPopulation(t, r, key, 4, map[string]int{"K1ABC": 1, "K1ABD": 3, "K1ABE": 0})
	if snapshot.Winner != "K1ABD" || snapshot.State != spot.ResolverStateConfident {
		t.Fatalf("genuine fresh B3/A1 must regain authority: %+v", snapshot)
	}
	cfg := correctionLossConfig()
	cfg.RecencySeconds = 60
	f := correctionLossFixture(t, r, cfg)
	s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
	s.SourceType = spot.SourceManual
	f.pipeline.processSpot(s, nil)
	f.check(t, correctionEvidenceDeliveryCase{
		subject: "K1ABC", wantCall: "K1ABD", wantGlyph: "C", wantPercent: 75, wantCorrections: 1,
	}, at)
}

func TestCorrectionLossDeliveredIncomparableNeighbor(t *testing.T) {
	for _, lost := range []bool{false, true} {
		name := "healthy_control"
		if lost {
			name = "loss_before_comparability"
		}
		t.Run(name, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			at := time.Now().UTC().Truncate(time.Second)
			observed := at.Add(-3 * time.Second)
			r, d, key := correctionLossResolver(t, 2, 8)
			correctionLossAdmit(t, r, d, key, "K1ABC", 14020, observed, 2*time.Minute, "N0AAA")
			correctionLossAdmit(t, r, d, key, "K1ABD", 14020, observed, 2*time.Minute, "N0BBB", "N0CCC", "N0DDD")
			neighbor := key
			neighbor.Bucket++
			correctionLossAdmit(t, r, d, neighbor, "DL9ZZZ", 14020.5, observed, 2*time.Minute, "N0EEE", "N0FFF", "N0GGG")
			correctionLossAdmit(t, r, d, neighbor, "W9WWW", 14020.5, observed, 2*time.Minute, "N0HHH")
			correctionLossPopulation(t, r, key, 4, map[string]int{"K1ABC": 1, "K1ABD": 3})
			if spot.CallDistance("K1ABC", "DL9ZZZ", "CW", "plain", "plain") <= 1 {
				t.Fatal("neighbor winner must be ineligible as a replacement for A")
			}
			if lost {
				correctionLossAdmit(t, r, d, neighbor, "K9YYY", 14020.5, observed, 2*time.Minute, "N0III")
				correctionLossEvictions(t, r, 1, 0)
				correctionLossPopulation(t, r, neighbor, 4, map[string]int{"DL9ZZZ": 3, "W9WWW": 0, "K9YYY": 1})
			} else {
				correctionLossEvictions(t, r, 0, 0)
				correctionLossPopulation(t, r, neighbor, 4, map[string]int{"DL9ZZZ": 3, "W9WWW": 1})
			}
			cfg := correctionLossConfig()
			cfg.ResolverNeighborhoodEnabled, cfg.ResolverNeighborhoodBucketRadius = true, 1
			f := correctionLossFixture(t, r, cfg)
			s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
			s.SourceType = spot.SourceManual
			f.pipeline.processSpot(s, nil)
			if lost {
				temporalDeliveryCheckWithoutPercent(t, f, correctionEvidenceDeliveryCase{
					subject: "K1ABC", wantCall: "K1ABC", wantGlyph: "?",
				}, at, true)
			} else {
				f.check(t, correctionEvidenceDeliveryCase{
					subject: "K1ABC", wantCall: "K1ABD", wantGlyph: "C", wantPercent: 75, wantCorrections: 1,
				}, at)
			}
		})
	}
}

func TestCorrectionLossDeliveredAcceptedIdentityAfterScheduler(t *testing.T) {
	temporalDeliveryDirectory(t)
	at := time.Now().UTC().Truncate(time.Second)
	observed := at.Add(-3 * time.Second)
	r, d, key := correctionLossResolver(t, 2, 8)
	correctionLossAdmit(t, r, d, key, "K1ABC", 14020, observed, 2*time.Minute, "N0AAA")
	correctionLossAdmit(t, r, d, key, "K1ABD", 14020, observed, 2*time.Minute, "N0BBB", "N0CCC", "N0DDD")
	correctionLossEvictions(t, r, 0, 0)
	correctionLossPopulation(t, r, key, 4, map[string]int{"K1ABC": 1, "K1ABD": 3})
	cfg := correctionLossConfig()
	cfg.MinConsensusReports, cfg.MinConfidencePercent = 3, 50
	cfg.TemporalDecoder = runtimeTemporalTestConfig(0).TemporalDecoder
	cfg.StabilizerEnabled, cfg.StabilizerMaxChecks, cfg.StabilizerTimeoutAction = true, 1, stabilizerTimeoutRelease
	f := correctionLossFixture(t, r, cfg)
	p := f.pipeline
	p.temporal = newRuntimeTemporalController(cfg)
	result := &runtimeTemporalResult{decoder: p.temporal.decoder, key: key, tracker: p.tracker}
	s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
	s.SourceType = spot.SourceManual
	original := captureOriginalPeerPublication(s)
	selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABC", time.Now().UTC())
	if maybeApplyResolverCorrectionWithSelectionOverride(s, r, spot.ResolverEvidence{Key: key}, true, cfg,
		nil, nil, p.tracker, nil, nil, nil, nil, nil, nil, nil, &selection, time.Now().UTC(), result) {
		t.Fatal("positive ordinary-gate acceptance was suppressed")
	}
	if normalizedDXCall(s) != "K1ABD" || s.Confidence != "C" || !s.ConfidencePercentOK || s.ConfidencePercent != 75 ||
		p.tracker.CallCorrections() != 1 || p.tracker.TemporalCommitted() != 1 || p.tracker.TemporalPathSwitches() != 0 || f.ring.GetCount() != 0 {
		t.Fatal("fixture must accept exactly one A-to-B mutation before scheduling or cap loss")
	}
	// C does not naturally enter the stabilizer. Explicitly transfer the real
	// accepted request to its scheduler, preserving original A and confirmation
	// context; the existing release path owns the subsequent reevaluation.
	p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 1)
	p.telnetStabilizer.Start()
	t.Cleanup(p.telnetStabilizer.Stop)
	if !p.telnetStabilizer.enqueueWithTemporalResult(s, 0, "accepted_fixture", key, true, true, result, original) {
		t.Fatal("actual bounded scheduler refused its free request slot")
	}
	correctionLossAdmit(t, r, d, key, "K1ABE", 14020, at.Add(-time.Second), 2*time.Minute, "N0EEE", "N0FFF", "N0GGG", "N0HHH")
	correctionLossEvictions(t, r, 1, 0)
	correctionLossPopulation(t, r, key, 7, map[string]int{"K1ABC": 0, "K1ABD": 3, "K1ABE": 4})
	release := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 0)
	if release.peerOriginal != original || release.temporalResult != result || release.spot != s {
		t.Fatal("actual scheduler lost original publication or accepted request ownership")
	}
	p.handleStabilizerRelease(release) // Final existing check budget: emit, rather than await another hold.
	if p.tracker.TemporalCommitted() != 1 || p.tracker.TemporalPathSwitches() != 0 ||
		p.temporal.decoder.ConfirmedHistoryMetrics().Keys != 1 || p.temporal.decoder.ConfirmCorrection(key, "K1ABD") {
		t.Errorf("loss reevaluation changed accepted history: commits=%d switches=%d history=%+v",
			p.tracker.TemporalCommitted(), p.tracker.TemporalPathSwitches(), p.temporal.decoder.ConfirmedHistoryMetrics())
	}
	if p.telnetStabilizer.Pending() != 0 {
		t.Error("released accepted request retained a scheduler reservation")
	}
	temporalDeliveryCheckWithoutPercent(t, f, correctionEvidenceDeliveryCase{
		subject: "K1ABC", wantCall: "K1ABD", wantGlyph: "?", wantCorrections: 1,
	}, at, true)
}

func TestCorrectionLossDeliveredIndependentSFloor(t *testing.T) {
	for _, source := range []string{"static_only", "history_only"} {
		t.Run(source, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			at := time.Now().UTC().Truncate(time.Second)
			observed := at.Add(-3 * time.Second)
			r, d, key := correctionLossResolver(t, 2, 8)
			correctionLossAdmit(t, r, d, key, "K1ABC", 14020, observed, 2*time.Minute, "N0AAA")
			correctionLossAdmit(t, r, d, key, "K1ABD", 14020, observed, 2*time.Minute, "N0BBB", "N0CCC")
			correctionLossAdmit(t, r, d, key, "K1ABE", 14020, observed, 2*time.Minute, "N0DDD")
			correctionLossEvictions(t, r, 1, 0)
			correctionLossPopulation(t, r, key, 3, map[string]int{"K1ABC": 0, "K1ABD": 2, "K1ABE": 1})
			// Encode only the selected independent recognition source. The history
			// rows deliberately have no learned static membership, and their
			// receivers differ from the current resolver population.
			raw := make(map[string][]byte)
			if source == "static_only" {
				value := make([]byte, 8)
				binary.BigEndian.PutUint64(value, uint64(at.Add(-time.Hour).Unix()))
				raw["m|K1ABC"] = value
			} else {
				for i, receiver := range []string{"H0AAA", "H0BBB", "H0CCC"} {
					value := make([]byte, 10)
					binary.BigEndian.PutUint64(value[:8], uint64(at.Add(-time.Minute).Unix()))
					binary.BigEndian.PutUint16(value[8:], uint16(101*(i+1)))
					raw["o|K1ABC|20m|cw|"+receiver] = value
				}
			}
			path := filepath.Join(t.TempDir(), "independent-scp")
			db, err := pebble.Open(path, &pebble.Options{})
			if err != nil {
				t.Fatal(err)
			}
			for name, value := range raw {
				if err := db.Set([]byte(name), value, pebble.Sync); err != nil {
					_ = db.Close()
					t.Fatal(err)
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			customSCPFreshnessAssertBytes(t, path, raw)
			store := customSCPFreshnessOpen(t, spot.CustomSCPOptions{
				Path: path, HorizonDays: 60, StaticHorizonDays: 395, MaxKeys: 8, MaxSpottersPerKey: 8,
				CoreMinScore: 3, CoreMinH3Cells: 2,
				SFloorMinScore: 3, SFloorExactMinH3Cells: 2, SFloorFamilyMinH3Cells: 2,
			})
			static := store.StaticContains("K1ABC")
			history := store.HasSFloorSupportExact("K1ABC", "20m", "CW", 3, at)
			if static != (source == "static_only") || history != (source == "history_only") {
				t.Fatalf("independent recognition fixture was not isolated: static=%t history=%t source=%s", static, history, source)
			}
			cfg := correctionLossConfig()
			cfg.CustomSCP = config.CallCorrectionCustomSCPConfig{
				Enabled: true, ResolverMinScore: 3, ResolverMinUniqueSpotters: 2, ResolverMinUniqueH3Cells: 2,
				SFloorMinScore: 3, SFloorMinUniqueSpottersExact: 3, SFloorMinUniqueH3CellsExact: 2,
				SFloorMinUniqueSpottersFamily: 4, SFloorMinUniqueH3CellsFamily: 2,
			}
			f := correctionLossFixture(t, r, cfg)
			f.pipeline.recentBandStore, f.pipeline.customSCPStore = store, store
			s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
			s.SourceType = spot.SourceManual
			f.pipeline.processSpot(s, nil)
			temporalDeliveryCheckWithoutPercent(t, f, correctionEvidenceDeliveryCase{
				subject: "K1ABC", wantCall: "K1ABC", wantGlyph: "S",
			}, at, true)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			customSCPFreshnessAssertBytes(t, path, raw)
		})
	}
}
