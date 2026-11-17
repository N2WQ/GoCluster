package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
)

func ccv2CanonicalReplayOwner(t *testing.T, cfg config.CallCorrectionConfig) (*replayRunner, spot.ResolverSignalKey) {
	t.Helper()
	r := &replayRunner{cfg: &config.Config{CallCorrection: cfg}, tracker: &stats.Tracker{}, temporalNextID: 1,
		temporalDecoder: correctionflow.NewTemporalDecoder(cfg), temporalPending: make(map[uint64]replayTemporalPending),
		stabilityCollector: newReplayStabilityCollector(replayStabilityConfig{})}
	r.resolver = spot.NewSignalResolver(spot.SignalResolverConfig{HysteresisWindows: 1, EvalMinInterval: time.Nanosecond, MaxEditDistance: 6, DistanceModelCW: "morse", DistanceModelRTTY: "baudot"})
	var err error
	r.driver, err = spot.NewSignalResolverDriver(r.resolver)
	if err != nil {
		t.Fatal(err)
	}
	r.ctyDB, err = cty.LoadCTYDatabaseFromReader(strings.NewReader(`<plist><dict><key>K</key><dict><key>Country</key><string>United States</string><key>Prefix</key><string>K</string><key>ADIF</key><integer>291</integer></dict><key>LZ</key><dict><key>Country</key><string>Bulgaria</string><key>Prefix</key><string>LZ</string><key>ADIF</key><integer>212</integer></dict></dict></plist>`))
	if err != nil {
		t.Fatal(err)
	}
	return r, spot.NewResolverSignalKey(14020, "20m", "CW", 500)
}

func ccv2CanonicalReplayRows(t *testing.T, r *replayRunner, key spot.ResolverSignalKey, call string, offset, count int, at time.Time, window time.Duration) {
	t.Helper()
	for n := offset; n < offset+count; n++ {
		if !r.resolver.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: call, Spotter: fmt.Sprintf("N0AA%c", 'A'+rune(n)), FrequencyKHz: 14020, RecencyWindow: window, Report: n, HasReport: true}) {
			t.Fatal("canonical replay evidence refused")
		}
	}
}

func TestCCV2CanonicalReplayParity(t *testing.T) {
	// Replay publishes glyph/count artifacts; it does not populate the live
	// Spot percentage field. Check its selected receiver union separately from
	// delivered identity/counters. Native TCP tests own wire-percent assertions.
	for _, tc := range []struct {
		name, subject, first, second, want, glyph string
		firstN, secondN, subjectN, percent        int
		licensed, changed                         bool
	}{
		{"alias_is_unchanged", "LZ/K1ABC", "LZ/K1ABC", "K1ABC/LZ", "LZ/K1ABC", "V", 1, 3, 0, 100, true, false},
		{"union_recovers_bare", "K1ABC", "LZ/K1ABC", "K1ABC/LZ", "LZ/K1ABC", "C", 2, 1, 1, 75, true, true},
		{"license_present", "LZ/K1ABC", "LZ/K1ABD", "K1ABD/LZ", "LZ/K1ABD", "C", 2, 1, 1, 75, true, true},
		{"license_missing", "LZ/K1ABC", "LZ/K1ABD", "K1ABD/LZ", "LZ/K1ABC", "P", 2, 1, 1, 25, false, false},
	} {
		for _, temporal := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/temporal_%t", tc.name, temporal), func(t *testing.T) {
				calls := []string{"K1ABC"}
				if tc.licensed {
					calls = append(calls, "K1ABD")
				}
				replayLicenseGlobals(t, replayLicenseDB(t, calls...))
				cfg := replayTemporalFreshConfig()
				cfg.MinConsensusReports = 3
				r, key := ccv2CanonicalReplayOwner(t, cfg)
				at := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
				ccv2CanonicalReplayRows(t, r, key, tc.first, 0, tc.firstN, at, time.Minute)
				ccv2CanonicalReplayRows(t, r, key, tc.second, tc.firstN, tc.secondN, at, time.Minute)
				ccv2CanonicalReplayRows(t, r, key, tc.subject, tc.firstN+tc.secondN, tc.subjectN, at, time.Minute)
				if r.driver.Drain() != tc.firstN+tc.secondN+tc.subjectN {
					t.Fatal("missing replay owner input")
				}
				r.driver.Sweep(at.Add(time.Millisecond))
				control := correctionflow.SelectResolverPrimarySnapshotForCallAt(r.resolver, key, cfg, tc.subject, at.Add(time.Millisecond))
				if percent, available := correctionflow.ResolverCallConfidencePercent(control.Snapshot, tc.want); !available || percent != tc.percent {
					t.Fatalf("replay receiver-union confidence control=%d/%t want %d", percent, available, tc.percent)
				}
				s := replayTemporalFreshSpot(tc.subject, at)
				evidence := spot.ResolverEvidence{Key: key}
				if temporal {
					if !r.maybeHoldTemporal(s, evidence, true, at) {
						t.Fatal("configured temporal owner did not hold canonical replay")
					}
					r.drainTemporal(at.Add(2*time.Second), false)
				} else {
					out := maybeApplyResolverCorrectionReplay(s, r.resolver, evidence, true, cfg, r.ctyDB, r.tracker, nil, nil, at)
					r.processOutcome(s, out, at)
				}
				want := uint64(0)
				if tc.changed {
					want = 1
				}
				if s.DXCallNorm != tc.want || s.Confidence != tc.glyph || r.tracker.CallCorrections() != want || uint64(len(r.stabilityCollector.corrections)) != want {
					t.Fatalf("replay differs from literal native contract: call=%s glyph=%s mutations=%d stability=%d", s.DXCallNorm, s.Confidence, r.tracker.CallCorrections(), len(r.stabilityCollector.corrections))
				}
				if temporal && (r.abMetrics.Temporal.Committed != want || r.abMetrics.Temporal.PathSwitches != 0) {
					t.Fatal("replay counted alias-only temporal mutation/switch")
				}
			})
		}
	}
}

func TestCCV2CanonicalReplayFreshDisplay(t *testing.T) {
	replayLicenseGlobals(t, replayLicenseDB(t, "K1ABC", "K1ABD"))
	cfg := replayTemporalFreshConfig()
	r, key := ccv2CanonicalReplayOwner(t, cfg)
	at := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	ccv2CanonicalReplayRows(t, r, key, "LZ/K1ABD", 0, 3, at.Add(-8*time.Second), 10*time.Second)
	ccv2CanonicalReplayRows(t, r, key, "K1ABD/LZ", 3, 2, at, 10*time.Second)
	ccv2CanonicalReplayRows(t, r, key, "LZ/K1ABC", 5, 1, at, 10*time.Second)
	if r.driver.Drain() != 6 {
		t.Fatal("missing raw aliases")
	}
	r.driver.Sweep(at.Add(time.Millisecond))
	s := replayTemporalFreshSpot("LZ/K1ABC", at)
	if !r.maybeHoldTemporal(s, spot.ResolverEvidence{Key: key}, true, at) {
		t.Fatal("no temporal hold")
	}
	control := correctionflow.SelectResolverPrimarySnapshotForCallAt(r.resolver, key, cfg, "LZ/K1ABC", at.Add(3*time.Second))
	if percent, ok := correctionflow.ResolverCallConfidencePercent(control.Snapshot, "K1ABD/LZ"); !ok || percent != 66 || control.Snapshot.Winner != "K1ABD/LZ" {
		t.Fatalf("fresh receiver/display control failed: %+v", control)
	}
	r.drainTemporal(at.Add(3*time.Second), false)
	if s.DXCallNorm != "K1ABD/LZ" || s.Confidence != "C" || r.tracker.CallCorrections() != 1 || r.abMetrics.Temporal.Committed != 1 {
		t.Fatalf("stale display survived fresh release: %+v", s)
	}
}
