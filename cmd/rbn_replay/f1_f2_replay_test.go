package main

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
)

func TestReplayF1F2ProtectionAndRecovery(t *testing.T) {
	for _, kind := range []string{"F1", "F2"} {
		for _, recovered := range []bool{false, true} {
			for _, temporal := range []bool{false, true} {
				for _, consumed := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/recovered=%t/temporal=%t/consumed=%t", kind, recovered, temporal, consumed), func(t *testing.T) {
						at := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
						cfg := replayTemporalFreshConfig()
						cfg.DistanceModelCW, cfg.ResolverNeighborhoodMaxDistance = "plain", 1
						cfg.ResolverNeighborhoodEnabled, cfg.ResolverNeighborhoodBucketRadius = true, 1
						cfg.MinSpotterReliability, cfg.MinConsensusReports, cfg.StabilizerMaxChecks = .95, 3, 12
						weights := spot.SpotterReliability{}
						if kind == "F2" {
							weights = spot.SpotterReliability{"N0AAA": .95, "N0AAB": .95, "N0AAC": .95}
						}
						resolver := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 128, HysteresisWindows: 100,
							EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
							MinSpotterReliability: .95, SpotterReliability: weights,
							FreqGuardMinSeparationKHz: .3, FreqGuardRunnerUpRatio: .6, MaxEditDistance: 6})
						driver, err := spot.NewSignalResolverDriver(resolver)
						if err != nil {
							t.Fatal(err)
						}
						r := &replayRunner{cfg: &config.Config{CallCorrection: cfg}, resolver: resolver, driver: driver,
							tracker: &stats.Tracker{}, temporalNextID: 1, temporalDecoder: correctionflow.NewTemporalDecoder(cfg),
							temporalPending: make(map[uint64]replayTemporalPending), stabilityCollector: newReplayStabilityCollector(replayStabilityConfig{})}
						key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
						b := []string{"N0BBA", "N0BBB", "N0BBC"}
						frequency := 14020.5
						if kind == "F2" {
							frequency = 14020
						}
						f1F2ReplayAdd(t, r, key, "K1ABD", b, frequency, at)
						s := replayTemporalFreshSpot("K1ABC", at.Add(4*time.Second))
						if temporal {
							if !r.maybeHoldTemporal(s, spot.ResolverEvidence{Key: key}, true, at) {
								t.Fatal("actual replay did not hold settled B3 proposal")
							}
							for _, pending := range r.temporalPending {
								if pending.selection.Snapshot.Winner != "K1ABD" || pending.selection.Snapshot.TotalReporters != 3 || pending.selection.Snapshot.ReceiverEvidence() != nil {
									t.Fatalf("saved proposal lacks actual compact B3 authority: %+v", pending)
								}
							}
							replayTemporalFreshProposal(t, r, at.Add(7*time.Second), false, correctionflow.TemporalDecisionActionApply, "K1ABD")
						}
						f1F2ReplayAdd(t, r, key, "K1ABC", []string{"N0AAA", "N0AAB", "N0AAC"}, 14020, at.Add(time.Second))
						if kind == "F1" {
							f1F2ReplayAdd(t, r, key, "K1ABD", append(b, "N0BBD", "N0BBE"), 14020, at.Add(2*time.Second))
							f1F2ReplayAdd(t, r, key, "K1ABD", []string{"N0CCA", "N0CCB", "N0CCC", "N0CCD"}, 14020.5, at.Add(3*time.Second))
						}
						if recovered {
							if kind == "F2" {
								f1F2ReplayAdd(t, r, key, "K1ABD", []string{"N0BBD", "N0BBE"}, 14020, at.Add(3*time.Second))
							}
							for i := 0; i < 100; i++ {
								driver.Sweep(at.Add(5*time.Second + time.Duration(i)*time.Millisecond))
							}
						}
						if !resolver.Enqueue(spot.ResolverEvidence{Key: key, DXCall: "K1ABC", Spotter: "N0AAA", FrequencyKHz: 14020,
							ObservedAt: at.Add(4 * time.Second), RecencyWindow: 2 * time.Minute}) {
							t.Fatal("incoming replay observation was refused")
						}
						if consumed {
							driver.Drain()
							driver.Sweep(at.Add(6 * time.Second))
						}
						call, glyph, percent, accepted := "K1ABC", "P", 25, uint64(0)
						if kind == "F2" {
							percent = 48
						}
						if recovered {
							call, glyph, percent, accepted = "K1ABD", "C", 75, 1
							if kind == "F2" {
								percent = 63
							}
						}
						if temporal {
							r.drainTemporal(at.Add(7*time.Second), false)
							replayTemporalFreshReleased(t, r, s, call, glyph, accepted, 1)
						} else {
							outcome := maybeApplyResolverCorrectionReplay(s, resolver, spot.ResolverEvidence{Key: key}, true, cfg, nil, r.tracker, nil, nil, at.Add(7*time.Second))
							if outcome.Applied != recovered || r.tracker.CallCorrections() != accepted {
								t.Fatalf("ordinary replay bypassed protection or failed recovery: %+v", outcome)
							}
						}
						if s.DXCall != call || s.DXCallNorm != call || s.Confidence != glyph || !s.Time.Equal(at.Add(4*time.Second)) {
							t.Fatalf("replay retained fields differ from literal population oracle: %+v want %s/%s/%d%%", s, call, glyph, percent)
						}
						current := correctionflow.SelectResolverPrimarySnapshotForCallAt(resolver, key, cfg, call, at.Add(7*time.Second))
						share, available := correctionflow.ResolverCallConfidencePercent(current.Snapshot, call)
						wantTotal := 12
						if kind == "F2" {
							wantTotal = 6
							if recovered {
								wantTotal = 8
							}
						}
						if !current.SnapshotOK || current.Snapshot.TotalReporters != wantTotal || !available || share != percent {
							t.Fatalf("replay shared population/share=%d/%t/%d want %d/%d%%: %+v", share, available, current.Snapshot.TotalReporters, wantTotal, percent, current)
						}
					})
				}
			}
		}
	}
}

func f1F2ReplayAdd(t *testing.T, r *replayRunner, key spot.ResolverSignalKey, call string, rows []string, frequency float64, at time.Time) {
	t.Helper()
	for _, receiver := range rows {
		if !r.resolver.Enqueue(spot.ResolverEvidence{Key: key, DXCall: call, Spotter: receiver, FrequencyKHz: frequency,
			ObservedAt: at, RecencyWindow: 2 * time.Minute}) {
			t.Fatal("fixture evidence refused")
		}
	}
	r.driver.Drain()
	r.driver.Sweep(at.Add(time.Millisecond))
}
