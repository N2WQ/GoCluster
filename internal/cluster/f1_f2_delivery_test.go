//go:build !qualification

// File role: Connects F1/F2 protection and recovery to actual TCP, sealed storage,
// original peer delivery and reopened archive fields. Driver ordering explicitly
// covers consumed and still-queued observations without adding an ingest barrier.
package cluster

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func f1F2DeliveryPopulation(t *testing.T, kind string, at time.Time) (*spot.SignalResolver, *spot.SignalResolverDriver, spot.ResolverSignalKey, config.CallCorrectionConfig, correctionflow.ResolverPrimarySelection) {
	t.Helper()
	cfg := ambiguityDeliveryConfig(true)
	cfg.DistanceModelCW, cfg.ResolverNeighborhoodMaxDistance, cfg.MaxEditDistance = "plain", 1, 3
	cfg.MinConsensusReports, cfg.MinAdvantage, cfg.MinConfidencePercent = 3, 1, 45
	cfg.MinSpotterReliability, cfg.StabilizerMaxChecks = .95, 12
	weights := spot.SpotterReliability{}
	if kind == "F2" {
		weights = spot.SpotterReliability{"N0AAA": .95, "N0AAB": .95, "N0AAC": .95}
	}
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 128, HysteresisWindows: 100,
		EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond, MinSpotterReliability: .95,
		SpotterReliability: weights, FreqGuardMinSeparationKHz: .3, FreqGuardRunnerUpRatio: .6, MaxEditDistance: 3})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
	b := []string{"N0BBA", "N0BBB", "N0BBC"}
	frequency := 14020.5
	if kind == "F2" {
		frequency = 14020
	}
	f1F2DeliveryAdd(t, r, d, key, "K1ABD", b, frequency, at)
	saved := correctionflow.SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABC")
	if saved.Snapshot.Winner != "K1ABD" || saved.Snapshot.TotalReporters != 3 {
		t.Fatalf("saved proposal lacked actual settled B3 publication: %+v", saved)
	}
	f1F2DeliveryAdd(t, r, d, key, "K1ABC", []string{"N0AAA", "N0AAB", "N0AAC"}, 14020, at.Add(time.Second))
	if kind == "F1" {
		f1F2DeliveryAdd(t, r, d, key, "K1ABD", append(b, "N0BBD", "N0BBE"), 14020, at.Add(2*time.Second))
		f1F2DeliveryAdd(t, r, d, key, "K1ABD", []string{"N0CCA", "N0CCB", "N0CCC", "N0CCD"}, 14020.5, at.Add(3*time.Second))
	}
	return r, d, key, cfg, saved
}

func f1F2DeliveryAdd(t *testing.T, r *spot.SignalResolver, d *spot.SignalResolverDriver, key spot.ResolverSignalKey, call string, rows []string, frequency float64, at time.Time) {
	t.Helper()
	for _, rx := range rows {
		ambiguityDeliveryAdd(t, r, key, call, rx, frequency, at)
	}
	d.Drain()
	d.Sweep(at.Add(time.Millisecond))
}

func TestF1F2ActualDeliveryProtectionAndRecovery(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, kind := range []string{"F1", "F2"} {
		for _, recovered := range []bool{false, true} {
			for _, path := range []string{"immediate", "stabilizer", "temporal"} {
				for _, consumed := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/recovered=%t/%s/consumed=%t", kind, recovered, path, consumed), func(t *testing.T) {
						start := time.Now().UTC().Truncate(time.Second).Add(-10 * time.Second)
						r, d, key, cfg, saved := f1F2DeliveryPopulation(t, kind, start)
						if recovered {
							if kind == "F2" {
								f1F2DeliveryAdd(t, r, d, key, "K1ABD", []string{"N0BBD", "N0BBE"}, 14020, start.Add(3*time.Second))
							}
							for i := 0; i < 100; i++ {
								d.Sweep(start.Add(5*time.Second + time.Duration(i)*time.Millisecond))
							}
						}
						at := start.Add(4 * time.Second)
						// Same receiver/candidate membership: consumption changes freshness,
						// never the number of admitted receivers or the expected shares.
						ambiguityDeliveryAdd(t, r, key, "K1ABC", "N0AAA", 14020, at)
						if consumed {
							if d.Drain() != 1 {
								t.Fatal("incoming observation was not consumed exactly once")
							}
							d.Sweep(start.Add(6 * time.Second))
						}
						fixture := newAmbiguityDeliveryFixture(t, r, cfg)
						s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
						s.SourceType = spot.SourceManual
						switch path {
						case "immediate":
							fixture.pipeline.processSpotBody(s, nil)
						case "stabilizer":
							fixture.pipeline.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: s,
								peerOriginal: captureOriginalPeerPublication(s), resolverKey: key, hasResolverKey: true,
								evidenceEnqueued: true, checksCompleted: 11})
						case "temporal":
							cfg.TemporalDecoder = runtimeTemporalTestConfig(0).TemporalDecoder
							cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 1, 1
							fixture.pipeline.correctionCfg = cfg
							controller := newRuntimeTemporalController(cfg)
							fixture.pipeline.temporal = controller
							temporalDeliveryObserve(t, controller, start, key, s, saved)
							release := temporalDeliveryDrainApply(t, controller, time.Now().UTC(), "K1ABD")
							fixture.pipeline.processSpotBody(release.pending.spot, &release)
						}
						want := correctionEvidenceDeliveryCase{subject: "K1ABC", wantCall: "K1ABC", wantGlyph: "P", wantPercent: 25}
						if kind == "F2" {
							want.wantPercent = 48 // floor(2850/5850*100)
						}
						if recovered {
							want.wantCall, want.wantGlyph, want.wantCorrections = "K1ABD", "C", 1
							want.wantPercent = 75 // B9 / (A3+B9)
							if kind == "F2" {
								want.wantPercent = 63 // floor(5000/7850*100)
							}
						}
						fixture.check(t, want, at)
					})
				}
			}
		}
	}
}
