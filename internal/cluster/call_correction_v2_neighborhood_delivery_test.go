//go:build !qualification

package cluster

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func TestCCV2NeighborhoodAdmissionDelivered(t *testing.T) {
	source, err := filepath.Abs(filepath.Join("..", "..", "data", "config"))
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(source)
	if err != nil {
		t.Fatal(err)
	}
	temporalDeliveryDirectory(t)
	for _, subject := range []string{"K1AXZ", "K1XYZ"} {
		for _, path := range []string{"immediate", "temporal", "stabilizer_retry"} {
			t.Run(subject+"/"+path, func(t *testing.T) {
				cfg := loaded.CallCorrection
				// Plain distance fixes literal two/three-edit inputs independently
				// of Morse tuning. All adaptive/temporal/stabilizer settings load
				// from the shipped configuration rather than disabling those rails.
				cfg.DistanceModelCW = "plain"
				at := time.Now().UTC().Truncate(time.Second).Add(-16 * time.Second)
				r := spot.NewSignalResolver(spot.SignalResolverConfig{HysteresisWindows: 1, EvalMinInterval: time.Nanosecond})
				d, err := spot.NewSignalResolverDriver(r)
				if err != nil {
					t.Fatal(err)
				}
				key := spot.NewResolverSignalKey(14020, "20m", "CW", cfg.FrequencyToleranceHz)
				for n := 0; n < 10; n++ {
					call, freq := "K1ABC", 14020.0
					if n == 6 {
						call = subject
					}
					if n >= 7 {
						call, freq = "DL9ZZZ", 14020+cfg.FrequencyToleranceHz/1000
					}
					if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: call, Spotter: fmt.Sprintf("N%dAAA", n), FrequencyKHz: freq, RecencyWindow: 5 * time.Minute}) {
						t.Fatal("input refused")
					}
				}
				if d.Drain() != 10 {
					t.Fatal("missing resolver input")
				}
				d.Sweep(at.Add(time.Millisecond))
				selection := correctionflow.SelectResolverPrimarySnapshotForCall(r, key, cfg, subject)
				if selection.UsedNeighborhood || selection.Snapshot.Winner != "K1ABC" {
					t.Fatalf("excluded neighbor removed exact candidate: %+v", selection)
				}
				f := newAmbiguityDeliveryFixture(t, r, cfg)
				f.pipeline.adaptiveMinReports = spot.NewAdaptiveMinReports(cfg)
				s := newDeliveryTestSpot(subject, "N0AAA", at)
				s.SourceType = spot.SourceManual
				switch path {
				case "immediate":
					f.pipeline.processSpotBody(s, nil)
				case "stabilizer_retry":
					f.pipeline.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: s, peerOriginal: captureOriginalPeerPublication(s), resolverKey: key, hasResolverKey: true, evidenceEnqueued: true})
				case "temporal":
					controller := newRuntimeTemporalController(cfg)
					f.pipeline.temporal = controller
					accepted, _, reason := controller.Observe(at, key, subject, runtimeTemporalPending{spot: s,
						peerOriginal: captureOriginalPeerPublication(s), evidence: spot.ResolverEvidence{Key: key}, hasEvidence: true,
						stabilizerResolverKey: key, hasStabilizerResolverKey: true, stabilizerEvidenceEnqueued: true, selection: selection})
					if !accepted {
						t.Fatalf("temporal refused: %s", reason)
					}
					releases := controller.Drain(at.Add(16*time.Second), false)
					if len(releases) != 1 || releases[0].decision.Action != correctionflow.TemporalDecisionActionApply {
						t.Fatalf("temporal did not propose exact candidate: %+v", releases)
					}
					f.pipeline.processSpotBody(releases[0].pending.spot, &releases[0])
				}
				f.check(t, correctionEvidenceDeliveryCase{subject: subject, wantCall: "K1ABC", wantGlyph: "C", wantPercent: 85, wantCorrections: 1}, at)
			})
		}
	}
}
