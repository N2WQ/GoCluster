//go:build !qualification

package cluster

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func ccv2CanonicalDeliveryOwner(t *testing.T, cfg config.CallCorrectionConfig) (*spot.SignalResolver, *spot.SignalResolverDriver, spot.ResolverSignalKey) {
	t.Helper()
	r := spot.NewSignalResolver(spot.SignalResolverConfig{HysteresisWindows: 1, EvalMinInterval: time.Nanosecond,
		FreqGuardMinSeparationKHz: cfg.FreqGuardMinSeparationKHz, FreqGuardRunnerUpRatio: cfg.FreqGuardRunnerUpRatio,
		MaxEditDistance: cfg.MaxEditDistance, DistanceModelCW: cfg.DistanceModelCW, DistanceModelRTTY: cfg.DistanceModelRTTY})
	d, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	return r, d, spot.NewResolverSignalKey(14020, "20m", "CW", cfg.FrequencyToleranceHz)
}

func ccv2CanonicalDeliveryRows(t *testing.T, r *spot.SignalResolver, key spot.ResolverSignalKey, call string, first, count int, at time.Time, window time.Duration) {
	t.Helper()
	for n := first; n < first+count; n++ {
		if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: call, Spotter: fmt.Sprintf("N0AA%c", 'A'+rune(n)), FrequencyKHz: 14020, RecencyWindow: window, Report: n, HasReport: true}) {
			t.Fatal("canonical fixture input refused")
		}
	}
}

func TestCCV2CanonicalDelivered(t *testing.T) {
	cfg := replacementLicenseConfig(t)
	cfg.MinConsensusReports = 3
	temporalDeliveryDirectory(t)
	for _, tc := range []struct {
		name, subject, first, second, want, glyph string
		firstN, secondN, subjectN, percent        int
		licensed, changed                         bool
	}{
		{"alias_display_is_not_mutation", "LZ/K1ABC", "LZ/K1ABC", "K1ABC/LZ", "LZ/K1ABC", "V", 1, 3, 0, 100, true, false},
		{"canonical_union_recovers_bare", "K1ABC", "LZ/K1ABC", "K1ABC/LZ", "LZ/K1ABC", "C", 2, 1, 1, 75, true, true},
		{"licensed_canonical_replacement", "LZ/K1ABC", "LZ/K1ABD", "K1ABD/LZ", "LZ/K1ABD", "C", 2, 1, 1, 75, true, true},
		{"missing_license_preserves_original", "LZ/K1ABC", "LZ/K1ABD", "K1ABD/LZ", "LZ/K1ABC", "P", 2, 1, 1, 25, false, false},
	} {
		for _, path := range []string{"immediate", "temporal", "stabilizer_retry"} {
			t.Run(tc.name+"/"+path, func(t *testing.T) {
				replacementLicenseFixture(t, tc.licensed)
				r, d, key := ccv2CanonicalDeliveryOwner(t, cfg)
				f := newAmbiguityDeliveryFixture(t, r, cfg)
				db := loadIngestCTY(t)
				f.pipeline.ctyLookup = func() *cty.CTYDatabase { return db }
				at := time.Now().UTC().Truncate(time.Second)
				ccv2CanonicalDeliveryRows(t, r, key, tc.first, 0, tc.firstN, at, 2*time.Minute)
				ccv2CanonicalDeliveryRows(t, r, key, tc.second, tc.firstN, tc.secondN, at, 2*time.Minute)
				ccv2CanonicalDeliveryRows(t, r, key, tc.subject, tc.firstN+tc.secondN, tc.subjectN, at, 2*time.Minute)
				wantRows := tc.firstN + tc.secondN + tc.subjectN
				if d.Drain() != wantRows {
					t.Fatal("owner did not consume canonical fixture")
				}
				d.Sweep(at.Add(time.Millisecond))
				selected := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, cfg, tc.subject, at.Add(time.Millisecond))
				if !selected.SnapshotOK || selected.Snapshot.TotalReporters != wantRows || correctionflow.ResolverSupportForCall(selected.Snapshot, tc.first) != tc.firstN+tc.secondN || correctionflow.ResolverSupportForCall(selected.Snapshot, tc.second) != tc.firstN+tc.secondN {
					t.Fatalf("aliases did not share distinct receiver authority: %+v", selected)
				}
				s := newDeliveryTestSpot(tc.subject, "N0AAA", at)
				s.SourceType = spot.SourceManual
				switch path {
				case "immediate":
					f.pipeline.processSpotBody(s, nil)
				case "stabilizer_retry":
					f.pipeline.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: s, peerOriginal: captureOriginalPeerPublication(s), resolverKey: key, hasResolverKey: true, evidenceEnqueued: true})
				case "temporal":
					temporalCfg := cfg
					temporalCfg.TemporalDecoder = runtimeTemporalTestConfig(0).TemporalDecoder
					f.pipeline.correctionCfg = temporalCfg
					controller := newRuntimeTemporalController(temporalCfg)
					f.pipeline.temporal = controller
					temporalDeliveryObserve(t, controller, at.Add(-3*time.Second), key, s, selected)
					releases := controller.Drain(time.Now().UTC(), false)
					if len(releases) != 1 || releases[0].decision.Action != correctionflow.TemporalDecisionActionApply {
						t.Fatalf("temporal alias fixture did not Apply: %+v", releases)
					}
					f.pipeline.processSpotBody(releases[0].pending.spot, &releases[0])
					wantCommits := uint64(0)
					if tc.changed {
						wantCommits = 1
					}
					if f.pipeline.tracker.TemporalCommitted() != wantCommits || f.pipeline.tracker.TemporalPathSwitches() != 0 {
						t.Fatal("display alias counted a temporal identity change/switch")
					}
				}
				wantCorrections := uint64(0)
				if tc.changed {
					wantCorrections = 1
				}
				f.check(t, correctionEvidenceDeliveryCase{subject: tc.subject, wantCall: tc.want, wantGlyph: tc.glyph, wantPercent: tc.percent, wantCorrections: wantCorrections}, at)
				if rows := f.ring.GetRecent(1); len(rows) != 1 || rows[0].DXMetadata.ADIF != 212 {
					t.Fatalf("observed portable display did not resolve Bulgaria before output: %+v", rows)
				}
				if !tc.licensed && f.pipeline.tracker.CorrectionDecisionReasons()[resolverDecisionLicenseMissing] != 1 {
					t.Fatal("canonical proposal did not reach base-call license guard")
				}
			})
		}
	}
}

func TestCCV2CanonicalFreshDisplayDelivered(t *testing.T) {
	cfg := replacementLicenseConfig(t)
	cfg.MinConsensusReports = 2
	temporalDeliveryDirectory(t)
	replacementLicenseFixture(t, true)
	r, d, key := ccv2CanonicalDeliveryOwner(t, cfg)
	f := newAmbiguityDeliveryFixture(t, r, cfg)
	db := loadIngestCTY(t)
	f.pipeline.ctyLookup = func() *cty.CTYDatabase { return db }
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	ccv2CanonicalDeliveryRows(t, r, key, "LZ/K1ABD", 0, 3, at.Add(-8*time.Second), 10*time.Second)
	ccv2CanonicalDeliveryRows(t, r, key, "K1ABD/LZ", 3, 2, at, 10*time.Second)
	ccv2CanonicalDeliveryRows(t, r, key, "LZ/K1ABC", 5, 1, at, 10*time.Second)
	if d.Drain() != 6 {
		t.Fatal("missing display fixture inputs")
	}
	d.Sweep(at.Add(time.Millisecond))
	selected := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "LZ/K1ABC", at.Add(time.Millisecond))
	if selected.Snapshot.Winner != "LZ/K1ABD" {
		t.Fatalf("initial observed display control=%+v", selected)
	}
	s := newDeliveryTestSpot("LZ/K1ABC", "N0AAA", at)
	s.SourceType = spot.SourceManual
	temporalCfg := cfg
	temporalCfg.TemporalDecoder = runtimeTemporalTestConfig(0).TemporalDecoder
	f.pipeline.correctionCfg = temporalCfg
	controller := newRuntimeTemporalController(temporalCfg)
	f.pipeline.temporal = controller
	temporalDeliveryObserve(t, controller, at.Add(-3*time.Second), key, s, selected)
	releases := controller.Drain(time.Now().UTC(), false)
	if len(releases) != 1 || releases[0].decision.Action != correctionflow.TemporalDecisionActionApply {
		t.Fatal("no held observed-display proposal")
	}
	f.pipeline.processSpotBody(releases[0].pending.spot, &releases[0])
	f.check(t, correctionEvidenceDeliveryCase{subject: "LZ/K1ABC", wantCall: "K1ABD/LZ", wantGlyph: "C", wantPercent: 66, wantCorrections: 1}, at)
	if f.pipeline.tracker.TemporalCommitted() != 1 || controller.decoder.ConfirmedHistoryMetrics().Keys != 1 {
		t.Fatal("fresh display lost accepted semantic confirmation")
	}
}
