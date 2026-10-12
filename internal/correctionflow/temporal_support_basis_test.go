// File role: Proves zero-weight decoder neutrality separately from preserved
// smoothing, transitions and confirmed identity history using actual owners.
package correctionflow

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func supportBasisActualZero(t testing.TB, count int) (*spot.SignalResolver, spot.ResolverSignalKey, time.Time, spot.ResolverSnapshot) {
	t.Helper()
	weights := make(spot.SpotterReliability, count)
	for i := 0; i < count; i++ {
		weights[fmt.Sprintf("N%03d", i)] = 0
	}
	r := spot.NewSignalResolver(spot.SignalResolverConfig{QueueSize: 32, MaxActiveKeys: 4,
		MaxCandidatesPerKey: 4, MaxReportersPerCand: 16, HysteresisWindows: 1,
		EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond, SpotterReliability: weights, MinSpotterReliability: 0})
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	for i := 0; i < count; i++ {
		if !r.Enqueue(spot.ResolverEvidence{Key: key, ObservedAt: at, DXCall: "K1ABC", Spotter: fmt.Sprintf("N%03d", i), FrequencyKHz: 7010, RecencyWindow: time.Minute}) {
			t.Fatal("actual zero-weight receiver was refused")
		}
	}
	if driver.Drain() != count {
		t.Fatal("zero-weight owner barrier consumed the wrong receiver population")
	}
	driver.Sweep(at.Add(time.Millisecond))
	snapshot, ok := r.Lookup(key)
	if !ok || snapshot.TotalReporters != count || snapshot.TotalWeightedSupportMilli != 0 ||
		snapshot.EffectiveSupportBasis() != spot.ResolverSupportBasisWeighted || snapshot.ReceiverEvidence() == nil || snapshot.ReceiverEvidence().Len() != count || ResolverSupportForCall(snapshot, "K1ABC") != count {
		t.Fatalf("actual all-zero owner publication was not proven: %+v", snapshot)
	}
	return r, key, at, snapshot
}

func TestTemporalSupportBasisActualAllZeroNeutrality(t *testing.T) {
	for _, count := range []int{1, 2, 10} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			r, key, at, snapshot := supportBasisActualZero(t, count)
			projected, ok := r.LookupEligible(key, at.Add(time.Second))
			if !ok {
				t.Fatal("actual eligible zero-weight publication disappeared")
			}
			d := b31GenerationDecoder()
			for _, subject := range []string{"K1ABC", "K1ZZZ"} {
				for _, s := range []spot.ResolverSnapshot{snapshot, snapshot.Compact(), projected, projected.Compact()} {
					event := temporalEvent{subject: subject, selection: ResolverPrimarySelection{SnapshotOK: true, Snapshot: s}}
					for _, candidateCount := range []int{1, 2, 4} {
						for _, call := range []string{"K1ABC", "K1ABD", "K1ZZZ"} {
							if score := d.emissionScore(event, call, candidateCount); score != 0 {
								t.Fatalf("N=%d subject=%s call=%s K=%d inherited zero-population support score=%d", count, subject, call, candidateCount, score)
							}
						}
						if percentage, ok := ResolverCallConfidencePercent(s, "K1ABC"); percentage != 0 || ok {
							t.Fatal("all-zero population supplied an available percentage")
						}
					}
				}
			}
		})
	}
}

func TestTemporalSupportBasisSmoothingAndUnavailable(t *testing.T) {
	d := b31GenerationDecoder()
	weighted := supportBasisSnapshot(spot.ResolverSupportBasisWeighted, 3, 1000, 2, 0)
	weighted.CandidateRanks = append(weighted.CandidateRanks, spot.ResolverCandidateSupport{Call: "K1ABD", Support: 1, WeightedSupportMilli: 1000})
	raw := supportBasisSnapshot(spot.ResolverSupportBasisReceivers, 3, 0, 2, 0)
	for _, tc := range []struct {
		name     string
		snapshot spot.ResolverSnapshot
		call     string
		want     int
	}{
		{"positive_total_known_zero", weighted, "K1ABC", 9},
		{"positive_total_absent", weighted, "K1ZZZ", 9},
		{"positive_total_supported", weighted, "K1ABD", 9990},
		{"raw_supported", raw, "K1ABC", 6000},
		{"raw_absent", raw, "K1ZZZ", 2000},
		{"unavailable_positive", supportBasisSnapshot(spot.ResolverSupportBasisUnavailable, 3, 1000, 2, 1000), "K1ABC", 0},
		{"ambiguous_zero", supportBasisSnapshot(spot.ResolverSupportBasisUnspecified, 3, 0, 2, 0), "K1ABC", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := temporalEvent{subject: tc.call, selection: ResolverPrimarySelection{SnapshotOK: true, Snapshot: tc.snapshot}}
			if got := d.emissionScore(event, tc.call, 2); got != tc.want {
				t.Fatalf("preserved smoothing score=%d want independent integer vector %d", got, tc.want)
			}
		})
	}
	weighted.EvidenceLoss = true
	if score := d.emissionScore(temporalEvent{subject: "K1ABC", selection: ResolverPrimarySelection{SnapshotOK: true, Snapshot: weighted}}, "K1ABC", 2); score != 9 {
		t.Fatal("S4 changed the separately preserved positive-weight loss emission policy")
	}
	missing := temporalEvent{subject: "K1ABC"}
	if d.emissionScore(missing, "K1ABC", 2) != 1 || d.emissionScore(missing, "K1ABD", 2) != 0 {
		t.Fatal("missing-snapshot subject fallback changed")
	}
}

func TestTemporalSupportBasisZeroProposalHasNoFreshAuthority(t *testing.T) {
	r, key, at, snapshot := supportBasisActualZero(t, 2)
	cfg := config.CallCorrectionConfig{DistanceModelCW: "plain", TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{
		Enabled: true, Scope: "all_correction_candidates", LagSeconds: 1, MaxWaitSeconds: 3, StayBonus: 17,
		SwitchPenalty: 31, FamilySwitchPenalty: 11, Edit1SwitchPenalty: 23,
		BeamSize: 4, MaxObsCandidates: 4, MaxPending: 8, MaxActiveKeys: 2, MaxEventsPerKey: 4,
		MinScore: 0, MinMarginScore: 0, OverflowAction: "fallback_resolver",
	}}
	d := NewTemporalDecoder(cfg)
	d.ConfirmCorrection(key, "K1ABD")
	prior := d.lastCommitted[key]
	selection := SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ZZZ", at.Add(time.Second))
	if !selection.SnapshotOK || selection.Snapshot.TotalWeightedSupportMilli != 0 {
		t.Fatal("current all-zero selection was not established")
	}
	for id := uint64(1); id <= 2; id++ {
		if ok, reason := d.Observe(TemporalObservation{ID: id, Key: key, SubjectCall: "K1ZZZ", ObservedAt: at.Add(time.Duration(id-1) * time.Second), Selection: selection}); !ok {
			t.Fatal(reason)
		}
	}
	proposal := d.Evaluate(1, at.Add(2*time.Second), false)
	if proposal.Action != TemporalDecisionActionApply || proposal.BestScore != 17 {
		t.Fatalf("zero support with unchanged stay transition did not prove proposal control: %+v", proposal)
	}
	if d.transitionScore("K1ABC", "K1ABC", "CW") != 17 || d.transitionScore("K1ABC", "K1ABD", "CW") != -23 || d.transitionScore("K1ABC", "DL9ZZZ", "CW") != -31 {
		t.Fatal("zero-support repair changed the separately selected transition policy")
	}
	if d.lastCommitted[key] != prior || d.ConfirmedHistoryMetrics().Keys != 1 {
		t.Fatal("unaccepted zero-support proposal changed confirmed identity history")
	}
	released := SelectTemporalReleaseSnapshot(r, key, cfg, "K1ZZZ", proposal, at.Add(2*time.Second))
	if released.Snapshot.State != spot.ResolverStateUncertain || released.Snapshot.TotalWeightedSupportMilli != 0 ||
		released.Snapshot.SupportBasis != snapshot.SupportBasis || classifyTemporalSnapshotState(released.Snapshot) != spot.ResolverStateUncertain {
		t.Fatalf("prior/zero-score proposal manufactured settled current confidence: %+v", released)
	}
	if percent, ok := ResolverCallConfidencePercent(released.Snapshot, proposal.Winner); percent != 0 || ok {
		t.Fatal("zero proposal manufactured a current percentage")
	}
}
