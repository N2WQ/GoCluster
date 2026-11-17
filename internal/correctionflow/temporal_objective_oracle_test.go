// Exact vectors were derived in a separate, non-steered model review. They
// preserve the additive ranking objective and START identity, not an HMM or
// calibrated probability. The beam covers every state in these finite examples.
package correctionflow

import (
	"testing"

	"dxcluster/config"
	"dxcluster/spot"
)

func ccv2TemporalOracleDecoder() *TemporalDecoder {
	return NewTemporalDecoder(config.CallCorrectionConfig{
		DistanceModelCW: "plain", DistanceModelRTTY: "plain",
		FamilyPolicy: config.CallCorrectionFamilyPolicyConfig{Truncation: config.CallCorrectionTruncationFamilyConfig{
			Enabled: true, MaxLengthDelta: 2, MinShorterLength: 3, AllowPrefixMatch: true, AllowSuffixMatch: true}},
		TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{Enabled: true, BeamSize: 8, MaxObsCandidates: 8,
			StayBonus: 120, SwitchPenalty: 160, FamilySwitchPenalty: 60, Edit1SwitchPenalty: 90},
	})
}

func ccv2TemporalOracleEvent(subject string, total int, ranks ...spot.ResolverCandidateSupport) temporalEvent {
	return temporalEvent{subject: subject, selection: ResolverPrimarySelection{SnapshotOK: true,
		Snapshot: spot.ResolverSnapshot{SupportBasis: spot.ResolverSupportBasisWeighted, State: spot.ResolverStateConfident,
			TotalReporters: total / 1000, TotalWeightedSupportMilli: total, CandidateRanks: ranks}}}
}

func TestCCV2TemporalIndependentFinitePathOracle(t *testing.T) {
	for _, tc := range []struct {
		name, a, b                       string
		switchCost, wantBest, wantMargin int
	}{
		{"unrelated", "DL1AAA", "K9XYZ", -160, 14836, 4717},
		{"truncation_family", "K1AB", "K1ABC", -60, 14936, 4817},
		{"edit_one", "K1ABC", "K1ABD", -90, 14906, 4787},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := ccv2TemporalOracleDecoder()
			events := []temporalEvent{
				ccv2TemporalOracleEvent(tc.a, 4000, spot.ResolverCandidateSupport{Call: tc.a, Support: 3, WeightedSupportMilli: 3000}, spot.ResolverCandidateSupport{Call: tc.b, Support: 1, WeightedSupportMilli: 1000}),
				ccv2TemporalOracleEvent(tc.a, 4000, spot.ResolverCandidateSupport{Call: tc.a, Support: 1, WeightedSupportMilli: 1000}, spot.ResolverCandidateSupport{Call: tc.b, Support: 3, WeightedSupportMilli: 3000}),
			}
			// These four literal emissions distinguish one milli-unit smoothing
			// from one receiver. All four legal paths are enumerated below.
			for step, emissions := range [][2]int{{7498, 2501}, {2501, 7498}} {
				for index, call := range []string{tc.a, tc.b} {
					if got := d.emissionScore(events[step], call, 2); got != emissions[index] {
						t.Fatalf("emission%d/%s=%d need%d", step, call, got, emissions[index])
					}
				}
			}
			if d.transitionScore(tc.a, tc.b, "USB") != tc.switchCost || d.transitionScore(tc.a, tc.a, "USB") != 120 {
				t.Fatal("transition contract changed")
			}
			paths := [4]int{7498 + 120 + 2501, 7498 + tc.switchCost + 7498, 2501 + tc.switchCost + 2501, 2501 + 120 + 7498}
			bestA, bestB := max(paths[0], paths[1]), max(paths[2], paths[3])
			if bestA != tc.wantBest || bestA-bestB != tc.wantMargin {
				t.Fatal("independent all-path table is inconsistent")
			}
			winner, best, margin, count := d.decodeWindowLocked(events, tc.a, "USB")
			if winner != tc.a || best != tc.wantBest || margin != tc.wantMargin || count != 2 {
				t.Fatalf("returned terminal identity or changed objective: winner%s score%d margin%d count%d", winner, best, margin, count)
			}
		})
	}
}

func TestCCV2TemporalIndependentLateOnlyOracle(t *testing.T) {
	d := ccv2TemporalOracleDecoder()
	const a, b, c = "DL1AAA", "K9XYZ", "JA7QRS"
	events := []temporalEvent{
		ccv2TemporalOracleEvent(a, 4000, spot.ResolverCandidateSupport{Call: a, Support: 3, WeightedSupportMilli: 3000}, spot.ResolverCandidateSupport{Call: b, Support: 1, WeightedSupportMilli: 1000}),
		ccv2TemporalOracleEvent(a, 1000, spot.ResolverCandidateSupport{Call: c, Support: 1, WeightedSupportMilli: 1000}),
	}
	// All legal paths: AA=7627, AC=17328; BA=2350, BB=2630, BC=12331.
	// C wins each best continuation, but is absent from the starting universe.
	if d.emissionScore(events[1], a, 2) != 9 || d.emissionScore(events[1], b, 2) != 9 || d.emissionScore(events[1], c, 2) != 9990 {
		t.Fatal("late-only emission or absence smoothing changed")
	}
	winner, best, margin, count := d.decodeWindowLocked(events, a, "USB")
	if winner != a || best != 17328 || margin != 4997 || count != 2 {
		t.Fatalf("late-only identity became a start or objective drifted: %s %d %d %d", winner, best, margin, count)
	}
}

func TestCCV2TemporalIndependentEquivalentSlashOracle(t *testing.T) {
	d := ccv2TemporalOracleDecoder()
	events := []temporalEvent{
		ccv2TemporalOracleEvent("KH6/W1AW", 1000, spot.ResolverCandidateSupport{Call: "KH6/W1AW", Support: 1, WeightedSupportMilli: 1000}),
		ccv2TemporalOracleEvent("W1AW/KH6", 1000, spot.ResolverCandidateSupport{Call: "W1AW/KH6", Support: 1, WeightedSupportMilli: 1000}),
	}
	winner, best, margin, count := d.decodeWindowLocked(events, "KH6/W1AW", "USB")
	if winner != "W1AW/KH6" || best != 20120 || margin != 20120 || count != 1 {
		t.Fatalf("alias spelling changed identity, smoothing or stay reward: %s %d %d %d", winner, best, margin, count)
	}
}
