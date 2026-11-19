package correctionflow

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestCCV2BSNRAvailability(t *testing.T) {
	for _, tc := range []struct {
		name      string
		report    int
		measured  bool
		conflict  bool
		available bool
	}{
		{"missing", 0, false, false, false}, {"conflict", 0, false, true, false},
		{"below", -1000, true, false, false}, {"lower_excluded", -999, true, false, false},
		{"lower_inside", -998, true, false, true}, {"measured_zero", 0, true, false, true},
		{"upper_inside", 999, true, false, true}, {"above", 1000, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := compatibilityRow("A", "R1", 1, tc.report)
			row.Observation.HasReport, row.Observation.SNRConflict = tc.measured, tc.conflict
			got := compatibilityAssess(t, []spot.ResolverEvidenceRow{row}, "C", false, compatibilityToyModel())
			if got.Available != tc.available || !tc.available && (got.SelectedOK || got.Hypotheses != nil || got.CompatibilityReason != "snr_unavailable") {
				t.Fatalf("SNR boundary: %+v", got)
			}
		})
	}
	rows := []spot.ResolverEvidenceRow{compatibilityRow("A", "R1", 1, 0), compatibilityRow("B", "R2", 0, 0)}
	rows[1].Observation.HasReport = false
	if got := compatibilityAssess(t, rows, "C", false, compatibilityToyModel()); !got.Available || len(got.Hypotheses) != 3 {
		t.Fatalf("zero arithmetic weight erased opponent or poisoned own-SNR evidence: %+v", got)
	}
	rows[0].Observation.WeightMilli = 0
	if got := compatibilityAssess(t, rows, "C", true, compatibilityToyModel()); got.Available || got.CompatibilityReason != "snr_unavailable" {
		t.Fatalf("raw unit observation ignored missing SNR: %+v", got)
	}
}

func TestCCV2BOpponentUnavailable(t *testing.T) {
	rows := []spot.ResolverEvidenceRow{compatibilityRow("A", "R1", 1, 0), compatibilityRow("B", "R2", 0, 0)}
	for _, floor := range []uint64{49, 50} {
		model := compatibilityToyModel()
		model.cells[0].opportunities[1] = floor
		got := compatibilityAssess(t, rows, "C", false, model)
		if got.Available != (floor == 50) || floor == 49 && (got.SelectedOK || got.Hypotheses != nil || got.CompatibilityReason != "ordinal_unavailable") {
			t.Fatalf("zero-weight opposing source floor %d: %+v", floor, got)
		}
		model = compatibilityToyModel()
		model.cells[0].gapStops = floor
		got = compatibilityAssess(t, rows, "C", false, model)
		if got.Available != (floor == 50) {
			t.Fatalf("gap floor %d: %+v", floor, got)
		}
	}
	model := compatibilityToyModel()
	model.cells[0].logEvents[1][0] = math.NaN()
	if got := compatibilityAssess(t, rows, "C", false, model); got.Available || got.SelectedOK || got.CompatibilityReason != "ordinal_unavailable" {
		t.Fatalf("nonfinite opponent score discarded: %+v", got)
	}
	if got := compatibilityAssess(t, rows, "C", false, nil); got.Available || got.SelectedOK || got.CompatibilityReason != "ordinal_unavailable" {
		t.Fatalf("nil model gained authority: %+v", got)
	}
	for _, opponent := range []string{"B?", strings.Repeat("B", 33)} {
		rows[1].Call = opponent
		got := compatibilityAssess(t, rows, "C", false, compatibilityToyModel())
		if got.Available || got.SelectedOK || got.ScoreCalls != 0 || got.CompatibilityReason != "unusable_identity" {
			t.Fatalf("unusable zero-weight opponent removed/truncated: %+v", got)
		}
	}
}

func TestCCV2BIneligibleOpponentWins(t *testing.T) {
	rows := []spot.ResolverEvidenceRow{compatibilityRow("A", "R1", 1, 0), compatibilityRow("B", "R2", 0, 0),
		compatibilityRow("A", "R3", 1, 0)}
	rows[1].Observation.FrequencyKHz = 0 // Retained unknown-frequency dissent, never a nominee.
	model := compatibilityToyModel()
	model.cells[0].logEvents[1][0] = 0
	got := compatibilityAssess(t, rows, "C", false, model)
	opponent := compatibilityMean(t, got, "B")
	if !compatibilityMean(t, got, "A").Eligible || opponent.Eligible || opponent.Mean != 0 || got.SelectedOK || got.CompatibilityReason != "ineligible_winner" {
		t.Fatalf("zero-weight non-nominee failed to block selection: %+v", got)
	}
	// C rejects A against a credible unrelated identity, but B must retain A
	// and B as opposing hypotheses rather than score only surviving nominees.
	rows = []spot.ResolverEvidenceRow{compatibilityRow("A", "R1", 1, 0),
		compatibilityRow("ZZZZ", "RX", 1, 0), compatibilityRow("ZZZZ", "RY", 1, 0)}
	got = compatibilityAssess(t, rows, "C", false, compatibilityToyModel())
	if len(got.Declines) != 1 || got.Declines[0].NomineeIdentity != "A" || compatibilityMean(t, got, "A").Eligible || len(got.Hypotheses) != 3 {
		t.Fatalf("C-disqualified opposing population lost: %+v", got)
	}
}

func TestCCV2BPopulationBoundsAndKeepDedup(t *testing.T) {
	var rows []spot.ResolverEvidenceRow
	for i := range 9 {
		rows = append(rows, compatibilityRow(string(rune('A'+i)), fmt.Sprintf("R%d", i), 1, 0))
	}
	for _, subject := range []string{"A", "J"} {
		got := compatibilityAssess(t, rows[:8], subject, false, compatibilityToyModel())
		want := 9
		if subject == "A" {
			want = 8
		}
		if !got.Available || len(got.Hypotheses) != want {
			t.Fatalf("exact eight/KEEP boundary: %+v", got)
		}
		rows[8].Observation.WeightMilli = 0
		got = compatibilityAssess(t, rows, subject, false, compatibilityToyModel())
		if got.Available || got.SelectedOK || got.ScoreCalls != 0 || got.CompatibilityReason != "population_limit" {
			t.Fatalf("ninth observed zero-weight identity truncated: %+v", got)
		}
	}
	aliasRows := []spot.ResolverEvidenceRow{compatibilityRow("KH6/K1ABC", "R1", 1, 0)}
	got := compatibilityAssess(t, aliasRows, "K1ABC/KH6", false, compatibilityToyModel())
	if !got.Available || len(got.Hypotheses) != 1 || !got.Hypotheses[0].Keep || got.SelectedOK || got.CompatibilityReason != "keep" {
		t.Fatalf("canonical KEEP alias duplicated: %+v", got)
	}
}

func TestCCV2BCanonicalJoinBeforeBudget(t *testing.T) {
	rows := []spot.ResolverEvidenceRow{compatibilityRow("KH6/K1ABC", "R1", 2, 0),
		compatibilityRow("K1ABC/KH6", "R1", 1, 0), compatibilityRow("K1ABC/KH6", "R2", 1, 12)}
	key, ordinary, sources, cfg := f0Fixture(t, rows)
	model := compatibilityToyModel()
	assess := func() ReceiverCompatibilityAssessment {
		return AssessReceiverCompatibilityProposals(key, "K1ABD/KH6", 7021, ordinary, sources, cfg, compatibilityOptions, model)
	}
	want := assess()
	if !want.Available || want.Denominator != 3 || len(want.Hypotheses) != 2 || want.ScoreCalls != 4 {
		t.Fatalf("alias memberships did not join before budget: %+v", want)
	}
	sources[3] = sources[2]
	sources[3].RequestedKey.Bucket++
	sources[3].Snapshot.Key = sources[3].RequestedKey
	if got := assess(); !got.Available || got.Denominator != want.Denominator || got.ScoreCalls != want.ScoreCalls || !reflect.DeepEqual(got.Hypotheses, want.Hypotheses) {
		t.Fatalf("source copies multiplied receiver contributions: %+v", got)
	}
	rows[1].Observation.Report = 1
	view, err := spot.NewStudyResolverEvidence(rows, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	sources[2].Evidence, sources[3].Evidence = view, view
	if got := assess(); got.Available || got.SelectedOK || got.CompatibilityReason != "snr_unavailable" {
		t.Fatalf("equal-time alias SNR conflict ignored: %+v", got)
	}
	newer := compatibilityRow("K1ABC/KH6", "R1", 2, 12)
	newer.Observation.ObservedAt = newer.Observation.ObservedAt.Add(time.Second)
	view, err = spot.NewStudyResolverEvidence([]spot.ResolverEvidenceRow{newer}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	sources[3].Evidence = view
	if got := assess(); !got.Available || got.Denominator != 3 {
		t.Fatalf("strictly newer SNR did not replace canonical conflict: %+v", got)
	}
}
