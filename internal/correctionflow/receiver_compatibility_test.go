package correctionflow

import (
	"math"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"dxcluster/spot"
)

var compatibilityOptions = FrequencyProposalOptions{150, time.Minute, 3}

// Literal private cells exercise the real Score path without training, loading
// an asset, or deriving expected numbers from B's aggregation implementation.
func compatibilityToyModel() *OrdinalModel {
	model := &OrdinalModel{}
	for b := range model.cells {
		cell := &model.cells[b]
		cell.gapStops = 50
		for source := range cell.opportunities {
			cell.opportunities[source] = 50
			for target := range cell.logEvents[source] {
				cell.logEvents[source][target] = -20
			}
			cell.logInsert[source] = -20
		}
		cell.logContinue = -20
		cell.logEvents[0][0], cell.logEvents[0][1] = -1, -4
		cell.logEvents[1][0], cell.logEvents[1][1] = -3, -1
	}
	return model
}

func compatibilityRow(call, receiver string, weight, report int) spot.ResolverEvidenceRow {
	row := f0Row(call, receiver, 7021, 0)
	row.Observation.WeightMilli, row.Observation.Report = weight, report
	return row
}

func compatibilityAssess(t testing.TB, rows []spot.ResolverEvidenceRow, subject string, raw bool, model *OrdinalModel) ReceiverCompatibilityAssessment {
	t.Helper()
	key, ordinary, sources, cfg := f0Fixture(t, rows)
	if raw {
		sources[2].Snapshot.SupportBasis = spot.ResolverSupportBasisReceivers
	}
	return AssessReceiverCompatibilityProposals(key, subject, 7021, ordinary, sources, cfg, compatibilityOptions, model)
}

func compatibilityMean(t testing.TB, got ReceiverCompatibilityAssessment, identity string) ReceiverCompatibilityScore {
	t.Helper()
	if !got.Available {
		t.Fatalf("comparison unavailable: %+v", got)
	}
	for _, hypothesis := range got.Hypotheses {
		if hypothesis.Identity == identity {
			return hypothesis
		}
	}
	t.Fatalf("missing hypothesis %q: %+v", identity, got)
	return ReceiverCompatibilityScore{}
}

func TestCCV2BWeightedReceiverBudget(t *testing.T) {
	rows := []spot.ResolverEvidenceRow{compatibilityRow("A", "R1", 2, 0),
		compatibilityRow("B", "R1", 1, 0), compatibilityRow("B", "R2", 1, 0)}
	got := compatibilityAssess(t, rows, "C", false, compatibilityToyModel())
	// R1 allocates 4/3 to A and 2/3 to B; R2 allocates one to B.
	// Denominator=3, hence A=(-4/3-20/3)/3 and B=(-4-5/3)/3.
	f1Near(t, got.Denominator, 3)
	f1Near(t, compatibilityMean(t, got, "A").Mean, -8.0/3)
	f1Near(t, compatibilityMean(t, got, "B").Mean, -17.0/9)
	if !got.SelectedOK || got.Selected.Identity != "B" || got.ScoreCalls != 6 {
		t.Fatalf("literal winner or aggregated calls: %+v", got)
	}
	for i := range rows {
		rows[i].Observation.WeightMilli = 0
	}
	got = compatibilityAssess(t, rows, "C", true, compatibilityToyModel())
	// Raw R1 contributes halves, raw R2 one: A=(-1/2-6)/2,
	// B=(-3/2-3/2)/2. Raw mode must not read the zero reliability tallies.
	f1Near(t, got.Denominator, 2)
	f1Near(t, compatibilityMean(t, got, "A").Mean, -13.0/4)
	f1Near(t, compatibilityMean(t, got, "B").Mean, -3.0/2)
	got = compatibilityAssess(t, rows, "C", false, compatibilityToyModel())
	if got.Available || got.SelectedOK || got.CompatibilityReason != "zero_positive_weight" || got.ScoreCalls != 0 {
		t.Fatalf("weighted zero silently became raw: %+v", got)
	}
}

func TestCCV2BOwnSNR(t *testing.T) {
	model := compatibilityToyModel()
	model.cells[0].logEvents[0][1], model.cells[0].logEvents[1][1] = -1, -5
	model.cells[2].logEvents[0][1], model.cells[2].logEvents[1][1] = -5, -1
	rows := []spot.ResolverEvidenceRow{compatibilityRow("A", "R1", 1, 0), compatibilityRow("B", "R2", 1, 12)}
	got := compatibilityAssess(t, rows, "C", false, model)
	f1Near(t, compatibilityMean(t, got, "A").Mean, -3)
	f1Near(t, compatibilityMean(t, got, "B").Mean, -2)
	// Using focal zero for both rows reverses the winner: A=-1, B=-4.
	rows[1].Observation.Report = 0
	wrongPopulation := compatibilityAssess(t, rows, "C", false, model)
	f1Near(t, compatibilityMean(t, wrongPopulation, "A").Mean, -1)
	f1Near(t, compatibilityMean(t, wrongPopulation, "B").Mean, -4)
}

func TestCCV2BDeadbandBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		gap  float64
		act  bool
	}{
		{"below", math.Nextafter(1e-9, 0), false}, {"equal", 1e-9, false},
		{"above", math.Nextafter(1e-9, math.Inf(1)), true}, {"tie", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ReceiverCompatibilityAssessment{Available: true,
				SelectiveFrequencyAssessment: SelectiveFrequencyAssessment{FrequencyAssessment: FrequencyAssessment{Proposals: []FrequencyProposal{{Identity: "A"}}}},
				Hypotheses:                   []ReceiverCompatibilityScore{{Identity: "A", Eligible: true}, {Identity: "B", Mean: -tc.gap}, {Identity: "C", Mean: -1, Keep: true}}}
			selectReceiverCompatibilityWinner(&got)
			if got.SelectedOK != tc.act || got.Tolerance != 1e-9 || !got.Available {
				t.Fatalf("strict margin: %+v", got)
			}
		})
	}
	got := ReceiverCompatibilityAssessment{Available: true, Hypotheses: []ReceiverCompatibilityScore{
		{Identity: "A", Eligible: true}, {Identity: "B", Mean: -5e-6}, {Identity: "C", Mean: -1e6, Keep: true}}}
	selectReceiverCompatibilityWinner(&got)
	if got.SelectedOK || got.CompatibilityReason != "numerical_tie" || got.Tolerance != .001 {
		t.Fatalf("ineligible opponent omitted from maxabs: %+v", got)
	}
	got.Hypotheses[0].Keep = true
	got.Hypotheses[1].Mean = -1
	got.Hypotheses[2].Mean = -2
	selectReceiverCompatibilityWinner(&got)
	if got.SelectedOK || got.CompatibilityReason != "keep" {
		t.Fatalf("KEEP winner authorized a replacement: %+v", got)
	}
}

func TestCCV2BCompensatedArithmetic(t *testing.T) {
	var sum receiverCompatibilitySum
	if !sum.add(-1e16) {
		t.Fatal("finite first term rejected")
	}
	for range 47 {
		if !sum.add(-1) {
			t.Fatal("finite small term rejected")
		}
	}
	// Exact integer -10000000000000047 rounds to this nearest even float.
	if sum.value() != -10000000000000048 {
		t.Fatalf("small same-sign terms lost: %.17g", sum.value())
	}
	rows := []spot.ResolverEvidenceRow{compatibilityRow("A", "R1", 2, 0)}
	model := compatibilityToyModel()
	model.cells[0].logEvents[0][0] = -math.MaxFloat64
	got := compatibilityAssess(t, rows, "C", false, model)
	if got.Available || got.SelectedOK || got.Hypotheses != nil || got.CompatibilityReason != "nonfinite_arithmetic" {
		t.Fatalf("nonfinite weighted numerator accepted: %+v", got)
	}
}

func TestCCV2BDeterminismOwnershipAndConcurrentReads(t *testing.T) {
	rows := []spot.ResolverEvidenceRow{compatibilityRow("A", "R1", 2, 0),
		compatibilityRow("B", "R1", 1, 12), compatibilityRow("B", "R2", 1, 24)}
	model := compatibilityToyModel()
	key, ordinary, sources, cfg := f0Fixture(t, rows)
	assess := func() ReceiverCompatibilityAssessment {
		return AssessReceiverCompatibilityProposals(key, "C", 7021, ordinary, sources, cfg, compatibilityOptions, model)
	}
	want := assess()
	if !want.Available || !want.SelectedOK {
		t.Fatalf("ownership control must score and select: %+v", want)
	}
	slices.Reverse(rows)
	reversed := compatibilityAssess(t, rows, "C", false, model)
	if !reflect.DeepEqual(want, reversed) {
		t.Fatal("input ordering changed the owned result")
	}
	altered := assess()
	altered.Hypotheses[0].Identity, altered.Hypotheses[0].Mean = "changed", 42
	altered.Proposals[0].Identity = "changed"
	altered.Selected.Identity = "changed"
	if got := assess(); !reflect.DeepEqual(got, want) {
		t.Fatal("returned values modified later results or input evidence")
	}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 16 {
				if got := assess(); !reflect.DeepEqual(got, want) {
					t.Error("concurrent immutable assessment changed")
				}
			}
		})
	}
	workers.Wait()
}
