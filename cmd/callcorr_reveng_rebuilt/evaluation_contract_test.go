package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type ccV2LegacyEvaluationTotals struct {
	rows, labels, matched, missed, extra                 int64
	unlabeledSeen, sampleRows, sampleApplied, sampleIdle int64
	predicted, unlabeledApplied                          int64
	agreement, sampledMixture                            float64
}

type ccV2EvaluationFixture struct {
	labels, predictions                []string
	candidateRows, labeledDecisionRows int64
	unlabeledSeen, unlabeledApplied    int64
	sample                             []bool
}

// These counts are hand-enumerated event multisets, independent of the report
// formulas. A wrong-winner prediction is consumed by legacy pairing and is not
// an extra prediction; duplicate labels and predictions are separate events.
func TestCCV2EvaluationCompatibility(t *testing.T) {
	cases := []struct {
		name    string
		fixture ccV2EvaluationFixture
		want    ccV2LegacyEvaluationTotals
	}{
		{
			name:    "correct",
			fixture: ccV2EvaluationFixture{labels: []string{"K1BBB"}, predictions: []string{"K1BBB"}, candidateRows: 1, labeledDecisionRows: 1},
			want:    ccV2LegacyEvaluationTotals{rows: 1, labels: 1, matched: 1, predicted: 1, agreement: 100, sampledMixture: 100},
		},
		{
			name:    "wrong",
			fixture: ccV2EvaluationFixture{labels: []string{"K1BBB"}, predictions: []string{"K1CCC"}, candidateRows: 1, labeledDecisionRows: 1},
			want:    ccV2LegacyEvaluationTotals{rows: 1, labels: 1, missed: 1, predicted: 1},
		},
		{
			name:    "missing_prediction",
			fixture: ccV2EvaluationFixture{labels: []string{"K1BBB"}, candidateRows: 1, labeledDecisionRows: 1},
			want:    ccV2LegacyEvaluationTotals{rows: 1, labels: 1, missed: 1},
		},
		{
			name:    "loaded_label_without_replay_row",
			fixture: ccV2EvaluationFixture{labels: []string{"K1BBB"}},
			want:    ccV2LegacyEvaluationTotals{labels: 1, missed: 1},
		},
		{
			name:    "extra_prediction",
			fixture: ccV2EvaluationFixture{labels: []string{"K1BBB"}, predictions: []string{"K1BBB", "K1CCC"}, candidateRows: 2, labeledDecisionRows: 2},
			want:    ccV2LegacyEvaluationTotals{rows: 2, labels: 1, matched: 1, extra: 1, predicted: 2, agreement: 100, sampledMixture: 100},
		},
		{
			name:    "duplicate_labels",
			fixture: ccV2EvaluationFixture{labels: []string{"K1BBB", "K1BBB"}, predictions: []string{"K1BBB"}, candidateRows: 1, labeledDecisionRows: 1},
			want:    ccV2LegacyEvaluationTotals{rows: 1, labels: 2, matched: 1, missed: 1, predicted: 1, agreement: 50, sampledMixture: 100},
		},
		{
			name:    "duplicate_predictions",
			fixture: ccV2EvaluationFixture{labels: []string{"K1BBB"}, predictions: []string{"K1BBB", "K1BBB"}, candidateRows: 2, labeledDecisionRows: 2},
			want:    ccV2LegacyEvaluationTotals{rows: 2, labels: 1, matched: 1, extra: 1, predicted: 2, agreement: 100, sampledMixture: 100},
		},
		{
			name:    "conflicting_labels_preserve_ordered_pairing",
			fixture: ccV2EvaluationFixture{labels: []string{"K1BBB", "K1CCC"}, predictions: []string{"K1CCC"}, candidateRows: 1, labeledDecisionRows: 1},
			want:    ccV2LegacyEvaluationTotals{rows: 1, labels: 2, missed: 2, predicted: 1},
		},
		{name: "zero_denominators"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, labels := ccV2MakeEvaluationFixture(tc.fixture)
			report := buildEvalReport(state, labels)
			ccV2AssertLegacyEvaluation(t, report, tc.want)
			ccV2AssertEvaluationDefinitions(t, report)
			ccV2AssertLabelAgreement(t, report, tc.want)
			if !reflect.DeepEqual(report.Replay, state.replay) || !reflect.DeepEqual(report.Labels, labels) || !reflect.DeepEqual(report.Counters, state.counters) {
				t.Fatal("legacy replay, labels, or primary counters changed")
			}
			denominators := report.EvaluationDenominators
			if denominators.EligibleReplayCandidateRows != tc.want.rows || denominators.PrimaryDecisionRowsEvaluated != tc.fixture.labeledDecisionRows ||
				denominators.CandidateRowsNotEvaluated != 0 || denominators.DecisionRowsAtExternalLabelKeys != tc.fixture.labeledDecisionRows ||
				denominators.DecisionRowsWithoutExternalLabelKeys != 0 {
				t.Fatalf("unexpected row populations: %+v", denominators)
			}
			var coverage *float64
			if tc.fixture.labeledDecisionRows > 0 {
				coverage = ccV2EvaluationNumber(100)
			}
			ccV2AssertEvaluationPercent(t, "label-key coverage", denominators.ExternalLabelKeyCoveragePercent, coverage)
			ccV2AssertEvaluationPercent(t, "unlabeled application rate", report.UnlabeledDecisions.ApplicationPercent, nil)
		})
	}
	t.Run("sample_size_changes_only_sample_metrics", ccV2CheckEvaluationSampleSizes)
	t.Run("unlabeled_only", ccV2CheckUnlabeledEvaluation)
	t.Run("human_summary", ccV2CheckEvaluationSummary)
}

func ccV2CheckEvaluationSampleSizes(t *testing.T) {
	// Five applied labeled-key rows: two matching duplicate labels, two wrong
	// winners consumed on misses, and one extra. Eight unlabeled decision rows
	// contain three applications. Two further accepted rows never reach a gate.
	fixture := ccV2EvaluationFixture{
		labels:        []string{"K1BBB", "K1BBB", "K1CCC", "K1DDD"},
		predictions:   []string{"K1BBB", "K1BBB", "K1EEE", "K1FFF", "K1GGG"},
		candidateRows: 15, labeledDecisionRows: 5, unlabeledSeen: 8, unlabeledApplied: 3,
	}
	cases := []struct {
		sample            []bool
		applied, idle     int64
		wantLegacyMixture float64
	}{
		{wantLegacyMixture: 100},
		{sample: []bool{true, false}, applied: 1, idle: 1, wantLegacyMixture: 66.67},
		{sample: []bool{true, false, true, false}, applied: 2, idle: 2, wantLegacyMixture: 50},
	}
	var firstAgreement externalLabelAgreement
	var firstDenominators evaluationDenominators
	for i, tc := range cases {
		fixture.sample = tc.sample
		state, labels := ccV2MakeEvaluationFixture(fixture)
		report := buildEvalReport(state, labels)
		want := ccV2LegacyEvaluationTotals{rows: 15, labels: 4, matched: 2, missed: 2, extra: 1,
			unlabeledSeen: 8, sampleRows: int64(len(tc.sample)), sampleApplied: tc.applied, sampleIdle: tc.idle,
			predicted: 8, unlabeledApplied: 3, agreement: 50, sampledMixture: tc.wantLegacyMixture}
		ccV2AssertLegacyEvaluation(t, report, want)
		ccV2AssertLabelAgreement(t, report, want)
		ccV2AssertEvaluationDefinitions(t, report)
		denominators := report.EvaluationDenominators
		if denominators.EligibleReplayCandidateRows != 15 || denominators.PrimaryDecisionRowsEvaluated != 13 ||
			denominators.CandidateRowsNotEvaluated != 2 || denominators.DecisionRowsAtExternalLabelKeys != 5 ||
			denominators.DecisionRowsWithoutExternalLabelKeys != 8 {
			t.Fatalf("sample %d changed the hand-counted row populations: %+v", i, denominators)
		}
		ccV2AssertEvaluationPercent(t, "label-key coverage", denominators.ExternalLabelKeyCoveragePercent, ccV2EvaluationNumber(38.46))
		unlabeled := report.UnlabeledDecisions
		if unlabeled.DecisionRows != 8 || unlabeled.Applications != 3 || unlabeled.NonApplications != 5 ||
			unlabeled.SampleRows != int64(len(tc.sample)) || unlabeled.SampleApplications != tc.applied || unlabeled.SampleNonApplications != tc.idle {
			t.Fatalf("sample %d has incorrect unlabeled counts: %+v", i, unlabeled)
		}
		ccV2AssertEvaluationPercent(t, "unlabeled application rate", unlabeled.ApplicationPercent, ccV2EvaluationNumber(37.5))
		if i == 0 {
			firstAgreement, firstDenominators = report.ExternalLabelAgreement, denominators
		} else if !reflect.DeepEqual(report.ExternalLabelAgreement, firstAgreement) || !reflect.DeepEqual(denominators, firstDenominators) {
			t.Fatal("sample size changed external-label agreement or replay coverage")
		}
	}
}

func ccV2CheckUnlabeledEvaluation(t *testing.T) {
	state, labels := ccV2MakeEvaluationFixture(ccV2EvaluationFixture{
		candidateRows: 3, unlabeledSeen: 3, unlabeledApplied: 1, sample: []bool{true, false, false},
	})
	report := buildEvalReport(state, labels)
	ccV2AssertLegacyEvaluation(t, report, ccV2LegacyEvaluationTotals{rows: 3, unlabeledSeen: 3,
		sampleRows: 3, sampleApplied: 1, sampleIdle: 2, predicted: 1, unlabeledApplied: 1})
	ccV2AssertEvaluationDefinitions(t, report)
	ccV2AssertEvaluationPercent(t, "agreement", report.ExternalLabelAgreement.AgreementPercent, nil)
	ccV2AssertEvaluationPercent(t, "label-key coverage", report.EvaluationDenominators.ExternalLabelKeyCoveragePercent, ccV2EvaluationNumber(0))
	ccV2AssertEvaluationPercent(t, "unlabeled application rate", report.UnlabeledDecisions.ApplicationPercent, ccV2EvaluationNumber(33.33))
}

func ccV2MakeEvaluationFixture(f ccV2EvaluationFixture) (*runState, otherLoadStats) {
	in := newInterner()
	key := labelKey{MinuteUnix: 60, FreqKey: 140000, SpotterID: in.intern("K1AAA"), SubjectID: in.intern("K1ZZZ")}
	state := &runState{in: in, freqBucketHz: 100, expectedByKey: map[labelKey][]uint32{}, predictedByKey: map[labelKey][]predictedEvent{},
		negSeen: f.unlabeledSeen, negSamples: f.sample, predOnUnlabeled: f.unlabeledApplied,
		replay: replayStats{RBNRows: f.candidateRows, CandidateRows: f.candidateRows, SkippedBadRow: 2, SkippedSchema: 1}}
	var conflictingLabels int64
	for i, winner := range f.labels {
		state.expectedByKey[key] = append(state.expectedByKey[key], in.intern(winner))
		if i > 0 && winner != f.labels[i-1] {
			conflictingLabels++
		}
	}
	for _, winner := range f.predictions {
		state.predictedByKey[key] = append(state.predictedByKey[key], predictedEvent{WinnerID: in.intern(winner), Reason: "fixture"})
	}
	unlabeledKey := key
	unlabeledKey.MinuteUnix = 120
	for i := int64(0); i < f.unlabeledApplied; i++ {
		state.predictedByKey[unlabeledKey] = append(state.predictedByKey[unlabeledKey], predictedEvent{WinnerID: in.intern("K1XXX"), Reason: "unlabeled"})
	}
	state.counters.DecisionTotal = f.labeledDecisionRows + f.unlabeledSeen
	state.counters.Applied = int64(len(f.predictions)) + f.unlabeledApplied
	state.counters.Rejected = state.counters.DecisionTotal - state.counters.Applied
	return state, otherLoadStats{Lines: int64(len(f.labels)) + 3, Parsed: int64(len(f.labels)), SkippedInvalid: 1, SkippedNoDB: 2, ConflictingLabel: conflictingLabels}
}

func ccV2AssertLegacyEvaluation(t *testing.T, report evalReport, want ccV2LegacyEvaluationTotals) {
	t.Helper()
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		t.Fatal(err)
	}
	// Every pre-contract numeric root key remains present with its original
	// value, including zero-denominator legacy percentages that serialize as 0.
	expected := map[string]float64{
		"rbn_rows": float64(want.rows), "positives_total": float64(want.labels),
		"correct_positive": float64(want.matched), "missed_positive": float64(want.missed),
		"predicted_extra_on_positives": float64(want.extra), "negatives_seen": float64(want.unlabeledSeen),
		"negatives_sampled": float64(want.sampleRows), "false_positive": float64(want.sampleApplied),
		"true_negative": float64(want.sampleIdle), "predicted_applied_total": float64(want.predicted),
		"predicted_on_unlabeled": float64(want.unlabeledApplied), "recall_percent": want.agreement,
		"precision_percent_sampled": want.sampledMixture,
	}
	for key, value := range expected {
		if got, ok := root[key].(float64); !ok || got != value {
			t.Errorf("legacy JSON %s = %v; want %v", key, root[key], value)
		}
	}
	denominators := root["evaluation_denominators"].(map[string]any)
	if value, present := denominators["external_label_events_with_replay_rows"]; !present || value != nil {
		t.Errorf("unknown label-event replay coverage must be explicit null, got %v / present=%v", value, present)
	}
	rfTruth := root["rf_truth"].(map[string]any)
	for _, key := range []string{"eligible_error_events", "scored_error_events", "precision_percent", "recall_percent"} {
		if value, present := rfTruth[key]; !present || value != nil {
			t.Errorf("RF truth %s must be explicit null, got %v / present=%v", key, value, present)
		}
	}
}

func ccV2AssertLabelAgreement(t *testing.T, report evalReport, want ccV2LegacyEvaluationTotals) {
	t.Helper()
	agreement := report.ExternalLabelAgreement
	if agreement.LabelEventsScored != want.labels || agreement.MatchedLabelEvents != want.matched || agreement.UnmatchedLabelEvents != want.missed ||
		agreement.AppliedPredictionsAtLabeledKeys != want.predicted-want.unlabeledApplied || agreement.ExtraPredictionsAfterLegacyPairing != want.extra {
		t.Fatalf("external-label agreement does not describe the legacy pairing: %+v", agreement)
	}
	var percent *float64
	if want.labels != 0 {
		percent = &want.agreement
	}
	ccV2AssertEvaluationPercent(t, "agreement", agreement.AgreementPercent, percent)
}

func ccV2AssertEvaluationDefinitions(t *testing.T, report evalReport) {
	t.Helper()
	if report.EvaluationContractVersion != 1 || report.RFTruth.Availability != "unavailable" || report.RFTruth.Reason == "" {
		t.Fatalf("missing version or RF-truth boundary: %+v", report.RFTruth)
	}
	for _, key := range []string{"false_positive", "true_negative", "recall_percent", "precision_percent_sampled"} {
		if !strings.Contains(report.DeprecatedMetrics[key], "Deprecated") {
			t.Errorf("legacy %s is not explicitly deprecated", key)
		}
	}
	for _, key := range []string{"external_label_agreement.matching_rule", "external_label_agreement.agreement_percent",
		"evaluation_denominators.eligible_replay_candidate_rows", "evaluation_denominators.primary_decision_rows_evaluated",
		"evaluation_denominators.external_label_key_coverage_percent", "evaluation_denominators.external_label_events_with_replay_rows",
		"unlabeled_decisions", "unlabeled_decisions.application_percent", "rf_truth"} {
		if report.MetricDefinitions[key] == "" {
			t.Errorf("metric definition missing for %s", key)
		}
	}
	warnings := strings.Join(report.Warnings, "\n")
	for _, message := range []string{"not RF accuracy", "ordered one-to-one", "unknown correctness", "sample size", "coverage is unavailable"} {
		if !strings.Contains(warnings, message) {
			t.Errorf("missing warning about %q", message)
		}
	}
}

func ccV2CheckEvaluationSummary(t *testing.T) {
	state, labels := ccV2MakeEvaluationFixture(ccV2EvaluationFixture{})
	report := buildEvalReport(state, labels)
	path := filepath.Join(t.TempDir(), "summary.txt")
	writeSummary(path, report, inferredMethod{})
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{"Evaluation contract v1 (external-label agreement):", "agreement_percent=unavailable (zero denominator)",
		"external_label_events_with_replay_rows=unavailable", "RF precision, recall, eligible error events, and scored error events: unavailable",
		"Deprecated legacy evaluation (compatibility values; not RF accuracy):",
		"false_positive means sampled unlabeled applications; true_negative means sampled unlabeled non-applications.",
		"recall_percent=0.00", "precision_percent_sampled=0.00"} {
		if !strings.Contains(text, want) {
			t.Errorf("human summary missing %q", want)
		}
	}
	if strings.Contains(text, "%!") {
		t.Fatal("human summary contains a formatting error")
	}
}

func ccV2EvaluationNumber(value float64) *float64 { return &value }

func ccV2AssertEvaluationPercent(t *testing.T, name string, got, want *float64) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %s; want %s", name, formatEvaluationPercent(got), formatEvaluationPercent(want))
	}
}
