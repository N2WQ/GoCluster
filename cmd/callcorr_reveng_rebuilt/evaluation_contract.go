// File role: Defines the offline report's external-label agreement contract.
// Legacy JSON values remain compatibility data; no report metric establishes
// transmitted-call truth or changes resolver admission or replay decisions.
package main

import "fmt"

const evaluationContractVersion = 1

type externalLabelAgreement struct {
	LabelEventsScored                  int64    `json:"label_events_scored"`
	MatchedLabelEvents                 int64    `json:"matched_label_events"`
	UnmatchedLabelEvents               int64    `json:"unmatched_label_events"`
	AppliedPredictionsAtLabeledKeys    int64    `json:"applied_predictions_at_labeled_keys"`
	ExtraPredictionsAfterLegacyPairing int64    `json:"extra_predictions_after_legacy_pairing"`
	AgreementPercent                   *float64 `json:"agreement_percent"`
}

type evaluationDenominators struct {
	EligibleReplayCandidateRows          int64    `json:"eligible_replay_candidate_rows"`
	PrimaryDecisionRowsEvaluated         int64    `json:"primary_decision_rows_evaluated"`
	CandidateRowsNotEvaluated            int64    `json:"candidate_rows_not_evaluated"`
	DecisionRowsAtExternalLabelKeys      int64    `json:"decision_rows_at_external_label_keys"`
	DecisionRowsWithoutExternalLabelKeys int64    `json:"decision_rows_without_external_label_keys"`
	ExternalLabelKeyCoveragePercent      *float64 `json:"external_label_key_coverage_percent"`
	ExternalLabelEventsWithReplayRows    *int64   `json:"external_label_events_with_replay_rows"`
}

type unlabeledDecisions struct {
	DecisionRows          int64    `json:"decision_rows"`
	Applications          int64    `json:"applications"`
	NonApplications       int64    `json:"non_applications"`
	ApplicationPercent    *float64 `json:"application_percent"`
	SampleRows            int64    `json:"sample_rows"`
	SampleApplications    int64    `json:"sample_applications"`
	SampleNonApplications int64    `json:"sample_non_applications"`
}

type rfTruthEvaluation struct {
	Availability        string   `json:"availability"`
	Reason              string   `json:"reason"`
	EligibleErrorEvents *int64   `json:"eligible_error_events"`
	ScoredErrorEvents   *int64   `json:"scored_error_events"`
	PrecisionPercent    *float64 `json:"precision_percent"`
	RecallPercent       *float64 `json:"recall_percent"`
}

// addEvaluationContract reads only completed replay state. Its added storage is
// fixed-size report metadata, independent of archive size. CandidateRows counts
// accepted CW/RTTY rows including beacons; each row reaches the primary gate at
// most once. Beacon and enqueue-refused rows return before DecisionTotal, and
// every evaluated row is classified by label-key presence, even if not applied.
func addEvaluationContract(report *evalReport, st *runState) {
	report.EvaluationContractVersion = evaluationContractVersion
	report.ExternalLabelAgreement = externalLabelAgreement{
		LabelEventsScored:                  report.PositivesTotal,
		MatchedLabelEvents:                 report.CorrectPositive,
		UnmatchedLabelEvents:               report.MissedPositive,
		AppliedPredictionsAtLabeledKeys:    report.PredictedAppliedTotal - report.PredictedOnUnlabeled,
		ExtraPredictionsAfterLegacyPairing: report.PredictedExtraOnPositives,
		AgreementPercent:                   evaluationPercent(report.CorrectPositive, report.PositivesTotal),
	}
	labeledDecisionRows := st.counters.DecisionTotal - st.negSeen
	report.EvaluationDenominators = evaluationDenominators{
		EligibleReplayCandidateRows:          st.replay.CandidateRows,
		PrimaryDecisionRowsEvaluated:         st.counters.DecisionTotal,
		CandidateRowsNotEvaluated:            st.replay.CandidateRows - st.counters.DecisionTotal,
		DecisionRowsAtExternalLabelKeys:      labeledDecisionRows,
		DecisionRowsWithoutExternalLabelKeys: st.negSeen,
		ExternalLabelKeyCoveragePercent:      evaluationPercent(labeledDecisionRows, st.counters.DecisionTotal),
		// Applied predictions are retained per key, but non-applied rows are
		// not. Key-level row counts cannot establish individual label overlap.
		ExternalLabelEventsWithReplayRows: nil,
	}
	report.UnlabeledDecisions = unlabeledDecisions{
		DecisionRows:          st.negSeen,
		Applications:          st.predOnUnlabeled,
		NonApplications:       st.negSeen - st.predOnUnlabeled,
		ApplicationPercent:    evaluationPercent(st.predOnUnlabeled, st.negSeen),
		SampleRows:            report.NegativesSampled,
		SampleApplications:    report.FalsePositive,
		SampleNonApplications: report.TrueNegative,
	}
	report.RFTruth = rfTruthEvaluation{
		Availability: "unavailable",
		Reason:       "External-cluster labels are not independently adjudicated RF truth; unlabeled decisions have unknown correctness.",
	}
	report.MetricDefinitions = evaluationMetricDefinitions()
	report.DeprecatedMetrics = map[string]string{
		"false_positive":            "Deprecated alias for unlabeled_decisions.sample_applications; unlabeled applications are not established false positives.",
		"true_negative":             "Deprecated alias for unlabeled_decisions.sample_non_applications; unlabeled non-applications are not established true negatives.",
		"recall_percent":            "Deprecated alias for external_label_agreement.agreement_percent, except the legacy zero denominator returns 0; not RF-error recall.",
		"precision_percent_sampled": "Deprecated compatibility statistic: 100 * correct_positive / (correct_positive + false_positive), rounded to two decimals, or 0 for a zero denominator. It mixes all matched labels with sampled unlabeled applications without sample-rate weighting; not RF precision.",
	}
	report.Warnings = []string{
		"External-label agreement measures agreement with loaded external-cluster correction events, not RF accuracy, precision, or recall.",
		"External-label agreement retains legacy ordered one-to-one pairing: duplicates remain separate and conflicting label order can affect the result.",
		"Unlabeled applications and non-applications have unknown correctness; they are not false positives or true negatives.",
		"Legacy precision_percent_sampled can change with the unlabeled sample size while replay predictions and whole-population unlabeled application counts remain unchanged.",
		"Label events are scored even when no matching replay row is known. Individual label-event replay coverage is unavailable; label-key coverage describes only rows that reached primary decision evaluation.",
	}
}

func evaluationMetricDefinitions() map[string]string {
	return map[string]string{
		"external_label_agreement.label_events_scored":                      "All loaded external correction-label events, including duplicates and events without a known replay row; equals legacy positives_total.",
		"external_label_agreement.matching_rule":                            "Legacy ordered one-to-one pairing within minute/frequency-bucket/spotter/subject keys: for each label, match the first unused prediction of the same winner; on a miss consume the first unused prediction, if any. Conflicting label order can affect matches; duplicate events remain separate.",
		"external_label_agreement.matched_label_events":                     "Labels matched by the legacy ordered scorer; equals correct_positive. This is external-label agreement, not an RF true-positive count.",
		"external_label_agreement.unmatched_label_events":                   "Loaded labels not matched by the legacy scorer; equals missed_positive and includes different-winner predictions and missing predictions.",
		"external_label_agreement.applied_predictions_at_labeled_keys":      "All applied replay predictions whose key appears in the loaded labels; predicted_applied_total minus predicted_on_unlabeled. Key presence does not establish individual event correspondence.",
		"external_label_agreement.extra_predictions_after_legacy_pairing":   "Predictions left unused at labeled keys after legacy pairing; different-winner predictions consumed on misses are excluded. Equals predicted_extra_on_positives; not an RF false-positive count.",
		"external_label_agreement.agreement_percent":                        "100 * matched_label_events / label_events_scored, rounded to two decimals; null when no label events were loaded.",
		"evaluation_denominators.eligible_replay_candidate_rows":            "Valid accepted CW/RTTY replay rows after parser and spot-class admission; includes beacons and rows later refused by resolver enqueue. Equals replay.candidate_rows; not all input rows or all genuine RF errors.",
		"evaluation_denominators.primary_decision_rows_evaluated":           "Replay rows that reached primary resolver gate evaluation once; equals resolver_primary_counters.decision_total. Evaluated does not mean RF truth was scored.",
		"evaluation_denominators.candidate_rows_not_evaluated":              "Eligible replay candidate rows minus primary decision rows evaluated; their external-label-key coverage is unknown because they return before key classification.",
		"evaluation_denominators.decision_rows_at_external_label_keys":      "Primary decision rows evaluated minus unlabeled_decisions.decision_rows. A loaded label key can cover multiple replay rows; this is not a count of matched or correct label events.",
		"evaluation_denominators.decision_rows_without_external_label_keys": "All evaluated replay rows whose minute/frequency-bucket/spotter/subject key is absent from loaded labels; equals negatives_seen. Missing labels do not establish negative truth.",
		"evaluation_denominators.external_label_key_coverage_percent":       "100 * decision_rows_at_external_label_keys / primary_decision_rows_evaluated, rounded to two decimals; null when no primary decision rows were evaluated. Coverage of individual loaded label events is unavailable.",
		"evaluation_denominators.external_label_events_with_replay_rows":    "Unavailable (null): only applied predictions and aggregate evaluated-row key coverage are retained, so a loaded label event with no prediction cannot be distinguished from one with no replay row.",
		"unlabeled_decisions":                     "Whole-population decision_rows = negatives_seen, applications = predicted_on_unlabeled, non_applications = decision_rows - applications. All have unknown RF truth. sample_rows/sample_applications/sample_non_applications describe the retained reservoir only, never a full-population estimate.",
		"unlabeled_decisions.application_percent": "100 * whole-population applications / decision_rows, rounded to two decimals; null when no unlabeled decision rows were evaluated. This is application frequency, not false-correction rate.",
		"rf_truth": "Eligible genuine RF-error events, scored genuine RF-error events, RF precision, and RF recall are unavailable (null); no independently adjudicated RF ground-truth input or complete RF-error denominator is present.",
	}
}

// evaluationPercent distinguishes an undefined denominator from measured zero.
func evaluationPercent(numerator, denominator int64) *float64 {
	if denominator == 0 {
		return nil
	}
	percent := round2(float64(numerator) * 100 / float64(denominator))
	return &percent
}

func formatEvaluationSummary(report evalReport) string {
	agreement := report.ExternalLabelAgreement
	denominators := report.EvaluationDenominators
	unlabeled := report.UnlabeledDecisions
	text := fmt.Sprintf(
		`Evaluation contract v%d (external-label agreement):
  label_events_scored=%d matched_label_events=%d unmatched_label_events=%d
  applied_predictions_at_labeled_keys=%d extra_predictions_after_legacy_pairing=%d
  agreement_percent=%s
  eligible_replay_candidate_rows=%d primary_decision_rows_evaluated=%d candidate_rows_not_evaluated=%d
  decision_rows_at_external_label_keys=%d decision_rows_without_external_label_keys=%d
  external_label_key_coverage_percent=%s
  external_label_events_with_replay_rows=unavailable
  unlabeled_decision_rows=%d applications=%d non_applications=%d application_percent=%s
  unlabeled_sample_rows=%d sample_applications=%d sample_non_applications=%d
  RF precision, recall, eligible error events, and scored error events: unavailable
`,
		report.EvaluationContractVersion,
		agreement.LabelEventsScored, agreement.MatchedLabelEvents, agreement.UnmatchedLabelEvents,
		agreement.AppliedPredictionsAtLabeledKeys, agreement.ExtraPredictionsAfterLegacyPairing,
		formatEvaluationPercent(agreement.AgreementPercent),
		denominators.EligibleReplayCandidateRows, denominators.PrimaryDecisionRowsEvaluated, denominators.CandidateRowsNotEvaluated,
		denominators.DecisionRowsAtExternalLabelKeys, denominators.DecisionRowsWithoutExternalLabelKeys,
		formatEvaluationPercent(denominators.ExternalLabelKeyCoveragePercent),
		unlabeled.DecisionRows, unlabeled.Applications, unlabeled.NonApplications, formatEvaluationPercent(unlabeled.ApplicationPercent),
		unlabeled.SampleRows, unlabeled.SampleApplications, unlabeled.SampleNonApplications,
	)
	for _, warning := range report.Warnings {
		text += "  Warning: " + warning + "\n"
	}
	return text
}

func formatEvaluationPercent(percent *float64) string {
	if percent == nil {
		return "unavailable (zero denominator)"
	}
	return fmt.Sprintf("%.2f", *percent)
}
