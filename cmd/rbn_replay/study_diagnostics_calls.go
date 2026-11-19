// File role: Observe only actual replay calls, after their existing guards.
// Captures are optional, selected-only local values. Errors enter the separate
// artifact latch; callers finish ordinary decisions/Step/retirement first.
package main

import (
	"time"

	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

type studyDiagnosticSelectorCall struct {
	Called bool
	Data   correctionflow.ResolverSelectionDiagnostics
	Err    error
}

type studyDiagnosticExecution struct {
	ConfirmChecked               bool   `json:"confirm_checked"`
	ConfirmSwitched              bool   `json:"confirm_switched"`
	FinalLicenseChecked          bool   `json:"final_license_checked"`
	FinalLicenseRejected         bool   `json:"final_license_rejected"`
	StabilizerEligibilityChecked bool   `json:"stabilizer_eligibility_checked"`
	StabilizerEligible           bool   `json:"stabilizer_eligible"`
	StabilizerChecked            bool   `json:"stabilizer_checked"`
	StabilizerDelay              bool   `json:"stabilizer_delay"`
	StabilizerReason             string `json:"stabilizer_reason"`
	OuterLearningHelperReached   bool   `json:"outer_learning_helper_reached"`
}

type studyDiagnosticExecutionRecord struct {
	SchemaVersion       int                            `json:"schema_version"`
	Binding             studyDiagnosticRawBinding      `json:"binding"`
	Sequence            int                            `json:"sequence"`
	Stage               string                         `json:"stage"`
	Execution           studyDiagnosticExecution       `json:"execution"`
	Calls               studyDiagnosticCallCounts      `json:"actual_calls"`
	Status              string                         `json:"status"`
	Reason              string                         `json:"reason"`
	FinalObserved       string                         `json:"final_observed"`
	FinalIdentity       string                         `json:"final_identity"`
	DecisionAtUTC       time.Time                      `json:"decision_at_utc"`
	Applied             bool                           `json:"applied"`
	Suppress            bool                           `json:"suppress"`
	Temporal            bool                           `json:"temporal"`
	GateEvaluated       bool                           `json:"gate_evaluated"`
	Confidence          string                         `json:"confidence_glyph"`
	ConfidencePercent   int                            `json:"confidence_percent"`
	ConfidencePercentOK bool                           `json:"confidence_percent_present"`
	Gate                spot.ResolverPrimaryGateResult `json:"gate"`
}

func (r *replayRunner) diagnosticSelected(s *spot.Spot) (studyDiagnosticRawBinding, bool) {
	if r.diagnostics == nil || r.study == nil {
		return studyDiagnosticRawBinding{}, false
	}
	pending := r.study.pending[s]
	if pending == nil {
		return studyDiagnosticRawBinding{}, false
	}
	state := r.diagnostics.selected[pending.RawID]
	if state == nil {
		return studyDiagnosticRawBinding{}, false
	}
	return state.Binding, true
}

func (r *replayRunner) applyResolverOutcome(s *spot.Spot, evidence spot.ResolverEvidence, hasEvidence bool, now time.Time) replayResolverApplyOutcome {
	binding, selected := r.diagnosticSelected(s)
	if !selected || r.diagnostics.owner.Err() != nil {
		ordinary := maybeApplyResolverCorrectionReplay(s, r.resolver, evidence, hasEvidence, r.cfg.CallCorrection, r.ctyDB, r.tracker, r.adaptiveMinReports, r.recentBandStore, now)
		return r.applyMethodRescue(s, evidence, hasEvidence, now, ordinary, "", nil)
	}
	var capture studyDiagnosticSelectorCall
	outcome := maybeApplyResolverCorrectionReplayDiagnosticCore(s, r.resolver, evidence, hasEvidence, r.cfg.CallCorrection, r.ctyDB, r.tracker, r.adaptiveMinReports, r.recentBandStore, now, nil, &capture)
	if capture.Called {
		r.diagnostics.capture(binding, "pipeline_selection", &capture.Data, capture.Err)
	}
	return outcome
}

func (r *replayRunner) selectTemporalInitial(s *spot.Spot, key spot.ResolverSignalKey, subject string) correctionflow.ResolverPrimarySelection {
	binding, selected := r.diagnosticSelected(s)
	if !selected || r.diagnostics.owner.Err() != nil {
		return correctionflow.SelectResolverPrimarySnapshotForCall(r.resolver, key, r.cfg.CallCorrection, subject)
	}
	var capture correctionflow.ResolverSelectionDiagnostics
	selection, err := correctionflow.SelectResolverPrimarySnapshotForCallWithDiagnostics(r.resolver, key, r.cfg.CallCorrection, subject, time.Time{}, &capture)
	r.diagnostics.capture(binding, "temporal_initial_selection", &capture, err)
	return selection
}

func (r *replayRunner) evaluateTemporal(id uint64, s *spot.Spot, now time.Time, force bool) correctionflow.TemporalDecision {
	binding, selected := r.diagnosticSelected(s)
	if !selected || r.diagnostics.owner.Err() != nil {
		return r.temporalDecoder.Evaluate(id, now, force)
	}
	var capture correctionflow.TemporalEvaluateDiagnostics
	decision, err := r.temporalDecoder.EvaluateWithDiagnostics(id, now, force, &capture)
	r.diagnostics.capture(binding, "temporal_evaluate", &capture, err)
	if err == nil && r.diagnostics.owner.Err() == nil {
		r.diagnostics.latch(r.diagnostics.wanted.addEvaluate(&capture))
	}
	return decision
}

func (r *replayRunner) selectTemporalRelease(s *spot.Spot, key spot.ResolverSignalKey, decision correctionflow.TemporalDecision, now time.Time) correctionflow.ResolverPrimarySelection {
	binding, selected := r.diagnosticSelected(s)
	if !selected || r.diagnostics.owner.Err() != nil {
		return correctionflow.SelectTemporalReleaseSnapshot(r.resolver, key, r.cfg.CallCorrection, normalizedDXCall(s), decision, now)
	}
	var capture correctionflow.TemporalReleaseDiagnostics
	selection, err := correctionflow.SelectTemporalReleaseSnapshotWithDiagnostics(r.resolver, key, r.cfg.CallCorrection, normalizedDXCall(s), decision, now, &capture)
	r.diagnostics.capture(binding, "temporal_release_selection", &capture, err)
	return selection
}

func (r *replayRunner) diagnosticBoundary() error {
	if owner := r.diagnosticArtifacts(); owner != nil {
		return owner.Err()
	}
	return nil
}
