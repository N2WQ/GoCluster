// File role: Stream terminal study observations and bound pending context.
// The sole extra pending map is bounded by temporal MaxPending+1:
// each held spot already has an existing bounded owner, plus one synchronous
// current row. Retirement deletes context before writing its terminal record.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"dxcluster/cty"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

type studyDecision struct {
	RawID                 string                `json:"raw_id"`
	RawSHA256             string                `json:"raw_sha256"`
	EpisodeID             string                `json:"episode_id"`
	Ordinal               int64                 `json:"ordinal"`
	Raw                   rbnHistoryRow         `json:"raw"`
	ReceiverFamily        string                `json:"receiver_family"`
	ReceiverLocation      studyReceiverLocation `json:"receiver_location"`
	SubjectIdentity       string                `json:"subject_identity"`
	FinalIdentity         string                `json:"final_identity"`
	FinalObserved         string                `json:"final_observed"`
	ProposedIdentity      string                `json:"proposed_identity,omitempty"`
	Status                string                `json:"status"`
	Reason                string                `json:"reason"`
	Held                  bool                  `json:"held"`
	TemporalAction        string                `json:"temporal_action,omitempty"`
	DecisionAtUTC         time.Time             `json:"decision_at_utc"`
	DecisionAfterCutoff   bool                  `json:"decision_after_cutoff"`
	Applied               bool                  `json:"applied"`
	IdentityCorrection    bool                  `json:"identity_correction"`
	PlainDistance         int                   `json:"plain_distance"`
	ConfiguredDistance    int                   `json:"configured_distance"`
	DistanceModelCW       string                `json:"distance_model_cw"`
	DistanceModelRTTY     string                `json:"distance_model_rtty"`
	GateEvaluated         bool                  `json:"gate_evaluated"`
	GateAllow             bool                  `json:"gate_allow"`
	WinnerSupport         int                   `json:"winner_support"`
	SubjectSupport        int                   `json:"subject_support"`
	TotalReporters        int                   `json:"total_reporters"`
	WinnerWeightedMilli   int                   `json:"winner_weighted_milli"`
	SubjectWeightedMilli  int                   `json:"subject_weighted_milli"`
	FocalSNRPresent       bool                  `json:"focal_snr_present"`
	SNRCoverageScope      string                `json:"snr_coverage_scope"`
	StabilizerProxyDelay  bool                  `json:"stabilizer_proxy_delay"`
	StabilizerProxyReason string                `json:"stabilizer_proxy_reason,omitempty"`
	R1                    studyReference        `json:"r1"`
	R2                    studyReference        `json:"r2"`
	R2Frozen              studyReference        `json:"r2_frozen"`
}

type studyReport struct {
	SchemaVersion              int                      `json:"schema_version"`
	InputScenario              studyInputScenario       `json:"input_scenario"`
	InputCoverage              studyInputCoverage       `json:"input_coverage"`
	Complete                   bool                     `json:"complete"`
	PlanSHA256                 string                   `json:"plan_sha256"`
	ReferenceVersion           string                   `json:"reference_version"`
	Arm                        string                   `json:"arm"`
	Candidate                  string                   `json:"candidate"`
	CandidateSHA256            string                   `json:"candidate_sha256"`
	DateUTC                    string                   `json:"date_utc"`
	Phase                      string                   `json:"phase"`
	SourceRevision             string                   `json:"source_revision"`
	ExecutableSHA256           string                   `json:"executable_sha256"`
	AssetsSHA256               string                   `json:"assets_sha256"`
	ConfigFilesSHA256          string                   `json:"config_files_sha256"`
	CallCorrectionSHA256       string                   `json:"call_correction_sha256"`
	PipelineWithoutBayesSHA256 string                   `json:"pipeline_without_bayes_sha256"`
	MaskSchemaPresent          bool                     `json:"mask_schema_present"`
	Decisions                  studyFile                `json:"decisions"`
	RawRows                    int                      `json:"raw_rows"`
	FocalRows                  int                      `json:"focal_rows"`
	TerminalRows               int                      `json:"terminal_rows"`
	PendingHighWater           int                      `json:"pending_high_water"`
	PendingBound               int                      `json:"pending_bound"`
	MaxRows                    int                      `json:"max_rows"`
	MaxCSVBytes                uint64                   `json:"max_csv_bytes"`
	WarmupSeconds              int                      `json:"warmup_seconds"`
	References                 studyReferenceCoverage   `json:"reference_coverage"`
	SampledMaxHeapAllocBytes   uint64                   `json:"sampled_max_heap_alloc_bytes"`
	HeapSampling               string                   `json:"heap_sampling"`
	LicenseInputs              replayLicenseInputs      `json:"license_inputs"`
	Population                 string                   `json:"population"`
	LegacyStatistics           string                   `json:"legacy_statistics"`
	ClaimLimits                []string                 `json:"claim_limits"`
	Diagnostics                *studyDiagnosticManifest `json:"diagnostics,omitempty"`
	Experimental               *studyMethodManifest     `json:"experimental,omitempty"`
	OrdinaryGuard              bool                     `json:"ordinary_guard,omitempty"`
}

type studyReferenceCoverage struct {
	Denominator              int            `json:"all_focal_rows"`
	R1Strong                 int            `json:"r1_strong"`
	R2Strong                 int            `json:"r2_strong"`
	R1Unknown                map[string]int `json:"r1_unknown_reasons"`
	R2Unknown                map[string]int `json:"r2_unknown_reasons"`
	StrongLabelDisagreements int            `json:"strong_label_disagreements"`
}

type studyObserver struct {
	day           *studyRawDay
	pending       map[*spot.Spot]*studyDecision
	pendingCap    int
	highWater     int
	expected      int
	terminal      int
	checked       int64
	lastReasons   map[string]uint64
	file          studyAuditSink
	encoder       *json.Encoder
	path          string
	err           error
	maxHeap       uint64
	coverage      studyReferenceCoverage
	ctyDB         *cty.CTYDatabase
	inputCoverage studyInputCoverage
	diagnostics   *studyDiagnosticRun
}

func newStudyObserver(day *studyRawDay, r *replayRunner, path string) (*studyObserver, error) {
	if err := studyValidateCTYImage(r.ctyDB); err != nil {
		return nil, err
	}
	file, err := r.openReplayArtifact(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	bound := 1
	if r.temporalDecoder != nil && r.temporalDecoder.Enabled() {
		bound += r.cfg.CallCorrection.TemporalDecoder.MaxPending
	}
	observer := &studyObserver{day: day, pending: make(map[*spot.Spot]*studyDecision),
		pendingCap: bound, lastReasons: make(map[string]uint64), file: file, encoder: json.NewEncoder(file), path: path, ctyDB: r.ctyDB, diagnostics: r.diagnostics}
	observer.coverage.R1Unknown, observer.coverage.R2Unknown = make(map[string]int), make(map[string]int)
	for i := range day.Rows {
		if studyFocalRow(day.Rows[i]) {
			observer.expected++
		}
	}
	observer.sampleHeap()
	return observer, nil
}

func (o *studyObserver) checkRaw(row rbnHistoryRow, valid bool, ordinal int64) error {
	if o.err != nil {
		return o.err
	}
	if ordinal != o.checked+1 || ordinal > int64(len(o.day.Rows)) {
		return errors.New("study replay raw ordinal differs from frozen input")
	}
	expected := o.day.Rows[ordinal-1]
	if expected.Valid != valid || expected.Row != row {
		return fmt.Errorf("study row/SNR binding differs at raw ordinal %d", ordinal)
	}
	o.checked = ordinal
	if ordinal%8192 == 0 {
		o.sampleHeap()
	}
	return nil
}

func (o *studyObserver) predictor(receiver string) bool {
	return studyCohortA(o.day.family(receiver))
}

func (o *studyObserver) begin(s *spot.Spot, ordinal int64) error {
	if ordinal < 1 || ordinal > int64(len(o.day.Rows)) || len(o.pending) >= o.pendingCap {
		return errors.New("study pending row context exceeds bounded temporal ownership")
	}
	raw := o.day.Rows[ordinal-1]
	if !studyFocalRow(raw) {
		return errors.New("study predictor observed a row outside the A-only focal population")
	}
	if _, duplicate := o.pending[s]; duplicate {
		return errors.New("study duplicate pending spot ownership")
	}
	r2 := o.day.reference(raw, true)
	location := studyDeriveReceiverLocation(o.ctyDB, raw.Row.Spotter)
	studyApplyReceiverLocation(s, location)
	o.inputCoverage.observe(location)
	o.pending[s] = &studyDecision{
		RawID: fmt.Sprintf("%s:%d", o.day.InputSHA256, ordinal), RawSHA256: hashAnyStable(raw.Row),
		EpisodeID: raw.Episode, Ordinal: ordinal, Raw: raw.Row, ReceiverFamily: raw.Family, ReceiverLocation: location,
		SubjectIdentity: spot.CorrectionVoteKey(raw.Row.DXCall),
		R1:              o.day.reference(raw, false), R2: r2, R2Frozen: r2,
		FocalSNRPresent: true, SNRCoverageScope: "focal parsed input only; candidate measurement coverage is not inferred"}
	o.highWater = max(o.highWater, len(o.pending))
	if diagnostic := o.diagnostics; diagnostic != nil {
		diagnostic.seen(o.pending[s])
	}
	return nil
}

func (o *studyObserver) noteTemporal(s *spot.Spot, action string) {
	if pending := o.pending[s]; pending != nil {
		pending.Held = pending.Held || action == "held"
		pending.TemporalAction = action
	}
}

func (o *studyObserver) noteStabilizer(s *spot.Spot, decision correctionflow.StabilizerDelayDecision) {
	if pending := o.pending[s]; pending != nil {
		pending.StabilizerProxyDelay = decision.ShouldDelay
		pending.StabilizerProxyReason = decision.Reason.String()
	}
}

func (o *studyObserver) finish(s *spot.Spot, outcome replayResolverApplyOutcome, now time.Time, status string, r *replayRunner) {
	o.finishWithDiagnostics(s, outcome, now, status, r, nil)
}

func (o *studyObserver) finishWithDiagnostics(s *spot.Spot, outcome replayResolverApplyOutcome, now time.Time, status string, r *replayRunner, execution *studyDiagnosticExecution) {
	pending, ok := o.pending[s]
	if !ok {
		o.err = errors.Join(o.err, errors.New("study terminal outcome has no original row context"))
		if r.diagnostics != nil {
			r.diagnostics.latch(o.err)
		}
		return
	}
	delete(o.pending, s)
	pending.FinalObserved = normalizedDXCall(s)
	pending.FinalIdentity = spot.CorrectionVoteKey(pending.FinalObserved)
	pending.ProposedIdentity = spot.CorrectionVoteKey(outcome.Selection.Snapshot.Winner)
	if outcome.methodWinner != "" {
		pending.ProposedIdentity = spot.CorrectionVoteKey(outcome.methodWinner)
	}
	pending.Status, pending.DecisionAtUTC, pending.Applied = status, now.UTC(), outcome.Applied
	pending.IdentityCorrection = outcome.Applied && pending.SubjectIdentity != pending.FinalIdentity
	pending.DecisionAfterCutoff = now.After(pending.Raw.Time.Add(studyDecisionSeconds * time.Second))
	if pending.DecisionAfterCutoff {
		pending.R2 = studyReference{Reason: "decision_after_cutoff"}
	}
	pending.PlainDistance = spot.CallDistance(pending.SubjectIdentity, pending.FinalIdentity, pending.Raw.Mode, "plain", "plain")
	pending.ConfiguredDistance = spot.CallDistance(pending.SubjectIdentity, pending.FinalIdentity, pending.Raw.Mode,
		r.cfg.CallCorrection.DistanceModelCW, r.cfg.CallCorrection.DistanceModelRTTY)
	pending.DistanceModelCW, pending.DistanceModelRTTY = r.cfg.CallCorrection.DistanceModelCW, r.cfg.CallCorrection.DistanceModelRTTY
	pending.GateEvaluated, pending.GateAllow = outcome.GateEvaluated, outcome.Gate.Allow
	pending.WinnerSupport, pending.SubjectSupport = outcome.Gate.WinnerSupport, outcome.Gate.SubjectSupport
	pending.WinnerWeightedMilli, pending.SubjectWeightedMilli = outcome.Gate.WinnerWeightedSupportMilli, outcome.Gate.SubjectWeightedSupportMilli
	pending.TotalReporters = outcome.Selection.Snapshot.TotalReporters
	pending.Reason = o.terminalReason(r, outcome, pending.TemporalAction)
	if status != "output" {
		pending.Reason = status + ":" + pending.Reason
	}
	o.coverage.Denominator++
	if pending.R1.Strong {
		o.coverage.R1Strong++
	} else {
		o.coverage.R1Unknown[pending.R1.Reason]++
	}
	if pending.R2.Strong {
		o.coverage.R2Strong++
	} else {
		o.coverage.R2Unknown[pending.R2.Reason]++
	}
	if pending.R1.Strong && pending.R2.Strong && pending.R1.Identity != pending.R2.Identity {
		o.coverage.StrongLabelDisagreements++
	}
	if r.diagnostics != nil {
		o.terminal++
		r.diagnostics.terminal(pending, s, outcome, execution)
		if r.diagnostics.owner.Err() == nil {
			data, err := r.diagnostics.owner.jsonBytes(pending, studyAuditRowBytes-1)
			if err == nil {
				data = append(data, '\n')
				_, err = o.file.Write(data)
			}
			if err != nil {
				o.err = errors.Join(o.err, err)
			}
		} else {
			o.err = errors.Join(o.err, r.diagnostics.owner.Err())
		}
		return
	}
	if owner := r.diagnosticArtifacts(); owner != nil {
		data, err := owner.jsonBytes(pending, studyAuditRowBytes-1)
		if err == nil {
			data = append(data, '\n')
			_, err = o.file.Write(data)
		}
		if err != nil {
			o.err = errors.Join(o.err, err)
		}
		o.terminal++
		return
	}
	if err := o.encoder.Encode(pending); err != nil {
		o.err = errors.Join(o.err, err)
	}
	o.terminal++
}

func (o *studyObserver) terminalReason(r *replayRunner, outcome replayResolverApplyOutcome, action string) string {
	current := r.tracker.CorrectionDecisionReasons()
	if outcome.methodReason != "" {
		// The receipt associates the ordinary rejection and optional rescue with
		// this same immutable input. Do not mistake its two substages for two
		// unassociated ordinary decisions, or relabel an accepted rescue rejected.
		o.lastReasons = current
		return outcome.methodReason
	}
	if len(current) > 128 {
		o.err = errors.Join(o.err, errors.New("study decision reason cardinality exceeds bounded vocabulary"))
		return "unavailable_reason"
	}
	reason := ""
	for key, count := range current {
		if count > o.lastReasons[key] {
			if reason != "" {
				o.err = errors.Join(o.err, errors.New("study terminal reason has multiple unassociated tracker increments"))
			}
			reason = key
		}
	}
	o.lastReasons = current
	if reason != "" {
		return reason
	}
	if action != "" {
		return "temporal_" + action
	}
	if outcome.GateEvaluated {
		return outcome.Gate.Reason
	}
	return "no_gate_evaluation"
}

func (o *studyObserver) sampleHeap() {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	o.maxHeap = max(o.maxHeap, stats.HeapAlloc)
}

func (o *studyObserver) complete() error {
	o.sampleHeap()
	if err := o.err; err != nil {
		return err
	}
	if o.checked != int64(len(o.day.Rows)) || len(o.pending) != 0 || o.expected != o.terminal || !o.inputCoverage.complete(o.expected) {
		return fmt.Errorf("incomplete study: raw=%d/%d terminal=%d/%d pending=%d",
			o.checked, len(o.day.Rows), o.terminal, o.expected, len(o.pending))
	}
	return nil
}

func (o *studyObserver) close() error {
	if o.file == nil {
		return nil
	}
	err := errors.Join(o.file.Sync(), o.file.Close())
	o.file = nil
	return err
}

func newStudyReport(p studyPlan, digest string, req studyCLIRequest, arm studyArm, candidate studyCandidate,
	in studyInput, o *studyObserver, maskSchema bool) studyReport {
	return studyReport{SchemaVersion: studySchemaVersion, InputScenario: p.InputScenario, InputCoverage: o.inputCoverage, Complete: true, PlanSHA256: digest,
		ReferenceVersion: p.ReferenceVersion, Arm: arm.Name, Candidate: candidate.ID, CandidateSHA256: hashAnyStable(candidate),
		DateUTC: in.DateUTC, Phase: req.Phase, SourceRevision: arm.SourceRevision, ExecutableSHA256: strings.ToLower(arm.ExecutableSHA256),
		AssetsSHA256: studyAssetDigest(arm.Assets), ConfigFilesSHA256: hashAnyStable(arm.ConfigFiles), MaskSchemaPresent: maskSchema,
		RawRows: len(o.day.Rows), FocalRows: o.expected, TerminalRows: o.terminal, PendingHighWater: o.highWater,
		PendingBound: o.pendingCap, MaxRows: in.MaxRows, MaxCSVBytes: in.MaxCSVBytes, WarmupSeconds: p.WarmupSeconds,
		References:               o.coverage,
		SampledMaxHeapAllocBytes: o.maxHeap, HeapSampling: "Go HeapAlloc at start/end and every 8192 raw rows; sampled, excludes RSS and unsampled peaks",
		Population:       "A-only prediction/activity/history/learning; each UTC day starts with empty history",
		LegacyStatistics: "manifest.json and gates.json retain deprecated legacy statistics; they are not independent study accuracy oracles",
		ClaimLimits: []string{"Archive reference agreement is not RF truth or calibrated probability",
			"Receiver locations are fixed CTY representative proxies, not reconstructed historical sites or reported grids; common imputation does not cancel nonlinear eligibility errors",
			"Receiver/date/episode dependence and site/decoder independence are unverified",
			"Largest-ZIP selection and model-training exposure can bias the population",
			"Empty daily A-history does not reconstruct the full historical horizon or production steady state",
			"Replay stabilizer is a delay proxy; output is not actual network delivery",
			"Full-input shadow/capacity results cannot inherit A-only reference accuracy",
			"Freeze/exposure declarations do not independently prove human holdout blinding"}}
}

func studyDecisionBinding(d studyDecision, archiveSHA string) bool {
	return d.RawID == fmt.Sprintf("%s:%d", strings.ToLower(archiveSHA), d.Ordinal) &&
		studyReceiverLocationBinding(d.ReceiverLocation, d.Raw.Spotter) &&
		d.RawSHA256 == hashAnyStable(d.Raw) && d.SubjectIdentity == spot.CorrectionVoteKey(d.Raw.DXCall) &&
		d.FinalIdentity == spot.CorrectionVoteKey(d.FinalObserved) &&
		d.IdentityCorrection == (d.Applied && d.SubjectIdentity != d.FinalIdentity) &&
		(d.Status == "output" || d.Status == "suppressed" || d.Status == "license_rejected")
}
