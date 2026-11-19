// File role: Own one exposed diagnostic invocation from setup to COMPLETE-last.
// One synchronous replay goroutine owns selected counts, four fixed streams,
// and bounded wanted IDs. Existing study pending entries remain the raw owner.
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

type studyDiagnosticPrepared struct {
	Selection studyDiagnosticSelection
	File      studyFile
	// Private request-local filesystem seam for lifecycle failure fixtures.
	// CLI preparation leaves it nil; it cannot change model/capture inputs.
	artifactOps *studyDiagnosticArtifactOps
}

//nolint:nilnil // A nil prepared value with no error means optional diagnostics are disabled.
func prepareStudyDiagnostics(req studyCLIRequest) (*studyDiagnosticPrepared, error) {
	if err := studyDiagnosticValidateRequest(req); err != nil {
		return nil, err
	}
	if req.DiagnosticSelection == "" {
		return nil, nil
	}
	selection, digest, err := loadStudyDiagnosticSelection(req.DiagnosticSelection, req.DiagnosticFreeze)
	if err != nil {
		return nil, err
	}
	path, err := filepath.Abs(req.DiagnosticSelection)
	if err != nil {
		return nil, err
	}
	return &studyDiagnosticPrepared{Selection: selection, File: studyFile{Path: path, SHA256: digest}}, nil
}

type studyDiagnosticSelectedState struct {
	Binding       studyDiagnosticRawBinding `json:"binding"`
	Seen          int                       `json:"seen"`
	Terminal      int                       `json:"terminal"`
	TraceCaptures int                       `json:"trace_captures"`
	Calls         studyDiagnosticCallCounts `json:"actual_calls"`
}

type studyDiagnosticCallCounts struct {
	InitialSelector  int `json:"initial_selector"`
	PipelineSelector int `json:"pipeline_selector"`
	Evaluate         int `json:"evaluate"`
	Release          int `json:"release"`
}

type studyDiagnosticScratch struct {
	Applicable        bool                               `json:"applicable"`
	Backend           string                             `json:"backend"`
	Path              string                             `json:"path"`
	MaxKeys           int                                `json:"max_keys"`
	MaxSpottersPerKey int                                `json:"max_spotters_per_key"`
	Measurement       *studyDiagnosticScratchMeasurement `json:"measurement,omitempty"`
	ClaimLimit        string                             `json:"claim_limit"`
}

// The completion marker points to a separately closed/hash-bound finite report.
// Presence also prevents this exposed diagnostic run becoming qualification.
type studyDiagnosticManifest struct {
	SchemaVersion    int       `json:"schema_version"`
	Exposed          bool      `json:"exposed"`
	NonQualification bool      `json:"non_qualification"`
	Report           studyFile `json:"report"`
}

type studyDiagnosticRun struct {
	owner                                  *studyDiagnosticArtifacts
	prepared                               *studyDiagnosticPrepared
	archive                                studyFile
	date                                   string
	selected                               map[string]*studyDiagnosticSelectedState
	traceFile, executionFile, bindingsFile *studyDiagnosticArtifactFile
	trace                                  *studyDiagnosticTraceWriter
	journal                                *studyDiagnosticJournal
	wanted                                 studyDiagnosticWanted
	attempts                               uint64
	files                                  map[string]*studyDiagnosticFileState
	copies                                 []studyDiagnosticCopyRecord
	licenseRoot                            string
	scratch                                studyDiagnosticScratch
}

func newStudyDiagnosticRun(output string, prepared *studyDiagnosticPrepared) (*studyDiagnosticRun, error) {
	if prepared == nil {
		return nil, errors.New("diagnostic run requires its frozen prepared selection")
	}
	var owner *studyDiagnosticArtifacts
	var err error
	if prepared != nil && prepared.artifactOps != nil {
		owner, err = newStudyDiagnosticArtifactsWithOps(output, studyAuditOutputBytes, studyAuditReserveBytes, *prepared.artifactOps)
	} else {
		owner, err = newStudyDiagnosticArtifacts(output)
	}
	if err != nil {
		return nil, err
	}
	d := &studyDiagnosticRun{owner: owner, prepared: prepared, files: make(map[string]*studyDiagnosticFileState), scratch: studyDiagnosticScratch{Backend: "not_constructed", ClaimLimit: "Post-existing-cleanup logical lengths only; inherited database Close errors remain unobserved; no peak, allocated-block or durability claim"}}
	d.trackFiles()
	return d, nil
}

// Hooks have no error return that could select a different ordinary outcome.
// They retain only the same owner's first cause, never replace or clear it.
func (d *studyDiagnosticRun) latch(err error) {
	if d != nil && err != nil && d.owner.first == nil {
		d.owner.first = err
	}
}

func (d *studyDiagnosticRun) bindDay(input studyInput, day *studyRawDay, startNextID uint64) error {
	bindings, err := d.prepared.Selection.bindDay(input, day)
	if err != nil {
		return d.owner.latch(err)
	}
	d.date, d.archive = day.Start.Format("20060102"), input.Archive
	d.selected = make(map[string]*studyDiagnosticSelectedState, len(bindings))
	for _, binding := range bindings {
		d.selected[binding.RawID] = &studyDiagnosticSelectedState{Binding: binding}
	}
	for _, stream := range []struct {
		name        string
		destination **studyDiagnosticArtifactFile
	}{{"diagnostic-trace.jsonl", &d.traceFile}, {"diagnostic-execution.jsonl", &d.executionFile}, {"diagnostic-temporal-bindings.jsonl", &d.bindingsFile}} {
		*stream.destination, err = d.owner.openFile(stream.name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600, false)
		if err != nil {
			return err
		}
	}
	d.trace, err = newStudyDiagnosticTraceWriter(d.owner, d.traceFile, bindings)
	if err != nil {
		return err
	}
	d.journal, err = newStudyDiagnosticJournal(d.owner, "diagnostic-attempts.jsonl", day, startNextID)
	return err
}

func (d *studyDiagnosticRun) binding(pending *studyDecision) studyDiagnosticRawBinding {
	return studyDiagnosticRawBinding{DateUTC: d.date, RawID: pending.RawID, Ordinal: pending.Ordinal, RawSHA256: pending.RawSHA256}
}

func (d *studyDiagnosticRun) seen(pending *studyDecision) {
	if state := d.selected[pending.RawID]; state != nil {
		state.Seen++
		if state.Seen != 1 || state.Binding != d.binding(pending) {
			d.latch(errors.New("selected diagnostic input seen more than once or with conflicting raw binding"))
		}
	}
}

func (d *studyDiagnosticRun) capture(binding studyDiagnosticRawBinding, stage string, capture any, captureErr error) {
	state := d.selected[binding.RawID]
	if state == nil || state.Binding != binding {
		d.latch(errors.New("actual diagnostic call has no selected immutable binding"))
		return
	}
	switch stage {
	case "temporal_initial_selection":
		state.Calls.InitialSelector++
	case "pipeline_selection":
		state.Calls.PipelineSelector++
	case "temporal_evaluate":
		state.Calls.Evaluate++
	case "temporal_release_selection":
		state.Calls.Release++
	default:
		d.latch(errors.New("unknown actual diagnostic call stage"))
		return
	}
	if captureErr != nil {
		d.latch(captureErr)
		return
	}
	if d.owner.Err() != nil {
		return
	}
	// Reserve exactly one of the64 detailed records for terminal execution.
	if state == nil || state.Seen != 1 || state.Terminal != 0 || state.Binding != binding || state.TraceCaptures >= 63 {
		d.latch(errors.New("selected diagnostic detailed-record count/order exceeds63 traces plus one terminal"))
		return
	}
	state.TraceCaptures++
	d.latch(d.trace.Capture(binding, stage, capture))
}

func (d *studyDiagnosticRun) observeAttempt(observer *studyObserver, s *spot.Spot, id uint64, key spot.ResolverSignalKey, nowTime time.Time, accepted bool, reason string) {
	d.attempts++
	if observer == nil || d.journal == nil {
		d.latch(errors.New("actual Observe attempt lacks its initialized study journal"))
		return
	}
	pending := observer.pending[s]
	if pending == nil {
		d.latch(errors.New("actual Observe attempt has no original study pending row"))
		return
	}
	d.latch(d.journal.Record(studyDiagnosticAttempt{ID: id, Binding: d.binding(pending), Key: studyDiagnosticKey(key), ObservedAt: nowTime, Accepted: accepted, Reason: reason}))
}

func studyDiagnosticKey(key spot.ResolverSignalKey) correctionflow.DiagnosticKey {
	return correctionflow.DiagnosticKey{Band: key.Band, Mode: key.Mode, Bucket: key.Bucket, ToleranceHz: key.ToleranceHz}
}

func (d *studyDiagnosticRun) terminal(pending *studyDecision, s *spot.Spot, outcome replayResolverApplyOutcome, execution *studyDiagnosticExecution) {
	state := d.selected[pending.RawID]
	if state == nil {
		return
	}
	state.Terminal++
	if state.Seen != 1 || state.Terminal != 1 || state.Binding != d.binding(pending) || state.TraceCaptures+state.Terminal > 64 {
		d.latch(errors.New("selected diagnostic terminal count/binding is incomplete or duplicated"))
		return
	}
	if d.owner.Err() != nil {
		return
	}
	if execution == nil {
		d.latch(errors.New("selected diagnostic terminal has no actual execution capture"))
		return
	}
	record := studyDiagnosticExecutionRecord{SchemaVersion: 1, Binding: state.Binding, Sequence: state.TraceCaptures + 1, Stage: "terminal_execution", Execution: *execution, Calls: state.Calls,
		Status: pending.Status, Reason: pending.Reason, FinalObserved: pending.FinalObserved, FinalIdentity: pending.FinalIdentity, DecisionAtUTC: pending.DecisionAtUTC,
		Applied: outcome.Applied, Suppress: outcome.Suppress, Temporal: outcome.temporal, GateEvaluated: outcome.GateEvaluated, Gate: outcome.Gate,
		Confidence: s.Confidence, ConfidencePercent: s.ConfidencePercent, ConfidencePercentOK: s.ConfidencePercentOK}
	d.latch(d.owner.writeRow(d.executionFile, record))
}

func (d *studyDiagnosticRun) closeStreams() error {
	var failures []error
	for _, file := range []*studyDiagnosticArtifactFile{d.traceFile, d.executionFile, d.bindingsFile} {
		if file != nil && !file.closed {
			failures = append(failures, file.Sync(), file.Close())
		}
	}
	if d.journal != nil && !d.journal.finalized {
		failures = append(failures, d.journal.Close())
	}
	return errors.Join(failures...)
}

func (d *studyDiagnosticRun) beforeCleanup(licenseRoot string) error {
	if err := d.verifyCopies(false); err != nil {
		return err
	}
	if licenseRoot != "" {
		root, err := filepath.Abs(licenseRoot)
		if err != nil || !strings.EqualFold(filepath.Dir(root), d.owner.root) || !strings.HasPrefix(filepath.Base(root), ".license-inputs-") {
			return d.owner.latch(errors.Join(err, errors.New("invalid actual license cleanup root")))
		}
		d.licenseRoot = root
	}
	return nil
}

type studyDiagnosticRunReport struct {
	SchemaVersion          int                               `json:"schema_version"`
	Exposed                bool                              `json:"exposed"`
	NonQualification       bool                              `json:"non_qualification"`
	Selection              studyFile                         `json:"selection"`
	Archive                studyFile                         `json:"archive"`
	Selected               []studyDiagnosticSelectedState    `json:"selected"`
	AttemptStartNext       uint64                            `json:"attempt_start_next"`
	AttemptEndNext         uint64                            `json:"attempt_end_next"`
	AttemptCount           uint64                            `json:"attempt_count"`
	WantedEvents           int                               `json:"wanted_events"`
	Outputs                []studyDiagnosticOutputFile       `json:"closed_outputs"`
	Copies                 []studyDiagnosticCopyRecord       `json:"copies"`
	Accounting             studyDiagnosticArtifactAccounting `json:"accounting"`
	RetainedEvidence       studyDiagnosticScratchMeasurement `json:"retained_evidence"`
	Scratch                studyDiagnosticScratch            `json:"scratch"`
	DeletedCopyBytes       int64                             `json:"deleted_copy_bytes"`
	UnretainedChargedBytes int64                             `json:"unretained_charged_bytes"`
	InventoryStage         string                            `json:"inventory_stage"`
	ClaimLimits            []string                          `json:"claim_limits"`
}

// complete runs only after existing runner cleanup has returned successfully.
// It closes the journal before reading, then binds every closed prior output.
func (d *studyDiagnosticRun) complete(endNextID uint64) (*studyDiagnosticManifest, error) {
	if d.owner.Err() != nil {
		return nil, d.owner.Err()
	}
	if d.prepared == nil || d.journal == nil || d.trace == nil {
		return nil, d.owner.latch(errors.New("diagnostic capture streams or frozen selection were not initialized"))
	}
	if d.scratch.Backend == "not_constructed" {
		return nil, d.owner.latch(errors.New("diagnostic history backend construction was not observed"))
	}
	for _, state := range d.selected {
		if state.Seen != 1 || state.Terminal != 1 || d.trace == nil || d.trace.counts[state.Binding.RawID] != state.TraceCaptures {
			return nil, d.owner.latch(errors.New("diagnostic selection is missing an input or terminal"))
		}
	}
	if err := d.verifyCopies(true); err != nil {
		return nil, err
	}
	if d.scratch.Applicable {
		measurement, err := measureStudyDiagnosticScratch(d.scratch.Path)
		if err != nil {
			return nil, d.owner.latch(err)
		}
		d.scratch.Measurement = &measurement
	}
	if err := d.journal.Close(); err != nil {
		return nil, err
	}
	bindings, err := d.journal.Join(d.wanted, endNextID, d.attempts)
	if err != nil {
		return nil, err
	}
	for i := range bindings {
		if err := d.owner.writeRow(d.bindingsFile, bindings[i]); err != nil {
			return nil, err
		}
	}
	if err := d.closeStreams(); err != nil {
		return nil, err
	}
	if err := studyCheckFile(d.prepared.File); err != nil {
		return nil, d.owner.latch(err)
	}
	if err := studyCheckFile(d.archive); err != nil {
		return nil, d.owner.latch(err)
	}
	for i := range d.prepared.Selection.Days {
		if err := studyCheckFile(d.prepared.Selection.Days[i].SourceDecisions); err != nil {
			return nil, d.owner.latch(err)
		}
	}
	outputs, deleted, err := d.closedOutputs()
	if err != nil {
		return nil, err
	}
	measurement, err := measureStudyDiagnosticEvidence(d.owner.root, d.scratch.Path)
	if err != nil {
		return nil, d.owner.latch(err)
	}
	var retained int64
	for _, output := range outputs {
		retained += output.Bytes
	}
	if measurement.RegularFiles != uint64(len(outputs)) || measurement.RegularFileBytes != retained || retained+deleted > d.owner.quota.used {
		return nil, d.owner.latch(errors.New("diagnostic retained inventory does not reconcile with actual quota-owned outputs"))
	}
	accounting, err := d.owner.accounting("after_cleanup_join_and_closed_output_hashes_before_diagnostic_report_and_complete")
	if err != nil {
		return nil, err
	}
	states := make([]studyDiagnosticSelectedState, 0, len(d.selected))
	for _, state := range d.selected {
		states = append(states, *state)
	}
	sort.Slice(states, func(i, j int) bool { return states[i].Binding.Ordinal < states[j].Binding.Ordinal })
	report := studyDiagnosticRunReport{SchemaVersion: 1, Exposed: true, NonQualification: true, Selection: d.prepared.File, Archive: d.archive, Selected: states,
		AttemptStartNext: d.journal.start, AttemptEndNext: endNextID, AttemptCount: d.attempts, WantedEvents: len(bindings), Outputs: outputs, Copies: d.copies, Accounting: accounting, RetainedEvidence: measurement, Scratch: d.scratch, DeletedCopyBytes: deleted, UnretainedChargedBytes: accounting.ChargedBytes - retained - deleted,
		InventoryStage: "closed prior outputs only; this diagnostic report and complete.json are excluded from their own hashes/accounting; external final reconciliation includes both",
		ClaimLimits:    []string{"Exposed outcome-selected diagnostic evidence; not model training, qualification, accuracy, calibration or deployment approval", "Raw receiver originating archive IDs remain unavailable unless actually journaled at Observe; no value/time equality guesses", "Outer learning-helper reachability is not proof of an inner store call or committed learning", "Final logical lengths are not peak disk use, allocated blocks, or durability; inherited database Close errors are unobserved", "All scalar gates/outputs are actual call results; frozen source references never enter a prediction"}}
	path := filepath.Join(d.owner.root, "diagnostic-report.json")
	if err := d.owner.atomicJSON(path, report); err != nil {
		return nil, err
	}
	digest, err := studyHashFile(path)
	if err != nil {
		return nil, d.owner.latch(err)
	}
	return &studyDiagnosticManifest{SchemaVersion: 1, Exposed: true, NonQualification: true, Report: studyFile{Path: path, SHA256: digest}}, nil
}

func (d *studyDiagnosticRun) failure(err error) error {
	d.latch(err)
	message := err.Error()
	if len(message) > 1024 {
		message = message[:1024]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
	}
	return d.owner.writeFailed(struct {
		SchemaVersion int    `json:"schema_version"`
		Error         string `json:"error"`
	}{1, message})
}

func (d *studyDiagnosticRun) scratchBackend(backend, path string, maxKeys, maxSpotters int) {
	d.scratch.Backend, d.scratch.Path, d.scratch.MaxKeys, d.scratch.MaxSpottersPerKey = backend, path, maxKeys, maxSpotters
	d.scratch.Applicable = backend == "custom_scp_runtime"
	if d.scratch.Applicable {
		info, err := os.Lstat(path)
		if err != nil || info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !strings.EqualFold(filepath.Clean(path), filepath.Join(d.owner.root, studyDiagnosticScratchChild)) {
			d.latch(errors.Join(err, fmt.Errorf("constructed scratch path is not the actual reserved root directory: %s", path)))
		}
	}
}
