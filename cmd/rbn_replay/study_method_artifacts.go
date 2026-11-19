// File role: Bound one replay-only method run's artifacts without pretending it
// is a selected diagnostic capture. The synchronous run reuses the v6 byte,
// file/copy inventory and scratch helpers; no diagnostic selection is installed.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"unicode/utf8"
)

type studyMethodManifest struct {
	SchemaVersion int       `json:"schema_version"`
	Experimental  bool      `json:"experimental"`
	Enabled       bool      `json:"enabled"`
	Plan          studyFile `json:"plan"`
	Model         studyFile `json:"model"`
	CandidateID   string    `json:"candidate_id"`
	Report        studyFile `json:"report"`
}

type studyMethodRun struct {
	prepared *studyMethodPrepared
	files    *studyDiagnosticRun
	receipts *studyDiagnosticArtifactFile
	plan     studyFile
	model    studyFile
}

func newStudyMethodRun(output string, prepared *studyMethodPrepared) (*studyMethodRun, error) {
	return newStudyMethodRunWithOps(output, prepared, studyDiagnosticArtifactFileOps())
}

// The private operations seam exercises real late file failures. It cannot
// change requested method/model inputs or introduce a second byte budget.
func newStudyMethodRunWithOps(output string, prepared *studyMethodPrepared, ops studyDiagnosticArtifactOps) (_ *studyMethodRun, err error) {
	if prepared == nil || prepared.Model == nil || !studyValidFile(prepared.File) {
		return nil, errors.New("method run requires its prepared frozen plan and loaded model")
	}
	if err := validateStudyMethodPlan(prepared.Plan); err != nil {
		return nil, err
	}
	owner, err := newStudyDiagnosticArtifactsWithOps(output, studyAuditOutputBytes, studyAuditReserveBytes, ops)
	if err != nil {
		return nil, err
	}
	run := &studyMethodRun{prepared: prepared, files: &studyDiagnosticRun{owner: owner, files: make(map[string]*studyDiagnosticFileState),
		scratch: studyDiagnosticScratch{Backend: "not_constructed", ClaimLimit: "Post-existing-cleanup logical lengths only; inherited database Close errors remain unobserved; no peak, allocated-block or durability claim"}}}
	run.files.trackFiles()
	defer func() {
		if err != nil {
			err = errors.Join(err, run.closeStreams(), run.failure(err))
		}
	}()
	run.plan, err = run.freezeInput("method-plan.json", prepared.File)
	if err != nil {
		return nil, err
	}
	run.model, err = run.freezeInput("method-model.json", prepared.Plan.Model)
	if err != nil {
		return nil, err
	}
	if err := run.files.verifyCopies(false); err != nil {
		return nil, err
	}
	run.receipts, err = owner.openFile("method-receipts.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600, false)
	if err != nil {
		return nil, err
	}
	return run, nil
}

func (m *studyMethodRun) freezeInput(name string, input studyFile) (studyFile, error) {
	source, err := os.Open(input.Path)
	if err != nil {
		return studyFile{}, m.files.owner.latch(err)
	}
	info, statErr := source.Stat()
	if statErr != nil || info == nil || !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > studyMaxJSONBytes {
		return studyFile{}, m.files.owner.latch(errors.Join(statErr, source.Close(), errors.New("method input must be a regular file at most 4 MiB")))
	}
	// A source that grows after Stat must not turn a bounded method file into
	// a quota-sized copy. One excess byte proves overflow without reading on.
	bounded := &studyMethodCopySource{File: source, reader: io.LimitReader(source, studyMaxJSONBytes+1)}
	receipt, copyErr := m.files.owner.copyFile(name, bounded)
	if err := errors.Join(copyErr, source.Close()); err != nil {
		return studyFile{}, m.files.owner.latch(err)
	}
	if receipt.BytesCopied > studyMaxJSONBytes || receipt.SHA256 != input.SHA256 {
		return studyFile{}, m.files.owner.latch(errors.New("method input changed while freezing"))
	}
	if err := m.files.noteCopy(receipt); err != nil {
		return studyFile{}, err
	}
	return studyFile{Path: receipt.Path, SHA256: receipt.SHA256}, nil
}

type studyMethodCopySource struct {
	*os.File
	reader io.Reader
}

func (s *studyMethodCopySource) Read(data []byte) (int, error) { return s.reader.Read(data) }

func (m *studyMethodRun) record(value any) error {
	return m.files.owner.writeRow(m.receipts, value)
}

func (m *studyMethodRun) closeStreams() error {
	if m.receipts == nil || m.receipts.closed {
		return nil
	}
	return errors.Join(m.receipts.Sync(), m.receipts.Close())
}

type studyMethodRunReport struct {
	SchemaVersion          int                               `json:"schema_version"`
	Experimental           bool                              `json:"experimental"`
	Enabled                bool                              `json:"enabled"`
	Candidate              studyMethodCandidate              `json:"candidate"`
	Plan                   studyFile                         `json:"plan"`
	Model                  studyFile                         `json:"model"`
	OriginalPlan           studyFile                         `json:"original_plan"`
	OriginalModel          studyFile                         `json:"original_model"`
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

// complete follows existing runner cleanup, including its actual license-copy
// removal. Every fallible stream close precedes the manifest; the caller alone
// publishes complete.json last through this same owner.
func (m *studyMethodRun) complete() (*studyMethodManifest, error) {
	owner := m.files.owner
	if err := errors.Join(owner.Err(), m.closeStreams()); err != nil {
		return nil, err
	}
	if m.files.scratch.Backend == "not_constructed" {
		return nil, owner.latch(errors.New("method history backend construction was not observed"))
	}
	if err := m.files.verifyCopies(true); err != nil {
		return nil, err
	}
	for _, input := range []studyFile{m.prepared.File, m.prepared.Plan.Model} {
		if err := studyMethodCheckInput(input); err != nil {
			return nil, owner.latch(err)
		}
	}
	if m.files.scratch.Applicable {
		measurement, err := measureStudyDiagnosticScratch(m.files.scratch.Path)
		if err != nil {
			return nil, owner.latch(err)
		}
		m.files.scratch.Measurement = &measurement
	}
	report, err := m.closedReport()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(owner.root, "method-report.json")
	if err := owner.atomicJSON(path, report); err != nil {
		return nil, err
	}
	digest, err := studyHashFile(path)
	if err != nil {
		return nil, owner.latch(err)
	}
	return &studyMethodManifest{SchemaVersion: m.prepared.Plan.SchemaVersion, Experimental: true, Enabled: m.prepared.Plan.Enabled, Plan: m.plan, Model: m.model,
		CandidateID: m.prepared.Candidate.ID, Report: studyFile{Path: path, SHA256: digest}}, nil
}

func studyMethodCheckInput(input studyFile) error {
	data, err := studyReadOrdinalAsset(input.Path)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != input.SHA256 {
		return errors.New("frozen method input SHA256 changed")
	}
	return nil
}

func (m *studyMethodRun) closedReport() (studyMethodRunReport, error) {
	var empty studyMethodRunReport
	owner := m.files.owner
	outputs, deleted, err := m.files.closedOutputs()
	if err != nil {
		return empty, err
	}
	measurement, err := measureStudyDiagnosticEvidence(owner.root, m.files.scratch.Path)
	if err != nil {
		return empty, owner.latch(err)
	}
	var retained int64
	for _, output := range outputs {
		retained += output.Bytes
	}
	if measurement.RegularFiles != uint64(len(outputs)) || measurement.RegularFileBytes != retained || retained+deleted > owner.quota.used {
		return empty, owner.latch(errors.New("method retained inventory does not reconcile with quota-owned outputs"))
	}
	accounting, err := owner.accounting("after_cleanup_and_closed_output_hashes_before_method_report_and_complete")
	if err != nil {
		return empty, err
	}
	return studyMethodRunReport{SchemaVersion: m.prepared.Plan.SchemaVersion, Experimental: true, Enabled: m.prepared.Plan.Enabled, Candidate: m.prepared.Candidate,
		Plan: m.plan, Model: m.model, OriginalPlan: m.prepared.File, OriginalModel: m.prepared.Plan.Model,
		Outputs: outputs, Copies: m.files.copies, Accounting: accounting, RetainedEvidence: measurement, Scratch: m.files.scratch,
		DeletedCopyBytes: deleted, UnretainedChargedBytes: accounting.ChargedBytes - retained - deleted,
		InventoryStage: "closed prior outputs only; this method report and complete.json are excluded from their own hashes/accounting; external final reconciliation includes both",
		ClaimLimits:    []string{"Experimental replay method; no automatic activation or live-delivery qualification", "Reported-frequency association is not RF truth or calibrated probability", "Final logical lengths are not peak disk use, allocated blocks or durability; inherited database Close errors are unobserved"}}, nil
}

func (m *studyMethodRun) failure(cause error) error {
	if cause == nil {
		return nil
	}
	owner := m.files.owner
	cause = owner.latch(cause)
	message := cause.Error()
	if len(message) > 1024 {
		message = message[:1024]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
	}
	return owner.writeFailed(struct {
		SchemaVersion int    `json:"schema_version"`
		Experimental  bool   `json:"experimental"`
		Error         string `json:"error"`
	}{1, true, message})
}
