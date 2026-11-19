// File role: Keep experimental evaluation streams inside the existing cumulative
// 4 GiB owner. Two 64 KiB buffers belong to this synchronous invocation; flush,
// sync, close and integrity checks all precede COMPLETE-last publication.
package main

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
)

type studyMethodEvalWriter struct {
	owner  *studyDiagnosticArtifacts
	file   *studyDiagnosticArtifactFile
	buffer *bufio.Writer
	closed bool
}

func newStudyMethodEvalWriter(owner *studyDiagnosticArtifacts, name string) (*studyMethodEvalWriter, error) {
	file, err := owner.openFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600, false)
	if err != nil {
		return nil, err
	}
	return &studyMethodEvalWriter{owner: owner, file: file, buffer: bufio.NewWriterSize(file, 64<<10)}, nil
}
func (w *studyMethodEvalWriter) record(value any) error {
	if w.closed {
		return w.owner.latch(os.ErrClosed)
	}
	if err := w.owner.Err(); err != nil {
		return err
	}
	data, err := w.owner.jsonBytes(value, studyAuditRowBytes-1)
	if err != nil {
		return err
	}
	if _, err := w.buffer.Write(data); err != nil {
		return w.owner.latch(err)
	}
	return w.owner.latch(w.buffer.WriteByte('\n'))
}
func (w *studyMethodEvalWriter) close() error {
	if w == nil || w.closed {
		return nil
	}
	w.closed = true
	return w.owner.latch(errors.Join(w.buffer.Flush(), w.file.Sync(), w.file.Close()))
}
func (w *studyMethodEvalWriter) binding() (studyFile, error) {
	if !w.closed {
		return studyFile{}, w.owner.latch(errors.New("evaluation cannot hash an open stream"))
	}
	if err := w.owner.Err(); err != nil {
		return studyFile{}, err
	}
	sum, err := studyHashFile(w.file.path)
	if err != nil {
		return studyFile{}, w.owner.latch(err)
	}
	return studyFile{Path: w.file.path, SHA256: sum}, nil
}

func runStudyMethodEvaluation(planPath, frozenSHA, out string) error {
	return runStudyMethodEvaluationWithOps(planPath, frozenSHA, out, studyDiagnosticArtifactFileOps())
}

func runStudyMethodEvaluationWithOps(planPath, frozenSHA, out string, ops studyDiagnosticArtifactOps) (err error) {
	return runStudyMethodEvaluationWithAuthority(planPath, frozenSHA, out, ops, studyMethodEvalOriginalAuthority())
}

// The authority parameter is a private synthetic-fixture seam, never a plan or
// CLI option. Both production entry points above supply the fixed E1 table.
func runStudyMethodEvaluationWithAuthority(planPath, frozenSHA, out string, ops studyDiagnosticArtifactOps, authority [4]studyMethodEvalAuthority) (err error) {
	p, err := prepareStudyMethodEvaluationWithAuthority(planPath, frozenSHA, authority)
	if err != nil {
		return err
	}
	loaded := make([]studyMethodEvalLoadedGroup, 0, len(p.Plan.Groups))
	for _, group := range p.Plan.Groups {
		value, err := p.loadGroup(group)
		if err != nil {
			return err
		}
		loaded = append(loaded, value)
	}
	if err := studyAuditNewDirectory(out); err != nil {
		return err
	}
	owner, err := newStudyDiagnosticArtifactsWithOps(out, studyAuditOutputBytes, studyAuditReserveBytes, ops)
	if err != nil {
		return err
	}
	files := &studyDiagnosticRun{owner: owner, files: make(map[string]*studyDiagnosticFileState)}
	files.trackFiles()
	var paired, receipts *studyMethodEvalWriter
	defer func() {
		err = errors.Join(err, paired.close(), receipts.close())
		if err != nil {
			failure := (&studyMethodRun{files: files}).failure(err)
			err = errors.Join(err, failure)
		}
	}()
	paired, err = newStudyMethodEvalWriter(owner, "paired_union.jsonl")
	if err != nil {
		return err
	}
	receipts, err = newStudyMethodEvalWriter(owner, "applied_receipts.jsonl")
	if err != nil {
		return err
	}
	result := newStudyMethodEvaluationResult(p)
	aliases, err := loadStudyReceiverAliases(p.Capture.ReceiverMetadata)
	if err != nil {
		return err
	}
	for groupIndex := range loaded {
		if err := studyMethodEvaluateDay(p, loaded[groupIndex], aliases, &result, paired, receipts); err != nil {
			return err
		}
	}
	studyMethodEvalFinish(&result, p.Plan.Phase)
	if err := errors.Join(paired.close(), receipts.close()); err != nil {
		return err
	}
	result.Paired, err = paired.binding()
	if err != nil {
		return err
	}
	result.Receipts, err = receipts.binding()
	if err != nil {
		return err
	}
	if err := studyMethodEvalRecheck(p, loaded); err != nil {
		return owner.latch(err)
	}
	outputs, _, err := files.closedOutputs()
	if err != nil {
		return err
	}
	result.Outputs = outputs
	result.Accounting, err = owner.accounting("closed_evaluation_streams_before_aggregates_and_COMPLETE")
	if err != nil {
		return err
	}
	if err := owner.atomicJSON("aggregates.json", result); err != nil {
		return err
	}
	path := filepath.Join(owner.root, "aggregates.json")
	sum, err := studyHashFile(path)
	if err != nil {
		return owner.latch(err)
	}
	return owner.atomicJSON("COMPLETE.json", struct {
		SchemaVersion  int       `json:"schema_version"`
		Experimental   bool      `json:"experimental"`
		Complete       bool      `json:"complete"`
		EvaluationPlan studyFile `json:"evaluation_plan"`
		Aggregates     studyFile `json:"aggregates"`
		Paired         studyFile `json:"full_paired_union"`
		Receipts       studyFile `json:"applied_receipts"`
	}{p.Plan.SchemaVersion, true, true, p.File, studyFile{Path: path, SHA256: sum}, result.Paired, result.Receipts})
}

func studyMethodEvalRecheck(p *studyMethodEvalPrepared, groups []studyMethodEvalLoadedGroup) error {
	files := []studyFile{p.File, p.Plan.CapturePlan, p.Plan.MethodPlan, p.Plan.ControlPlan, p.Plan.ParityCapturePlan, p.Plan.ParityControlPlan, p.Method.Model, p.Capture.ReceiverMetadata}
	for parityIndex := range p.Plan.Parity {
		parity := &p.Plan.Parity[parityIndex]
		files = append(files, parity.OriginalDecisions, parity.ControlDecisions)
	}
	allGroups := append(append([]studyMethodEvalLoadedGroup{}, groups...), p.ParityGroups...)
	for groupIndex := range allGroups {
		group := &allGroups[groupIndex]
		files = append(files, group.Input.Archive)
		for role, binding := range group.Bindings {
			report := group.Reports[role]
			files = append(files, binding.Complete, binding.Manifest, report.Decisions, report.Experimental.Report, report.Experimental.Plan, report.Experimental.Model)
		}
	}
	seen := make(map[string]string, len(files))
	for _, file := range files {
		key := studyDiagnosticPathKey(file.Path)
		if sum, ok := seen[key]; ok {
			if sum != file.SHA256 {
				return errors.New("evaluation input path has contradictory frozen hashes")
			}
			continue
		}
		seen[key] = file.SHA256
		if err := studyCheckFile(file); err != nil {
			return err
		}
	}
	return nil
}
