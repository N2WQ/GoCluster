// File role: Route only opted-in study artifacts through the run's byte owner.
// Existing wrappers retain ordinary file flags, errors and cleanup behavior.
// The diagnostic adapter latches ignored Write/Close errors for final refusal.
package main

import (
	"os"
)

type replayRunbookFile interface {
	studyAuditSink
	Name() string
}

type studyDiagnosticNamedArtifact struct {
	*studyDiagnosticArtifactFile
	path string
}

func (f *studyDiagnosticNamedArtifact) Name() string { return f.path }

func openReplayArtifactWithDiagnostics(path string, flags int, mode os.FileMode, owner *studyDiagnosticArtifacts) (replayRunbookFile, error) {
	if owner == nil {
		return os.OpenFile(path, flags, mode)
	}
	file, err := owner.openFile(path, flags, mode, false)
	if err != nil {
		return nil, err
	}
	return &studyDiagnosticNamedArtifact{studyDiagnosticArtifactFile: file, path: file.path}, nil
}

func (r *replayRunner) diagnosticArtifacts() *studyDiagnosticArtifacts {
	if r.diagnostics == nil {
		return nil
	}
	return r.diagnostics.owner
}

func (r *replayRunner) openReplayArtifact(path string, flags int, mode os.FileMode) (replayRunbookFile, error) {
	return openReplayArtifactWithDiagnostics(path, flags, mode, r.diagnosticArtifacts())
}

func (r *replayRunner) writeReplayJSON(path string, value any) error {
	if owner := r.diagnosticArtifacts(); owner != nil {
		return owner.atomicJSON(path, value)
	}
	return writeJSONAtomic(path, value)
}

// Source Close remains with the existing caller. Successful copies are retained
// as bounded integrity receipts; later deletion never refunds their byte debit.
func (r *replayRunner) copyFrozenLicenseFile(source *os.File, destination string, input *replayLicenseInput) error {
	if r.diagnostics == nil {
		return copyFrozenLicenseFile(source, destination, input)
	}
	receipt, err := r.diagnostics.owner.copyFile(destination, source)
	if err != nil {
		return err
	}
	input.Bytes, input.SHA256 = receipt.BytesCopied, receipt.SHA256
	return r.diagnostics.noteCopy(receipt)
}
