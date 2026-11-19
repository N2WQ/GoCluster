// File role: Measure final scratch logical bytes after inherited replay cleanup.
// The caller determines applicability from the backend actually constructed.
// This walker owns at most 17 directory handles, never follows links, and keeps
// at most one <=256-entry batch per level. It makes no peak/durability claim.
package main

import (
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

type studyDiagnosticScratchDirectory interface {
	ReadDir(int) ([]os.DirEntry, error)
	Close() error
}
type studyDiagnosticScratchOps struct {
	lstat func(string) (os.FileInfo, error)
	open  func(string) (studyDiagnosticScratchDirectory, error)
}

type studyDiagnosticScratchLimits struct {
	Batch     int    `json:"read_dir_batch"`
	Depth     int    `json:"depth_below_root"`
	PathBytes int    `json:"path_bytes"`
	Entries   uint64 `json:"visited_entries_including_root"`
}

type studyDiagnosticScratchMeasurement struct {
	Complete         bool                         `json:"complete"`
	Stage            string                       `json:"stage"`
	RegularFileBytes int64                        `json:"regular_file_bytes"`
	RegularFiles     uint64                       `json:"regular_files"`
	VisitedEntries   uint64                       `json:"visited_entries"`
	Limits           studyDiagnosticScratchLimits `json:"limits"`
	ClaimLimits      string                       `json:"claim_limits"`
}

func measureStudyDiagnosticScratch(root string) (studyDiagnosticScratchMeasurement, error) {
	return measureStudyDiagnosticScratchWithOps(root, studyDiagnosticScratchLimits{Batch: 256, Depth: 16, PathBytes: 1024, Entries: 1048576},
		studyDiagnosticScratchOps{lstat: os.Lstat, open: func(path string) (studyDiagnosticScratchDirectory, error) { return os.Open(path) }})
}

func measureStudyDiagnosticScratchWithOps(root string, limits studyDiagnosticScratchLimits, ops studyDiagnosticScratchOps) (studyDiagnosticScratchMeasurement, error) {
	return measureStudyDiagnosticTree(root, "", limits, ops)
}

// Only a backend actually constructed by this run may supply scratchRoot. Its
// exact root child is verified as a non-link directory before exclusion; the
// standalone scratch measurement above never excludes any directory.
func measureStudyDiagnosticEvidence(root, scratchRoot string) (studyDiagnosticScratchMeasurement, error) {
	result, err := measureStudyDiagnosticTree(root, scratchRoot, studyDiagnosticScratchLimits{Batch: 256, Depth: 16, PathBytes: 1024, Entries: 1048576},
		studyDiagnosticScratchOps{lstat: os.Lstat, open: func(path string) (studyDiagnosticScratchDirectory, error) { return os.Open(path) }})
	if err == nil {
		result.Stage = "retained_evidence_after_cleanup_before_diagnostic_report_and_complete"
	}
	return result, err
}

func measureStudyDiagnosticTree(root, scratchRoot string, limits studyDiagnosticScratchLimits, ops studyDiagnosticScratchOps) (studyDiagnosticScratchMeasurement, error) {
	var empty studyDiagnosticScratchMeasurement
	if limits.Batch < 1 || limits.Batch > 256 || limits.Depth < 0 || limits.Depth > 16 || limits.PathBytes < 1 || limits.PathBytes > 1024 || limits.Entries < 1 || limits.Entries > 1048576 || ops.lstat == nil || ops.open == nil {
		return empty, errors.New("invalid diagnostic scratch traversal limits")
	}
	if len(root) > limits.PathBytes || !utf8.ValidString(root) || strings.TrimSpace(root) == "" {
		return empty, errors.New("diagnostic scratch root requires a nonblank bounded UTF-8 path")
	}
	path, err := filepath.Abs(root)
	if err != nil {
		return empty, err
	}
	if len(path) > limits.PathBytes || !utf8.ValidString(path) {
		return empty, errors.New("diagnostic scratch root exceeds UTF-8 path bound")
	}
	info, err := ops.lstat(path)
	if err != nil {
		return empty, err
	}
	if info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return empty, errors.New("diagnostic scratch root must be a directory without links")
	}
	state := studyDiagnosticScratchWalk{limits: limits, ops: ops, visited: 1}
	if scratchRoot != "" {
		if len(scratchRoot) > limits.PathBytes || !utf8.ValidString(scratchRoot) {
			return empty, errors.New("diagnostic scratch exclusion exceeds path bound")
		}
		excluded, err := filepath.Abs(scratchRoot)
		if err != nil || !strings.EqualFold(excluded, filepath.Join(path, studyDiagnosticScratchChild)) {
			return empty, errors.Join(err, errors.New("diagnostic evidence may exclude only the exact constructed scratch root child"))
		}
		info, err := ops.lstat(excluded)
		if err != nil || info == nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return empty, errors.Join(err, errors.New("diagnostic scratch exclusion requires an existing non-link directory"))
		}
		state.exclude = excluded
	}
	if err := state.walk(path, 0); err != nil {
		return empty, err
	}
	if state.exclude != "" && !state.skipped {
		return empty, errors.New("constructed diagnostic scratch root child was not visited")
	}
	stage := "post_existing_cleanup_with_unobserved_database_close_errors"
	if scratchRoot != "" {
		stage = "retained_evidence_after_cleanup_before_diagnostic_report_and_complete"
	}
	return studyDiagnosticScratchMeasurement{Complete: true, Stage: stage,
		RegularFileBytes: state.bytes, RegularFiles: state.files, VisitedEntries: state.visited, Limits: limits,
		ClaimLimits: "Final regular logical file lengths only; not allocated blocks, peak WAL/SST/compaction bytes, or durable closure"}, nil
}

type studyDiagnosticScratchWalk struct {
	limits         studyDiagnosticScratchLimits
	ops            studyDiagnosticScratchOps
	visited, files uint64
	bytes          int64
	exclude        string
	skipped        bool
}

func (w *studyDiagnosticScratchWalk) walk(path string, depth int) (err error) {
	directory, err := w.ops.open(path)
	if err != nil {
		return err
	}
	if studyDiagnosticNil(directory) {
		return errors.New("diagnostic scratch opener returned a nil directory")
	}
	defer func() { err = errors.Join(err, directory.Close()) }()
	for {
		// Charge returned entries before descending. Pending parent batches are
		// already counted, so nested reads cannot overrun the one-probe allowance.
		remaining := w.limits.Entries - w.visited
		request := w.limits.Batch
		if uint64(request) > remaining+1 {
			request = int(remaining + 1)
		}
		entries, readErr := directory.ReadDir(request)
		if len(entries) > request {
			return errors.New("diagnostic scratch directory returned more than requested")
		}
		if uint64(len(entries)) > remaining {
			return errors.New("diagnostic scratch entry limit exceeded by overflow probe")
		}
		w.visited += uint64(len(entries))
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return readErr
		}
		for _, entry := range entries {
			if entry == nil {
				return errors.New("diagnostic scratch directory returned nil entry")
			}
			name := entry.Name()
			separatorBytes := 1
			if strings.HasSuffix(path, string(filepath.Separator)) {
				separatorBytes = 0
			}
			if len(name) > w.limits.PathBytes-len(path)-separatorBytes {
				return errors.New("diagnostic scratch path exceeds bound")
			}
			if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\") || !utf8.ValidString(name) {
				return errors.New("unsupported diagnostic scratch entry name")
			}
			child := filepath.Join(path, name)
			if len(child) > w.limits.PathBytes {
				return errors.New("diagnostic scratch path exceeds bound")
			}
			if depth >= w.limits.Depth {
				return errors.New("diagnostic scratch depth exceeds bound")
			}
			info, statErr := w.ops.lstat(child)
			if statErr != nil {
				return statErr
			}
			if info == nil || entry.Type()&os.ModeSymlink != 0 || info.Mode()&os.ModeSymlink != 0 {
				return errors.New("diagnostic scratch links are unsupported")
			}
			if depth == 0 && w.exclude != "" && strings.EqualFold(child, w.exclude) {
				if !info.IsDir() || w.skipped {
					return errors.New("diagnostic scratch exclusion changed or appeared twice")
				}
				w.skipped = true // Already charged once in the returned entry batch.
				continue
			}
			if info.IsDir() {
				if err := w.walk(child, depth+1); err != nil {
					return err
				}
				continue
			}
			if !info.Mode().IsRegular() {
				return errors.New("diagnostic scratch non-regular entry is unsupported")
			}
			size := info.Size()
			if size < 0 || size > math.MaxInt64-w.bytes {
				return errors.New("diagnostic scratch byte total is invalid or overflows")
			}
			w.files++
			w.bytes += size
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if len(entries) == 0 {
			return io.ErrNoProgress
		}
	}
}
