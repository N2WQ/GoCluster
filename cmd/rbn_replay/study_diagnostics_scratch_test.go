package main

import (
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

type diagnosticScratchInfo struct {
	name string
	size int64
	mode os.FileMode
}

func (i diagnosticScratchInfo) Name() string       { return i.name }
func (i diagnosticScratchInfo) Size() int64        { return i.size }
func (i diagnosticScratchInfo) Mode() os.FileMode  { return i.mode }
func (i diagnosticScratchInfo) ModTime() time.Time { return time.Time{} }
func (i diagnosticScratchInfo) IsDir() bool        { return i.mode.IsDir() }
func (i diagnosticScratchInfo) Sys() any           { return nil }

type diagnosticScratchEntry struct {
	name string
	mode os.FileMode
}

func (e diagnosticScratchEntry) Name() string      { return e.name }
func (e diagnosticScratchEntry) IsDir() bool       { return e.mode.IsDir() }
func (e diagnosticScratchEntry) Type() os.FileMode { return e.mode.Type() }
func (e diagnosticScratchEntry) Info() (os.FileInfo, error) {
	return nil, errors.New("walker must Lstat entries")
}

type diagnosticScratchDirectory struct {
	read  func(int) ([]os.DirEntry, error)
	close func() error
}

func (d *diagnosticScratchDirectory) ReadDir(n int) ([]os.DirEntry, error) { return d.read(n) }
func (d *diagnosticScratchDirectory) Close() error                         { return d.close() }

type diagnosticScratchTree struct {
	nodes                                 map[string]diagnosticScratchInfo
	children                              map[string][]os.DirEntry
	readErr, closeErr                     map[string]error
	active, high, opens, closes, returned int
	requests                              []int
}

func diagnosticScratchTestTree(root string) *diagnosticScratchTree {
	return &diagnosticScratchTree{nodes: map[string]diagnosticScratchInfo{root: {name: "root", mode: os.ModeDir}}, children: make(map[string][]os.DirEntry), readErr: make(map[string]error), closeErr: make(map[string]error)}
}
func (tree *diagnosticScratchTree) add(parent, name string, mode os.FileMode, size int64) string {
	path := filepath.Join(parent, name)
	tree.nodes[path] = diagnosticScratchInfo{name: name, mode: mode, size: size}
	tree.children[parent] = append(tree.children[parent], diagnosticScratchEntry{name: name, mode: mode})
	return path
}
func (tree *diagnosticScratchTree) ops() studyDiagnosticScratchOps {
	return studyDiagnosticScratchOps{lstat: func(path string) (os.FileInfo, error) {
		info, ok := tree.nodes[path]
		if !ok {
			return nil, os.ErrNotExist
		}
		return info, nil
	},
		open: func(path string) (studyDiagnosticScratchDirectory, error) {
			tree.opens++
			tree.active++
			tree.high = max(tree.high, tree.active)
			cursor := 0
			closed := false
			return &diagnosticScratchDirectory{read: func(n int) ([]os.DirEntry, error) {
				tree.requests = append(tree.requests, n)
				if err := tree.readErr[path]; err != nil {
					return nil, err
				}
				entries := tree.children[path]
				if cursor >= len(entries) {
					return nil, io.EOF
				}
				end := min(len(entries), cursor+n)
				out := append([]os.DirEntry(nil), entries[cursor:end]...)
				cursor = end
				tree.returned += len(out)
				if cursor == len(entries) {
					return out, io.EOF
				}
				return out, nil
			}, close: func() error {
				if closed {
					return errors.New("directory closed twice")
				}
				closed = true
				tree.closes++
				tree.active--
				return tree.closeErr[path]
			}}, nil
		}}
}
func diagnosticScratchTestLimits() studyDiagnosticScratchLimits {
	return studyDiagnosticScratchLimits{Batch: 256, Depth: 16, PathBytes: 1024, Entries: 1048576}
}
func diagnosticScratchAssertFailure(t *testing.T, result studyDiagnosticScratchMeasurement, err error) {
	t.Helper()
	if err == nil || !reflect.DeepEqual(result, studyDiagnosticScratchMeasurement{}) {
		t.Fatalf("error claimed partial complete totals: result=%+v err=%v", result, err)
	}
}

func TestDiagnosticScratchLiteralTotals(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{"a": "abc", filepath.Join("sub", "b"): "1234", filepath.Join("sub", "empty"): ""} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := measureStudyDiagnosticScratch(root)
	if err != nil || !got.Complete || got.RegularFileBytes != 7 || got.RegularFiles != 3 || got.VisitedEntries != 5 || got.Stage != "post_existing_cleanup_with_unobserved_database_close_errors" || got.Limits != diagnosticScratchTestLimits() {
		t.Fatalf("literal scratch total: %+v err=%v", got, err)
	}
	missing, err := measureStudyDiagnosticScratch(filepath.Join(root, "missing"))
	diagnosticScratchAssertFailure(t, missing, err)
}

func TestDiagnosticScratchReadCloseFailures(t *testing.T) {
	for _, fault := range []string{"root_stat", "root_open", "read", "entry_stat", "close", "child_read_parent_close", "entries_with_eof"} {
		t.Run(fault, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "scratch")
			tree := diagnosticScratchTestTree(root)
			tree.add(root, "a", 0, 3)
			sub := tree.add(root, "sub", os.ModeDir, 0)
			tree.add(sub, "b", 0, 4)
			ops := tree.ops()
			sentinel := errors.New(fault)
			secondary := errors.New("parent close")
			switch fault {
			case "root_stat":
				ops.lstat = func(string) (os.FileInfo, error) { return nil, sentinel }
			case "root_open":
				ops.open = func(string) (studyDiagnosticScratchDirectory, error) { return nil, sentinel }
			case "read":
				tree.readErr[root] = sentinel
			case "entry_stat":
				original := ops.lstat
				ops.lstat = func(path string) (os.FileInfo, error) {
					if filepath.Base(path) == "a" {
						return nil, sentinel
					}
					return original(path)
				}
			case "close":
				tree.closeErr[root] = sentinel
			case "child_read_parent_close":
				tree.readErr[sub] = sentinel
				tree.closeErr[root] = secondary
			}
			got, err := measureStudyDiagnosticScratchWithOps(root, diagnosticScratchTestLimits(), ops)
			if fault == "entries_with_eof" {
				if err != nil || !got.Complete || got.RegularFileBytes != 7 || got.RegularFiles != 2 || got.VisitedEntries != 4 {
					t.Fatalf("entries returned with EOF were dropped: %+v %v", got, err)
				}
			} else {
				diagnosticScratchAssertFailure(t, got, err)
				if !errors.Is(err, sentinel) || fault == "child_read_parent_close" && !errors.Is(err, secondary) {
					t.Fatalf("original read/close failure lost: %v", err)
				}
			}
			if tree.active != 0 || tree.opens != tree.closes {
				t.Fatalf("directory handle leaked: opens=%d closes=%d", tree.opens, tree.closes)
			}
		})
	}
}

func TestDiagnosticScratchTraversalBounds(t *testing.T) {
	for _, fixture := range []struct {
		name string
		root string
	}{
		{"empty_root_before_abs", ""},
		{"whitespace_root_before_abs", " \t\r\n"},
		{"oversized_root_before_abs", strings.Repeat("x"+string(filepath.Separator)+".."+string(filepath.Separator), 1<<18)},
		{"invalid_utf8_root_before_abs", "\xff" + string(filepath.Separator) + ".." + string(filepath.Separator) + "scratch"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			stats, opens := 0, 0
			ops := studyDiagnosticScratchOps{
				lstat: func(string) (os.FileInfo, error) {
					stats++
					return diagnosticScratchInfo{mode: os.ModeDir}, nil
				},
				open: func(string) (studyDiagnosticScratchDirectory, error) {
					opens++
					return nil, errors.New("invalid raw root reached open")
				},
			}
			got, err := measureStudyDiagnosticScratchWithOps(fixture.root, diagnosticScratchTestLimits(), ops)
			diagnosticScratchAssertFailure(t, got, err)
			if !strings.Contains(err.Error(), "root requires a nonblank bounded UTF-8 path") || stats != 0 || opens != 0 {
				t.Fatal("raw root was normalized or used before bounded preflight")
			}
		})
	}
	t.Run("oversized_name_before_join", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "scratch")
		tree := diagnosticScratchTestTree(root)
		tree.children[root] = []os.DirEntry{diagnosticScratchEntry{name: strings.Repeat("x", 1<<20)}}
		ops := tree.ops()
		lstat := ops.lstat
		stats := 0
		ops.lstat = func(path string) (os.FileInfo, error) {
			stats++
			if path != root {
				t.Error("oversized path reached Lstat")
			}
			return lstat(path)
		}
		got, err := measureStudyDiagnosticScratchWithOps(root, diagnosticScratchTestLimits(), ops)
		diagnosticScratchAssertFailure(t, got, err)
		if !strings.Contains(err.Error(), "path exceeds bound") || stats != 1 || tree.active != 0 || tree.opens != tree.closes {
			t.Fatal("oversized name bypassed path preflight or leaked its directory")
		}
	})
	for _, extra := range []bool{false, true} {
		t.Run(map[bool]string{false: "depth_exact", true: "depth_over"}[extra], func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "scratch")
			tree := diagnosticScratchTestTree(root)
			parent := root
			for depth := 1; depth <= 16; depth++ {
				parent = tree.add(parent, "d", os.ModeDir, 0)
			}
			if extra {
				tree.add(parent, "d", os.ModeDir, 0)
			}
			got, err := measureStudyDiagnosticScratchWithOps(root, diagnosticScratchTestLimits(), tree.ops())
			if extra {
				diagnosticScratchAssertFailure(t, got, err)
			} else if err != nil || got.VisitedEntries != 17 || !got.Complete {
				t.Fatal("exact depth refused:", err)
			}
			if tree.high != 17 || tree.active != 0 || tree.opens != tree.closes {
				t.Fatalf("depth/handle cap: high=%d active=%d", tree.high, tree.active)
			}
		})
	}
	for _, name := range []string{"x", "xx", "é", "éé"} {
		t.Run("path_"+name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "scratch")
			tree := diagnosticScratchTestTree(root)
			tree.add(root, name, 0, 1)
			limits := diagnosticScratchTestLimits()
			limits.PathBytes = len(root) + 2
			if strings.HasPrefix(name, "é") {
				limits.PathBytes = len(root) + 3
			}
			got, err := measureStudyDiagnosticScratchWithOps(root, limits, tree.ops())
			over := name == "xx" || name == "éé"
			if over {
				diagnosticScratchAssertFailure(t, got, err)
			} else if err != nil || got.RegularFileBytes != 1 {
				t.Fatal("exact UTF-8 path limit refused:", err)
			}
		})
	}
	for _, limit := range []uint64{2, 3} {
		root := filepath.Join(t.TempDir(), "scratch")
		tree := diagnosticScratchTestTree(root)
		tree.add(root, "a", 0, 1)
		tree.add(root, "b", 0, 1)
		limits := diagnosticScratchTestLimits()
		limits.Entries = limit
		got, err := measureStudyDiagnosticScratchWithOps(root, limits, tree.ops())
		if limit == 2 {
			diagnosticScratchAssertFailure(t, got, err)
		} else if err != nil || got.VisitedEntries != 3 {
			t.Fatal("exact root-inclusive entry limit refused")
		}
		for _, request := range tree.requests {
			if request > int(limit) || request < 1 {
				t.Fatal("last request exceeded remaining+one probe")
			}
		}
	}
	root := filepath.Join(t.TempDir(), "scratch")
	tree := diagnosticScratchTestTree(root)
	sub := tree.add(root, "sub", os.ModeDir, 0)
	tree.add(root, "pending_a", 0, 1)
	tree.add(root, "pending_b", 0, 1)
	tree.add(sub, "first", 0, 1)
	tree.add(sub, "probe", 0, 1)
	limits := diagnosticScratchTestLimits()
	limits.Entries = 5
	got, err := measureStudyDiagnosticScratchWithOps(root, limits, tree.ops())
	diagnosticScratchAssertFailure(t, got, err)
	if tree.returned+1 != 6 || tree.active != 0 {
		t.Fatal("pending parent batches were not charged before child reads")
	}
	for _, over := range []bool{false, true} {
		t.Run(map[bool]string{false: "million_exact", true: "million_overflow_probe"}[over], func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "scratch")
			limits := diagnosticScratchTestLimits()
			total := limits.Entries - 1
			if over {
				total++
			}
			var cursor uint64
			maxBatch, opens, closes := 0, 0, 0
			ops := studyDiagnosticScratchOps{lstat: func(path string) (os.FileInfo, error) {
				if path == root {
					return diagnosticScratchInfo{mode: os.ModeDir}, nil
				}
				return diagnosticScratchInfo{size: 1}, nil
			},
				open: func(string) (studyDiagnosticScratchDirectory, error) {
					opens++
					return &diagnosticScratchDirectory{read: func(n int) ([]os.DirEntry, error) {
						maxBatch = max(maxBatch, n)
						remaining := limits.Entries - 1 - cursor
						if uint64(n) > remaining+1 {
							t.Error("final ReadDir requested more than remaining plus one probe")
						}
						count := min(uint64(n), total-cursor)
						entries := make([]os.DirEntry, int(count))
						for i := range entries {
							entries[i] = diagnosticScratchEntry{name: "f" + strconv.FormatUint(cursor+uint64(i), 10)}
						}
						cursor += count
						if cursor == total {
							return entries, io.EOF
						}
						return entries, nil
					}, close: func() error { closes++; return nil }}, nil
				}}
			got, err := measureStudyDiagnosticScratchWithOps(root, limits, ops)
			if over {
				diagnosticScratchAssertFailure(t, got, err)
				if cursor != limits.Entries {
					t.Fatal("walker failed to stop at a single overflow probe")
				}
			} else if err != nil || got.VisitedEntries != limits.Entries || got.RegularFiles != limits.Entries-1 || got.RegularFileBytes != int64(limits.Entries-1) {
				t.Fatal("million-entry streaming total wrong:", err)
			}
			if maxBatch > 256 || opens != 1 || closes != 1 {
				t.Fatal("million boundary materialized entries or leaked handles")
			}
		})
	}
}

func TestDiagnosticScratchTopologyAndOverflow(t *testing.T) {
	for _, kind := range []string{"root_link", "child_link", "pipe", "negative_size", "byte_overflow"} {
		t.Run(kind, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "scratch")
			tree := diagnosticScratchTestTree(root)
			switch kind {
			case "root_link":
				tree.nodes[root] = diagnosticScratchInfo{mode: os.ModeDir | os.ModeSymlink}
			case "child_link":
				tree.add(root, "link", os.ModeSymlink, 0)
			case "pipe":
				tree.add(root, "pipe", os.ModeNamedPipe, 0)
			case "negative_size":
				tree.add(root, "negative", 0, -1)
			case "byte_overflow":
				tree.add(root, "maximum", 0, math.MaxInt64)
				tree.add(root, "overflow", 0, 1)
			}
			got, err := measureStudyDiagnosticScratchWithOps(root, diagnosticScratchTestLimits(), tree.ops())
			diagnosticScratchAssertFailure(t, got, err)
			if tree.active != 0 || tree.opens != tree.closes {
				t.Fatal("unsupported topology leaked directory handle")
			}
		})
	}
}
