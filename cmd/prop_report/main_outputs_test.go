package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// A child of a regular file cannot exist. Windows commonly reports absence;
// Unix reports ENOTDIR. This allowance belongs only to the blocked-parent cases.
func propBlockedChildAbsent(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ENOTDIR)
}

func TestPropBlockedChildAbsenceErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"absent", os.ErrNotExist, true},
		{"not-directory", syscall.ENOTDIR, true},
		{"stat-absent", &os.PathError{Op: "stat", Path: "blocked/child", Err: os.ErrNotExist}, true},
		{"stat-not-directory", &os.PathError{Op: "stat", Path: "blocked/child", Err: syscall.ENOTDIR}, true},
		{"successful-lookup", nil, false},
		{"permission", os.ErrPermission, false},
		{"stat-permission", &os.PathError{Op: "stat", Path: "blocked/child", Err: syscall.EACCES}, false},
		{"io-failure", &os.PathError{Op: "stat", Path: "blocked/child", Err: syscall.EIO}, false},
		{"invalid", os.ErrInvalid, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := propBlockedChildAbsent(tc.err); got != tc.want {
				t.Fatalf("blocked child error %v classified absent=%v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func assertPropJSON(t *testing.T, f propFixture) []byte {
	t.Helper()
	body, err := os.ReadFile(f.json)
	var summary struct {
		Date     string `json:"date_utc"`
		Log      string `json:"log_file"`
		Timezone string `json:"timezone"`
		Bands    []struct {
			Band  string `json:"band"`
			Hours []struct {
				Hour string `json:"hour"`
				F    int    `json:"f_med"`
				G    int    `json:"ge10_med"`
				L    int    `json:"lt1_med"`
			} `json:"hours"`
		} `json:"bands"`
	}
	if err != nil || json.Unmarshal(body, &summary) != nil || summary.Date != "2026-10-09" || summary.Timezone != "UTC" || summary.Log != f.log || len(summary.Bands) != 1 {
		t.Fatalf("invalid saved observation summary: %s / %v", body, err)
	}
	b := summary.Bands[0]
	if b.Band != "20m" || len(b.Hours) != 1 || b.Hours[0].Hour != "12:00" || b.Hours[0].F != 11 || b.Hours[0].G != 7 || b.Hours[0].L != 3 {
		t.Fatalf("literal input observations not reflected: %+v", b)
	}
	return body
}

func TestPropHardFailureOutcomes(t *testing.T) {
	for _, kind := range []string{"success", "invalid-date", "missing-config", "missing-log", "long-line", "json-parent", "json-directory", "report-parent", "report-directory", "closed-output"} {
		t.Run(kind, func(t *testing.T) {
			f := newPropFixture(t)
			mode, expected, diagnostic := "run", 1, "prop report generation failed"
			var blocked string
			const blockerContents = "not a directory"
			switch kind {
			case "success":
				expected, diagnostic = 0, ""
			case "missing-config":
				f.config = filepath.Join(t.TempDir(), "missing-config")
				diagnostic = "load config directory"
			case "missing-log":
				f.log = filepath.Join(t.TempDir(), "missing.log")
				diagnostic = "missing.log"
			case "long-line":
				if err := os.WriteFile(f.log, []byte(strings.Repeat("x", (1<<20)+1)+"\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				diagnostic = "token too long"
			case "json-parent", "report-parent":
				blocked = filepath.Join(t.TempDir(), "blocked")
				if err := os.WriteFile(blocked, []byte(blockerContents), 0o600); err != nil {
					t.Fatal(err)
				}
				if kind == "json-parent" {
					f.json = filepath.Join(blocked, "summary.json")
				} else {
					f.report = filepath.Join(blocked, "report.md")
				}
				diagnostic = "blocked"
			case "json-directory", "report-directory":
				path := f.json
				if kind == "report-directory" {
					path = f.report
				}
				if err := os.Mkdir(path, 0o755); err != nil {
					t.Fatal(err)
				}
				diagnostic = filepath.Base(path)
			case "closed-output":
				mode, diagnostic = kind, "prop report output failed"
			}
			args := append(f.args(), "-no-llm")
			if kind == "invalid-date" {
				args = append(args, "-date", "bad-date")
				diagnostic = "invalid date"
			}
			code, out, diag := propMainResult(t, args, mode)
			if code != expected || diagnostic != "" && !strings.Contains(diag, diagnostic) {
				t.Fatalf("status=%d stdout=%q stderr=%q; want %d/%q", code, out, diag, expected, diagnostic)
			}
			if blocked != "" {
				info, err := os.Lstat(blocked)
				if err != nil || !info.Mode().IsRegular() {
					t.Fatalf("output failure did not preserve regular blocker: %v", err)
				}
				body, err := os.ReadFile(blocked)
				if err != nil || string(body) != blockerContents {
					t.Fatalf("output failure changed blocker: %q / %v", body, err)
				}
			}
			jsonSaved := kind == "success" || kind == "report-parent" || kind == "report-directory" || kind == "closed-output"
			if jsonSaved {
				assertPropJSON(t, f)
			} else if kind != "json-directory" {
				_, err := os.Stat(f.json)
				absent := os.IsNotExist(err)
				if kind == "json-parent" {
					absent = propBlockedChildAbsent(err)
				}
				if !absent {
					t.Fatalf("unexpected early JSON output: %v", err)
				}
			}
			if kind == "success" || kind == "closed-output" {
				body, err := os.ReadFile(f.report)
				if err != nil || !strings.Contains(string(body), "2026-10-09") || strings.Contains(string(body), "LLM narrative") {
					t.Fatalf("local report absent/changed: %s / %v", body, err)
				}
			} else if kind != "report-directory" {
				_, err := os.Stat(f.report)
				absent := os.IsNotExist(err)
				if kind == "report-parent" {
					absent = propBlockedChildAbsent(err)
				}
				if !absent {
					t.Fatalf("unexpected Markdown effect: %v", err)
				}
			}
			if kind == "success" {
				want := fmt.Sprintf("Wrote JSON summary: %s\nWrote report: %s\n", f.json, f.report)
				if out != want || diag != "" {
					t.Fatalf("successful feedback mismatch: %q / %q", out, diag)
				}
			} else if out != "" {
				t.Fatalf("failed command reported success: %q", out)
			}
		})
	}
}
