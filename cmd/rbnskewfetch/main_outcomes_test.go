package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

const skewCSV = "callsign,skew,spots,correction_factor\nk1abc-1,2,5,1\nW2XYZ,-2,7,1.0001\nK3LOW,1.99,1000,1.0002\n"

func TestSkewCommandOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name, body, diagnostic string
	}{
		{"success", skewCSV, ""},
		{"http-error", "", "unexpected status 500"},
		{"truncated-body", "x", "read response: unexpected EOF"},
		{"bad-csv", "\"unterminated", "parse csv:"},
		{"incomplete-row", "K1ABC,3,20\n", "invalid row"},
		{"bad-number", "K1ABC,invalid,20,1\n", "parse skew"},
		{"json-marshal", "K1ABC,2,20,NaN\n", "marshal json:"},
		{"header-only", "callsign,skew,spots,correction_factor\n", "no data rows"},
		{"all-filtered", "K1ABC,1.99,20,1\n", "no entries after filtering"},
		{"blocked-parent", skewCSV, "skew: mkdir"},
		{"directory-output", skewCSV, "skew: write file"},
		{"closed-output", skewCSV, "failed to report skew table"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				if tc.name == "http-error" {
					w.WriteHeader(500)
				} else {
					if tc.name == "truncated-body" {
						w.Header().Set("Content-Length", "100")
					}
					fmt.Fprint(w, tc.body)
				}
			}))
			t.Cleanup(server.Close)
			cfg := skewFixtureConfig(t, server.URL)
			output := filepath.Join(t.TempDir(), "table.json")
			old := []byte("previous table preserved verbatim\n")
			switch tc.name {
			case "blocked-parent":
				if err := os.WriteFile(output, old, 0o600); err != nil {
					t.Fatal(err)
				}
				output = filepath.Join(output, "table.json")
			case "directory-output":
				if err := os.Mkdir(output, 0o755); err != nil {
					t.Fatal(err)
				}
			default:
				if err := os.WriteFile(output, old, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			mode := "run"
			if tc.name == "closed-output" {
				mode = tc.name
			}
			code, out, diag := skewMainResult(t, cfg, output, mode)
			wantCode := 1
			if tc.name == "success" {
				wantCode = 0
			}
			if code != wantCode || requests.Load() != 1 || tc.diagnostic != "" && !strings.Contains(diag, tc.diagnostic) {
				t.Fatalf("status=%d requests=%d stdout=%q stderr=%q", code, requests.Load(), out, diag)
			}
			if tc.name == "success" || tc.name == "closed-output" {
				if tc.name == "success" && (diag != "" || out != fmt.Sprintf("Wrote 2 skew entries to %s\n", output)) || tc.name == "closed-output" && out != "" {
					t.Fatalf("success feedback: %q / %q", out, diag)
				}
				body, err := os.ReadFile(output)
				var rows []struct {
					Callsign string  `json:"callsign"`
					Factor   float64 `json:"correction_factor"`
				}
				decoder := json.NewDecoder(bytes.NewReader(body))
				decoder.DisallowUnknownFields()
				if err != nil || decoder.Decode(&rows) != nil || len(rows) != 2 {
					t.Fatalf("saved JSON invalid: %s / %v", body, err)
				}
				if rows[0].Callsign != "K1ABC-1" || rows[0].Factor != 1 || rows[1].Callsign != "W2XYZ" || rows[1].Factor != 1.0001 {
					t.Fatalf("inclusive +/- threshold or factor-1 retention changed: %+v", rows)
				}
			} else {
				if strings.Contains(out, "Wrote") || !strings.Contains(diag, "failed to fetch skew table") {
					t.Fatalf("failure feedback: %q / %q", out, diag)
				}
				if tc.name != "blocked-parent" && tc.name != "directory-output" {
					body, err := os.ReadFile(output)
					if err != nil || !bytes.Equal(body, old) {
						t.Fatalf("pre-write failure changed existing output: %q / %v", body, err)
					}
				}
			}
		})
	}
}

func TestSkewConfigFailuresBeforeFetch(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		fmt.Fprint(w, skewCSV)
	}))
	t.Cleanup(server.Close)
	for _, kind := range []string{"missing", "malformed"} {
		t.Run(kind, func(t *testing.T) {
			cfg := filepath.Join(t.TempDir(), "missing-config")
			if kind == "malformed" {
				cfg = skewFixtureConfig(t, server.URL)
				if err := os.WriteFile(filepath.Join(cfg, "data.yaml"), []byte("skew: ["), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			output := filepath.Join(t.TempDir(), "table.json")
			code, out, diag := skewMainResult(t, cfg, output, "run")
			if code != 1 || out != "" || !strings.Contains(diag, "failed to load config") || requests.Load() != 0 {
				t.Fatalf("config failure entered fetch or succeeded: %d / %q / %q / %d", code, out, diag, requests.Load())
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Fatalf("config failure created output: %v", err)
			}
		})
	}
}
