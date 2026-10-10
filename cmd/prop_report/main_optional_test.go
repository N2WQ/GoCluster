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

	"gopkg.in/yaml.v3"
)

func propOpenAIConfig(t *testing.T, f propFixture, endpoint, kind string) {
	t.Helper()
	doc := map[string]any{"api_key": "fixture-only", "model": "fixture-model", "endpoint": endpoint,
		"max_tokens": 100, "temperature": 0, "system_prompt": "Summarize the supplied observations."}
	switch kind {
	case "invalid":
		doc["max_tokens"] = 0
	case "blank-key":
		doc["api_key"] = ""
	}
	body, err := yaml.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if kind == "malformed" {
		body = []byte("model: [")
	}
	if err := os.WriteFile(f.openai, body, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestPropOptionalConfigHardFailures(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { requests.Add(1); w.WriteHeader(503) }))
	t.Cleanup(server.Close)
	for _, kind := range []string{"missing", "malformed", "invalid", "blank-key"} {
		t.Run(kind, func(t *testing.T) {
			f := newPropFixture(t)
			if kind != "missing" {
				propOpenAIConfig(t, f, server.URL, kind)
			}
			code, out, diag := propMainResult(t, f.args(), "run")
			cause := map[string]string{"missing": "openai.yaml", "malformed": "yaml:", "invalid": "max_tokens", "blank-key": "API key missing"}[kind]
			if code != 1 || out != "" || !strings.Contains(diag, "load OpenAI config") || !strings.Contains(diag, cause) || requests.Load() != 0 {
				t.Fatalf("optional config failure bypassed: %d / %q / %q / %d", code, out, diag, requests.Load())
			}
			for _, path := range []string{f.json, f.report} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Fatalf("config failure wrote %s: %v", path, err)
				}
			}
		})
	}
}

func TestPropLocalNarrativeFallback(t *testing.T) {
	for _, kind := range []string{"http-error", "truncated", "malformed", "no-choices", "narrative", "empty-narrative"} {
		t.Run(kind, func(t *testing.T) {
			f := newPropFixture(t)
			code, _, diag := propMainResult(t, append(f.args(), "-no-llm"), "run")
			if code != 0 || diag != "" {
				t.Fatalf("local positive control failed: %d / %s", code, diag)
			}
			localJSON := assertPropJSON(t, f)
			localReport, err := os.ReadFile(f.report)
			if err != nil {
				t.Fatal(err)
			}
			f.json, f.report = filepath.Join(t.TempDir(), "summary.json"), filepath.Join(t.TempDir(), "report.md")
			var requests atomic.Int32
			var requestOK atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				var req struct {
					Model    string
					Messages []struct{ Role, Content string }
				}
				err := json.NewDecoder(r.Body).Decode(&req)
				requestOK.Store(err == nil && r.Method == "POST" && r.Header.Get("Authorization") == "Bearer fixture-only" && req.Model == "fixture-model" && len(req.Messages) == 2 && req.Messages[1].Content == string(localJSON))
				switch kind {
				case "http-error":
					w.WriteHeader(503)
				case "truncated":
					w.Header().Set("Content-Length", "100")
					fmt.Fprint(w, "{")
				case "malformed":
					fmt.Fprint(w, "not-json")
				case "no-choices":
					fmt.Fprint(w, `{"choices":[]}`)
				case "narrative":
					fmt.Fprint(w, `{"choices":[{"message":{"content":" Fixture narrative. "}}]}`)
				case "empty-narrative":
					fmt.Fprint(w, `{"choices":[{"message":{"content":"  "}}]}`)
				}
			}))
			t.Cleanup(server.Close)
			propOpenAIConfig(t, f, server.URL, "valid")
			code, out, diag := propMainResult(t, f.args(), "run")
			if code != 0 || requests.Load() != 1 || !requestOK.Load() || !strings.Contains(out, "Wrote report:") {
				t.Fatalf("request/output path absent: %d / %d / %v / %q / %q", code, requests.Load(), requestOK.Load(), out, diag)
			}
			if !bytes.Equal(localJSON, assertPropJSON(t, f)) {
				t.Fatal("optional narrative changed observation summary")
			}
			got, err := os.ReadFile(f.report)
			if err != nil {
				t.Fatal(err)
			}
			want := string(localReport)
			if kind == "narrative" {
				want = strings.TrimSuffix(want, "\n") + "\n\nLLM narrative\n\nFixture narrative.\n\n"
			}
			if string(got) != want {
				t.Fatalf("local fallback/narrative changed: got %q want %q", got, want)
			}
			failed := kind != "narrative" && kind != "empty-narrative"
			if failed != strings.Contains(diag, "Warning: OpenAI request failed:") || !failed && diag != "" {
				t.Fatalf("warning semantics changed: %q", diag)
			}
			cause := map[string]string{"http-error": "HTTP 503", "truncated": "read OpenAI response: unexpected EOF", "malformed": "parse OpenAI response:", "no-choices": "had no choices"}[kind]
			if failed && !strings.Contains(diag, cause) {
				t.Fatalf("wrong request failure was credited: %q; want %q", diag, cause)
			}
		})
	}
}
