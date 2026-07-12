package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

func TestGoListSeparatesStdoutAndStderr(t *testing.T) {
	repoRoot := t.TempDir()
	packageJSON, err := json.Marshal(goPackageRaw{
		ImportPath: "dxcluster/sample",
		Name:       "sample",
		Dir:        repoRoot,
	})
	if err != nil {
		t.Fatalf("marshal package fixture: %v", err)
	}

	tests := []struct {
		name      string
		stdout    string
		stderr    string
		exitCode  int
		invoke    func() error
		wantError string
	}{
		{
			name:   "module success ignores diagnostic stderr",
			stdout: "dxcluster\n",
			stderr: "go: downloading module metadata\n",
			invoke: func() error {
				modulePath, err := goListModule(repoRoot)
				if err != nil {
					return err
				}
				if modulePath != "dxcluster" {
					return fmt.Errorf("module path = %q", modulePath)
				}
				return nil
			},
		},
		{
			name:   "package success ignores download stderr",
			stdout: string(packageJSON) + "\n",
			stderr: "go: downloading github.com/example/dependency v1.0.0\n",
			invoke: func() error {
				packages, err := goListPackages(repoRoot, []string{"./sample"})
				if err != nil {
					return err
				}
				if len(packages) != 1 || packages[0].ImportPath != "dxcluster/sample" {
					return fmt.Errorf("unexpected packages: %+v", packages)
				}
				return nil
			},
		},
		{
			name:     "module failure preserves stderr",
			stderr:   "module lookup failed",
			exitCode: 1,
			invoke: func() error {
				_, err := goListModule(repoRoot)
				return err
			},
			wantError: "module lookup failed",
		},
		{
			name:     "package failure preserves stderr",
			stderr:   "package lookup failed",
			exitCode: 1,
			invoke: func() error {
				_, err := goListPackages(repoRoot, []string{"./sample"})
				return err
			},
			wantError: "package lookup failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			installGoCommandFixture(t, tt.stdout, tt.stderr, tt.exitCode)
			err := tt.invoke()
			if tt.wantError == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("error = %v, want diagnostic %q", err, tt.wantError)
			}
		})
	}
}

func installGoCommandFixture(t *testing.T, stdout, stderr string, exitCode int) {
	t.Helper()
	original := newGoCommand
	newGoCommand = func(ctx context.Context, args ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestGoCommandHelperProcess", "--")
		cmd.Env = append(os.Environ(),
			"CODEMAP_GO_HELPER=1",
			"CODEMAP_GO_STDOUT="+stdout,
			"CODEMAP_GO_STDERR="+stderr,
			"CODEMAP_GO_EXIT="+strconv.Itoa(exitCode),
		)
		return cmd
	}
	t.Cleanup(func() { newGoCommand = original })
}

func TestGoCommandHelperProcess(t *testing.T) {
	t.Helper()
	if os.Getenv("CODEMAP_GO_HELPER") != "1" {
		return
	}
	_, _ = fmt.Fprint(os.Stdout, os.Getenv("CODEMAP_GO_STDOUT"))
	_, _ = fmt.Fprint(os.Stderr, os.Getenv("CODEMAP_GO_STDERR"))
	exitCode, err := strconv.Atoi(os.Getenv("CODEMAP_GO_EXIT"))
	if err != nil {
		os.Exit(2)
	}
	os.Exit(exitCode)
}

func TestSplitMarkdownTableRow(t *testing.T) {
	cells := splitMarkdownTableRow("| ADR-0140 | Title | Accepted | 2026-06-04 | workflow, Codex | - | - | `docs/decisions/ADR-0140-example.md` |")
	if len(cells) != 8 {
		t.Fatalf("expected 8 cells, got %d", len(cells))
	}
	if cells[0] != "ADR-0140" {
		t.Fatalf("unexpected first cell %q", cells[0])
	}
	if got := extractBacktickedPath(cells[7]); got != "docs/decisions/ADR-0140-example.md" {
		t.Fatalf("unexpected path %q", got)
	}
}

func TestMatchADRsUsesAreaAndPathReferences(t *testing.T) {
	packages := []packageInfo{
		{ImportPath: "dxcluster/internal/cluster"},
		{ImportPath: "dxcluster/telnet"},
	}
	records := []adrRecord{
		{
			ID:     "ADR-0002",
			Status: "Accepted",
			Date:   "2026-01-02",
			Area:   "internal/cluster, runtime",
			Path:   "docs/decisions/ADR-0002-cluster.md",
		},
		{
			ID:      "ADR-0001",
			Status:  "Accepted",
			Date:    "2026-01-01",
			Area:    "docs",
			Path:    "docs/decisions/ADR-0001-telnet.md",
			Content: "Related docs: `telnet/README.md`",
		},
		{
			ID:      "ADR-0003",
			Status:  "Accepted",
			Date:    "2026-01-03",
			Area:    "config",
			Path:    "docs/decisions/ADR-0003-config.md",
			Content: "No scoped package reference.",
		},
	}

	matched := matchADRs("dxcluster", packages, records)
	if len(matched) != 2 {
		t.Fatalf("expected 2 matched ADRs, got %d", len(matched))
	}
	if matched[0].Record.ID != "ADR-0002" || !strings.Contains(matched[0].Match, "area:internal/cluster") {
		t.Fatalf("unexpected first match: %+v", matched[0])
	}
	if matched[1].Record.ID != "ADR-0001" || !strings.Contains(matched[1].Match, "path:telnet") {
		t.Fatalf("unexpected second match: %+v", matched[1])
	}
}

func TestRenderMarkdownIsGeneratedOnly(t *testing.T) {
	data := mapData{
		Spec: mapSpec{
			ID:     "sample",
			Title:  "Sample",
			Output: "docs/code-maps/sample.md",
		},
		ModulePath:  "dxcluster",
		Fingerprint: "abc123",
		Packages: []packageInfo{{
			ImportPath: "dxcluster/telnet",
			RelDir:     "telnet",
			GoFiles:    []string{"telnet/server.go"},
		}},
	}

	rendered := renderMarkdown(data)
	for _, want := range []string{
		"<!-- GENERATED by cmd/codemap. Do not edit by hand. -->",
		"- Source fingerprint: `abc123`",
		"## In-Scope Package Edges",
		"## Related ADRs",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered map missing %q\n%s", want, rendered)
		}
	}
	if strings.Contains(strings.ToLower(rendered), "last reviewed") {
		t.Fatalf("rendered map should not contain human review fields")
	}
}
