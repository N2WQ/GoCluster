package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
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

func TestPackageFromRawUnionsBuildTaggedFilesAndImports(t *testing.T) {
	repoRoot := t.TempDir()
	packageDir := filepath.Join(repoRoot, "sample")
	if err := os.Mkdir(packageDir, 0o755); err != nil {
		t.Fatalf("create package directory: %v", err)
	}
	files := map[string]string{
		"common.go":              "package sample\nimport _ \"dxcluster/common\"\n",
		"impl_windows.go":        "//go:build windows\n\npackage sample\nimport _ \"dxcluster/windowsdep\"\n",
		"impl_other.go":          "//go:build !windows\n\npackage sample\nimport _ \"dxcluster/otherdep\"\n",
		"feature_cgo.go":         "//go:build cgo\n\npackage sample\nimport _ \"dxcluster/cgodep\"\n",
		"feature_nocgo.go":       "//go:build !cgo\n\npackage sample\nimport _ \"dxcluster/nocgodep\"\n",
		"sample_test.go":         "package sample\nimport _ \"dxcluster/testonly\"\n",
		"sample_windows_test.go": "//go:build windows\n\npackage sample\nimport _ \"dxcluster/windowstestonly\"\n",
		"sample_other_test.go":   "//go:build !windows\n\npackage sample\nimport _ \"dxcluster/othertestonly\"\n",
		"_ignored.go":            "package sample\nimport _ \"dxcluster/ignored\"\n",
		"ordinary-not-go.md":     "not Go source\n",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(packageDir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	windowsRaw := goPackageRaw{
		ImportPath:  "dxcluster/sample",
		Name:        "sample",
		Dir:         packageDir,
		GoFiles:     []string{"common.go", "impl_windows.go", "feature_nocgo.go"},
		TestGoFiles: []string{"sample_test.go", "sample_windows_test.go"},
		Imports:     []string{"dxcluster/common", "dxcluster/windowsdep", "dxcluster/nocgodep"},
	}
	linuxRaw := goPackageRaw{
		ImportPath:  "dxcluster/sample",
		Name:        "sample",
		Dir:         packageDir,
		GoFiles:     []string{"common.go", "impl_other.go", "feature_cgo.go"},
		TestGoFiles: []string{"sample_test.go", "sample_other_test.go"},
		Imports:     []string{"dxcluster/common", "dxcluster/otherdep", "dxcluster/cgodep"},
	}

	windowsInfo, err := packageFromRaw(repoRoot, windowsRaw)
	if err != nil {
		t.Fatalf("package from Windows metadata: %v", err)
	}
	linuxInfo, err := packageFromRaw(repoRoot, linuxRaw)
	if err != nil {
		t.Fatalf("package from Linux metadata: %v", err)
	}
	if !reflect.DeepEqual(windowsInfo, linuxInfo) {
		t.Fatalf("platform metadata changed package info:\nWindows: %+v\nLinux:   %+v", windowsInfo, linuxInfo)
	}

	wantGoFiles := []string{
		"sample/common.go",
		"sample/feature_cgo.go",
		"sample/feature_nocgo.go",
		"sample/impl_other.go",
		"sample/impl_windows.go",
	}
	wantTestFiles := []string{
		"sample/sample_other_test.go",
		"sample/sample_test.go",
		"sample/sample_windows_test.go",
	}
	wantImports := []string{
		"dxcluster/cgodep",
		"dxcluster/common",
		"dxcluster/nocgodep",
		"dxcluster/otherdep",
		"dxcluster/windowsdep",
	}
	if !reflect.DeepEqual(windowsInfo.GoFiles, wantGoFiles) {
		t.Fatalf("Go files = %v, want %v", windowsInfo.GoFiles, wantGoFiles)
	}
	if !reflect.DeepEqual(windowsInfo.TestFiles, wantTestFiles) {
		t.Fatalf("test files = %v, want %v", windowsInfo.TestFiles, wantTestFiles)
	}
	if !reflect.DeepEqual(windowsInfo.Imports, wantImports) {
		t.Fatalf("imports = %v, want %v", windowsInfo.Imports, wantImports)
	}

	spec := mapSpec{ID: "sample", Title: "Sample", Output: "docs/code-maps/sample.md", Packages: []string{"./sample"}}
	windowsData := buildMapData(spec, "dxcluster", []packageInfo{windowsInfo}, nil)
	windowsData.Fingerprint = fingerprint(windowsData)
	linuxData := buildMapData(spec, "dxcluster", []packageInfo{linuxInfo}, nil)
	linuxData.Fingerprint = fingerprint(linuxData)
	wantOutsideDeps := []repoDep{
		{From: "dxcluster/sample", To: "dxcluster/cgodep"},
		{From: "dxcluster/sample", To: "dxcluster/common"},
		{From: "dxcluster/sample", To: "dxcluster/nocgodep"},
		{From: "dxcluster/sample", To: "dxcluster/otherdep"},
		{From: "dxcluster/sample", To: "dxcluster/windowsdep"},
	}
	if !reflect.DeepEqual(windowsData.OutsideRepoDeps, wantOutsideDeps) {
		t.Fatalf("outside dependencies = %v, want %v", windowsData.OutsideRepoDeps, wantOutsideDeps)
	}
	if windowsData.Fingerprint != linuxData.Fingerprint || renderMarkdown(windowsData) != renderMarkdown(linuxData) {
		t.Fatal("platform metadata changed rendered map or fingerprint")
	}
}

func TestPackageFromRawFailsOnMalformedInactiveSource(t *testing.T) {
	repoRoot := t.TempDir()
	packageDir := filepath.Join(repoRoot, "sample")
	if err := os.Mkdir(packageDir, 0o755); err != nil {
		t.Fatalf("create package directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(packageDir, "common.go"), []byte("package sample\n"), 0o644); err != nil {
		t.Fatalf("write common source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(packageDir, "broken_linux.go"), []byte("//go:build linux\n\npackage sample\nimport (\n"), 0o644); err != nil {
		t.Fatalf("write malformed source: %v", err)
	}

	_, err := packageFromRaw(repoRoot, goPackageRaw{ImportPath: "dxcluster/sample", Name: "sample", Dir: packageDir})
	if err == nil || !strings.Contains(err.Error(), "broken_linux.go") {
		t.Fatalf("error = %v, want malformed file diagnostic", err)
	}
}

func TestADRLineEndingsDoNotChangeMap(t *testing.T) {
	lfRecords := loadADRFixture(t, "\n")
	crlfRecords := loadADRFixture(t, "\r\n")
	if !reflect.DeepEqual(lfRecords, crlfRecords) {
		t.Fatalf("ADR records differ by line endings:\nLF:   %+v\nCRLF: %+v", lfRecords, crlfRecords)
	}

	spec := mapSpec{ID: "sample", Title: "Sample", Output: "docs/code-maps/sample.md", Packages: []string{"./sample"}}
	packages := []packageInfo{{ImportPath: "dxcluster/sample", Name: "sample", RelDir: "sample", GoFiles: []string{"sample/sample.go"}}}
	lfData := buildMapData(spec, "dxcluster", packages, lfRecords)
	lfData.Fingerprint = fingerprint(lfData)
	crlfData := buildMapData(spec, "dxcluster", packages, crlfRecords)
	crlfData.Fingerprint = fingerprint(crlfData)
	if lfData.Fingerprint != crlfData.Fingerprint {
		t.Fatalf("fingerprints differ: LF=%s CRLF=%s", lfData.Fingerprint, crlfData.Fingerprint)
	}
	if renderMarkdown(lfData) != renderMarkdown(crlfData) {
		t.Fatal("rendered maps differ by ADR line endings")
	}
}

func loadADRFixture(t *testing.T, lineEnding string) []adrRecord {
	t.Helper()
	repoRoot := t.TempDir()
	decisionDir := filepath.Join(repoRoot, "docs", "decisions")
	if err := os.MkdirAll(decisionDir, 0o755); err != nil {
		t.Fatalf("create decision directory: %v", err)
	}
	log := "| ADR | Title | Status | Date | Area | Supersedes | Superseded By | Links |\n" +
		"| ADR-0002 | Sample | Accepted | 2026-01-01 | sample | - | - | `docs/decisions/ADR-0002-sample.md` |\n"
	adr := "# ADR-0002: Sample\n\nRelated package: `sample`.\n"
	log = strings.ReplaceAll(log, "\n", lineEnding)
	adr = strings.ReplaceAll(adr, "\n", lineEnding)
	if err := os.WriteFile(filepath.Join(repoRoot, "docs", "decision-log.md"), []byte(log), 0o644); err != nil {
		t.Fatalf("write decision log: %v", err)
	}
	if err := os.WriteFile(filepath.Join(decisionDir, "ADR-0002-sample.md"), []byte(adr), 0o644); err != nil {
		t.Fatalf("write ADR: %v", err)
	}
	records, err := loadADRs(repoRoot)
	if err != nil {
		t.Fatalf("load ADRs: %v", err)
	}
	return records
}

func TestEqualTextContentNormalizesOnlyLineEndings(t *testing.T) {
	lf := []byte("first\nsecond\n")
	crlf := []byte("first\r\nsecond\r\n")
	if !equalTextContent(lf, crlf) {
		t.Fatal("equivalent LF and CRLF content should match")
	}
	if equalTextContent(lf, []byte("first\r\nchanged\r\n")) {
		t.Fatal("substantive content change should not match")
	}
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
		"union across all checked-in Go build configurations",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered map missing %q\n%s", want, rendered)
		}
	}
	if strings.Contains(strings.ToLower(rendered), "last reviewed") {
		t.Fatalf("rendered map should not contain human review fields")
	}
}
