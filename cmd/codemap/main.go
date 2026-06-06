package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
)

const defaultManifestPath = "docs/code-maps/manifest.json"

type manifest struct {
	Version int       `json:"version"`
	Maps    []mapSpec `json:"maps"`
}

type mapSpec struct {
	ID       string   `json:"id"`
	Title    string   `json:"title"`
	Output   string   `json:"output"`
	Packages []string `json:"packages"`
}

type goPackageRaw struct {
	ImportPath   string   `json:"ImportPath"`
	Name         string   `json:"Name"`
	Dir          string   `json:"Dir"`
	GoFiles      []string `json:"GoFiles"`
	TestGoFiles  []string `json:"TestGoFiles"`
	XTestGoFiles []string `json:"XTestGoFiles"`
	Imports      []string `json:"Imports"`
}

type packageInfo struct {
	ImportPath string
	Name       string
	Dir        string
	RelDir     string
	GoFiles    []string
	TestFiles  []string
	Imports    []string
}

type edge struct {
	From string
	To   string
}

type repoDep struct {
	From string
	To   string
}

type adrRecord struct {
	ID          string
	Title       string
	Status      string
	Date        string
	Area        string
	Path        string
	Content     string
	ContentHash string
}

type relatedADR struct {
	Record adrRecord
	Match  string
}

type mapData struct {
	Spec            mapSpec
	ModulePath      string
	Fingerprint     string
	Packages        []packageInfo
	Edges           []edge
	OutsideRepoDeps []repoDep
	RelatedADRs     []relatedADR
}

type packageTerm struct {
	ImportPath string
	RelPath    string
	Base       string
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "codemap: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("expected command: list, generate, or check")
	}

	command := args[0]
	flags := flag.NewFlagSet("codemap "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	manifestPath := flags.String("manifest", defaultManifestPath, "path to code-map manifest")
	mapID := flags.String("map", "", "map id to process")
	all := flags.Bool("all", false, "process all maps")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}

	repoRoot, err := findRepoRoot(".")
	if err != nil {
		return err
	}

	m, err := loadManifest(filepath.Join(repoRoot, filepath.FromSlash(*manifestPath)))
	if err != nil {
		return err
	}
	if err := validateManifest(m); err != nil {
		return err
	}

	specs, err := selectSpecs(m.Maps, *mapID, *all, command == "list")
	if err != nil {
		return err
	}

	switch command {
	case "list":
		for _, spec := range specs {
			fmt.Fprintf(stdout, "%s\t%s\t%s\n", spec.ID, spec.Title, spec.Output)
		}
		return nil
	case "generate":
		for _, spec := range specs {
			content, err := generateMap(repoRoot, spec)
			if err != nil {
				return fmt.Errorf("%s: %w", spec.ID, err)
			}
			outPath := filepath.Join(repoRoot, filepath.FromSlash(spec.Output))
			if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
				return fmt.Errorf("create output directory: %w", err)
			}
			if err := os.WriteFile(outPath, content, 0o644); err != nil {
				return fmt.Errorf("write %s: %w", spec.Output, err)
			}
			fmt.Fprintf(stdout, "generated %s\n", spec.Output)
		}
		return nil
	case "check":
		var stale []string
		for _, spec := range specs {
			content, err := generateMap(repoRoot, spec)
			if err != nil {
				return fmt.Errorf("%s: %w", spec.ID, err)
			}
			outPath := filepath.Join(repoRoot, filepath.FromSlash(spec.Output))
			existing, err := os.ReadFile(outPath)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					stale = append(stale, spec.Output+" (missing)")
					continue
				}
				return fmt.Errorf("read %s: %w", spec.Output, err)
			}
			if !bytes.Equal(existing, content) {
				stale = append(stale, spec.Output)
			}
		}
		if len(stale) > 0 {
			for _, file := range stale {
				fmt.Fprintf(stderr, "STALE %s\n", file)
			}
			return fmt.Errorf("%d code map(s) stale; run go run ./cmd/codemap generate -all", len(stale))
		}
		fmt.Fprintln(stdout, "code maps are fresh")
		return nil
	default:
		return fmt.Errorf("unknown command %q; expected list, generate, or check", command)
	}
}

func findRepoRoot(start string) (string, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("unable to find repository root containing go.mod")
		}
		dir = parent
	}
}

func loadManifest(path string) (manifest, error) {
	var m manifest
	// #nosec G304 G703 -- the manifest path is a developer-supplied repo workflow input.
	data, err := os.ReadFile(path)
	if err != nil {
		return m, fmt.Errorf("read manifest: %w", err)
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, fmt.Errorf("parse manifest: %w", err)
	}
	return m, nil
}

func validateManifest(m manifest) error {
	if m.Version != 1 {
		return fmt.Errorf("unsupported manifest version %d", m.Version)
	}
	seen := map[string]bool{}
	for _, spec := range m.Maps {
		if strings.TrimSpace(spec.ID) == "" {
			return errors.New("map id is required")
		}
		if seen[spec.ID] {
			return fmt.Errorf("duplicate map id %q", spec.ID)
		}
		seen[spec.ID] = true
		if strings.TrimSpace(spec.Title) == "" {
			return fmt.Errorf("%s: title is required", spec.ID)
		}
		if !strings.HasPrefix(filepath.ToSlash(spec.Output), "docs/code-maps/") || !strings.HasSuffix(spec.Output, ".md") {
			return fmt.Errorf("%s: output must be a Markdown file under docs/code-maps", spec.ID)
		}
		if len(spec.Packages) == 0 {
			return fmt.Errorf("%s: at least one package is required", spec.ID)
		}
	}
	return nil
}

func selectSpecs(specs []mapSpec, mapID string, all bool, listCommand bool) ([]mapSpec, error) {
	if listCommand && !all && strings.TrimSpace(mapID) == "" {
		return specs, nil
	}
	if all && strings.TrimSpace(mapID) != "" {
		return nil, errors.New("use either -all or -map, not both")
	}
	if !all && strings.TrimSpace(mapID) == "" {
		return nil, errors.New("use -all or -map <id>")
	}
	if all {
		return specs, nil
	}
	for _, spec := range specs {
		if spec.ID == mapID {
			return []mapSpec{spec}, nil
		}
	}
	return nil, fmt.Errorf("map %q not found", mapID)
}

func generateMap(repoRoot string, spec mapSpec) ([]byte, error) {
	modulePath, err := goListModule(repoRoot)
	if err != nil {
		return nil, err
	}
	packages, err := goListPackages(repoRoot, spec.Packages)
	if err != nil {
		return nil, err
	}
	adrs, err := loadADRs(repoRoot)
	if err != nil {
		return nil, err
	}

	data := buildMapData(spec, modulePath, packages, adrs)
	data.Fingerprint = fingerprint(data)
	return []byte(renderMarkdown(data)), nil
}

func goListModule(repoRoot string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "list", "-m")
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("go list -m failed: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	modulePath := strings.TrimSpace(string(out))
	if modulePath == "" {
		return "", errors.New("go list -m returned empty module path")
	}
	return modulePath, nil
}

func goListPackages(repoRoot string, patterns []string) ([]packageInfo, error) {
	args := append([]string{"list", "-json"}, patterns...)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("go %s failed: %w\n%s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}

	var packages []packageInfo
	dec := json.NewDecoder(bytes.NewReader(out))
	for {
		var raw goPackageRaw
		if err := dec.Decode(&raw); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode go list output: %w", err)
		}
		info, err := packageFromRaw(repoRoot, raw)
		if err != nil {
			return nil, err
		}
		packages = append(packages, info)
	}
	sort.Slice(packages, func(i, j int) bool {
		return packages[i].ImportPath < packages[j].ImportPath
	})
	return packages, nil
}

func packageFromRaw(repoRoot string, raw goPackageRaw) (packageInfo, error) {
	relDir, err := relPath(repoRoot, raw.Dir)
	if err != nil {
		return packageInfo{}, err
	}
	info := packageInfo{
		ImportPath: raw.ImportPath,
		Name:       raw.Name,
		Dir:        raw.Dir,
		RelDir:     relDir,
		GoFiles:    fileList(repoRoot, raw.Dir, raw.GoFiles),
		TestFiles:  fileList(repoRoot, raw.Dir, append(raw.TestGoFiles, raw.XTestGoFiles...)),
		Imports:    sortedUnique(raw.Imports),
	}
	return info, nil
}

func fileList(repoRoot, dir string, names []string) []string {
	files := make([]string, 0, len(names))
	for _, name := range names {
		rel, err := relPath(repoRoot, filepath.Join(dir, name))
		if err != nil {
			continue
		}
		files = append(files, rel)
	}
	return sortedUnique(files)
}

func relPath(repoRoot, absPath string) (string, error) {
	rel, err := filepath.Rel(repoRoot, absPath)
	if err != nil {
		return "", err
	}
	return filepath.ToSlash(rel), nil
}

func buildMapData(spec mapSpec, modulePath string, packages []packageInfo, adrs []adrRecord) mapData {
	scope := map[string]bool{}
	for i := range packages {
		pkg := &packages[i]
		scope[pkg.ImportPath] = true
	}

	var edges []edge
	var outside []repoDep
	seenEdges := map[string]bool{}
	seenOutside := map[string]bool{}
	for i := range packages {
		pkg := &packages[i]
		for _, imp := range pkg.Imports {
			if scope[imp] {
				key := pkg.ImportPath + "\x00" + imp
				if !seenEdges[key] {
					edges = append(edges, edge{From: pkg.ImportPath, To: imp})
					seenEdges[key] = true
				}
				continue
			}
			if isRepoImport(modulePath, imp) {
				key := pkg.ImportPath + "\x00" + imp
				if !seenOutside[key] {
					outside = append(outside, repoDep{From: pkg.ImportPath, To: imp})
					seenOutside[key] = true
				}
			}
		}
	}
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From == edges[j].From {
			return edges[i].To < edges[j].To
		}
		return edges[i].From < edges[j].From
	})
	sort.Slice(outside, func(i, j int) bool {
		if outside[i].From == outside[j].From {
			return outside[i].To < outside[j].To
		}
		return outside[i].From < outside[j].From
	})

	return mapData{
		Spec:            spec,
		ModulePath:      modulePath,
		Packages:        packages,
		Edges:           edges,
		OutsideRepoDeps: outside,
		RelatedADRs:     matchADRs(modulePath, packages, adrs),
	}
}

func isRepoImport(modulePath, importPath string) bool {
	return importPath == modulePath || strings.HasPrefix(importPath, modulePath+"/")
}

func loadADRs(repoRoot string) ([]adrRecord, error) {
	logPath := filepath.Join(repoRoot, "docs", "decision-log.md")
	data, err := os.ReadFile(logPath)
	if err != nil {
		return nil, fmt.Errorf("read decision log: %w", err)
	}
	var records []adrRecord
	for _, line := range strings.Split(string(data), "\n") {
		cells := splitMarkdownTableRow(line)
		if len(cells) < 8 || !strings.HasPrefix(cells[0], "ADR-") {
			continue
		}
		rec := adrRecord{
			ID:     cells[0],
			Title:  cells[1],
			Status: cells[2],
			Date:   cells[3],
			Area:   cells[4],
			Path:   extractBacktickedPath(cells[7]),
		}
		if rec.ID == "ADR-0001" || strings.Contains(rec.Path, "<") || strings.Contains(rec.Date, "YYYY") {
			continue
		}
		if rec.Path != "" {
			content, err := os.ReadFile(filepath.Join(repoRoot, filepath.FromSlash(rec.Path)))
			if err == nil {
				rec.Content = string(content)
				sum := sha256.Sum256(content)
				rec.ContentHash = hex.EncodeToString(sum[:])[:16]
			}
		}
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].ID > records[j].ID
	})
	return records, nil
}

func splitMarkdownTableRow(line string) []string {
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "|") || !strings.HasSuffix(line, "|") {
		return nil
	}
	line = strings.TrimPrefix(strings.TrimSuffix(line, "|"), "|")
	parts := strings.Split(line, "|")
	cells := make([]string, 0, len(parts))
	for _, part := range parts {
		cells = append(cells, strings.TrimSpace(part))
	}
	return cells
}

func extractBacktickedPath(cell string) string {
	start := strings.Index(cell, "`")
	if start < 0 {
		return ""
	}
	rest := cell[start+1:]
	end := strings.Index(rest, "`")
	if end < 0 {
		return ""
	}
	return strings.TrimSpace(rest[:end])
}

func matchADRs(modulePath string, packages []packageInfo, records []adrRecord) []relatedADR {
	terms := buildPackageTerms(modulePath, packages)
	var related []relatedADR
	for i := range records {
		rec := &records[i]
		reasons := map[string]bool{}
		for _, term := range terms {
			if matchADRArea(rec.Area, term) {
				reasons["area:"+term.RelPath] = true
			}
			if matchADRContent(rec.Content, term) {
				reasons["path:"+term.RelPath] = true
			}
		}
		if len(reasons) == 0 {
			continue
		}
		reasonList := make([]string, 0, len(reasons))
		for reason := range reasons {
			reasonList = append(reasonList, reason)
		}
		sort.Strings(reasonList)
		related = append(related, relatedADR{Record: *rec, Match: strings.Join(reasonList, ", ")})
	}
	sort.Slice(related, func(i, j int) bool {
		return related[i].Record.ID > related[j].Record.ID
	})
	return related
}

func buildPackageTerms(modulePath string, packages []packageInfo) []packageTerm {
	seen := map[string]bool{}
	var terms []packageTerm
	for i := range packages {
		pkg := &packages[i]
		var rel string
		if pkg.ImportPath == modulePath {
			rel = "."
		} else {
			rel = strings.TrimPrefix(pkg.ImportPath, modulePath+"/")
		}
		if rel == "." || seen[rel] {
			continue
		}
		seen[rel] = true
		terms = append(terms, packageTerm{
			ImportPath: pkg.ImportPath,
			RelPath:    rel,
			Base:       path.Base(rel),
		})
	}
	sort.Slice(terms, func(i, j int) bool {
		return terms[i].RelPath < terms[j].RelPath
	})
	return terms
}

func matchADRArea(area string, term packageTerm) bool {
	normalized := strings.ToLower(strings.ReplaceAll(area, "\\", "/"))
	for _, token := range strings.FieldsFunc(normalized, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '/' && r != '_' && r != '-'
	}) {
		if token == strings.ToLower(term.RelPath) || token == strings.ToLower(term.Base) {
			return true
		}
	}
	return false
}

func matchADRContent(content string, term packageTerm) bool {
	if strings.TrimSpace(content) == "" {
		return false
	}
	normalized := strings.ToLower(strings.ReplaceAll(content, "\\", "/"))
	rel := strings.ToLower(term.RelPath)
	importPath := strings.ToLower(term.ImportPath)
	candidates := []string{
		"`" + rel + "`",
		"`" + rel + "/",
		rel + "/",
		importPath,
	}
	for _, candidate := range candidates {
		if strings.Contains(normalized, candidate) {
			return true
		}
	}
	return false
}

func fingerprint(data mapData) string {
	h := sha256.New()
	fmt.Fprintf(h, "id:%s\n", data.Spec.ID)
	fmt.Fprintf(h, "title:%s\n", data.Spec.Title)
	fmt.Fprintf(h, "output:%s\n", data.Spec.Output)
	fmt.Fprintf(h, "module:%s\n", data.ModulePath)
	for _, pattern := range data.Spec.Packages {
		fmt.Fprintf(h, "pattern:%s\n", pattern)
	}
	for i := range data.Packages {
		pkg := &data.Packages[i]
		fmt.Fprintf(h, "pkg:%s:%s:%s\n", pkg.ImportPath, pkg.Name, pkg.RelDir)
		for _, file := range pkg.GoFiles {
			fmt.Fprintf(h, "gofile:%s\n", file)
		}
		for _, file := range pkg.TestFiles {
			fmt.Fprintf(h, "testfile:%s\n", file)
		}
		for _, imp := range pkg.Imports {
			fmt.Fprintf(h, "import:%s\n", imp)
		}
	}
	for i := range data.RelatedADRs {
		adr := &data.RelatedADRs[i]
		fmt.Fprintf(h, "adr:%s:%s:%s:%s:%s:%s\n", adr.Record.ID, adr.Record.Status, adr.Record.Date, adr.Record.Area, adr.Record.Path, adr.Record.ContentHash)
		fmt.Fprintf(h, "adrmatch:%s\n", adr.Match)
	}
	sum := h.Sum(nil)
	return hex.EncodeToString(sum)[:16]
}

func renderMarkdown(data mapData) string {
	var b strings.Builder
	b.WriteString("<!-- GENERATED by cmd/codemap. Do not edit by hand. -->\n")
	b.WriteString("# Code Map: " + data.Spec.Title + "\n\n")
	b.WriteString("- Map ID: `" + data.Spec.ID + "`\n")
	b.WriteString("- Source fingerprint: `" + data.Fingerprint + "`\n")
	b.WriteString("- Generated from: `docs/code-maps/manifest.json`\n")
	b.WriteString("- Regenerate: `go run ./cmd/codemap generate -map " + data.Spec.ID + "`\n")
	b.WriteString("- Check: `go run ./cmd/codemap check -map " + data.Spec.ID + "`\n\n")

	b.WriteString("## Scope Packages\n\n")
	b.WriteString("| Package | Directory | Go files | Test files |\n")
	b.WriteString("|---|---|---:|---:|\n")
	for i := range data.Packages {
		pkg := &data.Packages[i]
		fmt.Fprintf(&b, "| `%s` | `%s` | %d | %d |\n", escapeCell(pkg.ImportPath), escapeCell(pkg.RelDir), len(pkg.GoFiles), len(pkg.TestFiles))
	}
	b.WriteString("\n")

	b.WriteString("## In-Scope Package Edges\n\n")
	if len(data.Edges) == 0 {
		b.WriteString("- None.\n\n")
	} else {
		b.WriteString("| From | Imports |\n")
		b.WriteString("|---|---|\n")
		for _, edge := range data.Edges {
			fmt.Fprintf(&b, "| `%s` | `%s` |\n", escapeCell(edge.From), escapeCell(edge.To))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Direct Repo Dependencies Outside Scope\n\n")
	if len(data.OutsideRepoDeps) == 0 {
		b.WriteString("- None.\n\n")
	} else {
		b.WriteString("| From | Imports |\n")
		b.WriteString("|---|---|\n")
		for _, dep := range data.OutsideRepoDeps {
			fmt.Fprintf(&b, "| `%s` | `%s` |\n", escapeCell(dep.From), escapeCell(dep.To))
		}
		b.WriteString("\n")
	}

	b.WriteString("## Package Files\n\n")
	for i := range data.Packages {
		pkg := &data.Packages[i]
		b.WriteString("### `" + pkg.ImportPath + "`\n\n")
		b.WriteString("Source files:\n")
		writeList(&b, pkg.GoFiles)
		b.WriteString("\nTest files:\n")
		writeList(&b, pkg.TestFiles)
		b.WriteString("\n")
	}

	b.WriteString("## Related ADRs\n\n")
	if len(data.RelatedADRs) == 0 {
		b.WriteString("- None matched by package area or path references.\n\n")
	} else {
		b.WriteString("| ADR | Status | Date | Area | Match |\n")
		b.WriteString("|---|---|---|---|---|\n")
		for i := range data.RelatedADRs {
			adr := &data.RelatedADRs[i]
			link := adr.Record.ID
			if adr.Record.Path != "" {
				link = "[" + adr.Record.ID + "](" + adr.Record.Path + ")"
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s | `%s` |\n",
				link,
				escapeCell(adr.Record.Status),
				escapeCell(adr.Record.Date),
				escapeCell(adr.Record.Area),
				escapeCell(adr.Match),
			)
		}
		b.WriteString("\n")
	}

	b.WriteString("## Limits\n\n")
	b.WriteString("- Package imports are static compile-time metadata from `go list -json`.\n")
	b.WriteString("- This map does not prove interface dispatch, goroutine lifecycle, runtime feature flags, data flow, or concrete traffic paths.\n")
	b.WriteString("- ADR matching is deterministic text and metadata matching against scoped package paths and area terms; inspect the ADR before treating it as behavioral evidence.\n")
	return b.String()
}

func writeList(b *strings.Builder, values []string) {
	if len(values) == 0 {
		b.WriteString("- None.\n")
		return
	}
	for _, value := range values {
		b.WriteString("- `" + value + "`\n")
	}
}

func escapeCell(s string) string {
	s = strings.ReplaceAll(s, "\r", " ")
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "|", `\|`)
	return s
}

func sortedUnique(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
