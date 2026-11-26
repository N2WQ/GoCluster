package main

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"dxcluster/config"
	"dxcluster/internal/toxicity"
)

func TestPublicAssetClassification(t *testing.T) {
	// Check the shipped files against the actual loaders, then rename one
	// configured file and turn correction off. Startup still reads explicit
	// reliability files even when active correction is disabled.
	dir := filepath.Join("..", "..", "data", "config")
	cfg, err := config.Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	tc, _, err := toxicity.LoadConfig(filepath.Join(dir, "toxicity.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.CallCorrection.Enabled = false
	cfg.CallCorrection.ConfusionModelEnabled = false
	cfg.CallCorrection.SpotterReliabilityFile = "data/rbn/priors/renamed.txt"
	inputs := startupInputs(cfg, tc)
	var required []string
	for _, in := range inputs {
		paths, err := in.paths()
		if err != nil {
			t.Fatal(err)
		}
		required = append(required, paths...)
	}
	for _, name := range []string{"data/rbn/priors/renamed.txt", "data/fcc/allowlist.txt", "data/h3/res1.bin", "data/h3/res2.bin"} {
		if !slices.Contains(required, name) {
			t.Errorf("missing %s", name)
		}
	}
	if slices.Contains(required, cfg.CallCorrection.ConfusionModelFile) {
		t.Fatal("included disabled model")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) == ".yaml" {
			data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			if err != nil {
				t.Fatal(err)
			}
			if err := checkClassification(entry.Name(), data, inputs, false); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, body := range []string{
		"call_correction: {new_dictionary: local}",
		"call_correction: {new_dictionary: [local]}",
		"call_correction: {<<: &defaults {new_dictionary: local}}",
	} {
		err := checkClassification("pipeline.yaml", []byte(body), inputs, false)
		if err == nil || !strings.Contains(err.Error(), "call_correction.new_dictionary") {
			t.Fatalf("unclassified setting: %v", err)
		}
	}
	for _, scalar := range []string{"123", "null", "[]"} {
		err := checkClassification("pipeline.yaml", []byte("new_model_file: "+scalar), inputs, false)
		if err == nil || !strings.Contains(err.Error(), "new_model_file") {
			t.Errorf("unclassified path with %s: %v", scalar, err)
		}
	}
}

func TestRejectPrivateOrEscapingAsset(t *testing.T) {
	for _, tc := range []input{
		{"pipeline.yaml", "call_correction.spotter_reliability_file", "../secrets/weights.txt"},
		{"pipeline.yaml", "call_correction.confusion_model_file", "data/rbn/priors/../../secrets/token.json"},
		{"data.yaml", "cty.file", "data/fcc/fcc_uls.db"},
		{"data.yaml", "fcc_uls.allowlist_path", "data/fcc/private.txt"},
		{"data.yaml", "h3_table_path", "data/config.local"},
		{"toxicity.yaml", "safe_gate_file", "openai.yaml"},
		{"peering.yaml", "peering.topology.db_path", "data/archive/live.db"},
	} {
		if _, err := tc.paths(); err == nil {
			t.Errorf("accepted %s: %s", tc.setting, tc.value)
		}
	}
	for _, outside := range []string{`C:\running-node\logs`, `..\outside`, `\\host\share`, "/tmp/node",
		"data/NUL.txt", "data/file:alternate", "data/.. /outside", "data/archive/../outside"} {
		if err := checkLocalSetting("logging.dir", outside, false); err == nil {
			t.Errorf("permitted node state outside extraction: %q", outside)
		}
	}
	if err := checkClassification("app.yaml", []byte("logging: {dir: '../live'}"), nil, false); err == nil {
		t.Fatal("public YAML accepted escaping log directory")
	}
}

func TestCheckSmokeDirectory(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct{ file, key, inside, outside string }{
		{"reputation.yaml", "reputation", "{ipinfo_download_token: 'file:smoke-unused-token'}", "{ipinfo_download_token: 'file:../secrets/.env'}"},
		{"path_reliability.yaml", "voacap_fallback", "{voacap_home: 'data/smoke/voacap'}", "{voacap_home: 'C:\\itshfbc'}"},
		{"archive.yaml", "archive", "{db_path: 'data/smoke/archive'}", "{db_path: '../running/archive'}"},
	} {
		for _, value := range []string{tc.inside, tc.outside} {
			if err := os.WriteFile(filepath.Join(dir, tc.file), []byte(tc.key+": "+value+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := checkSmokeDirectory(dir); (err == nil) != (value == tc.inside) {
				t.Fatalf("%s %s: %v", tc.file, value, err)
			}
		}
		if err := os.Remove(filepath.Join(dir, tc.file)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStageCommittedContent(t *testing.T) {
	repo := t.TempDir()
	git := func(stdin string, args ...string) string {
		t.Helper()
		options := []string{"-c", "core.autocrlf=false", "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
			"-c", "commit.gpgsign=false", "-c", "core.hooksPath=.git/disabled-hooks"}
		cmd := exec.CommandContext(t.Context(), "git", append(options, args...)...)
		cmd.Dir, cmd.Stdin = repo, strings.NewReader(stdin)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	write := func(name, data string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(filepath.Join(repo, name)), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, name), []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
	}
	const lfs = "data/rbn/priors/weights.txt"
	const ordinary = "data/cty/cty.plist"
	const broken = "data/rbn/priors/bad.txt"
	const stray = "data/cty/stray.plist"
	const pipeline = "data/config/pipeline.yaml"
	const payload = "RX1 0.75\n"
	pointer := fmt.Sprintf("%s\noid sha256:%x\nsize %d\n", pointerHeader, sha256.Sum256([]byte(payload)), len(payload))
	git("", "init", "-q")
	write(".gitattributes", "data/rbn/** filter=lfs -text\n")
	write(ordinary, "A\nB\n")
	write(stray, pointer)
	write(pipeline, "# public config\n")
	git("", "add", ".gitattributes", ordinary, stray, pipeline)
	// Stage pointers directly to exercise Git's real tree and attributes
	// without relying on installed LFS filters or contacting a remote.
	for name, stored := range map[string]string{lfs: pointer, broken: payload} {
		write(name, payload)
		oid := git(stored, "hash-object", "-w", "--stdin", "--no-filters")
		git("", "update-index", "--add", "--cacheinfo", "100644,"+oid+","+name)
	}
	git("", "commit", "-qm", "release inputs")
	commit := git("", "rev-parse", "HEAD")
	write(ordinary, "A\r\nB\r\n")
	source, err := readTree(repo, commit)
	if err != nil {
		t.Fatal(err)
	}
	defer source.root.Close()
	stage, err := os.OpenRoot(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	if err := stage.MkdirAll("data/config", 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := checkedConfigFiles(source, stage); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("staged config omitted a tracked public file: %v", err)
	}
	if err := stage.WriteFile(pipeline, []byte("# public config\r\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := checkedConfigFiles(source, stage); err != nil {
		t.Fatalf("ordinary Windows checkout conversion: %v", err)
	}
	if err := stage.WriteFile(pipeline, []byte("# changed config\r\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := checkedConfigFiles(source, stage); err == nil || !strings.Contains(err.Error(), "differs from release commit") {
		t.Fatalf("dirty config silently canonicalized: %v", err)
	}
	for name, want := range map[string]string{lfs: payload, ordinary: "A\nB\n"} {
		sum, err := source.stageAsset(stage, name)
		if err != nil || sum != fmt.Sprintf("%x", sha256.Sum256([]byte(want))) {
			t.Fatalf("%s: %s %v", name, sum, err)
		}
		got, err := stage.ReadFile(name)
		if err != nil || !bytes.Equal(got, []byte(want)) {
			t.Fatalf("%s: staged %q, %v", name, got, err)
		}
	}
	for _, tc := range []struct{ name, content, wantError string }{
		{lfs, "RX1 0.76\n", "SHA-256 mismatch"},
		{lfs, pointer, "Git LFS"},
		{broken, payload, "missing canonical LFS OID"},
		{stray, pointer, "pointer in place"},
		{"data/missing", "untracked", "missing regular file"},
	} {
		write(tc.name, tc.content)
		if _, err := source.stageAsset(stage, tc.name); err == nil || !strings.Contains(err.Error(), tc.wantError) {
			t.Errorf("%s: expected %q, got %v", tc.name, tc.wantError, err)
		}
	}
}
