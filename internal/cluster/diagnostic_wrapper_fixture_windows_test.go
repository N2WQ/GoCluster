package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type diagWrapperFixture struct {
	root        string
	script      string
	executable  string
	private     string
	nonce       string
	rejectStart bool
}

// A host/kernel cleanup refusal fails closed for this fixture's later writes.
// Only sequential parent tests access this invocation-lifetime admission fence.
var diagFixtureRetirementBlocked bool

// The saved executable and copied script have fixed, ignored paths and one
// sequential test owner. Fresh private receipt/config directories avoid stale
// admission witnesses. No generated binaries are deleted; reuse occurs only
// after the preceding child's PowerShell and kernel process waits complete.
func newDiagWrapperFixture(t *testing.T, name string, gc int, executable, omitExit bool) diagWrapperFixture {
	t.Helper()
	if diagFixtureRetirementBlocked {
		t.Fatal("a prior diagnostic fixture did not confirm owner retirement")
	}
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(repo, ".tmp", "windows-tests", "dxcluster", "internal", "cluster", "diag-wrapper", name)
	if err := diagPlainFixturePath(repo, root); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "cmd"), 0o755); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(repo, "cmd", "run-with-diag.ps1"))
	if err != nil {
		t.Fatal(err)
	}
	if omitExit {
		const propagation = "    exit $LASTEXITCODE"
		if strings.Count(string(body), propagation) != 1 {
			t.Fatal("diagnostic wrapper propagation mutation must match exactly once")
		}
		body = []byte(strings.Replace(string(body), propagation, "    # fixture removes only child status propagation", 1))
	}
	script := filepath.Join(root, "cmd", "run-with-diag.ps1")
	if err := diagPlainFixturePath(repo, script); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(script, body, 0o600); err != nil {
		t.Fatal(err)
	}
	child := filepath.Join(root, "gocluster.exe")
	if err := diagPlainFixturePath(repo, child); err != nil {
		t.Fatal(err)
	}
	if executable {
		current, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		if err := diagCopyExecutable(current, child); err != nil {
			t.Fatal(err)
		}
	} else if _, err := os.Lstat(child); !os.IsNotExist(err) {
		t.Fatalf("missing-executable fixture must never contain an executable: %v", err)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	privateRoot := filepath.Join(root, "private")
	if err := diagPlainFixturePath(repo, privateRoot); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(privateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	private := filepath.Join(privateRoot, hex.EncodeToString(nonce[:]))
	if err := os.Mkdir(private, 0o700); err != nil {
		t.Fatal(err)
	}
	// Retain fixed-size per-invocation evidence rather than scheduling removal
	// beneath a child whose retirement might be refused by the host/kernel.
	fixture := diagWrapperFixture{root: root, script: script, executable: child,
		private: private, nonce: hex.EncodeToString(nonce[:])}
	if executable {
		diagCopyConfig(t, repo, filepath.Join(private, "config"), gc)
	}
	return fixture
}

func diagCopyExecutable(source, destination string) (err error) {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, in.Close()) }()
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o700)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, out.Close()) }()
	_, err = io.Copy(out, in)
	return err
}

// Every existing ancestor must resolve to itself. This refuses redirection of
// stable fixture writes through symlinks/junctions without changing user paths.
func diagPlainFixturePath(repo, path string) error {
	for current := path; current != repo; current = filepath.Dir(current) {
		if !strings.HasPrefix(strings.ToLower(current), strings.ToLower(repo+string(filepath.Separator))) {
			return &os.PathError{Op: "fixture boundary", Path: current, Err: os.ErrPermission}
		}
		resolved, err := filepath.EvalSymlinks(current)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !strings.EqualFold(filepath.Clean(resolved), filepath.Clean(current)) {
			return &os.PathError{Op: "fixture redirection", Path: current, Err: os.ErrPermission}
		}
	}
	return nil
}

func diagCopyConfig(t *testing.T, repo, destination string, gc int) {
	t.Helper()
	// Literal required-file inventory from config/config_files.go. Optional
	// credentials/tool files are never copied or loaded by this child.
	files := []string{"app.yaml", "archive.yaml", "data.yaml", "dedupe.yaml", "floodcontrol.yaml",
		"ingest.yaml", "mode_seeds.yaml", "peering.yaml", "pipeline.yaml", "prop_report.yaml",
		"reputation.yaml", "runtime.yaml", "path_reliability.yaml", "solarweather.yaml",
		"iaru_regions.yaml", "iaru_mode_inference.yaml", "spot_taxonomy.yaml"}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range files {
		body, err := os.ReadFile(filepath.Join(repo, "data", "config", name))
		if err != nil {
			t.Fatal(err)
		}
		if name == "runtime.yaml" {
			var document map[string]any
			if err := yaml.Unmarshal(body, &document); err != nil {
				t.Fatal(err)
			}
			runtime, ok := document["go_runtime"].(map[string]any)
			if !ok {
				t.Fatal("required runtime fixture lacks go_runtime map")
			}
			runtime["gc_percent"] = gc
			body, err = yaml.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := os.WriteFile(filepath.Join(destination, name), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
