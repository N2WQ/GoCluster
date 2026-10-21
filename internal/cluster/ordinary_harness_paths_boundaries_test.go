package cluster

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"dxcluster/internal/testsupport"
)

func TestOrdinaryHarnessPathBoundaries(t *testing.T) {
	t.Run("executable", ordinaryHarnessExecutableBoundaries)
	t.Run("artifact", ordinaryHarnessArtifactBoundaries)
}

func ordinaryHarnessExecutableBoundaries(t *testing.T) {
	roots := []struct{ name, repo, expected string }{
		{"relocated", `D:\relocated\gocluster`, `D:\relocated\gocluster\.tmp\windows-tests\dxcluster\internal\cluster\package.test.exe`},
		{"spaces", `C:\work trees\cluster copy`, `C:\work trees\cluster copy\.tmp\windows-tests\dxcluster\internal\cluster\package.test.exe`},
	}
	repo := `C:\work\gocluster`
	expected := `C:\work\gocluster\.tmp\windows-tests\dxcluster\internal\cluster\package.test.exe`
	wrong := []struct{ name, executable string }{
		{"wrong_checkout", `C:\src\gocluster\.tmp\windows-tests\dxcluster\internal\cluster\package.test.exe`},
		{"wrong_package", `C:\work\gocluster\.tmp\windows-tests\dxcluster\internal\peerdiag\package.test.exe`},
		{"wrong_name", `C:\work\gocluster\.tmp\windows-tests\dxcluster\internal\cluster\other.test.exe`},
		{"sibling_prefix", `C:\work\gocluster-other\.tmp\windows-tests\dxcluster\internal\cluster\package.test.exe`},
		{"executable_suffix", `C:\work\gocluster\.tmp\windows-tests\dxcluster\internal\cluster\package.test.exe.other`},
		{"basename_only", `package.test.exe`},
	}
	if runtime.GOOS != "windows" {
		roots = []struct{ name, repo, expected string }{
			{"relocated", "/relocated/gocluster", "/relocated/gocluster/.tmp/windows-tests/dxcluster/internal/cluster/package.test.exe"},
			{"spaces", "/work trees/cluster copy", "/work trees/cluster copy/.tmp/windows-tests/dxcluster/internal/cluster/package.test.exe"},
		}
		repo = "/work/gocluster"
		expected = "/work/gocluster/.tmp/windows-tests/dxcluster/internal/cluster/package.test.exe"
		wrong = []struct{ name, executable string }{
			{"wrong_checkout", "/src/gocluster/.tmp/windows-tests/dxcluster/internal/cluster/package.test.exe"},
			{"wrong_package", "/work/gocluster/.tmp/windows-tests/dxcluster/internal/peerdiag/package.test.exe"},
			{"wrong_name", "/work/gocluster/.tmp/windows-tests/dxcluster/internal/cluster/other.test.exe"},
			{"sibling_prefix", "/work/gocluster-other/.tmp/windows-tests/dxcluster/internal/cluster/package.test.exe"},
			{"executable_suffix", "/work/gocluster/.tmp/windows-tests/dxcluster/internal/cluster/package.test.exe.other"},
			{"basename_only", "package.test.exe"},
		}
	}
	for _, tc := range roots {
		t.Run(tc.name, func(t *testing.T) {
			if actual := ordinaryHarnessExecutablePath(tc.repo); actual != tc.expected {
				t.Fatalf("executable path %q, want literal %q", actual, tc.expected)
			}
			if err := ordinaryHarnessCheckExecutable(tc.repo, tc.expected); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("case_comparison", func(t *testing.T) {
		err := ordinaryHarnessCheckExecutable(repo, strings.ToUpper(expected))
		if (err == nil) != (runtime.GOOS == "windows") {
			t.Fatalf("platform case comparison: %v", err)
		}
	})
	for _, tc := range wrong {
		t.Run(tc.name, func(t *testing.T) {
			if err := ordinaryHarnessCheckExecutable(repo, tc.executable); err == nil {
				t.Fatalf("accepted wrong executable %q", tc.executable)
			}
		})
	}
}

func ordinaryHarnessArtifactBoundaries(t *testing.T) {
	checkout, outside := ordinaryHarnessBoundaryCheckout(t)
	allowed := filepath.Join(checkout, ".tmp", "path-reliability-first-wave")
	sentinel := filepath.Join(outside, "sentinel")
	t.Chdir(filepath.Join(checkout, "internal", "cluster"))
	t.Run("cwd_checkout", func(t *testing.T) {
		if actual := testsupport.RepoRoot(t); actual != checkout {
			t.Fatalf("cwd/go.mod root %q, want relocated checkout %q", actual, checkout)
		}
	})
	t.Run("default", func(t *testing.T) {
		t.Setenv("GOCLUSTER_PATH_SHARDING_ARTIFACT_DIR", "")
		dir := ordinaryHarnessArtifactDir(t, "default-")
		ordinaryHarnessRequireArtifact(t, dir, filepath.Join(checkout, ".tmp", "path-reliability-first-wave", "s4"), "default-")
	})
	t.Run("override", func(t *testing.T) {
		override := filepath.Join(allowed, "accepted override")
		t.Setenv("GOCLUSTER_PATH_SHARDING_ARTIFACT_DIR", override)
		dir := ordinaryHarnessArtifactDir(t, "override-")
		ordinaryHarnessRequireArtifact(t, dir, override, "override-")
	})
	t.Run("relative_override", func(t *testing.T) {
		dir, err := ordinaryHarnessCreateArtifactDir(checkout, filepath.Join("..", "..", ".tmp", "path-reliability-first-wave", "relative"), "relative-")
		if err == nil || dir != "" {
			t.Fatalf("relative traversal accepted: dir=%q error=%v", dir, err)
		}
		t.Chdir(checkout)
		dir, err = ordinaryHarnessCreateArtifactDir(checkout, filepath.Join(".tmp", "path-reliability-first-wave", "relative"), "relative-")
		if err != nil {
			t.Fatalf("contained relative override: dir=%q error=%v", dir, err)
		}
		ordinaryHarnessRequireArtifact(t, dir, filepath.Join(allowed, "relative"), "relative-")
	})
	sibling := allowed + "-other"
	if err := os.Mkdir(sibling, 0700); err != nil {
		t.Fatal(err)
	}
	siblingSentinel := filepath.Join(sibling, "sentinel")
	if err := os.WriteFile(siblingSentinel, []byte("sibling-kept"), 0600); err != nil {
		t.Fatal(err)
	}
	rejected := []struct{ name, path string }{
		{"traversal", allowed + string(filepath.Separator) + ".." + string(filepath.Separator) + "outside"},
		{"contained_traversal", allowed + string(filepath.Separator) + "s4" + string(filepath.Separator) + ".." + string(filepath.Separator) + "hidden"},
		{"drive_escape", `Z:\outside\ordinary-fixture`},
		{"unc_escape", `\\ordinary-fixture.invalid\share\artifacts`},
		{"sibling_prefix", allowed + "-other"},
	}
	if runtime.GOOS == "windows" {
		rejected = append(rejected, struct{ name, path string }{"win32_alias", allowed + `\.. \outside`})
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := ordinaryHarnessCreateArtifactDir(checkout, tc.path, "rejected-")
			if err == nil || dir != "" {
				t.Fatalf("accepted outside/traversing artifact root %q: dir=%q error=%v", tc.path, dir, err)
			}
			ordinaryHarnessRequireSentinel(t, sentinel, "outside-kept")
			ordinaryHarnessRequireSentinel(t, siblingSentinel, "sibling-kept")
			for _, protected := range []string{outside, sibling} {
				if entries, err := os.ReadDir(protected); err != nil || len(entries) != 1 {
					t.Fatalf("outside directory changed: %s entries=%v error=%v", protected, entries, err)
				}
			}
		})
	}
	t.Run("links", func(t *testing.T) {
		ordinaryHarnessArtifactLinkBoundaries(t, checkout, allowed, outside)
	})
	t.Run("io_errors", func(t *testing.T) {
		ordinaryHarnessArtifactIOBoundaries(t, checkout, allowed)
	})
	ordinaryHarnessPlatformPathBoundaries(t, allowed)
}

func ordinaryHarnessArtifactLinkBoundaries(t *testing.T, checkout, allowed, outside string) {
	inside := filepath.Join(allowed, "inside-target")
	if err := os.Mkdir(inside, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inside, "sentinel"), []byte("inside-kept"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, target string
		missingLeaf  bool
	}{
		{"existing_link", outside, false},
		{"missing_leaf_below_link", outside, true},
		{"inside_tree_link", inside, false},
		{"missing_leaf_below_inside_tree_link", inside, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			link := filepath.Join(allowed, tc.name)
			ordinaryHarnessBoundaryLink(t, link, tc.target)
			requested := link
			if tc.missingLeaf {
				requested = filepath.Join(link, "missing", "leaf")
			}
			dir, err := ordinaryHarnessCreateArtifactDir(checkout, requested, "rejected-")
			if err == nil || dir != "" || !strings.Contains(err.Error(), "link or reparse point") {
				t.Fatalf("redirected artifact root accepted or misclassified: dir=%q error=%v", dir, err)
			}
			ordinaryHarnessRequireSentinel(t, filepath.Join(outside, "sentinel"), "outside-kept")
			ordinaryHarnessRequireSentinel(t, filepath.Join(inside, "sentinel"), "inside-kept")
			for _, target := range []string{outside, inside} {
				if entries, err := os.ReadDir(target); err != nil || len(entries) != 1 {
					t.Fatalf("link target changed: %s entries=%v error=%v", target, entries, err)
				}
			}
		})
	}
	t.Run("linked_checkout_ancestor", func(t *testing.T) {
		otherCheckout := filepath.Join(checkout, "linked checkout")
		if err := os.Mkdir(otherCheckout, 0700); err != nil {
			t.Fatal(err)
		}
		ordinaryHarnessBoundaryLink(t, filepath.Join(otherCheckout, ".tmp"), outside)
		dir, err := ordinaryHarnessCreateArtifactDir(otherCheckout, "", "rejected-")
		if err == nil || dir != "" || !strings.Contains(err.Error(), "link or reparse point") {
			t.Fatalf("checkout ancestor redirection accepted: dir=%q error=%v", dir, err)
		}
		ordinaryHarnessRequireSentinel(t, filepath.Join(outside, "sentinel"), "outside-kept")
		if entries, err := os.ReadDir(outside); err != nil || len(entries) != 1 {
			t.Fatalf("checkout link target changed: entries=%v error=%v", entries, err)
		}
	})
}

func ordinaryHarnessArtifactIOBoundaries(t *testing.T, checkout, allowed string) {
	t.Run("file_ancestor", func(t *testing.T) {
		blocked := filepath.Join(allowed, "regular-file")
		if err := os.WriteFile(blocked, []byte("kept"), 0600); err != nil {
			t.Fatal(err)
		}
		dir, err := ordinaryHarnessCreateArtifactDir(checkout, filepath.Join(blocked, "missing"), "rejected-")
		if err == nil || dir != "" || !strings.Contains(err.Error(), "ancestor") {
			t.Fatalf("file ancestor not reported: dir=%q error=%v", dir, err)
		}
		ordinaryHarnessRequireSentinel(t, blocked, "kept")
	})
	t.Run("invalid_path", func(t *testing.T) {
		dir, err := ordinaryHarnessCreateArtifactDir(checkout, filepath.Join(allowed, "invalid\x00name"), "rejected-")
		if err == nil || dir != "" || (!strings.Contains(err.Error(), "resolve ordinary artifact path") && !strings.Contains(err.Error(), "inspect ordinary artifact ancestor")) {
			t.Fatalf("invalid path error not explicit: dir=%q error=%v", dir, err)
		}
	})
	for _, tc := range []struct {
		name  string
		cause error
	}{{"permission", os.ErrPermission}, {"metadata", errors.New("metadata failure")}} {
		t.Run(tc.name, func(t *testing.T) {
			leaf := filepath.Join(allowed, "missing-parent", "leaf")
			ancestor := filepath.Dir(leaf)
			probes := 0
			err := ordinaryHarnessCheckAncestors(leaf, func(path string) (os.FileInfo, error) {
				probes++
				if path == leaf {
					return nil, &os.PathError{Op: "lstat", Path: path, Err: os.ErrNotExist}
				}
				return nil, &os.PathError{Op: "lstat", Path: path, Err: tc.cause}
			})
			if !errors.Is(err, tc.cause) || !strings.Contains(err.Error(), "inspect ordinary artifact ancestor "+ancestor) || probes != 2 {
				t.Fatalf("ancestor IO error swallowed or scanning continued: probes=%d error=%v", probes, err)
			}
		})
	}
}

func ordinaryHarnessBoundaryCheckout(t *testing.T) (string, string) {
	t.Helper()
	base := filepath.Join(testsupport.RepoRoot(t), ".tmp")
	if err := os.MkdirAll(base, 0700); err != nil {
		t.Fatal(err)
	}
	fixture, err := os.MkdirTemp(base, "ordinary-harness-paths-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(fixture); err != nil {
			t.Error(err)
		}
	})
	checkout := filepath.Join(fixture, "relocated checkout")
	outside := filepath.Join(checkout, ".tmp", "outside")
	for _, dir := range []string{filepath.Join(checkout, "internal", "cluster"), outside} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(checkout, "go.mod"), []byte("module ordinaryfixture\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "sentinel"), []byte("outside-kept"), 0600); err != nil {
		t.Fatal(err)
	}
	return checkout, outside
}

func ordinaryHarnessRequireSentinel(t *testing.T, path, expected string) {
	t.Helper()
	if actual, err := os.ReadFile(path); err != nil || string(actual) != expected {
		t.Fatalf("sentinel changed: %s got=%q want=%q error=%v", path, actual, expected, err)
	}
}

func ordinaryHarnessRequireArtifact(t *testing.T, path, parent, prefix string) {
	t.Helper()
	if !filepath.IsAbs(path) || filepath.Dir(path) != parent || !strings.HasPrefix(filepath.Base(path), prefix) {
		t.Fatalf("artifact directory %q, want unique %s* below %q", path, prefix, parent)
	}
	if info, err := os.Stat(path); err != nil || !info.IsDir() {
		t.Fatalf("artifact directory was not created: %s error=%v", path, err)
	}
}
