package cluster

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

func mainHandoffFiles(t *testing.T) (repo, parent, image string) {
	t.Helper()
	repo = t.TempDir()
	parent = filepath.Join(repo, ".tmp", "windows-tests", "dxcluster", "internal", "cluster", "package.test.exe")
	image = filepath.Join(filepath.Dir(parent), "main", "gocluster.exe")
	if err := os.MkdirAll(filepath.Dir(image), 0700); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{parent, image} {
		if err := os.WriteFile(path, []byte("fixture; never executed"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return repo, parent, image
}

func TestPreparedMainImageHandoff(t *testing.T) {
	repo, parent, image := mainHandoffFiles(t)
	for _, tc := range []struct {
		name, current, supplied string
		valid                   bool
	}{
		{"prepared", parent, image, true},
		{"absent", parent, "", false},
		{"relative", parent, "gocluster.exe", false},
		{"arbitrary", parent, filepath.Join(t.TempDir(), "gocluster.exe"), false},
		{"wrong-checkout", parent, filepath.Join(t.TempDir(), ".tmp", "windows-tests", "dxcluster", "internal", "cluster", "main", "gocluster.exe"), false},
		{"unmanaged-parent", filepath.Join(t.TempDir(), "package.test.exe"), image, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validatePreparedMainImage(repo, tc.current, tc.supplied)
			if tc.valid {
				if err != nil || got != image {
					t.Fatalf("image=%q err=%v", got, err)
				}
			} else if err == nil || got != "" {
				t.Fatalf("accepted invalid handoff: image=%q err=%v", got, err)
			}
		})
	}
	if err := os.Remove(image); err != nil {
		t.Fatal(err)
	}
	if _, err := validatePreparedMainImage(repo, parent, image); err == nil {
		t.Fatal("accepted missing prepared executable")
	}
}

func TestPreparedMainImageRejectsRedirectedOutput(t *testing.T) {
	for _, leaf := range []bool{false, true} {
		t.Run(strconv.FormatBool(leaf), func(t *testing.T) {
			repo, parent, image := mainHandoffFiles(t)
			outside := t.TempDir()
			sentinel := filepath.Join(outside, "gocluster.exe")
			if err := os.WriteFile(sentinel, []byte("KEEP"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(image); err != nil {
				t.Fatal(err)
			}
			link := image
			if !leaf {
				link = filepath.Dir(image)
				if err := os.Remove(link); err != nil {
					t.Fatal(err)
				}
			}
			if output, err := exec.CommandContext(t.Context(), "cmd", "/c", "mklink", "/J", link, outside).CombinedOutput(); err != nil {
				t.Fatalf("create controlled output junction: %v %s", err, output)
			}
			t.Cleanup(func() {
				if err := os.Remove(link); err != nil {
					t.Error(err)
				}
			})
			if _, err := validatePreparedMainImage(repo, parent, image); err == nil {
				t.Fatal("accepted redirected output")
			}
			if got, err := os.ReadFile(sentinel); err != nil || string(got) != "KEEP" {
				t.Fatalf("external sentinel changed: %q %v", got, err)
			}
		})
	}
}

func TestPreparedMainImageRetiresAfterFixtureFailure(t *testing.T) {
	current, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	child := exec.CommandContext(ctx, current, "-test.v", "-test.run=^TestV6MainExecutablePeerLoggingWithoutCompanion$", "-test.timeout=50s")
	child.Env = append(os.Environ(), "GOCLUSTER_MAIN_TEST_FORCE_FAILURE=1")
	output, err := child.CombinedOutput()
	text := string(output)
	if err == nil || !strings.Contains(text, "controlled main fixture failure after child startup") ||
		!strings.Contains(text, "MAIN-CHILD verified image=") || !strings.Contains(text, "MAIN-CHILD retired pid=") {
		t.Fatalf("failure did not exercise verified child retirement: %v\n%s", err, output)
	}
	const marker = "MAIN-CHILD retired pid="
	fields := strings.Fields(text[strings.LastIndex(text, marker)+len(marker):])
	if len(fields) == 0 {
		t.Fatal("missing retired process identity")
	}
	pid, err := strconv.ParseUint(fields[0], 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := windows.OpenProcess(windows.SYNCHRONIZE, false, uint32(pid))
	if err == nil {
		defer func() {
			if err := windows.CloseHandle(handle); err != nil {
				t.Errorf("close retirement query handle: %v", err)
			}
		}()
		result, waitErr := windows.WaitForSingleObject(handle, 0)
		if waitErr != nil || result != windows.WAIT_OBJECT_0 {
			t.Fatalf("main child still live after failed fixture returned: wait=%d err=%v", result, waitErr)
		}
	} else if !errors.Is(err, windows.ERROR_INVALID_PARAMETER) {
		t.Fatalf("cannot establish child retirement: %v", err)
	}
}
