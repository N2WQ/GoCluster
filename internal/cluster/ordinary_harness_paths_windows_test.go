//go:build windows

package cluster

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func ordinaryHarnessRedirected(info os.FileInfo) (bool, error) {
	attributes, ok := info.Sys().(*syscall.Win32FileAttributeData)
	if !ok || attributes == nil {
		return false, errors.New("Windows reparse attributes unavailable")
	}
	return info.Mode()&os.ModeSymlink != 0 || attributes.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0, nil
}

func ordinaryHarnessBoundaryLink(t *testing.T, link, target string) {
	t.Helper()
	image := os.Getenv("GOCLUSTER_TEST_POWERSHELL")
	if !filepath.IsAbs(image) {
		t.Fatalf("ordinary junction fixture requires absolute GOCLUSTER_TEST_POWERSHELL identity from scripts/test-windows.ps1; got %q", image)
	}
	quote := func(path string) string { return "'" + strings.ReplaceAll(path, "'", "''") + "'" }
	script := "$ErrorActionPreference = 'Stop'; New-Item -ItemType Junction -Path " + quote(link) + " -Target " + quote(target) + " | Out-Null"
	cmd := exec.CommandContext(t.Context(), image, "-NoProfile", "-NonInteractive", "-Command", script)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("create actual ordinary fixture junction via %q: %v %s", image, err, output)
	}
	t.Cleanup(func() {
		if err := os.Remove(link); err != nil {
			t.Error(err)
		}
	})
	info, err := os.Lstat(link)
	if err != nil {
		t.Fatal(err)
	}
	attributes, ok := info.Sys().(*syscall.Win32FileAttributeData)
	if !ok || attributes == nil || attributes.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
		t.Fatal("junction fixture lacks native reparse attributes")
	}
}

type ordinaryHarnessAttributeInfo struct {
	os.FileInfo
	attributes *syscall.Win32FileAttributeData
}

func (info ordinaryHarnessAttributeInfo) Sys() any { return info.attributes }

func ordinaryHarnessPlatformPathBoundaries(t *testing.T, directory string) {
	t.Helper()
	t.Run("reparse_attributes", func(t *testing.T) {
		info, err := os.Lstat(directory)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name       string
			attributes uint32
			redirected bool
		}{
			{"ordinary_directory", syscall.FILE_ATTRIBUTE_DIRECTORY, false},
			{"opaque_reparse", syscall.FILE_ATTRIBUTE_DIRECTORY | syscall.FILE_ATTRIBUTE_REPARSE_POINT, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				fixture := ordinaryHarnessAttributeInfo{info, &syscall.Win32FileAttributeData{FileAttributes: tc.attributes}}
				if fixture.Mode()&os.ModeSymlink != 0 {
					t.Fatal("attribute-only vector unexpectedly has symlink mode")
				}
				redirected, err := ordinaryHarnessRedirected(fixture)
				if err != nil || redirected != tc.redirected {
					t.Fatalf("attribute-only reparse classification: redirected=%v error=%v", redirected, err)
				}
			})
		}
		if _, err := ordinaryHarnessRedirected(ordinaryHarnessAttributeInfo{FileInfo: info}); err == nil {
			t.Fatal("missing native attributes silently accepted")
		}
	})
}
