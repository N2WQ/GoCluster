package cluster

import (
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"dxcluster/config"
	"dxcluster/cty"

	"golang.org/x/sys/windows"
)

// This fixture alone owns both objects. Preserve their DACL and protection state
// before mutation, retain owner WRITE_DAC, and restore before TempDir cleanup.
// No machine policy, production file, inherited parent or privilege is changed.
func protectCTYFixture(t *testing.T, root, path string) func() {
	t.Helper()
	if filepath.Dir(path) != root {
		t.Fatal("protected fixture must be a direct child of its owned TempDir")
	}
	type original struct {
		path    string
		sd      *windows.SECURITY_DESCRIPTOR
		acl     *windows.ACL
		control windows.SECURITY_DESCRIPTOR_CONTROL
	}
	originals := make([]original, 0, 2)
	for _, name := range []string{root, path} {
		info, err := os.Lstat(name)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("invalid owned fixture %s: %v", name, err)
		}
		sd, err := windows.GetNamedSecurityInfo(name, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
		if err != nil {
			t.Fatal(err)
		}
		acl, _, err := sd.DACL()
		if err != nil {
			t.Fatal(err)
		}
		control, _, err := sd.Control()
		if err != nil {
			t.Fatal(err)
		}
		originals = append(originals, original{name, sd, acl, control})
	}
	var once sync.Once
	restore := func() {
		once.Do(func() {
			for i := len(originals) - 1; i >= 0; i-- {
				o := originals[i]
				flags := windows.SECURITY_INFORMATION(windows.DACL_SECURITY_INFORMATION | windows.UNPROTECTED_DACL_SECURITY_INFORMATION)
				if o.control&windows.SE_DACL_PROTECTED != 0 {
					flags = windows.DACL_SECURITY_INFORMATION | windows.PROTECTED_DACL_SECURITY_INFORMATION
				}
				if err := windows.SetNamedSecurityInfo(o.path, windows.SE_FILE_OBJECT, flags, nil, nil, o.acl, nil); err != nil {
					t.Errorf("restore owned fixture ACL %s: %v", o.path, err)
				}
			}
			for _, o := range originals {
				sd, err := windows.GetNamedSecurityInfo(o.path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
				if err != nil || sd.String() != o.sd.String() {
					t.Errorf("original fixture DACL/protection not restored for %s: %v", o.path, err)
				}
			}
		})
	}
	t.Cleanup(restore)
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	// FILE_DELETE_CHILD is 0x40 in WinNT.h; denying the file's DELETE alone
	// leaves parent deletion authority. Temp-file creation must remain allowed.
	// https://learn.microsoft.com/en-us/windows/win32/fileio/file-access-rights-constants
	for i, mask := range []windows.ACCESS_MASK{0x40, windows.DELETE | windows.FILE_WRITE_DATA | windows.FILE_APPEND_DATA} {
		o := originals[i]
		acl, err := windows.ACLFromEntries([]windows.EXPLICIT_ACCESS{{
			AccessPermissions: mask, AccessMode: windows.DENY_ACCESS,
			Trustee: windows.TRUSTEE{TrusteeForm: windows.TRUSTEE_IS_SID,
				TrusteeType: windows.TRUSTEE_IS_USER, TrusteeValue: windows.TrusteeValueFromSID(user.User.Sid)},
		}}, o.acl)
		runtime.KeepAlive(user) // TrusteeValue contains a uintptr rather than a Go pointer.
		if err != nil {
			t.Fatal(err)
		}
		if err := windows.SetNamedSecurityInfo(o.path, windows.SE_FILE_OBJECT,
			windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, acl, nil); err != nil {
			t.Fatal(err)
		}
	}
	return restore
}

func TestCTYProtectedInputPublication(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "cty.plist")
	old := `<plist version="1.0"><dict><key>K1</key><dict><key>Country</key><string>Alpha</string><key>Latitude</key><real>42.38</real><key>Longitude</key><real>71.65</real></dict></dict></plist>`
	fresh := strings.Replace(old, "Alpha", "Beta", 1)
	if err := os.WriteFile(path, []byte(old), 0o600); err != nil {
		t.Fatal(err)
	}
	restore := protectCTYFixture(t, root, path)
	probe, err := os.CreateTemp(root, "creation-must-work-")
	if err != nil {
		t.Fatalf("protection incorrectly blocks temp creation: %v", err)
	}
	if _, err := probe.WriteString("writable temporary body"); err != nil {
		_ = probe.Close()
		t.Fatal(err)
	}
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	var requests, sent atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		n, err := w.Write([]byte(fresh))
		if err == nil {
			sent.Add(int64(n))
		}
	}))
	defer server.Close()
	cfg := config.CTYConfig{Enabled: true, File: path, URL: server.URL}
	db, updated, err := refreshCTYDatabase(t.Context(), cfg)
	if err == nil || !errors.Is(err, os.ErrPermission) || !strings.Contains(err.Error(), "remove old file") || db != nil || updated {
		t.Fatalf("expected permission denial at real publication, got db=%v updated=%v err=%v", db != nil, updated, err)
	}
	if requests.Load() != 1 || sent.Load() != int64(len(fresh)) {
		t.Fatal("test did not supply genuinely changed HTTP 200 content")
	}
	bytes, err := os.ReadFile(path)
	if err != nil || sha256.Sum256(bytes) != sha256.Sum256([]byte(old)) {
		t.Fatalf("protected input changed: %v", err)
	}
	db, err = cty.LoadCTYDatabase(path) // same loader used for existing startup input
	if err != nil {
		t.Fatal(err)
	}
	info, ok := db.LookupCallsign("K1ABC")
	if !ok || info.Country != "Alpha" {
		t.Fatalf("protected startup input not consumed: %+v", info)
	}
	t.Setenv(envGridDBCheckOnMiss, "true")
	r := startCTYRolloutGrid(t, filepath.Join(t.TempDir(), "grid"), db)
	r.gridLookup("K1ABC")
	r.retireGridStore()
	if grid, derived, ok := r.metaCache.LookupGrid("K1ABC", nil); !ok || grid != "FN42" || !derived {
		t.Fatalf("protected input startup fallback = %s/%v/%v", grid, derived, ok)
	}
	restore()
	// Positive control: the same changed payload must publish when fixture
	// protection is removed. An unrelated downloader failure cannot pass.
	db, updated, err = refreshCTYDatabase(t.Context(), cfg)
	if err != nil || !updated || db == nil {
		t.Fatalf("unprotected publication control failed: updated=%v err=%v", updated, err)
	}
	info, ok = db.LookupCallsign("K1ABC")
	if !ok || info.Country != "Beta" || requests.Load() != 2 {
		t.Fatalf("changed publication control not observed: %+v", info)
	}
}
