package cluster

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"dxcluster/config"
	"dxcluster/cty"

	"golang.org/x/sys/windows"
)

// Record the persisted descriptor, including inheritance metadata. String
// conversion can fail silently in x/sys/windows; an empty result is no oracle.
func readCTYFixtureSecurity(t *testing.T, path, phase string) (*windows.SECURITY_DESCRIPTOR, windows.SECURITY_DESCRIPTOR_CONTROL, string, error) {
	t.Helper()
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.GROUP_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return nil, 0, "", fmt.Errorf("%s read %s: %w", phase, path, err)
	}
	control, _, err := sd.Control()
	if err != nil {
		return nil, 0, "", fmt.Errorf("%s control %s: %w", phase, path, err)
	}
	owner, _, ownerErr := sd.Owner()
	group, _, groupErr := sd.Group()
	if err := errors.Join(ownerErr, groupErr); err != nil {
		return nil, 0, "", fmt.Errorf("%s owner/group read for %s: %w", phase, path, err)
	}
	if owner == nil || group == nil || !owner.IsValid() || !group.IsValid() {
		return nil, 0, "", fmt.Errorf("%s invalid owner/group for %s", phase, path)
	}
	sddl := sd.String()
	if sddl == "" {
		return nil, 0, "", fmt.Errorf("%s SDDL conversion returned an empty descriptor for %s", phase, path)
	}
	t.Logf("CTY fixture security phase=%s path=%s control=%#x sddl=%q", phase, path, control, sddl)
	return sd, control, sddl, nil
}

const ctyFixtureOriginal = `<plist version="1.0"><dict><key>K1</key><dict><key>Country</key><string>Alpha</string><key>Latitude</key><real>42.38</real><key>Longitude</key><real>71.65</real></dict></dict></plist>`

// Create both objects in a declared modern inheritance model. The ambient
// TempDir is a container only: its security descriptor is never changed.
func newCTYPublicationFixture(t *testing.T) (root, path string) {
	t.Helper()
	outer := t.TempDir()
	_, outerControl, outerSDDL, err := readCTYFixtureSecurity(t, outer, "outer baseline")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, control, sddl, err := readCTYFixtureSecurity(t, outer, "outer final")
		if err != nil || control != outerControl || sddl != outerSDDL {
			t.Errorf("outer fixture security changed: %v; control=%#x want=%#x; sddl=%q want=%q", err, control, outerControl, sddl, outerSDDL)
		}
	})
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	root, path = filepath.Join(outer, "publication"), filepath.Join(outer, "publication", "cty.plist")
	for i, name := range []string{root, path} {
		inheritance := "OICI"
		if i == 1 {
			inheritance = ""
		}
		sd, err := windows.SecurityDescriptorFromString("D:PAI(A;" + inheritance + ";FA;;;" + user.User.Sid.String() + ")")
		if err != nil {
			t.Fatal(err)
		}
		attributes := windows.SecurityAttributes{Length: uint32(unsafe.Sizeof(windows.SecurityAttributes{})), SecurityDescriptor: sd}
		namePtr, err := windows.UTF16PtrFromString(name)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			err = windows.CreateDirectory(namePtr, &attributes)
		} else {
			err = writeCTYFixtureFile(namePtr, name, &attributes)
		}
		if err == nil {
			// Creation alone does not persist AI. Finish constructing the
			// declared inheritance model through the supported DACL setter,
			// before capturing either original or installing any deny ACE.
			var acl *windows.ACL
			acl, _, err = sd.DACL()
			if err == nil {
				err = windows.SetNamedSecurityInfo(name, windows.SE_FILE_OBJECT,
					windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, acl, nil)
			}
		}
		runtime.KeepAlive(sd)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Validate both fully constructed objects before any deny mutation, rather
	// than assuming that creation persisted the requested control flags.
	for i, name := range []string{root, path} {
		sd, control, _, err := readCTYFixtureSecurity(t, name, "constructed")
		if err != nil {
			t.Fatal(err)
		}
		assertCTYFixtureACL(t, sd, control, user.User.Sid, i == 0, 0)
	}
	return root, path
}

// Transfer the native handle once and close it even on write failure. An open
// creation handle must never manufacture the publication failure being tested.
func writeCTYFixtureFile(namePtr *uint16, name string, attributes *windows.SecurityAttributes) error {
	handle, err := windows.CreateFile(namePtr, windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		attributes, windows.CREATE_NEW, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(handle), name)
	if file == nil {
		return errors.Join(errors.New("invalid CTY fixture file handle"), windows.CloseHandle(handle))
	}
	_, writeErr := file.WriteString(ctyFixtureOriginal)
	return errors.Join(writeErr, file.Close())
}

// The fixture has one explicit full-control allow, plus at most one deny.
// Inspect native ACE structure; comparing a round-trip SDDL to itself cannot
// prove that the intended trustee, mask and inheritance were installed.
func assertCTYFixtureACL(t *testing.T, sd *windows.SECURITY_DESCRIPTOR, control windows.SECURITY_DESCRIPTOR_CONTROL, sid *windows.SID, directory bool, denial windows.ACCESS_MASK) {
	t.Helper()
	want := windows.SECURITY_DESCRIPTOR_CONTROL(windows.SE_DACL_PRESENT | windows.SE_DACL_PROTECTED | windows.SE_DACL_AUTO_INHERITED)
	if control&want != want || control&windows.SE_DACL_AUTO_INHERIT_REQ != 0 {
		t.Fatalf("fixture security control=%#x, require P/AI and no AR", control)
	}
	acl, _, err := sd.DACL()
	count := uint16(1)
	if denial != 0 {
		count++
	}
	if err != nil || acl == nil || acl.AceCount != count {
		t.Fatalf("fixture must have %d explicit ACEs: %v", count, err)
	}
	for i := uint16(0); i < count; i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(acl, uint32(i), &ace); err != nil {
			t.Fatal(err)
		}
		// FILE_ALL_ACCESS from WinNT.h is not exported by x/sys/windows.
		kind, flags, mask := uint8(windows.ACCESS_ALLOWED_ACE_TYPE), uint8(0), windows.ACCESS_MASK(0x001F01FF)
		if directory {
			flags = windows.OBJECT_INHERIT_ACE | windows.CONTAINER_INHERIT_ACE
		}
		if i == 0 && denial != 0 {
			kind, flags, mask = windows.ACCESS_DENIED_ACE_TYPE, 0, denial
		}
		trustee := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if ace.Header.AceType != kind || ace.Header.AceFlags != flags || ace.Mask != mask || !trustee.IsValid() || !trustee.Equals(sid) {
			t.Fatalf("unexpected fixture ACE %d: type=%d flags=%#x mask=%#x", i, ace.Header.AceType, ace.Header.AceFlags, ace.Mask)
		}
	}
	runtime.KeepAlive(sd)
}

// This fixture alone owns both objects. Preserve their DACL and protection state
// before mutation, retain current-token WRITE_DAC, and restore before cleanup.
// No machine policy, production file, inherited parent or privilege is changed.
func protectCTYFixture(t *testing.T, root, path string) func() {
	t.Helper()
	if filepath.Dir(path) != root {
		t.Fatal("protected fixture must be a direct child of its owned directory")
	}
	type original struct {
		path    string
		sd      *windows.SECURITY_DESCRIPTOR
		acl     *windows.ACL
		control windows.SECURITY_DESCRIPTOR_CONTROL
		sddl    string
	}
	originals := make([]original, 0, 2)
	for _, name := range []string{root, path} {
		info, err := os.Lstat(name)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("invalid owned fixture %s: %v", name, err)
		}
		sd, control, sddl, err := readCTYFixtureSecurity(t, name, "baseline")
		if err != nil {
			t.Fatal(err)
		}
		acl, _, err := sd.DACL()
		if err != nil {
			t.Fatal(err)
		}
		originals = append(originals, original{name, sd, acl, control, sddl})
	}
	var once sync.Once
	restore := func() {
		once.Do(func() {
			// Restore the parent first: its inheritable ACEs may propagate to
			// the child. Final equality still covers both complete descriptors.
			for _, o := range originals {
				flags := windows.SECURITY_INFORMATION(windows.DACL_SECURITY_INFORMATION | windows.UNPROTECTED_DACL_SECURITY_INFORMATION)
				if o.control&windows.SE_DACL_PROTECTED != 0 {
					flags = windows.DACL_SECURITY_INFORMATION | windows.PROTECTED_DACL_SECURITY_INFORMATION
				}
				if err := windows.SetNamedSecurityInfo(o.path, windows.SE_FILE_OBJECT, flags, nil, nil, o.acl, nil); err != nil {
					t.Errorf("restore owned fixture ACL %s: %v", o.path, err)
				}
				for _, current := range originals {
					if _, _, _, err := readCTYFixtureSecurity(t, current.path, "after restore "+o.path); err != nil {
						t.Error(err)
					}
				}
			}
			for _, o := range originals {
				_, control, sddl, err := readCTYFixtureSecurity(t, o.path, "final")
				if err != nil || control != o.control || sddl != o.sddl {
					t.Errorf("original fixture DACL/protection not restored for %s: %v; control=%#x want=%#x; sddl=%q want=%q", o.path, err, control, o.control, sddl, o.sddl)
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
		sd, control, _, err := readCTYFixtureSecurity(t, o.path, "protected")
		if err != nil {
			t.Fatal(err)
		}
		assertCTYFixtureACL(t, sd, control, user.User.Sid, i == 0, mask)
	}
	return restore
}

func TestCTYFixtureAllowsUnprotectedPublication(t *testing.T) {
	_, path := newCTYPublicationFixture(t)
	fresh := strings.Replace(ctyFixtureOriginal, "Alpha", "Beta", 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := w.Write([]byte(fresh)); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	db, updated, err := refreshCTYDatabase(t.Context(), config.CTYConfig{Enabled: true, File: path, URL: server.URL})
	if err != nil || !updated || db == nil {
		t.Fatalf("constructed fixture blocked ordinary publication: updated=%v err=%v", updated, err)
	}
	if info, ok := db.LookupCallsign("K1ABC"); !ok || info.Country != "Beta" {
		t.Fatalf("constructed fixture did not consume changed content: %+v", info)
	}
}

func TestCTYFixtureRestoresDuringCleanup(t *testing.T) {
	root, path := newCTYPublicationFixture(t)
	// Return without an explicit restore: registered cleanup must still prove
	// exact restoration before the outer-container check and TempDir removal.
	_ = protectCTYFixture(t, root, path)
}

func TestCTYProtectedInputPublication(t *testing.T) {
	root, path := newCTYPublicationFixture(t)
	old := ctyFixtureOriginal
	fresh := strings.Replace(old, "Alpha", "Beta", 1)
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
	if err == nil || !errors.Is(err, os.ErrPermission) || !strings.Contains(err.Error(), "download: replace file") || db != nil || updated {
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
	// denial ACEs are removed. An unrelated downloader failure cannot pass.
	db, updated, err = refreshCTYDatabase(t.Context(), cfg)
	if err != nil || !updated || db == nil {
		t.Fatalf("unprotected publication control failed: updated=%v err=%v", updated, err)
	}
	info, ok = db.LookupCallsign("K1ABC")
	if !ok || info.Country != "Beta" || requests.Load() != 2 {
		t.Fatalf("changed publication control not observed: %+v", info)
	}
}
