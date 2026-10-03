//go:build sqlite3_qualification && windows

// Package qualificationfixture provides native fixtures, never production VFS
// behavior. In particular it creates the requested GUID reparse target itself
// so a shell/provider cannot silently replace that target with a drive name.
package qualificationfixture

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/windows"
)

// GUIDJunction creates a junction entry within root pointing to the existing
// owned target. Target retains its literal suffix; it may have a trailing dot
// or space and be accessed through an extended path by the caller. Cleanup
// removes only the junction entry, before TempDir removes the owned target.
func GUIDJunction(t *testing.T, root, link, target string) string {
	t.Helper()
	return junction(t, root, link, target, true)
}

// DriveJunction preserves the absolute NT drive-form target, including literal
// trailing dots/spaces. Native readback proves a provider did not clean it.
func DriveJunction(t *testing.T, root, link, target string) string {
	t.Helper()
	return junction(t, root, link, target, false)
}

func junction(t *testing.T, root, link, target string, useGUID bool) string {
	t.Helper()
	for _, path := range []string{link, target} {
		rel, err := filepath.Rel(root, path)
		if err != nil || !filepath.IsAbs(root) || !filepath.IsAbs(path) || !filepath.IsLocal(rel) || rel == "." {
			t.Fatalf("GUID fixture escaped its owned root: root=%q path=%q: %v", root, path, err)
		}
	}
	displayTarget, wantedNative := target, `\??\`+target
	if useGUID {
		input, err := syscall.UTF16PtrFromString(root)
		if err != nil {
			t.Fatal(err)
		}
		var mount, volume [1024]uint16
		if err := windows.GetVolumePathName(input, &mount[0], uint32(len(mount))); err != nil {
			t.Fatalf("find fixture's existing volume mount: %v", err)
		}
		if err := windows.GetVolumeNameForVolumeMountPoint(&mount[0], &volume[0], uint32(len(volume))); err != nil {
			t.Fatalf("read fixture's existing volume GUID: %v", err)
		}
		volumeName := syscall.UTF16ToString(volume[:])
		if !strings.HasPrefix(volumeName, `\\?\Volume{`) || !strings.HasSuffix(volumeName, `\`) {
			t.Fatalf("native volume query did not return a GUID root: %q", volumeName)
		}
		mountName := syscall.UTF16ToString(mount[:])
		if len(target) < len(mountName) || !strings.EqualFold(target[:len(mountName)], mountName) {
			t.Fatalf("target is outside the fixture's existing volume: target=%q mount=%q", target, mountName)
		}
		// Preserve literal dot components in the native target. filepath.Rel
		// here would clean away the very semantics the fixture must observe.
		rel := target[len(mountName):]
		if !filepath.IsLocal(rel) {
			t.Fatalf("target escaped the fixture's existing volume: %q", target)
		}
		displayTarget = volumeName + rel
		wantedNative = `\??\` + displayTarget[4:]
	} else if volume := filepath.VolumeName(target); len(volume) != 2 || volume[1] != ':' {
		t.Fatalf("DRIVE-form fixture requires an actual drive path: %q", target)
	}
	substitute, err := syscall.UTF16FromString(wantedNative)
	if err != nil {
		t.Fatal(err)
	}
	printName, err := syscall.UTF16FromString(displayTarget)
	if err != nil {
		t.Fatal(err)
	}
	// REPARSE_DATA_BUFFER: eight-byte header, eight-byte mount-point
	// descriptor, then the two NUL-terminated UTF-16 names. Offsets are bytes
	// relative to PathBuffer; lengths exclude their respective terminators.
	// https://learn.microsoft.com/windows-hardware/drivers/ddi/ntifs/ns-ntifs-_reparse_data_buffer
	length := 16 + 2*(len(substitute)+len(printName))
	if length > syscall.MAXIMUM_REPARSE_DATA_BUFFER_SIZE {
		t.Fatal("fixture reparse data exceeds the native fixed buffer")
	}
	data := make([]byte, length)
	binary.LittleEndian.PutUint32(data[0:4], windows.IO_REPARSE_TAG_MOUNT_POINT)
	binary.LittleEndian.PutUint16(data[4:6], uint16(length-8))
	binary.LittleEndian.PutUint16(data[10:12], uint16(2*(len(substitute)-1)))
	binary.LittleEndian.PutUint16(data[12:14], uint16(2*len(substitute)))
	binary.LittleEndian.PutUint16(data[14:16], uint16(2*(len(printName)-1)))
	for i, unit := range substitute {
		binary.LittleEndian.PutUint16(data[16+2*i:], unit)
	}
	for i, unit := range printName {
		binary.LittleEndian.PutUint16(data[16+2*(len(substitute)+i):], unit)
	}
	if err := os.Mkdir(link, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// The absolute entry was checked above. This is nonrecursive and
		// never enumerates or deletes through the junction's target.
		if err := os.Remove(link); err != nil {
			t.Error("remove fixture junction entry", err)
		}
	})
	name, err := syscall.UTF16PtrFromString(link)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := windows.CreateFile(name, windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT|windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		t.Fatal("open owned fixture entry", err)
	}
	var returned uint32
	setErr := windows.DeviceIoControl(handle, windows.FSCTL_SET_REPARSE_POINT,
		&data[0], uint32(len(data)), nil, 0, &returned, nil)
	var actual [syscall.MAXIMUM_REPARSE_DATA_BUFFER_SIZE]byte
	var readErr error
	if setErr == nil {
		readErr = windows.DeviceIoControl(handle, windows.FSCTL_GET_REPARSE_POINT,
			nil, 0, &actual[0], uint32(len(actual)), &returned, nil)
	}
	closeErr := windows.CloseHandle(handle)
	if setErr != nil || readErr != nil || closeErr != nil {
		t.Fatalf("create/read native GUID junction: set=%v read=%v close=%v", setErr, readErr, closeErr)
	}
	if returned < 16 || returned > uint32(len(actual)) ||
		binary.LittleEndian.Uint32(actual[:4]) != windows.IO_REPARSE_TAG_MOUNT_POINT {
		t.Fatalf("native readback is not a complete mount-point record: bytes=%d", returned)
	}
	payloadEnd := 8 + int(binary.LittleEndian.Uint16(actual[4:6]))
	offset := int(binary.LittleEndian.Uint16(actual[8:10]))
	nameBytes := int(binary.LittleEndian.Uint16(actual[10:12]))
	if payloadEnd < 16 || payloadEnd > int(returned) || offset%2 != 0 || nameBytes%2 != 0 ||
		offset > payloadEnd-16 || nameBytes > payloadEnd-16-offset {
		t.Fatal("native readback has invalid target bounds", payloadEnd, offset, nameBytes)
	}
	actualName := make([]uint16, nameBytes/2)
	for i := range actualName {
		actualName[i] = binary.LittleEndian.Uint16(actual[16+offset+2*i:])
	}
	nativeTarget := syscall.UTF16ToString(actualName)
	if nativeTarget != wantedNative {
		t.Fatalf("native fixture target differs: got=%q requested=%q", nativeTarget, wantedNative)
	}
	t.Logf("native junction readback: %q -> %q", link, nativeTarget)
	return displayTarget
}
