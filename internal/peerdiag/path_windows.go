//go:build windows

// Portions copyright 2011 The Go Authors. All rights reserved.
// The adapted path-normalization algorithm uses the BSD-style license in
// ../../third_party/go-sqlite3/provenance/GO-LICENSE.txt.

package peerdiag

import (
	"errors"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/windows"
)

var errNativePathBudget = errors.New("diagnostic native path reservation exhausted")

const executableNativeUnits = 64 << 10

var diagnosticGetCurrentDirectory = windows.GetCurrentDirectory
var diagnosticGetModuleFileName = windows.GetModuleFileName
var diagnosticGetFullPathName = syscall.GetFullPathName

// The helper does not change cwd or environment. Match the exact Go 1.26.4
// runtime initLongPathSupport predicate, without changing its PEB/OS state.
var helperLongPaths = func() bool {
	v := windows.RtlGetVersion()
	return v.MajorVersion > 10 || (v.MajorVersion == 10 && (v.MinorVersion > 0 || v.BuildNumber >= 15063))
}()

func parentNativePathReservation() uint64 {
	if helperLongPaths {
		return 0
	}
	// Legacy NUL preparation and its retained os.File name coexist with
	// launch argv/environment/process owners. This uses parent credit; the
	// helper's separate two-MiB admission is not a parent-memory witness.
	return nativePathExpansionReservation
}

// Count the bytes returned by pinned syscall.UTF16ToString without allocating.
// Paired surrogates encode four bytes; unpaired surrogates retain WTF-8 triples.
func helperWTF8Bytes(input []uint16) uint64 {
	var count uint64
	for i := 0; i < len(input) && input[i] != 0; i++ {
		u := input[i]
		switch {
		case u < 0x80:
			count++
		case u < 0x800:
			count += 2
		case u >= 0xd800 && u <= 0xdbff && i+1 < len(input) && input[i+1] >= 0xdc00 && input[i+1] <= 0xdfff:
			count += 4
			i++
		default:
			count += 3
		}
	}
	return count
}

func helperCurrentDirectoryBytes(directory, overlong uint64) (uint64, error) {
	n, err := diagnosticGetCurrentDirectory(0, nil)
	if err != nil {
		return 0, err
	}
	// n includes NUL. Every non-NUL UTF-16 unit needs at least one WTF-8
	// byte, so this lower-bound refusal is valid before allocating the buffer.
	if n == 0 || uint64(n) > nativePathExpansionReservation/2 || !helperOptionsFit(directory, overlong, uint64(n-1)) {
		return 0, errNativePathBudget
	}
	buffer := make([]uint16, int(n))
	read, err := diagnosticGetCurrentDirectory(n, &buffer[0])
	if err != nil {
		return 0, err
	}
	if read == 0 || read >= n {
		return 0, errNativePathBudget
	}
	length := helperWTF8Bytes(buffer[:read])
	if !helperOptionsFit(directory, overlong, length) {
		return 0, errNativePathBudget
	}
	return length, nil
}

func helperExecutablePath(environmentBytes int) (string, error) {
	// Fixed acquisition storage: 128 KiB native output + at most 192 KiB
	// conversion backing. With queue/control (<600 KiB) this fits parent1MiB.
	// No native size return can select another allocation or a retry.
	var buffer [executableNativeUnits]uint16
	n, err := diagnosticGetModuleFileName(0, &buffer[0], uint32(len(buffer)))
	if err != nil {
		return "", err
	}
	if n == 0 || n >= uint32(len(buffer)) {
		return "", errNativePathBudget
	}
	// UTF16ToString's capacity may overestimate a surrogate pair by two
	// bytes. Three per unit bounds backing before conversion, not just length.
	backing := int(n) * 3
	if !parentLaunchFits(backing, backing+len(helperExecutable)+1, environmentBytes) {
		return "", errNativePathBudget
	}
	return syscall.UTF16ToString(buffer[:n]), nil
}

func helperOSPath(path string) (string, error) {
	if helperLongPaths || path == "" {
		return path, nil
	}
	cwd, err := helperCurrentDirectoryBytes(0, 0)
	if err != nil {
		return "", err
	}
	return helperLegacyOSPath(path, cwd)
}

// Adapted from Go 1.26.4 os/path_windows.go:addExtendedPrefix (Go Authors,
// BSD license retained in third_party/go-sqlite3/provenance/GO-LICENSE.txt).
// Only OS-facing names pass here; logical Options and active-log naming stay
// unchanged. Native workspace admission replaces the OS-sized retry.
func helperLegacyOSPath(path string, cwdBytes uint64) (string, error) {
	if path == "" || len(path) >= 4 && (path[:4] == `\??\` || isPathSeparator(path[0]) && isPathSeparator(path[1]) && path[2] == '?' && isPathSeparator(path[3])) {
		return path, nil
	}
	absolute := filepath.IsAbs(path)
	length := uint64(len(path))
	if !absolute {
		if cwdBytes > ^uint64(0)-length-1 {
			return "", errNativePathBudget
		}
		length += cwdBytes + 1
	}
	if absolute && length < 248 {
		return path, nil
	}
	var prefix string
	if length >= 248 {
		if len(path) >= 2 && isPathSeparator(path[0]) && isPathSeparator(path[1]) {
			if len(path) < 4 || path[2] != '.' || !isPathSeparator(path[3]) {
				prefix = `\\?\UNC\`
			}
		} else {
			prefix = `\\?\`
		}
	}
	return helperBoundedFullPath(path, prefix)
}

func isPathSeparator(b byte) bool { return b == '\\' || b == '/' }

func helperBoundedFullPath(path, prefix string) (string, error) {
	return helperBoundedFullPathWithin(path, prefix, nativePathExpansionReservation)
}

func helperBoundedFullPathWithin(path, prefix string, retainedBytes uint64) (string, error) {
	// Both native input and native-output/WTF-8 conversion coexist. Prefixes
	// are ASCII literals; their buffers are included before the size query.
	if uint64(len(path))+1 > nativePathExpansionReservation/2 {
		return "", errNativePathBudget
	}
	input, err := syscall.UTF16FromString(path)
	if err != nil {
		return "", err
	}
	n, err := diagnosticGetFullPathName(&input[0], 0, nil, nil)
	if err != nil {
		return "", err
	}
	remaining := uint64(nativePathExpansionReservation) - 2*uint64(len(path)+1)
	if n == 0 || uint64(n)+uint64(len(prefix)) > remaining/5 {
		return "", errNativePathBudget
	}
	buffer := make([]uint16, int(n)+len(prefix))
	read, err := diagnosticGetFullPathName(&input[0], n, &buffer[len(prefix)], nil)
	if err != nil {
		return "", err
	}
	if read == 0 || read >= n {
		return "", errNativePathBudget
	}
	buffer = buffer[:int(read)+len(prefix)]
	if prefix == `\\?\UNC\` {
		if read < 2 {
			return "", errNativePathBudget
		}
		buffer = buffer[2:]
	}
	for i := range prefix {
		buffer[i] = uint16(prefix[i])
	}
	if helperWTF8Bytes(buffer) > retainedBytes {
		return "", errNativePathBudget
	}
	// UTF16ToString retains its capacity, not only the returned string length:
	// a paired surrogate uses six capacity bytes for four result bytes. The
	// acquisition workspace above charges three bytes per native unit; retained
	// metadata paths additionally stay within 1.5 times their admitted byte credit.
	var backing uint64
	for _, unit := range buffer {
		if unit == 0 {
			break
		}
		switch {
		case unit < 0x80:
			backing++
		case unit < 0x800:
			backing += 2
		default:
			backing += 3
		}
	}
	if retainedBytes > ^uint64(0)-retainedBytes/2 || backing > retainedBytes+retainedBytes/2 {
		return "", errNativePathBudget
	}
	// A legacy long \\.\ device name deliberately receives no prefix. Helper
	// operations consume it directly through native APIs, never through a
	// second stdlib fixLongPath. Parent launch retains its existing short NUL
	// route and separate reservation.
	return syscall.UTF16ToString(buffer), nil
}
