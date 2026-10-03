// Portions copyright 2011 The Go Authors. All rights reserved.
// The adapted legacy path preparation uses the BSD-style license in
// ../third_party/go-sqlite3/provenance/GO-LICENSE.txt.

package peer

import (
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/sys/windows"
)

const topologyDirectoryBytes = 1024

var topologyDirectoryFullPath = syscall.GetFullPathName
var topologyDirectoryLongPaths = func() bool {
	// Exactly Go 1.26.4 runtime.initLongPathSupport's version predicate.
	// Read the version; do not change OS state or require a newer baseline.
	v := windows.RtlGetVersion()
	return v.MajorVersion > 10 || (v.MajorVersion == 10 && (v.MinorVersion > 0 || v.BuildNumber >= 15063))
}()

// The raw DSN's directory is selected by the caller, exactly as before. Only
// its OS-facing spelling changes: Windows Stat retains an absolute name even
// on modern Windows, so a relative MkdirAll can otherwise allocate an OS-sized
// FullPath buffer before the bounded VFS is entered.
func topologyDirectoryPath(path string) (string, error) {
	if len(path) > topologyDirectoryBytes {
		return "", errTopologyBudget
	}
	if strings.IndexByte(path, 0) >= 0 {
		return "", syscall.EINVAL
	}
	name := path
	var err error
	if !filepath.IsAbs(name) {
		name, err = topologyDirectoryAbsolute(name)
		if err != nil {
			return "", err
		}
	}
	if topologyDirectoryLongPaths {
		return name, nil
	}
	return topologyDirectoryLegacy(name)
}

// One fixed output and bounded input/conversion backing. A size return never
// selects a retry or a larger allocation. Pinned syscall conversion preserves
// invalid-UTF-8/WTF-8 behavior, including unpaired UTF-16 surrogates.
func topologyDirectoryAbsolute(path string) (string, error) {
	if len(path) > topologyDirectoryBytes {
		return "", errTopologyBudget
	}
	input, err := syscall.UTF16FromString(path)
	if err != nil {
		return "", err
	}
	var output [topologyDirectoryBytes + 1]uint16
	n, err := topologyDirectoryFullPath(&input[0], uint32(len(output)), &output[0], nil)
	if err != nil {
		return "", err
	}
	if n == 0 || n >= uint32(len(output)) || output[n] != 0 {
		return "", errTopologyBudget
	}
	bytes, backing := 0, 0
	for i := 0; i < int(n); i++ {
		u := output[i]
		if u == 0 {
			return "", errTopologyBudget
		}
		switch {
		case u < 0x80:
			bytes++
			backing++
		case u < 0x800:
			bytes += 2
			backing += 2
		case u >= 0xd800 && u <= 0xdbff && i+1 < int(n) && output[i+1] >= 0xdc00 && output[i+1] <= 0xdfff:
			bytes += 4
			backing += 6 // syscall sizes both surrogate units before encoding.
			i++
		default:
			bytes += 3
			backing += 3
		}
		if bytes > topologyDirectoryBytes || backing > 3*topologyDirectoryBytes {
			return "", errTopologyBudget
		}
	}
	name := syscall.UTF16ToString(output[:n])
	if !filepath.IsAbs(name) {
		return "", errTopologyBudget
	}
	return name, nil
}

// Match pinned Go's legacy prefix selection for an admitted absolute name.
// Literal extended/device names retain their namespace. The owner-local
// metadata/CreateDirectory callers pass this result directly to native APIs;
// no second stdlib pathname normalization or OS-sized retry follows.
func topologyDirectoryLegacy(path string) (string, error) {
	separator := func(b byte) bool { return b == '\\' || b == '/' }
	if len(path) < 248 || len(path) >= 4 && (path[:4] == `\??\` || separator(path[0]) && separator(path[1]) && path[2] == '?' && separator(path[3])) {
		return path, nil
	}
	full, err := topologyDirectoryAbsolute(path)
	if err != nil {
		return "", err
	}
	prefix := `\\?\`
	if len(path) >= 2 && separator(path[0]) && separator(path[1]) {
		if len(path) >= 4 && path[2] == '.' && separator(path[3]) {
			return full, nil
		}
		if len(full) < 2 || !separator(full[0]) || !separator(full[1]) {
			return "", errTopologyBudget
		}
		prefix, full = `\\?\UNC\`, full[2:]
	}
	if len(full) > topologyDirectoryBytes-len(prefix) {
		return "", errTopologyBudget
	}
	return prefix + full, nil
}
