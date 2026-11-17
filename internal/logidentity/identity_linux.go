//go:build linux

package logidentity

import (
	"os"
	"syscall"
)

func inspect(file *os.File) (metadata, error) {
	info, err := file.Stat()
	if err != nil {
		return metadata{}, err
	}
	native, ok := info.Sys().(*syscall.Stat_t)
	if !ok || native == nil || native.Ino == 0 {
		return metadata{}, ErrInvalidIdentity
	}
	return metadata{
		id:    identity{volume: statUint64(native.Dev), object: native.Ino, valid: true},
		mode:  info.Mode(),
		links: statUint64(native.Nlink),
	}, nil
}

// Linux Stat_t uses different unsigned widths by architecture: Dev is uint32
// on MIPS, and Nlink is uint32 on ARM64 and several other targets.
func statUint64[T ~uint32 | ~uint64](value T) uint64 {
	return uint64(value)
}
