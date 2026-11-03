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
		id:    identity{volume: uint64(native.Dev), object: native.Ino, valid: true},
		mode:  info.Mode(),
		links: uint64(native.Nlink),
	}, nil
}
