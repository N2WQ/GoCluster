//go:build linux

package cluster

import (
	"errors"
	"os"
	"syscall"
)

func logArchiveSingleLink(file *os.File) (bool, error) {
	info, err := file.Stat()
	if err != nil {
		return false, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false, errors.New("archive link count unavailable")
	}
	return stat.Nlink == 1, nil
}
