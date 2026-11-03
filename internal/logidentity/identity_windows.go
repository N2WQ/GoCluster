//go:build windows

package logidentity

import (
	"os"

	"golang.org/x/sys/windows"
)

func inspect(file *os.File) (metadata, error) {
	info, err := file.Stat()
	if err != nil {
		return metadata{}, err
	}
	var native windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &native); err != nil {
		return metadata{}, err
	}
	index := uint64(native.FileIndexHigh)<<32 | uint64(native.FileIndexLow)
	if index == 0 {
		return metadata{}, ErrInvalidIdentity
	}
	return metadata{
		id:    identity{volume: uint64(native.VolumeSerialNumber), object: index, valid: true},
		mode:  info.Mode(),
		links: uint64(native.NumberOfLinks),
	}, nil
}
