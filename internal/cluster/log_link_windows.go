//go:build windows

package cluster

import (
	"os"

	"golang.org/x/sys/windows"
)

func logArchiveSingleLink(file *os.File) (bool, error) {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &info); err != nil {
		return false, err
	}
	return info.NumberOfLinks == 1, nil
}
