//go:build windows

package main

import (
	"os"

	"golang.org/x/sys/windows"
)

func enableVirtualTerminal() bool {
	handle := windows.Handle(os.Stdout.Fd())
	var mode uint32
	if err := windows.GetConsoleMode(handle, &mode); err != nil {
		return false
	}
	mode |= windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING
	if err := windows.SetConsoleMode(handle, mode); err != nil {
		return false
	}
	return true
}
