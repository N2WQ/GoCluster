//go:build !windows

package main

func enableVirtualTerminal() bool {
	return true
}
