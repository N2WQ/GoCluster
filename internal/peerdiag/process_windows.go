//go:build windows

package peerdiag

import (
	"os"
	"syscall"
)

const helperExecutable = "peerdiag.exe"

// This is an enforced workspace reservation, not an assumed OS path limit.
// Native required lengths are admitted before allocation or conversion.
const nativePathExpansionReservation = 7 * 32768
const cwdPathFactor = 64

func helperProcessAttributes() *syscall.SysProcAttr { return &syscall.SysProcAttr{HideWindow: true} }
func helperEnvironmentRoot() string                 { return os.Getenv("SYSTEMROOT") }
func helperEnvironmentBytes(root string) int {
	return len("SYSTEMROOT=") + len(root) + len("GOMAXPROCS=1")
}
func helperEnvironment(root string) []string {
	return []string{"SYSTEMROOT=" + root, "GOMAXPROCS=1"}
}
