//go:build windows

package cluster

import (
	"net"
	"syscall"
	"testing"
)

// runtimeRetirementReservePort excludes even production's SO_REUSEADDR bind.
// Windows requires SO_EXCLUSIVEADDRUSE before bind; its Winsock value is the
// complement of SO_REUSEADDR. Reserve the same wildcard address family as Start.
func runtimeRetirementReservePort(t *testing.T) int {
	t.Helper()
	lc := net.ListenConfig{Control: func(_, _ string, raw syscall.RawConn) error {
		var optionErr error
		if err := raw.Control(func(fd uintptr) {
			optionErr = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, ^syscall.SO_REUSEADDR, 1)
		}); err != nil {
			return err
		}
		return optionErr
	}}
	listener, err := lc.Listen(t.Context(), "tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().(*net.TCPAddr).Port
}
