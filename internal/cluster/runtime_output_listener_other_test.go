//go:build !windows

package cluster

import (
	"net"
	"testing"
)

func runtimeRetirementReservePort(t *testing.T) int {
	t.Helper()
	lc := net.ListenConfig{}
	listener, err := lc.Listen(t.Context(), "tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener.Addr().(*net.TCPAddr).Port
}
