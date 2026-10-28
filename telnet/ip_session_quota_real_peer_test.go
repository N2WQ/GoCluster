package telnet

import (
	"io"
	"net"
	"net/netip"
	"testing"
)

// Exercise the actual TCP peer addresses, including their changing ephemeral
// ports, without overriding the listener or the server-side connection.
func TestIPSessionQuotaRealLoopbackPeerAndPreloginTickets(t *testing.T) {
	s := newIPQuotaServer(t, 0, 0, 1)
	address := startSessionCeilingListener(t, s)
	dial := func() net.Conn {
		t.Helper()
		conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		ceilingReadThrough(t, conn, "login: ")
		waitSessionCeiling(t, func() bool { return s.PreloginAdmissionMetricSnapshot().Active == 1 })
		return conn
	}
	login := func(conn net.Conn, call, marker string) {
		t.Helper()
		if _, err := io.WriteString(conn, call+"\r\n"); err != nil {
			t.Fatal(err)
		}
		ceilingReadThrough(t, conn, marker)
	}

	first := dial()
	firstLocal, ok := first.LocalAddr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("first local address is %T, want TCP", first.LocalAddr())
	}
	peerIP, ok := netip.AddrFromSlice(firstLocal.IP)
	if !ok || !peerIP.Unmap().IsLoopback() {
		t.Fatalf("first local IP %q is not loopback", firstLocal.IP)
	}
	key := peerIP.Unmap().String()
	login(first, "K1ABC-1", "READY\r\n")
	waitSessionCeiling(t, func() bool { return s.PreloginAdmissionMetricSnapshot().Active == 0 })
	ipQuotaCounts(t, s, key, 1, 0)

	second := dial()
	secondLocal, ok := second.LocalAddr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("second local address is %T, want TCP", second.LocalAddr())
	}
	secondIP, ok := netip.AddrFromSlice(secondLocal.IP)
	if !ok || secondIP.Unmap() != peerIP.Unmap() || secondLocal.Port == firstLocal.Port {
		t.Fatalf("loopback peers: first=%s second=%s; want same IP and distinct source ports", firstLocal, secondLocal)
	}
	login(second, "K2XYZ-1", "IP session limit reached")
	ceilingWaitEOF(t, second)
	waitSessionCeiling(t, func() bool { return s.PreloginAdmissionMetricSnapshot().Active == 0 })
	if s.GetClientCount() != 1 {
		t.Fatalf("registered sessions after rejection=%d, want 1", s.GetClientCount())
	}
	recheckNoProfile(t, "K2XYZ-1")
	ipQuotaCounts(t, s, key, 1, 0)

	_ = first.Close()
	waitSessionCeiling(t, func() bool { return s.GetClientCount() == 0 })
	ipQuotaCounts(t, s, key, 0, 0)
	third := dial()
	login(third, "K2XYZ-1", "READY\r\n")
	waitSessionCeiling(t, func() bool { return s.PreloginAdmissionMetricSnapshot().Active == 0 })
	ipQuotaCounts(t, s, key, 1, 0)
	if s.GetClientCount() != 1 {
		t.Fatalf("registered sessions after departure=%d, want 1", s.GetClientCount())
	}
}
