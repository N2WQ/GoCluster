package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/filter"
)

func TestBaseSessionQuotaListenerTenAndRejectedFiles(t *testing.T) {
	s := newBaseQuotaServer(t, 20, 10)
	address := startSessionCeilingListener(t, s)
	sessions := make([]net.Conn, 10)
	for i := range sessions {
		sessions[i] = ceilingConnect(t, address, fmt.Sprintf("LZ3NY-%d", i))
		ceilingReadThrough(t, sessions[i], "READY\r\n")
	}
	baseQuotaCounts(t, s, "LZ3NY", 10, 0)

	// This older profile and shared preset library would be upgraded or cleaned
	// on admission. Install them only after the first ten logins have settled.
	profile := filepath.Join(filter.UserDataDir, "LZ3NY-10.yaml")
	original := oldLoginRecord(4)
	if err := os.WriteFile(profile, original, 0o600); err != nil {
		t.Fatal(err)
	}
	library := presetLibraryPathFor("LZ3NY-10")
	if err := os.MkdirAll(filepath.Dir(library), 0o700); err != nil {
		t.Fatal(err)
	}
	oldPreset := strings.Split(string(original), "recent_ips:")[0]
	libraryBytes := []byte("presets:\n  OLD:\n    " + strings.ReplaceAll(strings.TrimSuffix(oldPreset, "\n"), "\n", "\n    ") + "\n")
	if err := os.WriteFile(library, libraryBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	rejected := ceilingConnect(t, address, "LZ3NY-10")
	ceilingReadThrough(t, rejected, "Station session limit reached")
	ceilingWaitEOF(t, rejected)
	waitSessionCeiling(t, func() bool { return s.PreloginAdmissionMetricSnapshot().Active == 0 })
	for path, want := range map[string][]byte{profile: original, library: libraryBytes} {
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("station rejection changed %s: %v", path, err)
		}
		if _, err := os.Stat(path + ".pre-v5.bak"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("station rejection created a backup for %s: %v", path, err)
		}
	}
	if got := s.PreloginAdmissionMetricSnapshot(); got.BaseSessionRejects != 1 || got.TrackedSessionBases != 1 || s.GetClientCount() != 10 {
		t.Fatalf("after station rejection: %+v, registered=%d", got, s.GetClientCount())
	}

	// Existing full normalized identity remains replaceable at the station cap.
	replacement := ceilingConnect(t, address, "lz3ny-0/p")
	ceilingReadThrough(t, replacement, "READY\r\n")
	ceilingReadThrough(t, sessions[0], "Replaced by another login")
	ceilingWaitEOF(t, sessions[0])
	if s.GetClientCount() != 10 {
		t.Fatalf("replacement changed registered count: %d", s.GetClientCount())
	}

	independent := ceilingConnect(t, address, "K1ABC-1")
	ceilingReadThrough(t, independent, "READY\r\n")
	if s.GetClientCount() != 11 {
		t.Fatalf("different station could not use global capacity: %d", s.GetClientCount())
	}
	_ = sessions[1].Close()
	waitSessionCeiling(t, func() bool { return s.GetClientCount() == 10 })
	freed := ceilingConnect(t, address, "LZ3NY-10")
	ceilingReadThrough(t, freed, "READY\r\n")
	if s.GetClientCount() != 11 {
		t.Fatalf("departing session did not free station capacity: %d", s.GetClientCount())
	}
}

type baseQuotaObservedConn struct {
	net.Conn
	writeStarted chan struct{}
	closed       chan struct{}
	writeOnce    sync.Once
	closeOnce    sync.Once
}

func (c *baseQuotaObservedConn) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("Station session limit reached")) {
		c.writeOnce.Do(func() { close(c.writeStarted) })
	}
	return c.Conn.Write(p)
}

func (c *baseQuotaObservedConn) Close() error {
	err := c.Conn.Close()
	c.closeOnce.Do(func() { close(c.closed) })
	return err
}

func TestBaseSessionQuotaNonreadingRejectedPeer(t *testing.T) {
	s := newBaseQuotaServer(t, 0, 1)
	s.maxPreloginSessions = 1
	s.rejectWriteDeadline = 150 * time.Millisecond
	owner := configurationTestClient(s, "K1ABC-1")
	s.registerClient(owner)
	t.Cleanup(func() { s.unregisterClient(owner) })
	ticket, reason := s.tryAcquirePrelogin(tcpAddr("203.0.113.5", 1000))
	if ticket == nil || reason != "" {
		t.Fatalf("prelogin ticket: %v, %q", ticket, reason)
	}
	serverEnd, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close(); _ = serverEnd.Close() })
	observed := &baseQuotaObservedConn{Conn: serverEnd, writeStarted: make(chan struct{}), closed: make(chan struct{})}
	handlerDone := make(chan struct{})
	go func() { defer close(handlerDone); s.handleClient(observed, ticket) }()
	ceilingReadThrough(t, peer, "login: ")
	if _, err := io.WriteString(peer, "K1ABC-2\r\n"); err != nil {
		t.Fatal(err)
	}
	waitConfigurationTest(t, observed.writeStarted)
	if got := s.PreloginAdmissionMetricSnapshot().Active; got != 1 {
		t.Fatalf("blocked rejection retained %d prelogin tickets, want 1", got)
	}
	select {
	case <-observed.closed:
		t.Fatal("connection closed before blocked write was checked")
	case <-handlerDone:
		t.Fatal("handler released ownership before rejection finished")
	default:
	}
	waitConfigurationTest(t, observed.closed)
	waitConfigurationTest(t, handlerDone)
	if got := s.PreloginAdmissionMetricSnapshot(); got.Active != 0 || got.BaseSessionRejects != 1 || s.GetClientCount() != 1 {
		t.Fatalf("after blocked rejection: %+v registered=%d", got, s.GetClientCount())
	}
	recheckNoProfile(t, "K1ABC-2")
}
