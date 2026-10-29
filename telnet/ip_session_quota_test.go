package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dxcluster/filter"
)

func newIPQuotaServer(t *testing.T, global, station, ip int) *Server {
	t.Helper()
	s := newBaseQuotaServer(t, global, station)
	s.maxSessionsPerIP = ip
	return s
}

func ipQuotaClient(s *Server, call, address string) *Client {
	c := configurationTestClient(s, call)
	c.address = address
	return c
}

func ipQuotaLogin(t *testing.T, s *Server, call, address string) *Client {
	t.Helper()
	c := ipQuotaClient(s, call, address)
	if _, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(5*time.Second)); err != nil {
		t.Fatalf("login %s from %s: %v", call, address, err)
	}
	return c
}

func ipQuotaTry(s *Server, c *Client) error {
	_, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(5*time.Second))
	return err
}

func ipQuotaCounts(t *testing.T, s *Server, ip string, registered, pending int) {
	t.Helper()
	s.clientsMutex.RLock()
	got := s.ipSessionCounts[ip]
	s.clientsMutex.RUnlock()
	if got.registered != registered || got.pending != pending {
		t.Fatalf("IP %s registered=%d pending=%d, want %d and %d", ip, got.registered, got.pending, registered, pending)
	}
}

func ipQuotaCurrent(t *testing.T, s *Server, call string, want *Client) {
	t.Helper()
	s.clientsMutex.RLock()
	got := s.clients[call]
	s.clientsMutex.RUnlock()
	if got != want {
		t.Fatalf("current owner of %s = %p, want %p", call, got, want)
	}
}

func TestIPSessionQuotaCeilingAndPrecedence(t *testing.T) {
	for _, tc := range []struct {
		name                string
		global, station, ip int
		want                error
	}{
		{"global_precedes_both", 1, 1, 1, errSessionFull},
		{"station_precedes_ip", 0, 1, 1, errBaseSessionFull},
		{"ip_only", 0, 0, 1, errIPSessionFull},
		{"ip_disabled_station", 0, 1, 0, errBaseSessionFull},
		{"ip_disabled_global", 1, 0, 0, errSessionFull},
		{"all_disabled", 0, 0, 0, nil},
		{"negative_disabled_api", -1, -1, -1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newIPQuotaServer(t, tc.global, tc.station, tc.ip)
			owner := ipQuotaLogin(t, s, "K1ABC-1", "192.0.2.1:1001")
			defer s.unregisterClient(owner)
			candidate := ipQuotaClient(s, "K1ABC-2", "192.0.2.1:1002")
			if err := ipQuotaTry(s, candidate); !errors.Is(err, tc.want) {
				t.Fatalf("new identity: got %v, want %v", err, tc.want)
			} else if err == nil {
				defer s.unregisterClient(candidate)
			} else {
				recheckNoProfile(t, candidate.callsign)
			}
		})
	}
}

func TestIPSessionQuotaExactReplacementAndTransfer(t *testing.T) {
	s := newIPQuotaServer(t, 1, 1, 1)
	old := ipQuotaLogin(t, s, "K1ABC-1", "192.0.2.1:1001")
	defer func() {
		s.clientsMutex.RLock()
		current := s.clients[old.callsign]
		s.clientsMutex.RUnlock()
		if current != nil {
			s.unregisterClient(current)
		}
	}()
	same := ipQuotaLogin(t, s, old.callsign, "192.0.2.1:1002")
	ipQuotaCurrent(t, s, old.callsign, same)
	ipQuotaCounts(t, s, "192.0.2.1", 1, 0)
	s.unregisterClient(old)
	ipQuotaCounts(t, s, "192.0.2.1", 1, 0)

	// The global and station ceilings remain full, but moving the same
	// identity needs only one free destination-IP slot.
	moved := ipQuotaLogin(t, s, old.callsign, "192.0.2.2:1003")
	ipQuotaCurrent(t, s, old.callsign, moved)
	ipQuotaCounts(t, s, "192.0.2.1", 0, 0)
	ipQuotaCounts(t, s, "192.0.2.2", 1, 0)
	s.unregisterClient(same)
	ipQuotaCounts(t, s, "192.0.2.2", 1, 0)
	again := ipQuotaLogin(t, s, old.callsign, "192.0.2.3:1004")
	ipQuotaCurrent(t, s, old.callsign, again)
	s.unregisterClient(moved)
	ipQuotaCounts(t, s, "192.0.2.2", 0, 0)
	ipQuotaCounts(t, s, "192.0.2.3", 1, 0)
	if got := s.GetClientCount(); got != 1 {
		t.Fatalf("registered sessions after replacements = %d, want 1", got)
	}
}

func TestIPSessionQuotaCrossIPRejectionPreservesOwnerAndFiles(t *testing.T) {
	s := newIPQuotaServer(t, 2, 1, 1)
	owner := ipQuotaLogin(t, s, "K1ABC-1", "192.0.2.1:1001")
	destination := ipQuotaLogin(t, s, "W1XYZ-1", "192.0.2.2:1002")
	defer s.unregisterClient(owner)
	defer s.unregisterClient(destination)
	baseline := testSavedPreset(t, filter.V6Configuration{Settings: filter.SettingsConfiguration{Dialect: "go", NoiseClass: "QUIET", DedupePolicy: "FAST"}})
	if err := filter.SavePreset(owner.callsign, "CONTEST", baseline); err != nil {
		t.Fatal(err)
	}
	profile := filepath.Join(filter.UserDataDir, owner.callsign+".yaml")
	library := presetLibraryPathFor(owner.callsign)
	for _, path := range []string{profile + ".pre-v6.bak", library + ".pre-v6.bak"} {
		if err := os.WriteFile(path, []byte("existing backup "+filepath.Base(path)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	paths := []string{profile, library, profile + ".pre-v6.bak", library + ".pre-v6.bak"}
	before := make(map[string][]byte, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = data
	}
	replacement := ipQuotaClient(s, owner.callsign, "192.0.2.2:1003")
	if err := ipQuotaTry(s, replacement); !errors.Is(err, errIPSessionFull) {
		t.Fatalf("cross-IP replacement at full destination: %v", err)
	}
	ipQuotaCurrent(t, s, owner.callsign, owner)
	select {
	case <-owner.done:
		t.Fatal("rejected replacement interrupted the existing owner")
	default:
	}
	for _, path := range paths {
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(before[path], after) {
			t.Fatalf("capacity rejection changed %s: %v", path, err)
		}
	}
	ipQuotaCounts(t, s, "192.0.2.1", 1, 0)
	ipQuotaCounts(t, s, "192.0.2.2", 1, 0)
}

func TestIPSessionQuotaTrackedIPChurn(t *testing.T) {
	s := newIPQuotaServer(t, 0, 0, 1)
	for i := 1; i <= 25; i++ {
		addr := fmt.Sprintf("192.0.2.%d:1000", i)
		c := ipQuotaLogin(t, s, fmt.Sprintf("K%dABC", i), addr)
		if got := s.PreloginAdmissionMetricSnapshot().TrackedSessionIPs; got != 1 {
			t.Fatalf("tracked IPs with owner %d = %d, want 1", i, got)
		}
		s.unregisterClient(c)
		s.clientsMutex.RLock()
		entries := len(s.ipSessionCounts)
		s.clientsMutex.RUnlock()
		if entries != 0 || s.PreloginAdmissionMetricSnapshot().TrackedSessionIPs != 0 {
			t.Fatalf("IP state retained after owner %d: entries=%d", i, entries)
		}
	}
}
