package telnet

import (
	"fmt"
	"testing"
	"time"
)

func TestBulletinDedupeSuppressesWWVWCYAndAnnouncements(t *testing.T) {
	tests := []struct {
		name      string
		broadcast func(*Server)
	}{
		{
			name: "wwv",
			broadcast: func(s *Server) {
				s.BroadcastWWV("WWV", "WWV de TEST <00> : SFI=1 A=1 K=1")
			},
		},
		{
			name: "wcy",
			broadcast: func(s *Server) {
				s.BroadcastWWV("WCY", "WCY de TEST <00> : K=1 expK=2")
			},
		},
		{
			name: "to all announcement",
			broadcast: func(s *Server) {
				s.BroadcastAnnouncement("To ALL de TEST: hello")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
			server, client := newBulletinDedupeTestServer(now, 10*time.Minute, 8)

			tc.broadcast(server)
			tc.broadcast(server)

			if got := len(client.controlChan); got != 1 {
				t.Fatalf("expected one delivered bulletin after duplicate suppression, got %d", got)
			}
			snap := server.BulletinDedupeSnapshot()
			if snap.Accepted != 1 || snap.Suppressed != 1 || snap.Tracked != 1 {
				t.Fatalf("unexpected snapshot: %+v", snap)
			}
		})
	}
}

func TestBulletinDedupeExpiresAfterWindow(t *testing.T) {
	now := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	server, client := newBulletinDedupeTestServer(now, time.Minute, 8)

	server.BroadcastAnnouncement("To ALL de TEST: hello")
	server.nowFn = func() time.Time { return now.Add(time.Minute + time.Second) }
	server.BroadcastAnnouncement("To ALL de TEST: hello")

	if got := len(client.controlChan); got != 2 {
		t.Fatalf("expected duplicate to deliver after window expiry, got %d", got)
	}
	snap := server.BulletinDedupeSnapshot()
	if snap.Accepted != 2 || snap.Suppressed != 0 || snap.Tracked != 1 {
		t.Fatalf("unexpected snapshot after expiry: %+v", snap)
	}
}

func TestBulletinDedupeDisabledDeliversDuplicates(t *testing.T) {
	now := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	server, client := newBulletinDedupeTestServer(now, 0, 0)

	server.BroadcastWWV("WWV", "WWV de TEST <00> : SFI=1 A=1 K=1")
	server.BroadcastWWV("WWV", "WWV de TEST <00> : SFI=1 A=1 K=1")

	if got := len(client.controlChan); got != 2 {
		t.Fatalf("expected disabled dedupe to deliver duplicates, got %d", got)
	}
	if snap := server.BulletinDedupeSnapshot(); snap.Enabled {
		t.Fatalf("expected disabled snapshot, got %+v", snap)
	}
}

func TestBulletinDedupeDoesNotConsumeKeyWithoutEligibleClients(t *testing.T) {
	now := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	server, client := newBulletinDedupeTestServer(now, 10*time.Minute, 8)
	client.filter.SetWWVEnabled(false)

	server.BroadcastWWV("WWV", "WWV de TEST <00> : SFI=1 A=1 K=1")
	if got := len(client.controlChan); got != 0 {
		t.Fatalf("expected blocked client to receive no bulletin, got %d", got)
	}
	if snap := server.BulletinDedupeSnapshot(); snap.Accepted != 0 || snap.Tracked != 0 {
		t.Fatalf("expected no dedupe key consumption, got %+v", snap)
	}

	client.filter.SetWWVEnabled(true)
	server.BroadcastWWV("WWV", "WWV de TEST <00> : SFI=1 A=1 K=1")
	if got := len(client.controlChan); got != 1 {
		t.Fatalf("expected first eligible delivery to be sent, got %d", got)
	}
	if snap := server.BulletinDedupeSnapshot(); snap.Accepted != 1 || snap.Tracked != 1 {
		t.Fatalf("unexpected snapshot after eligible delivery: %+v", snap)
	}
}

func TestBulletinDedupeBoundsRetainedKeysUnderChurn(t *testing.T) {
	now := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	server, client := newBulletinDedupeTestServer(now, time.Hour, 3)

	for i := 0; i < 5; i++ {
		server.BroadcastAnnouncement(fmt.Sprintf("To ALL de TEST: bulletin %d", i))
	}

	if got := len(client.controlChan); got != 5 {
		t.Fatalf("expected all unique bulletins to deliver, got %d", got)
	}
	snap := server.BulletinDedupeSnapshot()
	if snap.Tracked != 3 {
		t.Fatalf("expected tracked keys capped at 3, got %+v", snap)
	}
	if snap.Evicted != 2 {
		t.Fatalf("expected two evictions, got %+v", snap)
	}
}

func newBulletinDedupeTestServer(now time.Time, window time.Duration, maxEntries int) (*Server, *Client) {
	server := &Server{
		clients:        make(map[string]*Client),
		bulletinDedupe: newBulletinDedupeCache(window, maxEntries),
		nowFn:          func() time.Time { return now },
	}
	client := &Client{
		callsign:    "ALLOW",
		controlChan: make(chan controlMessage, 8),
		filter:      unrestrictedTestFilter(),
	}
	server.clients[client.callsign] = client
	return server, client
}
