package telnet

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func peerMembershipTestServer() *Server {
	return &Server{clients: make(map[string]*Client)}
}

func peerMembershipTestClient(server *Server, login, address string) *Client {
	return &Client{
		server:      server,
		callsign:    login,
		address:     address,
		controlChan: make(chan controlMessage, 8),
		done:        make(chan struct{}),
	}
}

func TestPeerMembershipSnapshotOwnershipAndMetadata(t *testing.T) {
	server := peerMembershipTestServer()
	first := peerMembershipTestClient(server, "N0CALL-0", "192.0.2.1:12345")
	second := peerMembershipTestClient(server, "K1ABC/P", "[2001:db8::1]:54321")
	server.registerClient(first)
	server.registerClient(second)
	snapshot := server.CurrentPeerMembership()
	if !snapshot.Complete || snapshot.RawCount != 2 || snapshot.Revision != 2 || len(snapshot.Users) != 2 {
		t.Fatalf("unexpected complete snapshot: %+v", snapshot)
	}
	if snapshot.Users[0].Login != "K1ABC/P" || snapshot.Users[0].IP != "2001:db8::1" || snapshot.Users[1].IP != "192.0.2.1" {
		t.Fatalf("snapshot did not preserve sorted raw identities and address metadata: %+v", snapshot.Users)
	}
	if snapshot.Users[0].SessionID == 0 || snapshot.Users[0].SessionID == snapshot.Users[1].SessionID {
		t.Fatalf("session IDs are not unique: %+v", snapshot.Users)
	}
	snapshot.Users[0].Login = "MUTATED"
	if got := server.CurrentPeerMembership(); got.Users[0].Login != "K1ABC/P" {
		t.Fatal("caller mutation changed retained server membership")
	}
}

func TestPeerMembershipReplacementAndStaleUnregister(t *testing.T) {
	server := peerMembershipTestServer()
	var peerNotifications, dashboardNotifications int
	server.SetPeerMembershipListener(func() {
		peerNotifications++
		_ = server.CurrentPeerMembership() // Callback must run outside the lock.
	})
	server.SetClientListListener(func() { dashboardNotifications++ })
	old := peerMembershipTestClient(server, "N0CALL", "192.0.2.1:1")
	server.registerClient(old)
	before := server.CurrentPeerMembership()
	replacement := peerMembershipTestClient(server, old.callsign, "192.0.2.2:2")
	server.registerClient(replacement)
	after := server.CurrentPeerMembership()
	if after.Revision != before.Revision+1 || after.RawCount != 1 || after.Users[0].IP != "192.0.2.2" || after.Users[0].SessionID == before.Users[0].SessionID {
		t.Fatalf("replacement identity/metadata not published: before=%+v after=%+v", before, after)
	}
	server.unregisterClient(old)
	if got := server.CurrentPeerMembership(); got.Revision != after.Revision || got.RawCount != 1 || got.Users[0] != after.Users[0] {
		t.Fatalf("stale cleanup changed current owner: %+v", got)
	}
	if peerNotifications != 2 || dashboardNotifications != 2 {
		t.Fatalf("listeners clobbered or notified for stale removal: peer=%d dashboard=%d", peerNotifications, dashboardNotifications)
	}
	server.unregisterClient(replacement)
	if got := server.CurrentPeerMembership(); got.RawCount != 0 || len(got.Users) != 0 || got.Revision != after.Revision+1 {
		t.Fatalf("current removal not visible: %+v", got)
	}
	server.SetPeerMembershipListener(nil)
	server.registerClient(peerMembershipTestClient(server, "K1NEW", ""))
}

func TestPeerMembershipOverflowDoesNotPublishPartialOrChangeAdmission(t *testing.T) {
	server := peerMembershipTestServer()
	var last *Client
	for i := 0; i <= MaxPeerMembershipSessions; i++ {
		last = peerMembershipTestClient(server, fmt.Sprintf("K%dAA", i), "192.0.2.1:1")
		server.registerClient(last)
	}
	got := server.CurrentPeerMembership()
	if got.Complete || got.RawCount != MaxPeerMembershipSessions+1 || len(got.Users) != 0 {
		t.Fatalf("overflow published partial membership: raw=%d complete=%v entries=%d", got.RawCount, got.Complete, len(got.Users))
	}
	if server.GetClientCount() != MaxPeerMembershipSessions+1 {
		t.Fatal("publication bound changed local admission")
	}
	server.unregisterClient(last)
	got = server.CurrentPeerMembership()
	if !got.Complete || len(got.Users) != MaxPeerMembershipSessions {
		t.Fatalf("full population did not recover after fitting: complete=%v entries=%d", got.Complete, len(got.Users))
	}
}

func TestPeerMembershipChurnHasOnlyCurrentOwners(t *testing.T) {
	server := peerMembershipTestServer()
	for i := 0; i < 2000; i++ {
		client := peerMembershipTestClient(server, fmt.Sprintf("K%dAA", i), "")
		server.registerClient(client)
		server.unregisterClient(client)
	}
	got := server.CurrentPeerMembership()
	if got.RawCount != 0 || len(got.Users) != 0 || len(server.clients) != 0 || got.Revision != 4000 {
		t.Fatalf("historical owners retained after churn: %+v", got)
	}
}

func TestCurrentDirectMessageRejectsStaleOwnerAndMembershipRevision(t *testing.T) {
	server := peerMembershipTestServer()
	client := peerMembershipTestClient(server, "N0CALL", "")
	server.registerClient(client)
	snapshot := server.CurrentPeerMembership()
	user := snapshot.Users[0]
	for i := 0; i < 2; i++ {
		if !server.SendCurrentDirectMessage(user.Login, user.SessionID, snapshot.Revision, "same private text") {
			t.Fatal("current private message was refused or incorrectly deduplicated")
		}
	}
	if len(client.controlChan) != 2 {
		t.Fatal("direct messages passed through bulletin dedupe")
	}
	// This second login remains locally admitted; it canonically collides in
	// DXSpider. Even before the peer manager processes its wakeup, the previous
	// snapshot must no longer authorize private delivery.
	collision := peerMembershipTestClient(server, "N0CALL-0", "")
	server.registerClient(collision)
	if server.SendCurrentDirectMessage(user.Login, user.SessionID, snapshot.Revision, "stale unique target") {
		t.Fatal("message admitted after a population change introduced ambiguity")
	}
	replacement := peerMembershipTestClient(server, client.callsign, "")
	server.registerClient(replacement)
	current := server.CurrentPeerMembership()
	if server.SendCurrentDirectMessage(user.Login, user.SessionID, current.Revision, "stale session") {
		t.Fatal("message redirected from old session to replacement")
	}
	if len(replacement.controlChan) != 0 {
		t.Fatal("replacement received stale private message")
	}
	replacement.close("test closed")
	if server.SendCurrentDirectMessage(replacement.callsign, replacement.peerSessionID, current.Revision, "closed target") {
		t.Fatal("closed current target received message")
	}
}

func TestCurrentDirectMessageOverflowReporterOutsideMembershipLock(t *testing.T) {
	server := peerMembershipTestServer()
	var reports atomic.Int32
	server.connectionReporter = func(ConnectionEvent) {
		_ = server.GetClientCount()
		reports.Add(1)
	}
	client := peerMembershipTestClient(server, "N0CALL", "")
	server.registerClient(client)
	snapshot := server.CurrentPeerMembership()
	for i := 0; i < cap(client.controlChan); i++ {
		if !server.SendCurrentDirectMessage(client.callsign, client.peerSessionID, snapshot.Revision, "message") {
			t.Fatal("queue refused before full")
		}
	}
	if server.SendCurrentDirectMessage(client.callsign, client.peerSessionID, snapshot.Revision, "overflow") {
		t.Fatal("full control queue admitted private message")
	}
	select {
	case <-client.done:
	default:
		t.Fatal("full control queue did not disconnect")
	}
	if reports.Load() != 1 {
		t.Fatalf("disconnect reports=%d, want 1", reports.Load())
	}
}

func TestPeerMembershipConcurrentReplacementAndDelivery(t *testing.T) {
	server := peerMembershipTestServer()
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for i := 0; i < 300; i++ {
			client := peerMembershipTestClient(server, "N0CALL", fmt.Sprintf("192.0.2.1:%d", i))
			server.registerClient(client)
			server.unregisterClient(client)
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < 300; i++ {
			snapshot := server.CurrentPeerMembership()
			for _, user := range snapshot.Users {
				server.SendCurrentDirectMessage(user.Login, user.SessionID, snapshot.Revision, "private text")
			}
		}
	}()
	workers.Wait()
	if got := server.CurrentPeerMembership(); got.RawCount != 0 {
		t.Fatalf("concurrent ownership churn left current users: %+v", got)
	}
}
