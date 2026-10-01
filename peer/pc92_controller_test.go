package peer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
)

func controllerTestOwner(t *testing.T) (*protocolController, *session, *session, time.Time) {
	t.Helper()
	m := &Manager{
		localCall: "N0LOCAL", cfg: config.PeeringConfig{PC92Bitmap: 5, NodeVersion: "5457", NodeBuild: "633", HopCount: 99},
		sessions: newFixedIndex[string, *session](64), candidates: newFixedIndex[*session, *candidateState](128), blockedPeers: newFixedIndex[string, bool](64),
	}
	p := newProtocolController(m)
	m.protocol = p
	addSession := func(call string) *session {
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		s := &session{
			id: call, remoteCall: call, localCall: m.localCall, pc9x: true, manager: m,
			ctx: ctx, cancel: cancel, priorityLineCh: make(chan string, 128), writeCh: make(chan string, 128),
			peer: PeerEndpoint{family: config.PeeringPeerFamilyDXSpider},
		}
		m.sessions.Set(s.id, s)
		return s
	}
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	p.graph = newProtocolGraph(now)
	return p, addSession("N1PEER"), addSession("N4PEER"), now
}

func receiveControllerWire(t *testing.T, p *protocolController, source *session, wire string, now time.Time) *Frame {
	t.Helper()
	frame, err := ParseFrame(wire)
	if err != nil {
		t.Fatalf("frame fixture: %v", err)
	}
	p.receive(frame, source, now)
	return frame
}

func TestPC92ControllerMalformedCDoesNotPoisonFreshnessOrPartiallyApply(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^5N2AAA^1K1OLD^H10^", now)
	receiveControllerWire(t, p, source, "PC92^N2AAA^43202^C^5N2AAA^1K1NEW^9BAD^H10^", now)
	if p.graph.freshness.Value("N2AAA").Value != 43200 || p.graph.nodes.Value("N2AAA").Members.Value("K1OLD").Call != "K1OLD" || p.graph.users.Value("K1NEW") != 0 {
		t.Fatal("malformed C changed shared freshness or partially replaced membership")
	}
	receiveControllerWire(t, p, source, "PC92^N2AAA^43201^A^^1K1NEW^H10^", now)
	if p.graph.freshness.Value("N2AAA").Value != 43201 || p.graph.users.Value("K1NEW") != 1 {
		t.Fatal("invalid future C poisoned the later valid update")
	}
}

func TestPC92ControllerUnsupportedAndOwnOriginCannotMutate(t *testing.T) {
	p, source, destination, now := controllerTestOwner(t)
	for _, wire := range []string{
		"PC92^N2AAA^43200^F^5N2AAA^1K1USER^H10^",
		"PC92^N2AAA^43200^R^5N2AAA^1K1USER^H10^",
		"PC92^N2AAA^43200^X^5N2AAA^1K1USER^H10^",
		"PC92^N0LOCAL^43200^C^5N0LOCAL^1K1USER^H10^",
	} {
		receiveControllerWire(t, p, source, wire, now)
	}
	if p.graph.nodes.Len() != 0 || p.graph.freshness.Len() != 0 || len(destination.priorityLineCh) != 0 {
		t.Fatal("unsupported or own-origin record changed authority or was relayed")
	}
	if count, _, _ := p.pc92.occupancy(); count != 0 {
		t.Fatal("unsupported record consumed payload-cache capacity")
	}
}

func TestPC92ControllerHopsAndDuplicateAlternateIngress(t *testing.T) {
	p, first, second, now := controllerTestOwner(t)
	receiveControllerWire(t, p, first, "PC92^N2AAA^43200^C^5N2AAA^1K1USER^H0^", now)
	if p.graph.nodes.Len() != 0 {
		t.Fatal("H0 altered state")
	}
	receiveControllerWire(t, p, first, "PC92^N2AAA^43200^C^5N2AAA^1K1USER^H1^", now)
	if p.graph.users.Value("K1USER") != 1 || len(second.priorityLineCh) != 0 {
		t.Fatal("H1 did not apply exactly once without forwarding")
	}
	receiveControllerWire(t, p, second, "PC92^N2AAA^43200^C^5N2AAA^1K1USER^H7^", now.Add(time.Minute))
	if p.graph.users.Value("K1USER") != 1 || p.graph.edges != 1 || p.graph.ingress.Len() != 2 {
		t.Fatal("duplicate replay reapplied membership or lost alternate ingress")
	}
	if p.graph.nodes.Value("N2AAA").Seen != now || p.graph.freshness.Value("N2AAA").Accepted != now {
		t.Fatal("duplicate payload renewed topology or freshness lifetime")
	}
}

func TestPC92ControllerExternalSubjectSharesFreshnessAcrossOrigins(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^7N3EXT:5401^1K1USER^H10^", now)
	if wm, ok := p.graph.freshness.Get("N3EXT"); !ok || wm.Value != 43200 {
		t.Fatal("external subject did not acquire its reference freshness authority")
	}
	receiveControllerWire(t, p, source, "PC92^N5BBB^43199^C^7N3EXT:5401^1K2STALE^H10^", now)
	if p.graph.nodes.Value("N3EXT").Members.Value("K1USER").Call != "K1USER" || p.graph.users.Value("K2STALE") != 0 {
		t.Fatal("another origin used stale external-subject state to replace current membership")
	}
}

func TestPC92ControllerCapacityRefusalLeavesFrameRetryable(t *testing.T) {
	p, first, second, now := controllerTestOwner(t)
	p.pc92 = newBoundedDedupe(600*time.Second, 1, 1024)
	if p.pc92.admit("occupied", now) != dedupeAccepted {
		t.Fatal("failed to occupy test cache")
	}
	frame := receiveControllerWire(t, p, first, "PC92^N2AAA^43200^C^5N2AAA^1K1USER^H10^", now)
	if first.ctx.Err() == nil || p.graph.freshness.Len() != 0 || p.graph.nodes.Len() != 0 || p.pc92.contains(pc92Key(frame), now) {
		t.Fatal("authoritative refusal failed to close affected link or poisoned global authority")
	}
	later := now.Add(601 * time.Second)
	p.pc92.prune(later)
	p.receive(frame, second, later)
	if p.graph.users.Value("K1USER") != 1 || p.graph.freshness.Value("N2AAA").Value != 43200 || second.ctx.Err() != nil {
		t.Fatal("same still-fresh frame could not succeed through alternate ingress after capacity returned")
	}
}

func TestPC93ControllerSharesFreshnessWithPC92(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	var deliveries int
	p.manager.SetAnnouncementBroadcast(func(string) { deliveries++ })
	receiveControllerWire(t, p, source, "PC93^N2AAA^43201^*^K1FROM^*^hello^H10^", now)
	receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^5N2AAA^1K1OLD^H10^", now)
	if deliveries != 1 || p.graph.nodes.Len() != 0 || p.graph.freshness.Value("N2AAA").Value != 43201 {
		t.Fatal("PC93 and PC92 did not share origin ordering")
	}
	receiveControllerWire(t, p, source, "PC92^N2AAA^43202^C^5N2AAA^1K1NEW^H10^", now)
	if p.graph.users.Value("K1NEW") != 1 || p.graph.messageOrigins != 0 {
		t.Fatal("topology promotion failed or retained the message-only origin reservation")
	}
}

func TestPC93ControllerCanonicalOwnOriginRejected(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	var deliveries int
	p.manager.SetAnnouncementBroadcast(func(string) { deliveries++ })
	receiveControllerWire(t, p, source, "PC93^N0LOCAL-0^43200^*^K1FROM^*^looped^H10^", now)
	if deliveries != 0 || p.graph.freshness.Len() != 0 {
		t.Fatal("canonical own-origin PC93 bypassed loop rejection")
	}
}

func TestPC93ControllerEmptyMessageCannotPoisonTopologyFreshness(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	receiveControllerWire(t, p, source, "PC93^N2AAA^43201^*^K1FROM^*^^H10^", now)
	receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^5N2AAA^1K1USER^H10^", now)
	if p.graph.users.Value("K1USER") != 1 || p.graph.freshness.Value("N2AAA").Value != 43200 {
		t.Fatal("invalid empty message advanced shared authority and blocked valid topology")
	}
}

func TestPC93ControllerCacheFullDoesNotAdvanceFreshness(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	p.pc93 = newBoundedDedupe(600*time.Second, 1, 1024)
	p.pc93.admit("occupied", now)
	var deliveries int
	p.manager.SetAnnouncementBroadcast(func(string) { deliveries++ })
	frame := receiveControllerWire(t, p, source, "PC93^N2AAA^43200^*^K1FROM^*^hello^H10^", now)
	if deliveries != 0 || p.graph.freshness.Len() != 0 || source.ctx.Err() != nil {
		t.Fatal("message pressure changed freshness or closed an otherwise healthy peer")
	}
	later := now.Add(601 * time.Second)
	p.pc93.prune(later)
	p.receive(frame, source, later)
	if deliveries != 1 || p.graph.freshness.Value("N2AAA").Value != 43200 {
		t.Fatal("message did not remain retryable after capacity returned")
	}
}

func TestPC93PrivateCanonicalMappingAndAmbiguity(t *testing.T) {
	p, _, _, _ := controllerTestOwner(t)
	snapshot := LocalMembership{Revision: 7, RawCount: 1, Complete: true, Users: []LocalUser{{SessionID: 42, Login: "K1USER-00", IP: "192.0.2.1"}}}
	p.manager.SetMembershipProvider(func() LocalMembership { return snapshot })
	var delivered int
	p.manager.SetCurrentDirectMessage(func(login string, id, revision uint64, line string) bool {
		if login != "K1USER-00" || id != 42 || revision != snapshot.Revision || line == "" {
			t.Fatalf("private message resolved to wrong actual owner: %q id=%d revision=%d", login, id, revision)
		}
		delivered++
		return true
	})
	msg := pc93Message{NodeCall: "N2AAA", To: "K1USER-0", From: "K2FROM", Via: "*", Text: "private"}
	p.manager.routePC93(msg)
	if delivered != 1 {
		t.Fatal("canonical wire destination did not resolve to the unique current raw login")
	}
	snapshot.Revision++
	snapshot.RawCount++
	snapshot.Users = append(snapshot.Users, LocalUser{SessionID: 43, Login: "K1USER"})
	msg.To = "K1USER"
	p.manager.routePC93(msg)
	if delivered != 1 {
		t.Fatal("ambiguous canonical destination received private message")
	}
}

func TestPC92ControllerExternalNeedsTwoFreshnessSlotsAtomically(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	for i := 0; i < maxFreshnessOrigins-1; i++ {
		p.graph.commitWatermark(fmt.Sprintf("W%dAA", i), 43200, now, false)
	}
	frame := receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^7N3EXT:5401^1K1USER^H10^", now)
	if source.ctx.Err() == nil || p.graph.nodes.Len() != 0 || p.graph.freshness.Len() != maxFreshnessOrigins-1 || p.pc92.contains(pc92Key(frame), now) {
		t.Fatal("external record partially consumed the one remaining authority slot")
	}
	if _, exists := p.graph.freshness.Get("N2AAA"); exists {
		t.Fatal("failed external-subject admission poisoned origin freshness")
	}
}

func TestPC93ControllerMessageOriginsCannotConsumeTopologyReservation(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	for i := 0; i < maxMessageOrigins; i++ {
		p.graph.commitWatermark(fmt.Sprintf("W%dAA", i), 43200, now, true)
	}
	receiveControllerWire(t, p, source, "PC93^N2AAA^43200^*^K1FROM^*^new message origin^H10^", now)
	if _, exists := p.graph.freshness.Get("N2AAA"); exists || source.ctx.Err() != nil {
		t.Fatal("message-only flood consumed reserved authority or disconnected link")
	}
	receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^5N2AAA^1K1USER^H10^", now)
	if p.graph.users.Value("K1USER") != 1 || p.graph.messageOrigins != maxMessageOrigins {
		t.Fatal("message-only budget blocked topology authority")
	}
	var delivered int
	p.manager.SetAnnouncementBroadcast(func(string) { delivered++ })
	receiveControllerWire(t, p, source, "PC93^N2AAA^43201^*^K1FROM^*^existing topology origin^H10^", now)
	if delivered != 1 || p.graph.messageOrigins != maxMessageOrigins {
		t.Fatal("existing topology authority could not also carry PC93 without a new message slot")
	}
}
