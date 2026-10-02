package peer

import "testing"

func TestPC93TalkRouting(t *testing.T) {
	frame, err := ParseFrame("PC93^GB7TLH^81701^WR3D-2^G1TLH-2^*^wot?^H98^")
	if err != nil {
		t.Fatalf("parse frame: %v", err)
	}
	msg, ok := parsePC93(frame)
	if !ok {
		t.Fatalf("expected PC93 to parse")
	}
	line := formatPC93Line(msg)
	if line != "To WR3D-2 de G1TLH-2: wot?" {
		t.Fatalf("unexpected format: %q", line)
	}
	target, broadcast := pc93Target(msg)
	if broadcast {
		t.Fatalf("expected talk message to be direct")
	}
	if target != "WR3D-2" {
		t.Fatalf("unexpected target: %q", target)
	}
}

func TestPC93AnnouncementRouting(t *testing.T) {
	frame, err := ParseFrame("PC93^IZ7AUH-6^79200^*^IZ7AUH-6^*^hello^H97^")
	if err != nil {
		t.Fatalf("parse frame: %v", err)
	}
	msg, ok := parsePC93(frame)
	if !ok {
		t.Fatalf("expected PC93 to parse")
	}
	line := formatPC93Line(msg)
	if line != "To ALL de IZ7AUH-6: hello" {
		t.Fatalf("unexpected format: %q", line)
	}
	target, broadcast := pc93Target(msg)
	if !broadcast {
		t.Fatalf("expected announcement to broadcast")
	}
	if target != "" {
		t.Fatalf("expected empty target, got %q", target)
	}
}

func TestPC93NamedGroupLocalDelivery(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	var lines []string
	p.manager.SetAnnouncementBroadcast(func(line string) { lines = append(lines, line) })
	wire := "PC93^N2AAA^43200^LOGGER^K1FROM^*^hello%5Eworld^H10^"
	frame, err := ParseFrame(wire)
	if err != nil {
		t.Fatal(err)
	}
	p.manager.HandleFrame(frame, source)
	select {
	case input := <-p.input:
		queued, err := ParseFrame(input.wire)
		if err != nil {
			t.Fatal(err)
		}
		p.receive(queued, source, now)
	default:
		t.Fatal("named group was not admitted through manager")
	}
	receiveControllerWire(t, p, source, wire, now)
	if len(lines) != 1 || lines[0] != "To LOGGER de K1FROM: hello^world" || p.graph.freshness.Value("N2AAA").Value != 43200 {
		t.Fatalf("delivery=%q", lines)
	}
}

func TestPC93UnrepresentablePrivateTargetNeverBroadcasts(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	announcements, direct := 0, 0
	p.manager.SetAnnouncementBroadcast(func(string) { announcements++ })
	p.manager.SetDirectMessage(func(string, string) { direct++ })
	for _, target := range []string{"W1AW/P", "K1ABC-01/P", "K1ABC.P", "K1ABC-123"} {
		frame, err := ParseFrame("PC93^N2AAA^43200^" + target + "^K1FROM^*^private^H10^")
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := parsePC93(frame); ok {
			t.Errorf("unrepresentable private target accepted: %s", target)
		}
		p.receive(frame, source, now)
		p.manager.routePC93(pc93Message{NodeCall: "N2AAA", To: target, From: "K1FROM", Text: "private"})
	}
	if announcements != 0 || direct != 0 || p.graph.freshness.Len() != 0 {
		t.Fatalf("private rejection leaked: broadcasts=%d direct=%d freshness=%d", announcements, direct, p.graph.freshness.Len())
	}
}
