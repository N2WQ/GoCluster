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
