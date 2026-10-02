package peer

import (
	"strings"
	"testing"
)

func TestDXSpiderReferenceCanonicalIdentity(t *testing.T) {
	reference, first := startDXReference(t, false, "EA8/N0CALL/P-00")
	referenceField(t, first.Channel, "call", "N0CALL")
	for _, raw := range []string{"K1ABC", "K1ABC-00", "K1ABC-01", "K1ABC/P", "EA8/K1ABC/MM-00", "K1ABC/P-01", "W1AW/P", "K1ABC-01/P", "K1ABC/", "K1ABC-123"} {
		result := reference.step(map[string]any{"command": "normalise", "call": raw})
		got, valid := CanonicalPC92Call(raw)
		if result.Valid != valid || valid && result.Normalized != got {
			t.Fatalf("%q receiver=%q,%v Go=%q,%v", raw, result.Normalized, result.Valid, got, valid)
		}
	}
	stamp := NewTimestampGenerator()
	call, ok := CanonicalPC92Call("EA8/K1USER/P-00")
	if !ok {
		t.Fatal("alias fixture")
	}
	for _, action := range []string{"A", "D", "C", "C"} {
		ts, err := stamp.Next()
		if err != nil {
			t.Fatal(err)
		}
		record := &PC92Record{Origin: "N0CALL", Timestamp: ts, Action: action, Subject: PC92Entry{Call: "N0CALL", Flags: 5, Version: "5457"}, Members: []PC92Entry{{Call: call, Flags: 1}}, Hop: 99}
		wire, err := EncodePC92(record)
		if err != nil {
			t.Fatal(err)
		}
		result := reference.frame(wire, call)
		_, present := result.RouteUsers[call]
		if present != (action != "D") {
			t.Fatalf("receiver %s membership=%v", action, result.RouteUsers)
		}
		if strings.Contains(wire, "EA8/") {
			t.Fatal("local canonical encoder leaked alias")
		}
	}
	ts, err := stamp.Next()
	if err != nil {
		t.Fatal(err)
	}
	result := reference.frame("PC92^N0CALL^"+ts+"^C^^H99^", call)
	if _, exists := result.RouteUsers[call]; exists {
		t.Fatal("receiver implicit empty C retained user")
	}
}
