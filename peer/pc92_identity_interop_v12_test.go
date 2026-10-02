package peer

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDXSpiderReferenceRawPC92Identity(t *testing.T) {
	reference, _ := startDXReference(t, false, "N0CALL")
	for _, tc := range []struct {
		call                 string
		rawValid, entryValid bool
	}{
		{"K1ABC", true, true}, {"K1ABC/P", true, true}, {"EA8/K1ABC", true, true},
		{"K1ABC-00", true, true}, {"K1ABC-01", true, true},
		{"W1AW/P", true, true}, {"K1ABC-01/P", true, true},
		{"K1ABC/", false, false}, {"/K1ABC", false, false}, {"K1ABC-000", false, false},
		{"k1abc", false, false}, {" K1ABC", false, false}, {"K1ABC  ", false, true},
		{"EA8/K1ABC/P-00", false, false},
	} {
		raw := reference.step(map[string]any{"command": "raw_callsign", "call": tc.call})
		entry := reference.step(map[string]any{"command": "decode_pc92_entry", "entry": "1" + tc.call + ":5457:633"})
		if raw.Valid != tc.rawValid || entry.Valid != tc.entryValid || entry.Valid && entry.Normalized != strings.TrimRight(tc.call, " ") {
			t.Fatalf("raw-role oracle %q: raw=%+v entry=%+v", tc.call, raw, entry)
		}
	}
	// Exercise framing and the actual receiver, separately from the pure raw
	// grammar. Its malformed-member C advances lastid and becomes empty; that
	// partial-C behavior is deliberately not Go's whole-record atomicity oracle.
	at := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	stamp := 43200
	for _, member := range []string{"K1ABC/", "/K1ABC", "K1ABC-000", "k1abc", " K1ABC", "K1ABC  ", "K1ABC/P", "K1ABC-01"} {
		// Isolate acceptance from the receiver's separate C reconciliation:
		// replacing an existing canonical member by its raw portable spelling
		// can delete that member after _add_thingy normalizes it.
		stamp++
		reference.frameAt(fmt.Sprintf("PC92^N0CALL^%d^C^5N0CALL^H99^", stamp), at)
		stamp++
		wire := fmt.Sprintf("PC92^N0CALL^%d^C^5N0CALL:5457:633^1%s^H99^", stamp, member)
		result := reference.frameAt(wire, at, "K1ABC", "K1ABC-1")
		want := member == "K1ABC  " || member == "K1ABC/P" || member == "K1ABC-01"
		call := "K1ABC"
		if member == "K1ABC-01" {
			call = "K1ABC-1"
		}
		if _, present := result.RouteUsers[call]; present != want {
			t.Fatalf("actual receiver member=%q routes=%v", member, result.RouteUsers)
		}
		referenceField(t, result.RouteNodes["N0CALL"], "lastid", fmt.Sprint(stamp))
	}
	// A malformed origin is rejected by upstream field validation; even a
	// syntactically valid member must not replace the preceding receiver state.
	for _, origin := range []string{"N0CALL/", "/N0CALL", "N0CALL-000", "n0call", " N0CALL", "N0CALL "} {
		wire := fmt.Sprintf("PC92^%s^%d^C^5N0CALL^1K2NEW^H99^", origin, stamp+1)
		result := reference.frameAt(wire, at, "K2NEW")
		if _, present := result.RouteUsers["K2NEW"]; present {
			t.Fatalf("actual receiver accepted malformed origin %q", origin)
		}
		referenceField(t, result.RouteNodes["N0CALL"], "lastid", fmt.Sprint(stamp))
	}
}
