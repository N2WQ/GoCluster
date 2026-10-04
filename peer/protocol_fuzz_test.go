package peer

import (
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func FuzzParseFrameHopSuffix(f *testing.F) {
	seeds := []string{
		"PC00^H0^^H0^^H0",
		"PC00^DATA^H7^^H3^",
		"PC00^DATA^H7^ ^H3^",
		"PC00^DATA^H9x^\t^H3^",
		"PC00^H0^",
		"PC00^^H0^",
		"PC00^",
		"PC92^NODE^123^A^^9CALL:ver^H95^",
		"PC92^NODE^123^A^^9CALL:ver^H95^H94^H93^",
		"PC92^NODE^123^A^^9CALL:ver^H99^H9x^",
		"PC11^14074.0^K1ABC^23-Dec-2025^2001Z^CQ TEST^W1XYZ^ORIGIN^H3^",
		"PC61^14074^K1ABC^04-Oct-2026^1200Z^CQ~TEST^W1XYZ^N0CALL^2001:0DB8:0000::1^H3^",
		"PC26^14074^K1ABC^04-Oct-2026^1200Z^~CQ~^W1XYZ^N0CALL^",
		"PC26^14074^K1ABC^04-Oct-2026^1200Z^CQ^W1XYZ^N0CALL^H1ABC^H3^",
		"PC93^GB7TLH^81701^WR3D-2^G1TLH-2^*^wot?^H98^",
		"PC92^N1NODE^123^K^5N1NODE^3^21^^H98^H99^",
		"PC93^N1NODE^123^LOGGER^K1ABC^*^H9x^H1ABC^^H99^",
		"PC00^" + strings.Repeat("A", MaxPeerFrameBytes-9),
		"PC00^" + strings.Repeat("A", MaxPeerFrameBytes-8),
		"PC00^" + strings.Repeat("A", MaxPeerFrameBytes-5),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, line string) {
		line = strings.TrimSpace(line)
		if line == "" {
			return
		}
		frame, err := ParseFrame(line)
		if err != nil {
			return
		}
		before := snapshotProtocolFrame(frame)
		reencoded := frame.Encode(before.Hop)
		if !reflect.DeepEqual(*frame, before) {
			t.Fatalf("Encode mutated source frame: before=%+v after=%+v", before, frame)
		}
		// Parsing can remove bytes; adding the default ^H0^ is the largest
		// possible growth. Enforce both bounds before any size-refusal branch.
		if len(reencoded) > MaxPeerFrameBytes+4 || len(reencoded) > len(strings.TrimRight(line, "\r\n~"))+4 {
			t.Fatalf("unexpected encoding growth: input=%d output=%d", len(line), len(reencoded))
		}
		// This lexical projection is independent of suffix parsing. A gap can
		// protect H7 as payload, and zero fields differ from one empty field.
		parts := strings.Split(reencoded, "^")
		if len(parts) < 3 || parts[0] != before.Type || parts[len(parts)-1] != "" || parts[len(parts)-2] != "H"+strconv.Itoa(before.Hop) {
			t.Fatalf("invalid encoded framing: type=%q hop=%d encoded=%q", before.Type, before.Hop, reencoded)
		}
		if payload := parts[1 : len(parts)-2]; !slices.Equal(payload, before.Fields) {
			t.Fatalf("encoded payload changed: expected=%q encoded=%q", before.Fields, payload)
		}
		reparsed, err := ParseFrame(reencoded)
		if len(reencoded) > MaxPeerFrameBytes {
			if err == nil {
				t.Fatalf("parser accepted overlimit encoding: %d bytes", len(reencoded))
			}
			return
		}
		if err != nil {
			t.Fatalf("reparse failed: %v; line=%q encoded=%q", err, line, reencoded)
		}
		if reparsed.Type != before.Type || reparsed.Hop != before.Hop || !reflect.DeepEqual(reparsed.Fields, before.Fields) {
			t.Fatalf("frame changed after roundtrip: before=%+v after=%+v line=%q encoded=%q", before, reparsed, line, reencoded)
		}
		if again := reparsed.Encode(reparsed.Hop); again != reencoded {
			t.Fatalf("encoding was not canonical: first=%q second=%q", reencoded, again)
		}
	})
}
