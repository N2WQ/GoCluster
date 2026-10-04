package peer

import (
	"reflect"
	"strings"
	"testing"
)

func FuzzParseFrameHopSuffix(f *testing.F) {
	seeds := []string{
		"PC92^NODE^123^A^^9CALL:ver^H95^",
		"PC92^NODE^123^A^^9CALL:ver^H95^H94^H93^",
		"PC92^NODE^123^A^^9CALL:ver^H99^H9x^",
		"PC11^14074.0^K1ABC^23-Dec-2025^2001Z^CQ TEST^W1XYZ^ORIGIN^H3^",
		"PC93^GB7TLH^81701^WR3D-2^G1TLH-2^*^wot?^H98^",
		"PC92^N1NODE^123^K^5N1NODE^3^21^^H98^H99^",
		"PC93^N1NODE^123^LOGGER^K1ABC^*^H9x^H1ABC^^H99^",
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
		reencoded := frame.Encode(frame.Hop)
		reparsed, err := ParseFrame(reencoded)
		if err != nil {
			t.Fatalf("reparse failed: %v; line=%q encoded=%q", err, line, reencoded)
		}
		if reparsed.Hop != frame.Hop {
			t.Fatalf("hop mismatch after roundtrip: start=%d end=%d line=%q encoded=%q", frame.Hop, reparsed.Hop, line, reencoded)
		}
		if frame.Type == "PC92" || frame.Type == "PC93" || isPeerSpotFrame(frame.Type) {
			if !reflect.DeepEqual(frame.Fields, reparsed.Fields) {
				t.Fatalf("authority payload lost: %q => %q", frame.Fields, reparsed.Fields)
			}
		} else if hasTrailingHopLikeToken(reparsed.Fields) {
			t.Fatalf("trailing hop-like suffix remained after roundtrip: fields=%v line=%q encoded=%q", reparsed.Fields, line, reencoded)
		}
	})
}

func hasTrailingHopLikeToken(fields []string) bool {
	i := len(fields) - 1
	for i >= 0 && strings.TrimSpace(fields[i]) == "" {
		i--
	}
	if i < 0 {
		return false
	}
	_, isHopLike, _ := parseHopToken(fields[i])
	return isHopLike
}
