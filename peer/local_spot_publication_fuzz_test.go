package peer

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"dxcluster/spot"
)

func FuzzLocalPeerPublicationRoundTrip(f *testing.F) {
	for _, comment := range []string{"CQ", "  CQ\t~TEST~  ", " ", "\t", "~", "A\xfeB", "", "A^B", "A\xffB", "A\x9fB"} {
		for _, modern := range []bool{false, true} {
			f.Add(14074.0249, comment, "2001:0dB8:0000:0000::0001", modern)
		}
	}
	f.Add(0.00001, "CQ", " 192.0.2.1 ", true)
	f.Add(4294967295.999, "CQ", "192.0.2.1", false)
	f.Add(14074.0, strings.Repeat("X", MaxPeerFrameBytes), "192.0.2.1", true)
	f.Fuzz(func(t *testing.T, frequency float64, comment, ip string, modern bool) {
		if len(comment) > 2*MaxPeerFrameBytes || len(ip) > 2*MaxPeerFrameBytes {
			return
		}
		original := localPublicationSpot()
		original.Frequency, original.Comment, original.SpotterIP = frequency, comment, ip
		kind := "PC11"
		if modern {
			kind = "PC61"
		}
		// The existing admission rule is the grammar oracle. This independently
		// formed expected frame checks numeric and exact field preservation; the
		// literal unit vectors additionally lock spellings and wire boundaries.
		fields := []string{strconv.FormatFloat(frequency, 'f', -1, 64), "K1ABC", "11-Oct-2026", "0415Z", comment, "W1XYZ", "N0LOCAL"}
		if modern {
			fields = append(fields, ip)
		}
		want := kind + "^" + strings.Join(fields, "^") + "^H3^~"
		wantFrame := &Frame{Type: kind, Fields: fields, Hop: 3}
		_, admissionErr := validateOriginalPeerSpot(wantFrame)
		var got string
		if modern {
			got = formatPC61(original, "N0LOCAL", 3)
		} else {
			got = formatPC11(original, "N0LOCAL", 3)
		}
		if admissionErr != nil || len(want) > MaxPeerFrameBytes {
			if got != "" {
				t.Fatalf("unrepresentable %s emitted bytes=%d prefix=%q", kind, len(got), got[:min(len(got), 100)])
			}
			return
		}
		if got != want {
			t.Fatalf("local original fields changed: gotBytes=%d wantBytes=%d gotPrefix=%q wantPrefix=%q", len(got), len(want), got[:min(len(got), 100)], want[:min(len(want), 100)])
		}
		frame, err := ParseFrame(got)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := validateOriginalPeerSpot(frame); err != nil || frame.Type != kind || frame.Hop != 3 || !reflect.DeepEqual(frame.Fields, fields) {
			t.Fatalf("admitted local frame did not round trip: type=%s fields=%d hop=%d err=%v", frame.Type, len(frame.Fields), frame.Hop, err)
		}
		parsedFrequency, err := strconv.ParseFloat(frame.Fields[0], 64)
		if err != nil || parsedFrequency != original.Frequency || frame.Fields[1] != original.DXCall || frame.Fields[5] != original.DECall {
			t.Fatalf("parsed local identity/frequency differs from input: frame=%+v err=%v", frame, err)
		}
		if original.SourceType != spot.SourceManual {
			t.Fatal("formatter mutated source policy")
		}
	})
}
