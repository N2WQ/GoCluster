package peer

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func localPublicationSpot() *spot.Spot {
	return &spot.Spot{
		DXCall: "K1ABC", DECall: "W1XYZ", Frequency: 14074.0249,
		Comment: "  CQ\t~TEST~  ", SpotterIP: "2001:0dB8:0000:0000::0001",
		Time:       time.Date(2026, time.October, 11, 4, 15, 37, 0, time.UTC),
		SourceType: spot.SourceManual,
	}
}

func localPublicationManager(t *testing.T, modern bool) (*Manager, *session) {
	t.Helper()
	dst := &session{id: "N1DST", remoteCall: "N1DST", pc9x: modern,
		ctx: context.Background(), writeCh: make(chan string, 8)}
	m := &Manager{cfg: config.PeeringConfig{ForwardSpots: true, HopCount: 3}, localCall: "N0LOCAL",
		sessions: sessionTestIndex(map[string]*session{dst.id: dst})}
	return m, dst
}

func TestLocalPeerPublicationExactNativePayload(t *testing.T) {
	for _, modern := range []bool{false, true} {
		for _, comment := range []string{"  CQ\t~TEST~  ", " ", "\t", "~", "CQ~~PC61~", "A\xfeB", "A\xc3\xa9B"} {
			t.Run(fmt.Sprintf("modern=%t/comment=%q", modern, comment), func(t *testing.T) {
				dst, remote := newTransportTestSession(t)
				dst.id, dst.pc9x = "N1DST", modern
				m := &Manager{cfg: config.PeeringConfig{ForwardSpots: true, HopCount: 3}, localCall: "N0LOCAL",
					sessions: sessionTestIndex(map[string]*session{dst.id: dst})}
				original := localPublicationSpot()
				original.Comment = comment
				before := []any{original.DXCall, original.DECall, original.Frequency, original.Comment, original.SpotterIP, original.Time, original.SourceType}
				m.PublishDX(original)
				if len(dst.writeCh) != 1 {
					t.Fatalf("native queue entries=%d want=1", len(dst.writeCh))
				}
				wire, frame := readNativePeerSpotWriter(t, dst, remote)
				kind, ip := "PC11", ""
				if modern {
					kind, ip = "PC61", "^2001:0dB8:0000:0000::0001"
				}
				want := kind + "^14074.0249^K1ABC^11-Oct-2026^0415Z^" + comment + "^W1XYZ^N0LOCAL" + ip + "^H3^~\r\n"
				if wire != want || frame.Type != kind || frame.Fields[4] != comment || frame.Hop != 3 {
					t.Fatalf("original local payload changed: wire=%q want=%q frame=%+v", wire, want, frame)
				}
				after := []any{original.DXCall, original.DECall, original.Frequency, original.Comment, original.SpotterIP, original.Time, original.SourceType}
				if !reflect.DeepEqual(before, after) {
					t.Fatal("publication changed the source Spot")
				}
			})
		}
	}
}

func TestLocalPeerPublicationFrequencyVectors(t *testing.T) {
	// Literal decimal vectors distinguish accepted numeric input from additional
	// one-decimal rounding or exponent notation in local publication.
	for _, test := range []struct {
		frequency float64
		text      string
	}{
		{0, "0"}, {0.00001, "0.00001"}, {14074.0249, "14074.0249"},
		{14074.025, "14074.025"}, {2450000.123456, "2450000.123456"}, {4294967295.99, "4294967295.99"},
	} {
		for _, modern := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/modern=%t", test.text, modern), func(t *testing.T) {
				m, dst := localPublicationManager(t, modern)
				original := localPublicationSpot()
				original.Frequency = test.frequency
				m.PublishDX(original)
				if len(dst.writeCh) != 1 {
					t.Fatalf("frequency publication entries=%d want=1", len(dst.writeCh))
				}
				frame, err := ParseFrame(<-dst.writeCh)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := validateOriginalPeerSpot(frame); err != nil {
					t.Fatalf("valid original frequency became inadmissible: %v", err)
				}
				got, err := strconv.ParseFloat(frame.Fields[0], 64)
				if err != nil || got != test.frequency || frame.Fields[0] != test.text {
					t.Fatalf("frequency=%q parsed=%v err=%v want=%q (%v)", frame.Fields[0], got, err, test.text, test.frequency)
				}
			})
		}
	}
}

func TestLocalPeerPublicationOriginalIPv6Spellings(t *testing.T) {
	for _, ip := range originalPeerIPv6Vectors() {
		t.Run(ip, func(t *testing.T) {
			m, dst := localPublicationManager(t, true)
			original := localPublicationSpot()
			original.SpotterIP = ip
			m.PublishDX(original)
			if len(dst.writeCh) != 1 {
				t.Fatalf("valid original IP refused: %q", ip)
			}
			frame, err := ParseFrame(<-dst.writeCh)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := validateOriginalPeerSpot(frame); err != nil || frame.Fields[7] != ip {
				t.Fatalf("original IP spelling changed or became invalid: got=%q want=%q err=%v", frame.Fields[7], ip, err)
			}
		})
	}
}

func TestLocalPeerPublicationRefusesUnrepresentableFields(t *testing.T) {
	bad := []struct {
		name   string
		change func(*spot.Spot, *Manager)
	}{
		{"empty comment", func(s *spot.Spot, _ *Manager) { s.Comment = "" }},
		{"negative frequency", func(s *spot.Spot, _ *Manager) { s.Frequency = -1 }},
		{"nonfinite frequency", func(s *spot.Spot, _ *Manager) { s.Frequency = math.NaN() }},
		{"infinite frequency", func(s *spot.Spot, _ *Manager) { s.Frequency = math.Inf(1) }},
		{"overflow frequency", func(s *spot.Spot, _ *Manager) { s.Frequency = 1 << 32 }},
		{"rounded overflow frequency", func(s *spot.Spot, _ *Manager) { s.Frequency = 4294967295.999 }},
		{"padded DX", func(s *spot.Spot, _ *Manager) { s.DXCall = " K1ABC" }},
		{"lowercase DX", func(s *spot.Spot, _ *Manager) { s.DXCall = "k1abc" }},
		{"invalid DE", func(s *spot.Spot, _ *Manager) { s.DECall = "NOCALL" }},
		{"invalid origin", func(_ *spot.Spot, m *Manager) { m.localCall = "LOCAL" }},
		{"unrepresentable year", func(s *spot.Spot, _ *Manager) { s.Time = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
	}
	for value := 0; value <= 255; value++ {
		allowed := value == 9 || (value >= 32 && value <= 127 && value != '^') || (value >= 160 && value <= 254)
		if !allowed {
			value := byte(value)
			bad = append(bad, struct {
				name   string
				change func(*spot.Spot, *Manager)
			}{fmt.Sprintf("comment byte %02x", value), func(s *spot.Spot, _ *Manager) { s.Comment = "A" + string([]byte{value}) + "B" }})
		}
	}
	for _, modern := range []bool{false, true} {
		for _, test := range bad {
			t.Run(fmt.Sprintf("modern=%t/%s", modern, test.name), func(t *testing.T) {
				m, dst := localPublicationManager(t, modern)
				original := localPublicationSpot()
				test.change(original, m)
				m.PublishDX(original)
				// PublishDX completes synchronous queue admission. No timed absence
				// check or return-value assumption can hide an invalid publication.
				if len(dst.writeCh) != 0 {
					t.Fatalf("unrepresentable fields queued: %q", <-dst.writeCh)
				}
			})
		}
	}
}

func TestLocalPeerPublicationVariantRefusalAndFraming(t *testing.T) {
	base := "PC11^14074.0249^K1ABC^11-Oct-2026^0415Z^^W1XYZ^N0LOCAL^H3^~"
	for _, test := range []struct {
		name       string
		comment    string
		ip         string
		wantLegacy int
		wantModern int
	}{
		{"blank IP", "CQ", "", 1, 0},
		{"padded IP", "CQ", " 192.0.2.1 ", 1, 0},
		{"zoned IP", "CQ", "fe80::1%eth0", 1, 0},
		{"legacy exact envelope", strings.Repeat("X", MaxPeerFrameBytes-len(base)), "192.0.2.1", 1, 0},
		{"modern exact envelope", strings.Repeat("X", MaxPeerFrameBytes-len(base)-len("^192.0.2.1")), "192.0.2.1", 1, 1},
		{"both over envelope", strings.Repeat("X", MaxPeerFrameBytes-len(base)+1), "192.0.2.1", 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			m, legacy := localPublicationManager(t, false)
			modern := &session{id: "N2DST", remoteCall: "N2DST", pc9x: true, ctx: context.Background(), writeCh: make(chan string, 8)}
			m.sessions.Set(modern.id, modern)
			original := localPublicationSpot()
			original.Comment, original.SpotterIP = test.comment, test.ip
			m.PublishDX(original)
			for _, variant := range []struct {
				session *session
				kind    string
				want    int
			}{{legacy, "PC11", test.wantLegacy}, {modern, "PC61", test.wantModern}} {
				if got := len(variant.session.writeCh); got != variant.want {
					t.Fatalf("%s entries=%d want=%d", variant.kind, got, variant.want)
				}
				if variant.want == 0 {
					continue
				}
				line := <-variant.session.writeCh
				if len(line) > MaxPeerFrameBytes || !strings.HasSuffix(line, "^~") {
					t.Fatalf("%s emitted invalid output envelope: bytes=%d tail=%q", variant.kind, len(line), line[max(0, len(line)-8):])
				}
				frame, err := ParseFrame(line)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := validateOriginalPeerSpot(frame); err != nil || frame.Type != variant.kind || frame.Fields[4] != test.comment {
					t.Fatalf("variant lost original content/admission: want=%s type=%s commentBytes=%d err=%v", variant.kind, frame.Type, len(frame.Fields[4]), err)
				}
			}
		})
	}
}

func TestLocalPeerPublicationExactEnvelopeNativeReceipt(t *testing.T) {
	for _, modern := range []bool{false, true} {
		t.Run(fmt.Sprintf("modern=%t", modern), func(t *testing.T) {
			dst, remote := newTransportTestSession(t)
			dst.id, dst.pc9x = "N1DST", modern
			m := &Manager{cfg: config.PeeringConfig{ForwardSpots: true, HopCount: 3}, localCall: "N0LOCAL",
				sessions: sessionTestIndex(map[string]*session{dst.id: dst})}
			original := localPublicationSpot()
			base := "PC11^14074.0249^K1ABC^11-Oct-2026^0415Z^^W1XYZ^N0LOCAL^H3^~"
			if modern {
				base = "PC61^14074.0249^K1ABC^11-Oct-2026^0415Z^^W1XYZ^N0LOCAL^2001:0dB8:0000:0000::0001^H3^~"
			}
			original.Comment = strings.Repeat("X", MaxPeerFrameBytes-len(base))
			m.PublishDX(original)
			if len(dst.writeCh) != 1 {
				t.Fatalf("exact-sized variant refused: entries=%d", len(dst.writeCh))
			}
			wire, frame := readNativePeerSpotWriter(t, dst, remote)
			if len(wire) != MaxPeerFrameBytes+2 || frame.Fields[4] != original.Comment {
				t.Fatalf("exact-sized native receipt changed: wireBytes=%d commentBytes=%d wantCommentBytes=%d", len(wire), len(frame.Fields[4]), len(original.Comment))
			}
		})
	}
}

func TestLocalPeerPublicationForwardingAgeAndQueueGates(t *testing.T) {
	for _, forward := range []bool{false, true} {
		for _, test := range []struct {
			name   string
			source spot.SourceType
			test   bool
			stale  bool
			want   int
		}{
			{"manual", spot.SourceManual, false, false, 1},
			{"upstream", spot.SourceUpstream, false, false, 0},
			{"received peer", spot.SourcePeer, false, false, 0},
			{"test spotter", spot.SourceManual, true, false, 0},
			{"stale manual", spot.SourceManual, false, true, 0},
		} {
			t.Run(fmt.Sprintf("forward=%t/%s", forward, test.name), func(t *testing.T) {
				m, dst := localPublicationManager(t, true)
				m.cfg.ForwardSpots, m.maxAgeSeconds = forward, 60
				original := localPublicationSpot()
				original.SourceType, original.IsTestSpotter, original.Time = test.source, test.test, time.Now()
				if test.stale {
					original.Time = original.Time.Add(-2 * time.Minute)
				}
				m.PublishDX(original)
				if got := len(dst.writeCh); got != test.want {
					t.Fatalf("gated publication entries=%d want=%d", got, test.want)
				}
			})
		}
	}
	t.Run("full destination remains bounded", func(t *testing.T) {
		m, dst := localPublicationManager(t, true)
		for i := 0; i < cap(dst.writeCh); i++ {
			if err := dst.sendLine("existing queued record"); err != nil {
				t.Fatal(err)
			}
		}
		m.PublishDX(localPublicationSpot())
		if len(dst.writeCh) != cap(dst.writeCh) {
			t.Fatal("full publication queue changed capacity or removed earlier output")
		}
		for len(dst.writeCh) != 0 {
			if line := <-dst.writeCh; line != "existing queued record" {
				t.Fatalf("full queue accepted or replaced output: %q", line)
			}
		}
	})
}
