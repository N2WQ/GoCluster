package peer

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func peerSpotTestManager(forward, modern bool) (*Manager, *session, *session, chan *spot.Spot) {
	source := &session{id: "N1SRC", remoteCall: "N1SRC", ctx: context.Background(), writeCh: make(chan string, 4)}
	destination := &session{id: "N2DST", remoteCall: "N2DST", pc9x: modern, ctx: context.Background(), writeCh: make(chan string, 4)}
	ingest := make(chan *spot.Spot, 4)
	manager := &Manager{cfg: config.PeeringConfig{ForwardSpots: forward}, dedupe: newBoundedDedupe(time.Minute, 8, 4096), ingest: ingest,
		sessions: sessionTestIndex(map[string]*session{source.id: source, destination.id: destination})}
	return manager, source, destination, ingest
}

func TestPeerSpotCorrectionIsolationBeforeRelay(t *testing.T) {
	for _, kind := range []string{"PC11", "PC61"} {
		for _, modern := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/modern=%v", kind, modern), func(t *testing.T) {
				wire := originalSpotTestWire(kind, "  FT8 -10 dB 1100Z  CQ\tDX \xfe  ")
				frame, err := ParseFrame(wire)
				if err != nil {
					t.Fatal(err)
				}
				m, source, destination, ingest := peerSpotTestManager(true, modern)
				// The existing cache mutex is a test-only barrier after local
				// handoff, before serialization/fanout. No production hooks.
				m.dedupe.mu.Lock()
				done := make(chan struct{})
				go func() { m.HandleFrame(frame, source); close(done) }()
				select {
				case local := <-ingest:
					if local.DXCall != "K1ABC" || local.DECall != "W1XYZ" || local.Frequency != 14074.02 {
						m.dedupe.mu.Unlock()
						t.Fatalf("local normalization lost: %+v", local)
					}
					local.DXCall, local.DECall = "K9CHANGED", "W9CHANGED"
					local.Comment, local.Mode, local.Frequency = "corrected", "CW", 7001.25
					local.Time, local.SourceNode, local.SpotterIP = time.Now().Add(time.Hour), "N9CHANGED", "192.0.2.9"
				case <-time.After(5 * time.Second):
					m.dedupe.mu.Unlock()
					t.Fatal("local handoff did not occur")
				}
				m.dedupe.mu.Unlock()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("relay did not finish")
				}
				want := strings.TrimSuffix(wire, "H3^") + "H2^~"
				if kind == "PC61" && !modern {
					want = "PC11" + strings.TrimSuffix(wire[4:], "^2001:0db8:0:0::0001^H3^") + "^H2^~"
				}
				select {
				case line := <-destination.writeCh:
					if line != want {
						t.Fatalf("original sentence changed:\ngot  %q\nwant %q", line, want)
					}
					// Verify the supplied sentence through the native sole writer,
					// including its CRLF (not merely a queued-prefix assertion).
					server, client := net.Pipe()
					defer server.Close()
					defer client.Close()
					var sent bytes.Buffer
					writer := &session{conn: server, writer: bufio.NewWriter(&sent)}
					if err := writer.writeLine(line); err != nil || sent.String() != want+"\r\n" {
						t.Fatalf("native writer output %q, err=%v", sent.String(), err)
					}
				default:
					t.Fatal("relay missing")
				}
				wantKey := fmt.Sprintf("dx:%s:K1ABC:W1XYZ:14074.0:%d", kind, time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC).Unix())
				if !m.dedupe.contains(wantKey, time.Now()) || m.dedupe.items.Len() != 1 {
					t.Fatal("local mutation entered peer dedupe identity")
				}
				if len(source.writeCh) != 0 {
					t.Fatal("source received its own relay")
				}
			})
		}
	}
}

func TestMalformedPeerSpotCannotPoisonAdmission(t *testing.T) {
	for _, forward := range []bool{false, true} {
		for _, kind := range []string{"PC11", "PC61", "PC26"} {
			bad := []struct {
				index int
				value string
			}{
				{0, "1.4074e4"}, {1, " K1ABC-123"}, {2, "31-Feb-2026"}, {3, "2400Z"},
				{4, ""}, {4, "A\x01B"}, {4, "A\xc3\x81B"}, {4, "A\xffB"}, {4, "A~B"}, {5, "W1XYZ-# "}, {6, ""},
			}
			if kind == "PC61" {
				bad = append(bad, struct {
					index int
					value string
				}{7, "bad-IP"})
			}
			if kind == "PC26" {
				bad = append(bad, struct {
					index int
					value string
				}{7, "  "})
			}
			for _, test := range bad {
				t.Run(fmt.Sprintf("%s/forward=%v/%d=%q", kind, forward, test.index, test.value), func(t *testing.T) {
					m, source, destination, ingest := peerSpotTestManager(forward, true)
					valid, err := ParseFrame(originalSpotTestWire(kind, "CQ"))
					if err != nil {
						t.Fatal(err)
					}
					fields := append([]string(nil), valid.Fields...)
					fields[test.index] = test.value
					invalid := &Frame{Type: kind, Fields: fields, Hop: 3}
					m.HandleFrame(invalid, source)
					entries, keyBytes, refused := m.dedupe.occupancy()
					if len(ingest) != 0 || len(destination.writeCh) != 0 || entries != 0 || keyBytes != 0 || refused != 0 {
						t.Fatal("malformed original entered a queue or peer dedupe")
					}
					m.HandleFrame(valid, source)
					if len(ingest) != 1 || len(destination.writeCh) != map[bool]int{false: 0, true: 1}[forward] {
						t.Fatal("valid same-identity control was suppressed")
					}
				})
			}
		}
	}
}

func TestPeerSpotRelayGates(t *testing.T) {
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		for _, hop := range []int{0, 1, 2} {
			for _, forward := range []bool{false, true} {
				m, source, destination, ingest := peerSpotTestManager(forward, true)
				frame, err := ParseFrame(strings.TrimSuffix(originalSpotTestWire(kind, "CQ"), "H3^") + fmt.Sprintf("H%d^", hop))
				if err != nil {
					t.Fatal(err)
				}
				m.HandleFrame(frame, source)
				want := 0
				if forward && hop > 1 {
					want = 1
				}
				if len(ingest) != 1 || len(destination.writeCh) != want || len(source.writeCh) != 0 {
					t.Fatalf("%s H%d forward=%v: local=%d relay=%d", kind, hop, forward, len(ingest), len(destination.writeCh))
				}
			}
		}
		for _, failure := range []string{"stale", "full", "duplicate", "legacy"} {
			m, source, destination, ingest := peerSpotTestManager(true, failure != "legacy")
			frame, _ := ParseFrame(originalSpotTestWire(kind, "CQ"))
			switch failure {
			case "stale":
				m.maxAgeSeconds = 1
			case "full":
				for range cap(ingest) {
					ingest <- &spot.Spot{}
				}
			case "duplicate":
				m.HandleFrame(frame, source)
				<-ingest
				<-destination.writeCh
			}
			m.HandleFrame(frame, source)
			wantRelay := 0
			if failure == "legacy" && kind != "PC26" {
				wantRelay = 1
			}
			if len(destination.writeCh) != wantRelay {
				t.Fatalf("%s %s relay gate failed", kind, failure)
			}
			if failure == "stale" && len(ingest) != 0 {
				t.Fatal("stale spot entered ingest")
			}
			if failure == "full" && len(ingest) != cap(ingest) {
				t.Fatal("full queue changed")
			}
			if (failure == "duplicate" || failure == "legacy") && len(ingest) != 1 {
				t.Fatal("valid local admission changed")
			}
			if (failure == "stale" || failure == "full") && m.dedupe.items.Len() != 0 {
				t.Fatal("failed local admission entered peer dedupe")
			}
		}
	}
}

func TestPeerSpotSecondAgeCheckUsesOriginalTimestamp(t *testing.T) {
	for _, kind := range []string{"PC11", "PC61", "PC26"} {
		m, source, destination, ingest := peerSpotTestManager(true, true)
		now := time.Now().UTC()
		wire := strings.ReplaceAll(originalSpotTestWire(kind, "CQ"), "01-oCt-2026^1200Z", now.Format("02-Jan-2006^1504Z"))
		frame, err := ParseFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		stamp := now.Truncate(time.Minute)
		m.maxAgeSeconds = int(time.Since(stamp)/time.Second) + 2
		m.dedupe.mu.Lock()
		done := make(chan struct{})
		go func() { m.HandleFrame(frame, source); close(done) }()
		select {
		case local := <-ingest:
			local.Time = now.Add(time.Hour)
		case <-time.After(5 * time.Second):
			m.dedupe.mu.Unlock()
			t.Fatal("initial age check incorrectly refused spot")
		}
		expiry := stamp.Add(time.Duration(m.maxAgeSeconds)*time.Second + 10*time.Millisecond)
		<-time.After(time.Until(expiry))
		m.dedupe.mu.Unlock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("second age check blocked")
		}
		want := 0
		if kind == "PC26" {
			want = 1
		}
		if len(destination.writeCh) != want {
			t.Fatalf("%s second age semantics changed: relays=%d", kind, len(destination.writeCh))
		}
	}
}

func TestPeerSpotVariantSizeRefusal(t *testing.T) {
	for _, kind := range []string{"PC11", "PC61"} {
		m, source, modern, ingest := peerSpotTestManager(true, true)
		legacy := &session{id: "N3OLD", ctx: context.Background(), writeCh: make(chan string, 4)}
		m.sessions.Set(legacy.id, legacy)
		wire := originalSpotTestWire(kind, "X")
		wire = strings.Replace(wire, "^X^", "^"+strings.Repeat("X", MaxPeerFrameBytes-len(wire)+1)+"^", 1)
		if len(wire) != MaxPeerFrameBytes {
			t.Fatal("fixture is not exactly at incoming size limit")
		}
		frame, err := ParseFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		m.HandleFrame(frame, source)
		if len(ingest) != 1 || len(modern.writeCh) != 0 {
			t.Fatal("oversized modern output was emitted or local admission lost")
		}
		if kind == "PC61" {
			select {
			case line := <-legacy.writeCh:
				want := "PC11" + strings.TrimSuffix(wire[4:], "^2001:0db8:0:0::0001^H3^") + "^H2^~"
				if line != want || len(line) > MaxPeerFrameBytes {
					t.Fatal("fitting legacy conversion was changed or refused")
				}
			default:
				t.Fatal("fitting legacy conversion missing")
			}
		} else if len(legacy.writeCh) != 0 {
			t.Fatal("oversized PC11 reached legacy peer")
		}
		m.HandleFrame(frame, source)
		if m.dedupe.items.Len() != 1 || len(modern.writeCh) != 0 || len(legacy.writeCh) != 0 {
			t.Fatal("size refusal reset valid dedupe admission")
		}
	}
}
