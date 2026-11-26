package telnet

import (
	"bytes"
	"strings"
	"sync"
	"testing"

	"dxcluster/spot"
)

// The live fixture's fixed 78-column record, independent of the formatter.
func cwPublicationWire(comment string) []byte {
	line := bytes.Repeat([]byte{' '}, 78)
	copy(line, "DX de W1AW:")
	copy(line[17:], "14025.00")
	copy(line[27:], "K1ABC")
	copy(line[39:], comment)
	copy(line[73:], "0304Z")
	return append(line, '\r', '\n')
}

func TestCWPublicationSharedAcrossRecipients(t *testing.T) {
	for _, route := range []string{"broadcast", "direct-self"} {
		t.Run(route, func(t *testing.T) {
			s := NewServer(ServerOptions{BroadcastWorkers: 2}, nil)
			sp := cwSpeedFixture()
			clients := []*Client{newBroadcastWorkerTestClient(s, 1), newBroadcastWorkerTestClient(s, 1)}
			for i, login := range []string{"K1ABC-1", "K1ABC-2"} {
				clients[i].setLoginCallsign(login)
				clients[i].filter.SetSelfEnabled(true)
				s.clients[login] = clients[i]
			}
			if route == "direct-self" {
				s.DeliverSelfSpotOwned("K1ABC", sp)
			} else {
				s.workerQueues = []chan broadcastJob{make(chan broadcastJob, 1), make(chan broadcastJob, 1)}
				if !s.TryBroadcastSpotOwned(sp, true, true, true) {
					t.Fatal("refused broadcast")
				}
				s.broadcastSpot(<-s.broadcast)
				for _, q := range s.workerQueues {
					select {
					case job := <-q:
						s.deliverJob(job)
					default:
						t.Fatal("missing shard")
					}
				}
			}
			envs := make([]*spotEnvelope, len(clients))
			for i, c := range clients {
				select {
				case envs[i] = <-c.spotChan:
				default:
					t.Fatal("missing admission")
				}
			}
			pub := envs[0].publication
			if pub == nil || pub.cw == nil || envs[1].publication != pub ||
				envs[0].spot != sp || envs[1].spot != sp {
				t.Fatal("recipients did not share this publication")
			}
			clients[0].setDiagMode(diagModeSource)
			if got := s.formatSpotEnvelopeForClient(clients[0], envs[0]); !strings.Contains(got, "RBN") {
				t.Fatalf("missing diagnostic: %q", got)
			}
			if pub.cw.off.line != "" || pub.cw.on.line != "" {
				t.Fatal("diagnostic or admission initialized a CW line")
			}
			clients[0].setDiagMode(diagModeOff)
			// Change the preference after both envelopes have reached their clients.
			clients[1].configuredSettings.CWSpeed = true
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range 16 {
				wg.Go(func() {
					<-start
					j := i % len(clients)
					comment := "CW POTA"
					if j == 1 {
						comment += " 22 WPM 30 WPM"
					}
					got := appendWriterNormalized(nil, s.formatSpotEnvelopeForClient(clients[j], envs[j]))
					if !bytes.Equal(got, cwPublicationWire(comment)) {
						t.Errorf("wrong recipient bytes: %q", got)
					}
				})
			}
			close(start)
			wg.Wait()
			if pub.cw.off.line == "" || pub.cw.on.line == "" {
				t.Fatal("demanded lines were not published")
			}
			if allocs := testing.AllocsPerRun(100, func() {
				_ = s.formatSpotEnvelopeForClient(clients[0], envs[0])
				_ = s.formatSpotEnvelopeForClient(clients[1], envs[1])
			}); allocs != 0 {
				t.Fatalf("warm two-recipient formatting allocs=%f", allocs)
			}
		})
	}
}

func TestCWPublicationRetainedWidth(t *testing.T) {
	width := spot.CurrentDXClusterLayout().LineLength
	t.Cleanup(func() { _ = spot.SetDXClusterLineLength(width) })
	sp := cwSpeedFixture()
	c := newTestClient()
	c.configuredSettings.CWSpeed = true
	lines := &cwBroadcastLines{}
	env := &spotEnvelope{spot: sp, publication: &spotPublication{cw: lines}}
	for _, w := range []int{maxCWBroadcastLineBytes, maxCWBroadcastLineBytes - 1, 65, 4096} {
		if err := spot.SetDXClusterLineLength(w); err != nil {
			t.Fatal(err)
		}
		if canShareCWLines(sp) != (w < maxCWBroadcastLineBytes) {
			t.Fatalf("sharing threshold at width %d", w)
		}
		line := env.cwLineForClient(c)
		if line != sp.FormatDXClusterCW(true)+"\n" || len(line) != w+1 {
			t.Fatalf("changed wire at width %d", w)
		}
		if len(lines.on.line) > maxCWBroadcastLineBytes || (w == maxCWBroadcastLineBytes && lines.on.line != "") {
			t.Fatalf("retained %d bytes at width %d", len(lines.on.line), w)
		}
	}
}

func TestCWGlyphDoesNotCountTerminatorAsLayoutColumn(t *testing.T) {
	width := spot.CurrentDXClusterLayout().LineLength
	t.Cleanup(func() { _ = spot.SetDXClusterLineLength(width) })
	if err := spot.SetDXClusterLineLength(76); err != nil {
		t.Fatal(err)
	}
	sp := cwSpeedFixture()
	sp.CWDisplayAvailable = false
	line := (&spotEnvelope{spot: sp}).cwLineForClient(newTestClient())
	if err := spot.SetDXClusterLineLength(77); err != nil {
		t.Fatal(err)
	}
	if len(line) != 77 || injectCWLineGlyph(line, "S") != line {
		t.Fatal("glyph overwrote a short cached body")
	}
}
