//go:build qualification

package cluster

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"dxcluster/telnet"
)

func TestQualificationFramerEveryMarkerSplit(t *testing.T) {
	const prefix = "DL1CAA de NODE> "
	start := time.Now()
	for _, marker := range []string{"DX de ", "To ", "WWV de ", "WCY de "} {
		wire := []byte(prefix + marker + "DL1AAA QID0000000\r\n")
		for split := 1; split < len(wire); split++ {
			r := qualificationLineReader{}
			count := 0
			accept := func(line []byte, at time.Time) {
				count++
				want := start
				if split <= len(prefix) {
					want = start.Add(time.Millisecond)
				}
				if string(line) != string(wire[:len(wire)-2]) || !at.Equal(want) {
					t.Fatalf("marker=%q split=%d line=%q at=%s want=%s", marker, split, line, at, want)
				}
			}
			if err := r.consume(wire[:split], start, accept); err != nil {
				t.Fatal(err)
			}
			if err := r.consume(wire[split:], start.Add(time.Millisecond), accept); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("split=%d delivered %d records", split, count)
			}
		}
		// A marker can span more than two reads. Keep the first byte's time
		// even when all subsequent marker bytes arrive in different reads.
		r := qualificationLineReader{}
		for i := range wire {
			if err := r.consume(wire[i:i+1], start.Add(time.Duration(i)*time.Millisecond), func(_ []byte, at time.Time) {
				if !at.Equal(start.Add(time.Duration(len(prefix)) * time.Millisecond)) {
					t.Fatalf("byte-wise marker timestamp=%s", at)
				}
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestQualificationFramerBorrowedSpansAndBound(t *testing.T) {
	start := time.Now()
	r := qualificationLineReader{peer: true}
	line := append(bytes.Repeat([]byte{'x'}, 65538), '\n')
	count := 0
	accept := func(record []byte, at time.Time) {
		count++
		if len(record) != 65538 || !at.Equal(start) {
			t.Fatal("large split record lost bytes or original time")
		}
	}
	for i := 0; i < len(line); i += 8192 {
		if err := r.consume(line[i:min(i+8192, len(line))], start.Add(time.Duration(i)), accept); err != nil {
			t.Fatal(err)
		}
	}
	if count != 1 {
		t.Fatalf("large frame count=%d", count)
	}
	if err := r.consume(line[:65538], start, accept); err != nil {
		t.Fatal(err)
	}
	if err := r.consume([]byte{'x', '\n'}, start, accept); err == nil {
		t.Fatal("oversized fragmented record accepted")
	}
	r = qualificationLineReader{peer: true}
	wire := []byte("first\nsecond\r\n")
	if err := r.consume(wire, start, func(record []byte, _ time.Time) {
		if &record[0] != &wire[0] && &record[0] != &wire[6] {
			t.Fatal("complete line unnecessarily copied")
		}
	}); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkQualificationFramer(b *testing.B) {
	for _, peer := range []bool{false, true} {
		name, wire := "telnet-20-records", []byte(strings.Repeat("DX de DL1AAA: 14020.0 DL1AAAAAAA QID0000000 CW\r\n", 20))
		if peer {
			name, wire = "peer-64KiB", []byte("PC92^"+strings.Repeat("x", 63990)+"^H1^\r\n")
		}
		b.Run(name, func(b *testing.B) {
			r := qualificationLineReader{peer: peer, line: make([]byte, 0, 65538)}
			at := time.Now()
			count := 0
			accept := func(_ []byte, _ time.Time) { count++ }
			b.ReportAllocs()
			b.SetBytes(int64(len(wire)))
			b.ResetTimer()
			for b.Loop() {
				for i := 0; i < len(wire); i += 8192 {
					if err := r.consume(wire[i:min(i+8192, len(wire))], at, accept); err != nil {
						b.Fatal(err)
					}
				}
			}
			if count == 0 {
				b.Fatal("framer bypassed callback")
			}
		})
	}
}

func BenchmarkQualificationFullSpotObservation(b *testing.B) {
	for _, enqueue := range []bool{false, true} {
		name := "receive"
		if enqueue {
			name = "enqueue"
		}
		b.Run(name, func(b *testing.B) {
			o := newQualificationOracle(1, 1, 0, 1)
			o.epoch = time.Now()
			o.sessionIDs[0] = 1
			_, token, _ := o.add(true, -1, 0, 0)
			at := o.epoch.Add(time.Millisecond)
			s := qualificationSocket{driver: &qualificationDriver{oracle: o}, row: o.clients[0]}
			line := []byte("DX de DL1AAA: 14020.0 " + qualificationDXCall(0)[:10] + " " + token + " CW")
			event := telnet.QualificationEnqueue{Login: "DL1CAA", SessionID: 1, Comment: token + " CW", DXCall: qualificationDXCall(0), ObservedAt: at}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if enqueue {
					o.clients[0].enqueue[0].Store(0)
					o.enqueued(event)
				} else {
					o.clients[0].read[0].Store(0)
					s.observe(line, at)
				}
			}
			if o.failures.Load() != 0 {
				b.Fatal("observation checker failed")
			}
		})
	}
}
