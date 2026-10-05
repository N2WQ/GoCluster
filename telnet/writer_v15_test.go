package telnet

import (
	"strings"
	"testing"
	"time"
)

func TestWriterV15FormatterBranches(t *testing.T) {
	for _, mode := range []struct {
		name      string
		diag      diagMode
		nilServer bool
	}{
		{name: "normal"}, {name: "diag-source", diag: diagModeSource},
		{name: "nil-server", nilServer: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			server := &Server{writerBatchMaxBytes: 4096, writerBatchWait: time.Millisecond}
			if mode.nilServer {
				server = nil
			}
			sp, expectedSpot := writerV15Spot(), writerV15Spot()
			line := expectedSpot.FormatDXCluster()
			if mode.diag == diagModeSource {
				line = expectedSpot.FormatDXClusterWithComment("MAN")
			}
			conn := &writerV15Conn{expected: []byte(writerV15Oracle(line + "\n")), target: 1, reached: make(chan struct{})}
			client := writerV15Client(server, conn)
			client.setDiagMode(mode.diag)
			client.spotChan <- &spotEnvelope{spot: sp}
			done := writerV15Start(client)
			defer func() { client.close(""); writerV15Wait(t, done, "cleanup") }()
			writerV15Wait(t, conn.reached, "spot bytes")
			client.close("")
			writerV15Wait(t, done, "shutdown")
			writerV15Check(t, conn)
		})
	}
}

func TestWriterV15BaseCachePriming(t *testing.T) {
	// The alternate diagnostic formatter does not itself prime the base cache.
	// Keep the tested spot untouched until writerLoop has joined; the independent
	// expected spot avoids accidentally priming the cache through the oracle.
	sp, expectedSpot := writerV15Spot(), writerV15Spot()
	want := writerV15Oracle(expectedSpot.FormatDXClusterWithComment("MAN") + "\n")
	conn := &writerV15Conn{expected: []byte(want), target: 1, reached: make(chan struct{})}
	client := writerV15Client(&Server{writerBatchWait: time.Millisecond}, conn)
	client.setDiagMode(diagModeSource)
	client.spotChan <- &spotEnvelope{spot: sp}
	done := writerV15Start(client)
	defer func() { client.close(""); writerV15Wait(t, done, "cleanup") }()
	writerV15Wait(t, conn.reached, "diagnostic spot")
	client.close("")
	writerV15Wait(t, done, "shutdown")
	writerV15Check(t, conn)
	sp.Time = sp.Time.Add(11*time.Hour + 11*time.Minute)
	base := sp.FormatDXCluster()
	if !strings.Contains(base, "1234Z") || strings.Contains(base, "2345Z") {
		t.Fatalf("diagnostic writer did not preserve base cache priming: %q", base)
	}
}

func TestWriterV15RecordBoundaryOvershoot(t *testing.T) {
	for _, boundary := range []struct {
		name  string
		extra int
	}{
		{name: "one-byte-overshoot", extra: -1},
		{name: "exact-threshold"},
		{name: "second-whole-record", extra: 1},
	} {
		t.Run(boundary.name, func(t *testing.T) {
			extra := boundary.extra
			sp := writerV15Spot()
			line := writerV15Oracle(writerV15Spot().FormatDXCluster() + "\n")
			control := "control\r\n"
			first := len(control) + len(line)
			conn := &writerV15Conn{expected: []byte(control + line + line), target: 1,
				reached: make(chan struct{}), lengths: make([]int, 0, 3)}
			client := writerV15Client(&Server{writerBatchMaxBytes: first + extra, writerBatchWait: time.Millisecond}, conn)
			client.controlChan <- controlMessage{line: "control\n"}
			client.spotChan <- &spotEnvelope{spot: sp}
			client.spotChan <- &spotEnvelope{spot: sp}
			done := writerV15Start(client)
			defer func() { client.close(""); writerV15Wait(t, done, "cleanup") }()
			writerV15Wait(t, conn.reached, "all records")
			client.close("")
			writerV15Wait(t, done, "shutdown")
			writerV15Check(t, conn)
			if extra <= 0 {
				if len(conn.lengths) != 2 || conn.lengths[0] != first || conn.lengths[1] != len(line) {
					t.Fatalf("whole-record boundary changed: writes=%v, first=%d", conn.lengths, first)
				}
			} else if len(conn.lengths) != 1 || conn.lengths[0] != len(conn.expected) {
				t.Fatalf("whole-record overshoot changed: writes=%v", conn.lengths)
			}
		})
	}
}

func TestWriterV15PausedSpotDoesNotPrimeCache(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	conn := &writerV15Conn{expected: []byte("control\r\n"), target: 1, reached: make(chan struct{})}
	client := writerV15Client(&Server{writerBatchWait: time.Millisecond, nowFn: func() time.Time { return now }}, conn)
	sp := writerV15Spot()
	client.startReadPause(now, 30*time.Second)
	client.spotChan <- &spotEnvelope{spot: sp, enqueueAt: now.Add(-time.Second)}
	client.controlChan <- controlMessage{line: "control\n"}
	done := writerV15Start(client)
	defer func() { client.close(""); writerV15Wait(t, done, "cleanup") }()
	writerV15Wait(t, conn.reached, "control bytes")
	client.close("")
	writerV15Wait(t, done, "shutdown")
	writerV15Check(t, conn)
	_, _, suppressed := client.readPauseStatus(now)
	if suppressed != 1 {
		t.Fatalf("suppressed=%d, want 1", suppressed)
	}
	sp.Time = sp.Time.Add(11*time.Hour + 11*time.Minute)
	if got := sp.FormatDXCluster(); !strings.Contains(got, "2345Z") || strings.Contains(got, "1234Z") {
		t.Fatalf("paused spot unexpectedly primed base cache: %q", got)
	}
}

func TestWriterV15ControlBytesAndClose(t *testing.T) {
	// Empty-but-non-nil raw intentionally differs from nil raw in the old
	// whitespace guard. Raw bytes themselves never enter newline normalization.
	want := "raw\n\r\x00\r\n \t\r\ntext\r\n\r\r\n"
	conn := &writerV15Conn{expected: []byte(want), target: 1, reached: make(chan struct{})}
	client := writerV15Client(&Server{writerBatchWait: time.Millisecond}, conn)
	client.controlChan <- controlMessage{raw: []byte("raw\n\r\x00\r\n")}
	client.controlChan <- controlMessage{line: " \t\n"}
	client.controlChan <- controlMessage{line: " \t\n", raw: []byte{}}
	client.controlChan <- controlMessage{line: "text\r\n\r\r\n"}
	client.controlChan <- controlMessage{closeAfter: true}
	client.spotChan <- &spotEnvelope{spot: writerV15Spot()}
	done := writerV15Start(client)
	defer func() { client.close(""); writerV15Wait(t, done, "cleanup") }()
	writerV15Wait(t, done, "control close")
	writerV15Check(t, conn)
	if len(client.spotChan) != 1 {
		t.Fatal("close-after-control consumed the queued spot")
	}
}
