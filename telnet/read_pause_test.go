package telnet

import (
	"bufio"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestRenderedOutputRows(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    int
	}{
		{name: "empty", message: "", want: 0},
		{name: "single trailing newline", message: "one\n", want: 1},
		{name: "crlf trailing newline", message: "one\r\ntwo\r\n", want: 2},
		{name: "blank separator counts", message: "one\n\ntwo\n", want: 3},
		{name: "one final newline only", message: "one\n\n", want: 2},
		{name: "bare carriage return", message: "one\rtwo", want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := renderedOutputRows(tt.message); got != tt.want {
				t.Fatalf("renderedOutputRows(%q) = %d, want %d", tt.message, got, tt.want)
			}
		})
	}
}

func TestSendCommandResponseStartsAutoReadPause(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	server := &Server{
		autoReadPauseMinRows:  3,
		autoReadPauseDuration: 30 * time.Second,
		nowFn:                 func() time.Time { return now },
	}
	client := &Client{
		server:      server,
		callsign:    "N0CALL",
		controlChan: make(chan controlMessage, 1),
		spotChan:    make(chan *spotEnvelope, 1),
		done:        make(chan struct{}),
	}

	if !server.sendCommandResponse(client, "one\ntwo\nthree\n", "test command") {
		t.Fatal("sendCommandResponse returned false")
	}

	msg := <-client.controlChan
	if !strings.Contains(msg.line, "Live spots paused for 30s after 3 output rows.") {
		t.Fatalf("expected read-pause footer in %q", msg.line)
	}
	active, remaining, suppressed := client.readPauseStatus(now)
	if !active || remaining != 30*time.Second || suppressed != 0 {
		t.Fatalf("pause status = active:%t remaining:%s suppressed:%d, want active 30s 0", active, remaining, suppressed)
	}
}

func TestReadPauseCommandsReportAndResume(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	server := &Server{nowFn: func() time.Time { return now }}
	client := &Client{server: server}
	client.startAutoReadPause(now, 30*time.Second)
	client.readPauseSuppressed.Store(2)

	now = now.Add(12 * time.Second)
	resp, handled := server.handleReadPauseCommand(client, "SHOW HOLD")
	if !handled {
		t.Fatal("SHOW HOLD was not handled")
	}
	if !strings.Contains(resp, "Live spots auto-paused for 18s more. Suppressed spots: 2.") {
		t.Fatalf("unexpected SHOW HOLD response: %q", resp)
	}

	resp, handled = server.handleReadPauseCommand(client, "RESUME")
	if !handled {
		t.Fatal("RESUME was not handled")
	}
	if resp != "Live spots resumed. Suppressed spots: 2.\n" {
		t.Fatalf("unexpected RESUME response: %q", resp)
	}
	active, _, suppressed := client.readPauseStatus(now)
	if active || suppressed != 0 {
		t.Fatalf("pause after RESUME = active:%t suppressed:%d, want inactive 0", active, suppressed)
	}
}

func TestResumeAllowsFutureSpotsBeforeOriginalPauseDeadline(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	server := &Server{nowFn: func() time.Time { return now }}
	client := &Client{
		server:   server,
		callsign: "N0CALL",
		spotChan: make(chan *spotEnvelope, 1),
		done:     make(chan struct{}),
	}
	dxSpot := spot.NewSpot("K1ABC", "N0CALL", 14074.0, "FT8")
	client.startAutoReadPause(now, 30*time.Second)

	now = now.Add(5 * time.Second)
	active, _ := client.resumeReadPause(now)
	if !active {
		t.Fatal("expected RESUME to clear an active pause")
	}

	client.enqueueSpot(&spotEnvelope{spot: dxSpot, enqueueAt: now.Add(time.Second)})
	select {
	case <-client.spotChan:
	default:
		t.Fatal("expected future spot after RESUME to enqueue before original pause deadline")
	}
}

func TestReadPauseSuppressesSpotsWithoutSlowClientDrops(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	server := &Server{
		dropExtremeRate:   0.8,
		dropExtremeWindow: 30 * time.Second,
		dropExtremeMinAtt: 1,
		nowFn:             func() time.Time { return now },
	}
	client := &Client{
		server:     server,
		callsign:   "N0CALL",
		spotChan:   make(chan *spotEnvelope, 1),
		done:       make(chan struct{}),
		dropWindow: newDropWindow(server.dropExtremeWindow),
	}
	dxSpot := spot.NewSpot("K1ABC", "N0CALL", 14074.0, "FT8")
	client.startAutoReadPause(now, 30*time.Second)

	for i := 0; i < 5; i++ {
		client.enqueueSpot(&spotEnvelope{spot: dxSpot, enqueueAt: now.Add(time.Duration(i) * time.Second)})
	}

	if got := len(client.spotChan); got != 0 {
		t.Fatalf("spot queue length = %d, want 0", got)
	}
	if drops := atomic.LoadUint64(&client.dropCount); drops != 0 {
		t.Fatalf("slow-client drops = %d, want 0", drops)
	}
	select {
	case <-client.done:
		t.Fatal("read-pause suppression should not disconnect client")
	default:
	}
	_, _, suppressed := client.readPauseStatus(now)
	if suppressed != 5 {
		t.Fatalf("suppressed spots = %d, want 5", suppressed)
	}
}

func TestWriterLoopDropsQueuedReadPauseSpotsButWritesControl(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	server := &Server{
		writerBatchMaxBytes: 4096,
		writerBatchWait:     time.Millisecond,
		latency:             newLatencyMetrics(),
		nowFn:               func() time.Time { return now },
	}
	conn := &recordingConn{}
	client := &Client{
		conn:        conn,
		writer:      bufio.NewWriter(conn),
		server:      server,
		callsign:    "N0CALL",
		spotChan:    make(chan *spotEnvelope, 1),
		controlChan: make(chan controlMessage, 1),
		done:        make(chan struct{}),
	}
	dxSpot := spot.NewSpot("K1ABC", "N0CALL", 14074.0, "FT8")
	client.startAutoReadPause(now, 30*time.Second)
	client.spotChan <- &spotEnvelope{spot: dxSpot, enqueueAt: now.Add(-time.Second)}
	client.controlChan <- controlMessage{line: "control\n"}

	loopDone := make(chan struct{})
	go func() {
		client.writerLoop()
		close(loopDone)
	}()

	deadline := time.After(2 * time.Second)
	for conn.WriteCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for writer loop flush")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	client.close("test shutdown")
	select {
	case <-loopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("writerLoop did not stop after client close")
	}

	output := string(conn.Bytes())
	if !strings.Contains(output, normalizeOutboundLine("control\n")) {
		t.Fatalf("expected control output, got %q", output)
	}
	if strings.Contains(output, dxSpot.FormatDXCluster()) {
		t.Fatalf("expected queued spot to be dropped during read pause, got %q", output)
	}
	_, _, suppressed := client.readPauseStatus(now)
	if suppressed != 1 {
		t.Fatalf("suppressed spots = %d, want 1", suppressed)
	}
}
