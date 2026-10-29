package telnet

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/bulletinhistory"
)

func bulletinReadThrough(t *testing.T, conn net.Conn, reader *bufio.Reader, marker string) string {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for b.Len() < 100000 {
		line, err := reader.ReadString('\n')
		b.WriteString(line)
		if strings.Contains(b.String(), marker) {
			return b.String()
		}
		if err != nil {
			t.Fatalf("waiting for %q: %v; got %q", marker, err, b.String())
		}
	}
	t.Fatal("bulletin transcript exceeds bound")
	return ""
}

func TestBulletinLoginAndShowBypassLiveFiltersTCP(t *testing.T) {
	store := bulletinhistory.Open(filepath.Join(t.TempDir(), "history.json"), 10*time.Minute, 64)
	t.Cleanup(func() { _ = store.Close() })
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.BulletinHistory = store
		opts.LoginGreeting = "READY\n"
	})
	// No client is registered when these two source callbacks arrive.
	s.BroadcastWWVAt("WWV", "WWV de K1AAA: first-wwv", time.Now().Add(-time.Minute))
	s.BroadcastWWVAt("WCY", "WCY de K1AAA: first-wcy", time.Now().Add(-time.Minute))
	conn, reader, done, login := connectUpgradeSession(t, s, "K1BUL")
	if !strings.Contains(login, "first-wwv") || !strings.Contains(login, "first-wcy") || !strings.Contains(login, "WCY history may be incomplete.") {
		t.Fatalf("login missing bulletin or warning: %q", login)
	}
	for _, kind := range []string{"WWV", "WCY"} {
		if _, err := io.WriteString(conn, "REJECT "+kind+"\r\n"); err != nil {
			t.Fatal(err)
		}
		bulletinReadThrough(t, conn, reader, kind+" bulletins disabled")
	}
	closeHandshakeTranscriptSession(t, conn, done)

	conn2, reader2, _, login := connectUpgradeSession(t, s, "K1BUL")
	if !strings.Contains(login, "first-wwv") || !strings.Contains(login, "first-wcy") || !strings.Contains(login, "WCY history may be incomplete.") {
		t.Fatalf("filtered login missing bulletin or warning: %q", login)
	}
	// Both are archived even though the user receives no live bulletin.
	s.BroadcastWWVAt("WWV", "WWV de K1AAA: later-wwv", time.Now())
	s.BroadcastWWVAt("WCY", "WCY de K1AAA: later-wcy", time.Now())
	if err := conn2.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, err := reader2.Peek(1); err == nil {
		t.Fatal("disabled filters allowed an unsolicited live bulletin")
	} else {
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("unexpected read error while checking live filters: %v", err)
		}
	}
	for _, tc := range []struct{ command, want, absent string }{
		{"SHOW WWV", "later-wwv", "later-wcy"},
		{"SH WCY", "later-wcy", "later-wwv"},
		{"SHOW WWV FULL", "later-wwv", "later-wcy"},
		{"SH WCY FULL", "later-wcy", "later-wwv"},
	} {
		if _, err := io.WriteString(conn2, tc.command+"\r\n"); err != nil {
			t.Fatal(err)
		}
		got := bulletinReadThrough(t, conn2, reader2, tc.want)
		if strings.Contains(got, tc.absent) {
			t.Fatalf("%s leaked another type: %q", tc.command, got)
		}
	}
	if _, err := io.WriteString(conn2, "DIALECT CC\r\n"); err != nil {
		t.Fatal(err)
	}
	bulletinReadThrough(t, conn2, reader2, "Dialect set to CC")
	if _, err := io.WriteString(conn2, "SHOW WCY FULL\r\n"); err != nil {
		t.Fatal(err)
	}
	if got := bulletinReadThrough(t, conn2, reader2, "later-wcy"); !strings.Contains(got, "first-wcy") {
		t.Fatalf("CC full history did not include both records: %q", got)
	}
}

func TestBulletinResponseBudgetReleasedOnDiscard(t *testing.T) {
	store := bulletinhistory.Open(filepath.Join(t.TempDir(), "history.json"), 0, 0)
	t.Cleanup(func() { _ = store.Close() })
	store.Record("WWV", "WWV de K1AAA: data", time.Now())
	s := &Server{bulletinHistory: store}
	c := &Client{server: s, done: make(chan struct{}), controlChan: make(chan controlMessage, 128)}
	for i := 0; i < bulletinResponseBudget/bulletinResponseReservation; i++ {
		if !s.sendBulletinHistoryResponse(c, "WWV", i%2 == 0, "") {
			t.Fatalf("reservation %d failed", i)
		}
	}
	if got := s.bulletinResponseBytes.Load(); got != bulletinResponseBudget {
		t.Fatalf("reserved %d bytes", got)
	}
	if !s.sendBulletinHistoryResponse(c, "WWV", true, "") {
		t.Fatal("busy response failed")
	}
	last := len(c.controlChan) - 1
	for i := 0; i < last; i++ {
		msg := <-c.controlChan
		c.releaseBulletinReservations(msg.bulletinReservation)
	}
	if msg := <-c.controlChan; !strings.Contains(msg.line, "busy") {
		t.Fatalf("missing busy response: %q", msg.line)
	}
	if got := s.bulletinResponseBytes.Load(); got != 0 {
		t.Fatalf("reservation leak: %d", got)
	}
}

func TestBulletinResponseBudgetReleasedOnQueueFull(t *testing.T) {
	store := bulletinhistory.Open(filepath.Join(t.TempDir(), "history.json"), 0, 0)
	t.Cleanup(func() { _ = store.Close() })
	store.Record("WWV", "WWV de K1AAA: data", time.Now())
	s := &Server{bulletinHistory: store}
	c := &Client{server: s, done: make(chan struct{}), controlChan: make(chan controlMessage, 1)}
	c.controlChan <- controlMessage{line: "occupied\n"}
	if s.sendBulletinHistoryResponse(c, "WWV", false, "") {
		t.Fatal("full control queue accepted a history reply")
	}
	if got := s.bulletinResponseBytes.Load(); got != 0 {
		t.Fatalf("queue refusal retained %d response bytes", got)
	}
	select {
	case <-c.done:
	default:
		t.Fatal("full control queue did not close the client")
	}
}

func TestBulletinResponseBudgetReleasedAfterWriterDisconnect(t *testing.T) {
	store := bulletinhistory.Open(filepath.Join(t.TempDir(), "history.json"), 0, 0)
	t.Cleanup(func() { _ = store.Close() })
	store.Record("WWV", "WWV de K1AAA: data", time.Now())
	serverConn, clientConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })
	s := &Server{bulletinHistory: store}
	c := &Client{server: s, conn: serverConn, writer: bufio.NewWriter(serverConn), done: make(chan struct{}), controlChan: make(chan controlMessage, 128)}
	for i := 0; i < bulletinResponseBudget/bulletinResponseReservation; i++ {
		if !s.sendBulletinHistoryResponse(c, "WWV", true, "") {
			t.Fatal("failed to queue bounded response")
		}
	}
	finished := make(chan struct{})
	go func() { c.writerLoop(); close(finished) }()
	// The peer never reads; closing it forces any active write to fail and the
	// writer must release both its batch and all still-queued reservations.
	_ = clientConn.Close()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not exit after disconnect")
	}
	if got := s.bulletinResponseBytes.Load(); got != 0 {
		t.Fatalf("writer leaked %d response bytes", got)
	}
}

func TestBulletinV6BothDialectsSelectFiveAndLoginOne(t *testing.T) {
	store := bulletinhistory.Open(filepath.Join(t.TempDir(), "history.json"), 0, 0)
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	for i := 0; i < 6; i++ {
		for _, kind := range []string{"WWV", "WCY"} {
			store.Record(kind, fmt.Sprintf("%s-item-%02d", kind, i), now.Add(time.Duration(i-6)*time.Minute))
		}
	}
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.BulletinHistory = store
		opts.LoginGreeting = "READY\n"
	})
	conn, reader, _, login := connectUpgradeSession(t, s, "K1BUL")
	if strings.Count(login, "WWV received ") != 1 || strings.Count(login, "WCY received ") != 1 ||
		!strings.Contains(login, "WWV-item-05") || !strings.Contains(login, "WCY-item-05") || strings.Contains(login, "item-04") {
		t.Fatalf("login must show exactly the latest per type: %q", login)
	}
	for _, kind := range []string{"WWV", "WCY"} {
		if _, err := io.WriteString(conn, "REJECT "+kind+"\r\n"); err != nil {
			t.Fatal(err)
		}
		bulletinReadThrough(t, conn, reader, kind+" bulletins disabled")
	}
	for _, dialect := range []string{"GO", "CC"} {
		if dialect == "CC" {
			if _, err := io.WriteString(conn, "DIALECT CC\r\n"); err != nil {
				t.Fatal(err)
			}
			bulletinReadThrough(t, conn, reader, "Dialect set to CC")
		}
		for _, kind := range []string{"WWV", "WCY"} {
			other := "WCY"
			if kind == "WCY" {
				other = "WWV"
			}
			for _, head := range []string{"SHOW " + kind, "SH " + kind, "SHOW/" + kind, "SH/" + kind} {
				for _, full := range []bool{false, true} {
					command := head
					if full {
						command += " FULL"
					}
					if _, err := io.WriteString(conn, command+"\r\n"); err != nil {
						t.Fatal(err)
					}
					got := bulletinReadThrough(t, conn, reader, kind+" history may be incomplete.")
					first, count := 1, 5
					if full {
						first, count = 0, 6
					}
					if strings.Count(got, kind+" received ") != count || strings.Contains(got, other+"-item-") || (!full && strings.Contains(got, kind+"-item-00")) {
						t.Fatalf("%s %s returned wrong selection: %q", dialect, command, got)
					}
					last := -1
					for i := first; i < 6; i++ {
						pos := strings.Index(got, fmt.Sprintf("%s-item-%02d", kind, i))
						if pos <= last {
							t.Fatalf("%s %s missing or out-of-order item %d: %q", dialect, command, i, got)
						}
						last = pos
					}
				}
			}
		}
	}
}

func TestBulletinFull48MaxUnicodeTCP(t *testing.T) {
	store := bulletinhistory.Open(filepath.Join(t.TempDir(), "history.json"), 0, 0)
	t.Cleanup(func() { _ = store.Close() })
	now := time.Now().UTC()
	for i := 0; i < 49; i++ {
		// Six ASCII characters plus 250 four-byte runes are retained; the
		// final character must be marked as truncated in each record.
		store.Record("WWV", fmt.Sprintf("B%05d", i)+strings.Repeat("🛰", 250)+"X", now.Add(time.Duration(i-49)*time.Second))
	}
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.BulletinHistory = store
		opts.LoginGreeting = "READY\n"
		opts.AutoReadPauseMinRows = 10
		opts.AutoReadPauseDuration = 30 * time.Second
	})
	conn, reader, _, login := connectUpgradeSession(t, s, "K1BUL")
	if strings.Count(login, "WWV received ") != 1 || !strings.Contains(login, "B00048") {
		t.Fatalf("login did not show only newest WWV: %q", login)
	}
	if _, err := io.WriteString(conn, "SHOW/WWV FULL\r\n"); err != nil {
		t.Fatal(err)
	}
	got := bulletinReadThrough(t, conn, reader, "Missed spots are not replayed.")
	if len(got) <= 30<<10 || len(got) > 60<<10 || strings.Count(got, "WWV received ") != 48 ||
		strings.Count(got, "[truncated]") != 48 || strings.Contains(got, "B00000") ||
		!strings.Contains(got, "WWV history is incomplete") || !strings.Contains(got, "Live spots paused") ||
		strings.Contains(got, "response too large") {
		t.Fatalf("invalid maximum history response: bytes=%d rows=%d truncated=%d", len(got), strings.Count(got, "WWV received "), strings.Count(got, "[truncated]"))
	}
	last := -1
	for i := 1; i <= 48; i++ {
		pos := strings.Index(got, fmt.Sprintf("B%05d", i))
		if pos <= last {
			t.Fatalf("missing or out-of-order record %d", i)
		}
		last = pos
	}
	if strings.Count(got, "\r\n") < 50 {
		t.Fatalf("history response was not normalized to CRLF")
	}
	deadline := time.Now().Add(time.Second)
	for s.bulletinResponseBytes.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := s.bulletinResponseBytes.Load(); got != 0 {
		t.Fatalf("successful writer retained %d response bytes", got)
	}
}
