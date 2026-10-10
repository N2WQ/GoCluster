package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/spot"
)

func showHoldPauseState(c *Client) readbackPauseTestState {
	c.readPauseMu.Lock()
	defer c.readPauseMu.Unlock()
	return readbackPauseTestState{
		until: c.readPauseUntilUnixNano.Load(), cutoff: c.readPauseDiscardBefore.Load(),
		count: c.readPauseSuppressed.Load(), epoch: c.readPauseEpoch,
		pending: c.readPausePending.Load(), closed: c.readPauseClosed,
	}
}

func requireShowHoldReply(t *testing.T, s *Server, c *Client, input, expected string, handled bool) {
	t.Helper()
	before := showHoldPauseState(c)
	response, gotHandled := s.handleReadPauseCommand(c, input)
	if gotHandled != handled || response != expected {
		t.Fatalf("HOLD owner %q -> %q handled=%t; want %q handled=%t", input, response, gotHandled, expected, handled)
	}
	assertReadbackPauseState(t, c, before)
}

func TestShowHoldLiteralStatusesAreReadOnly(t *testing.T) {
	now := time.Unix(1700000000, 0)
	s := &Server{nowFn: func() time.Time { return now }}
	for _, tc := range []struct {
		name, expected string
		state          readbackPauseTestState
	}{
		{"inactive", "Live spots are not paused.\n", readbackPauseTestState{}},
		{"retained count", "Live spots are not paused. Suppressed spots in last pause: 7.\n", readbackPauseTestState{count: 7, epoch: 3}},
		{"fraction rounds up", "Live spots paused for 2s more. Suppressed spots: 7.\n", readbackPauseTestState{until: 1700000001200000000, cutoff: 1700000001200000000, count: 7, epoch: 3}},
		{"one nanosecond before expiry", "Live spots paused for 1s more. Suppressed spots: 7.\n", readbackPauseTestState{until: 1700000000000000001, cutoff: 1700000000000000001, count: 7, epoch: 3}},
		{"exact expiry", "Live spots are not paused. Suppressed spots in last pause: 7.\n", readbackPauseTestState{until: 1700000000000000000, cutoff: 1700000000000000000, count: 7, epoch: 3}},
		{"pending delivery", "Live spots paused while a response is being delivered. Suppressed spots: 7.\n", readbackPauseTestState{count: 7, epoch: 3, pending: true}},
		{"pending with finite deadline", "Live spots paused for 30s more. Suppressed spots: 7.\n", readbackPauseTestState{until: 1700000030000000000, cutoff: 1700000030000000000, count: 7, epoch: 3, pending: true}},
		{"closed retained diagnostic state", "Live spots paused for 10s more. Suppressed spots: 7.\n", readbackPauseTestState{until: 1700000010000000000, cutoff: 1700000010000000000, count: 7, epoch: 4, closed: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Client{}
			c.readPauseUntilUnixNano.Store(tc.state.until)
			c.readPauseDiscardBefore.Store(tc.state.cutoff)
			c.readPauseSuppressed.Store(tc.state.count)
			c.readPauseEpoch, c.readPauseClosed = tc.state.epoch, tc.state.closed
			c.readPausePending.Store(tc.state.pending)
			requireShowHoldReply(t, s, c, "sHoW   hOlD", tc.expected, true)
		})
	}
}

func TestShowHoldWholeTokensAndAPISpaceArity(t *testing.T) {
	now := time.Unix(1700000000, 0)
	s := &Server{nowFn: func() time.Time { return now }}
	c := &Client{}
	c.startReadPause(now, time.Minute)
	c.readPauseSuppressed.Store(9)
	for _, line := range []string{"SHOW HOLD", "show hold", "  ShOw  HoLd  ", "SHOW       HOLD", "\tshow\thold\t", "show \t hold"} {
		// TAB is an owner/API token delimiter, not an admitted human wire byte.
		requireShowHoldReply(t, s, c, line, "Live spots paused for 60s more. Suppressed spots: 9.\n", true)
	}
	for _, line := range []string{"SHOW HOLD EXTRA", "show  hold  off 60", "SHOW HOLD ALL", "\tshow\thold\textra"} {
		requireShowHoldReply(t, s, c, line, "Usage: SHOW HOLD\n", true)
	}
	for _, line := range []string{"SHOW", "SH HOLD", "SHOW/HOLD", "SH/HOLD", "SHOW HOLDS", "SHOWHOLD", "SHOW HOLDX EXTRA", "HELP SHOW HOLD", "RESUME EXTRA"} {
		requireShowHoldReply(t, s, c, line, "", false)
	}
}

func TestShowHoldRealReadersPreserveWireAlphabet(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		for _, tc := range []struct{ input, decoded string }{
			{"  sHoW   hOlD  \r\n", "  SHOW   HOLD  "},
			{"show  holx\bd\n", "SHOW  HOLD"},
			{"show  bad\x17hold\n", "SHOW  HOLD"},
			{"junk\x15show  hold\n", "SHOW  HOLD"},
		} {
			t.Run(fmt.Sprintf("ziutek=%t/%q", ziutek, tc.input), func(t *testing.T) {
				c, _ := taxonomyInputReader(t, tc.input, ziutek)
				line, err := c.readCommandLine(128)
				if err != nil || line != tc.decoded {
					t.Fatalf("real reader changed literal HOLD input: %q / %v", line, err)
				}
				requireShowHoldReply(t, c.server, c, line, "Live spots are not paused.\n", true)
			})
		}
		t.Run(fmt.Sprintf("ziutek=%t/TAB refusal and continuation", ziutek), func(t *testing.T) {
			c, conn := taxonomyInputReader(t, "show\thold EXTRA\nshow  hold\n", ziutek)
			before := showHoldPauseState(c)
			line, err := c.readCommandLine(128)
			var invalid *InputValidationError
			var terminal *humanInputError
			if line != "" || !errors.As(err, &invalid) || errors.As(err, &terminal) || conn.closed.Load() {
				t.Fatalf("TAB was admitted or failed to recover its physical line: %q / %v", line, err)
			}
			assertReadbackPauseState(t, c, before)
			line, err = c.readCommandLine(128)
			if err != nil || line != "SHOW  HOLD" {
				t.Fatalf("TAB refusal damaged the next physical command: %q / %v", line, err)
			}
			requireShowHoldReply(t, c.server, c, line, "Live spots are not paused.\n", true)
		})
	}
}

func TestShowHoldMalformedCommandsPreserveFullOwners(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, ziutek := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ziutek=%t", dialect, ziutek), func(t *testing.T) {
				x := newHelpOwnerSession(t, dialect)
				c := x.client
				if !strings.Contains(runHistory(t, x.server, c, "SH MYDX 1"), "K1ABC") || c.historySearch == nil {
					t.Fatal("malformed controls require a populated archive cursor")
				}
				c.startReadPause(x.server.now(), time.Minute)
				row := spot.NewSpot("K1ABC", "VE3ABC", 14030, "CW")
				row.Comment, row.IsHuman = "CQ DX", true
				if !c.filter.Matches(row) {
					t.Fatal("suppression control must satisfy the actual current filter")
				}
				c.enqueueSpot(&spotEnvelope{spot: row, enqueueAt: x.server.now()})
				if c.readPauseSuppressed.Load() != 1 || len(c.spotChan) != 0 {
					t.Fatal("eligible enqueue did not produce the suppression control")
				}
				before := captureHistoryCommentAliasState(t, c)
				files, pause, path := helpOwnerFiles(t), showHoldPauseState(c), c.pathSnapshot()
				minutes, next, diag, dedupe := c.getSolarSummaryMinutes(), c.solarNextSummaryAt, c.getDiagMode(), c.getDedupePolicy()
				writes, reads := x.writes, x.reads
				for _, command := range []string{"SHOW HOLD EXTRA", "SHOW  HOLD  OFF", "show hold 60 all"} {
					requireShowHoldReply(t, x.server, c, helpOwnerInput(t, command, ziutek), "Usage: SHOW HOLD\n", true)
					before.assertOwners(t, c, x.writes-writes)
					after := captureHistoryCommentAliasState(t, c)
					if x.reads != reads || !reflect.DeepEqual(files, helpOwnerFiles(t)) || c.pathSnapshot() != path ||
						c.getSolarSummaryMinutes() != minutes || !c.solarNextSummaryAt.Equal(next) || c.getDiagMode() != diag || c.getDedupePolicy() != dedupe ||
						before.search != after.search || before.query != after.query || before.token != after.token || !bytes.Equal(before.position, after.position) ||
						before.unreadable != after.unreadable || before.hadMatches != after.hadMatches || before.generation != after.generation || before.closed != after.closed {
						t.Fatalf("malformed %q changed runtime, cursor or exact saved library bytes", command)
					}
					assertReadbackPauseState(t, c, pause)
				}
			})
		}
	}
}

func showHoldSessionWrite(t *testing.T, conn net.Conn, command string) {
	t.Helper()
	if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, command+"\r\n"); err != nil {
		t.Fatal(err)
	}
}

func showHoldSessionExchange(t *testing.T, conn net.Conn, command, expected string) {
	t.Helper()
	showHoldSessionWrite(t, conn, command)
	response := readUntilContains(t, conn, expected, 5*time.Second)
	if strings.Contains(response, "after 1 output rows") || strings.Contains(response, "Missed spots are not replayed.") {
		t.Fatalf("HOLD reply took the automatic-pause path: %q", response)
	}
}

func TestShowHoldRealSessionsBypassOneRowAutomaticPause(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, transport := range []string{"native", "ziutek"} {
			t.Run(string(dialect)+"/"+transport, func(t *testing.T) {
				s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
					opts.Transport, opts.EchoMode = transport, telnetEchoOff
					opts.AutoReadPauseMinRows, opts.AutoReadPauseDuration = 1, 90*time.Second
				})
				var clock atomic.Int64
				clock.Store(1700000000000000000)
				s.nowFn = func() time.Time { return time.Unix(0, clock.Load()) }
				serverConn, conn, ownerDone := startHandshakeTranscriptSession(t, s)
				// This joins handleClient after closing both pipes. Its internal
				// writer has no join seam; this is not a goroutine-leak verdict.
				t.Cleanup(func() {
					_ = serverConn.Close()
					closeHandshakeTranscriptSession(t, conn, ownerDone)
				})
				readUntilContains(t, conn, "login: ", 5*time.Second)
				showHoldSessionWrite(t, conn, "N0CALL")
				readUntilContains(t, conn, "UTC>", 5*time.Second)
				s.clientsMutex.RLock()
				c := s.clients["N0CALL"]
				s.clientsMutex.RUnlock()
				if c == nil || s.useZiutek != (transport == "ziutek") {
					t.Fatal("real session did not select the requested codec")
				}
				if dialect == DialectCC {
					showHoldSessionWrite(t, conn, "DIALECT CC")
					readUntilContains(t, conn, "Missed spots are not replayed.\r\n", 5*time.Second)
					showHoldSessionWrite(t, conn, "RESUME")
					readUntilContains(t, conn, "Live spots resumed. Suppressed spots: 0.\r\n", 5*time.Second)
				}
				if c.dialect != dialect {
					t.Fatal("real session did not select the requested dialect/codec")
				}
				initial := showHoldPauseState(c)
				showHoldSessionExchange(t, conn, "  sHoW   hOlD  ", "Live spots are not paused.\r\n")
				showHoldSessionExchange(t, conn, "SHOW HOLD EXTRA", "Usage: SHOW HOLD\r\n")
				assertReadbackPauseState(t, c, initial)
				showHoldSessionWrite(t, conn, "PAUSE 60")
				readUntilContains(t, conn, "Missed spots are not replayed.\r\n", 5*time.Second)
				c.enqueueSpot(&spotEnvelope{spot: writerV15Spot(), enqueueAt: s.now()})
				want := readbackPauseTestState{until: 1700000060000000000, cutoff: 1700000060000000000, count: 1, epoch: initial.epoch + 1}
				assertReadbackPauseState(t, c, want)
				clock.Store(1700000010000000000)
				showHoldSessionExchange(t, conn, "show  hold", "Live spots paused for 50s more. Suppressed spots: 1.\r\n")
				showHoldSessionExchange(t, conn, "SHOW   HOLD OFF", "Usage: SHOW HOLD\r\n")
				assertReadbackPauseState(t, c, want)
				showHoldSessionWrite(t, conn, "RESUME")
				readUntilContains(t, conn, "Live spots resumed. Suppressed spots: 1.\r\n", 5*time.Second)
				want = readbackPauseTestState{cutoff: 1700000010000000000, epoch: initial.epoch + 2}
				showHoldSessionExchange(t, conn, "SHOW HOLD", "Live spots are not paused.\r\n")
				assertReadbackPauseState(t, c, want)
			})
		}
	}
}
