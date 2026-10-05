package telnet

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestManualReadPauseDurations(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	for _, tc := range []struct {
		line    string
		seconds int
	}{
		{line: "PAUSE", seconds: 30},
		{line: "PAUSE 60", seconds: 60},
		{line: "PAUSE 1", seconds: 1},
		{line: "PAUSE 300", seconds: 300},
		{line: " \tPaUsE\t 60 \t ", seconds: 60},
		{line: "PAUSE 060", seconds: 60},
		{line: "PAUSE +60", seconds: 60},
	} {
		t.Run(tc.line, func(t *testing.T) {
			for _, auto := range []struct {
				rows     int
				duration time.Duration
			}{{rows: 10, duration: 90 * time.Second}, {duration: 90 * time.Second}, {rows: 10}} {
				server := &Server{autoReadPauseMinRows: auto.rows, autoReadPauseDuration: auto.duration, nowFn: func() time.Time { return now }}
				client, other := &Client{server: server}, &Client{server: server}
				server.clients = map[string]*Client{"N0CALL": client, "N1CALL": other}
				response, handled := server.handleReadPauseCommand(client, tc.line)
				want := fmt.Sprintf("Live spots paused for %ds. Type RESUME to resume now.\nMissed spots are not replayed.\n", tc.seconds)
				if !handled || response != want {
					t.Fatalf("PAUSE response = %q, handled=%t, want %q", response, handled, want)
				}
				active, remaining, suppressed := client.readPauseStatus(now)
				if !active || remaining != time.Duration(tc.seconds)*time.Second || suppressed != 0 {
					t.Fatalf("pause status = %t, %s, %d", active, remaining, suppressed)
				}
				if other.readPauseUntilUnixNano.Load() != 0 || other.readPauseDiscardBefore.Load() != 0 {
					t.Fatal("PAUSE changed another client's state")
				}
			}
		})
	}
}

func TestReadPauseInvalidCommandsPreserveState(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	server := &Server{nowFn: func() time.Time { return now }}
	for _, tc := range []struct {
		line    string
		handled bool
	}{
		{line: "PAUSE 0", handled: true},
		{line: "PAUSE -1", handled: true},
		{line: "PAUSE 301", handled: true},
		{line: "PAUSE 1.5", handled: true},
		{line: "PAUSE 60s", handled: true},
		{line: "PAUSE invalid", handled: true},
		{line: "PAUSE 60 extra", handled: true},
		{line: "PAUSE 9223372036854775808", handled: true},
		{line: "PAUSE 9999999999999999999999999999999999999999", handled: true},
		{line: "PAUSES"},
		{line: "PAUSE/60"},
		{line: "RESUME 60"},
		{line: "HELP PAUSE"},
	} {
		t.Run(tc.line, func(t *testing.T) {
			for _, deadline := range []int64{0, now.Add(time.Minute).UnixNano()} {
				client := &Client{}
				client.readPauseUntilUnixNano.Store(deadline)
				client.readPauseDiscardBefore.Store(now.Add(-time.Second).UnixNano())
				client.readPauseSuppressed.Store(7)
				response, handled := server.handleReadPauseCommand(client, tc.line)
				if handled != tc.handled {
					t.Fatalf("handled=%t, want %t", handled, tc.handled)
				}
				want := ""
				if tc.handled {
					want = "Usage: PAUSE [seconds 1-300] (default 30)\n"
				}
				if response != want {
					t.Fatalf("response=%q, want %q", response, want)
				}
				if client.readPauseUntilUnixNano.Load() != deadline || client.readPauseDiscardBefore.Load() != now.Add(-time.Second).UnixNano() || client.readPauseSuppressed.Load() != 7 {
					t.Fatal("invalid or unrelated command changed pause state")
				}
			}
		})
	}
}

func TestReadPauseReplacementAndCounters(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	now := start
	server := &Server{nowFn: func() time.Time { return now }}
	client := &Client{}
	server.handleReadPauseCommand(client, "PAUSE 60")
	client.readPauseSuppressed.Store(3)
	now = start.Add(10 * time.Second)
	server.handleReadPauseCommand(client, "PAUSE 5")
	wantDeadline := start.Add(15 * time.Second)
	if client.readPauseUntilUnixNano.Load() != wantDeadline.UnixNano() || client.readPauseDiscardBefore.Load() != wantDeadline.UnixNano() || client.readPauseSuppressed.Load() != 3 {
		t.Fatal("manual replacement did not shorten both deadlines while keeping the count")
	}
	if active, _, _ := client.readPauseStatus(wantDeadline.Add(-time.Nanosecond)); !active {
		t.Fatal("pause expired before its deadline")
	}
	if active, _, suppressed := client.readPauseStatus(wantDeadline); active || suppressed != 3 {
		t.Fatal("pause did not expire at its deadline with its count retained")
	}
	now = wantDeadline.Add(time.Second)
	server.handleReadPauseCommand(client, "PAUSE")
	if active, remaining, suppressed := client.readPauseStatus(now); !active || remaining != 30*time.Second || suppressed != 0 {
		t.Fatalf("new pause after expiry = %t, %s, %d", active, remaining, suppressed)
	}
}

func TestAutoReadPausePreservesOrExtendsDeadline(t *testing.T) {
	start := time.Unix(1700000000, 0).UTC()
	for _, tc := range []struct {
		name           string
		pauseSeconds   int
		elapsed        time.Duration
		wantSeconds    int
		wantSuppressed uint64
	}{
		{name: "preserves longer manual pause", pauseSeconds: 60, elapsed: 10 * time.Second, wantSeconds: 50, wantSuppressed: 3},
		{name: "extends shorter active pause", pauseSeconds: 10, elapsed: 2 * time.Second, wantSeconds: 30, wantSuppressed: 3},
		{name: "starts after expiry", pauseSeconds: 10, elapsed: 10 * time.Second, wantSeconds: 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := start
			server := &Server{autoReadPauseMinRows: 1, autoReadPauseDuration: 30 * time.Second, nowFn: func() time.Time { return now }}
			client := &Client{}
			server.handleReadPauseCommand(client, fmt.Sprintf("PAUSE %d", tc.pauseSeconds))
			client.readPauseSuppressed.Store(3)
			now = start.Add(tc.elapsed)
			response := server.maybeApplyAutoReadPause(client, "output\n")
			if !strings.Contains(response, fmt.Sprintf("Live spots paused for %ds after 1 output rows.", tc.wantSeconds)) {
				t.Fatalf("footer does not report effective pause: %q", response)
			}
			wantDeadline := now.Add(time.Duration(tc.wantSeconds) * time.Second).UnixNano()
			if client.readPauseUntilUnixNano.Load() != wantDeadline || client.readPauseDiscardBefore.Load() != wantDeadline || client.readPauseSuppressed.Load() != tc.wantSuppressed {
				t.Fatal("automatic pause changed the deadline or count incorrectly")
			}
		})
	}
}

func TestReadPauseSessionTranscript(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, rows := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/auto-rows-%d", dialect, rows), func(t *testing.T) {
				server := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
					opts.AutoReadPauseMinRows = rows
					opts.AutoReadPauseDuration = 30 * time.Second
				})
				server.filterEngine.defaultDialect = dialect
				var clock atomic.Int64
				clock.Store(time.Unix(1700000000, 0).UnixNano())
				server.nowFn = func() time.Time { return time.Unix(0, clock.Load()) }
				serverConn, conn, done := startHandshakeTranscriptSession(t, server)
				defer serverConn.Close()
				defer closeHandshakeTranscriptSession(t, conn, done)
				readUntilContains(t, conn, "login: ", 2*time.Second)
				if _, err := io.WriteString(conn, "N0CALL\r\n"); err != nil {
					t.Fatal(err)
				}
				readUntilContains(t, conn, "UTC>", 2*time.Second)
				server.clientsMutex.RLock()
				client := server.clients["N0CALL"]
				server.clientsMutex.RUnlock()
				if client == nil {
					t.Fatal("logged-in client was not registered")
				}
				for _, command := range []struct {
					line, want string
					advance    time.Duration
					seconds    int
				}{
					{line: "PAUSE 60", want: "Missed spots are not replayed.\r\n", seconds: 60},
					{line: "SHOW HOLD", want: "Live spots paused for 50s more. Suppressed spots: 0.\r\n", advance: 10 * time.Second, seconds: 50},
					{line: "PAUSE 5", want: "Missed spots are not replayed.\r\n", seconds: 5},
					{line: "PAUSE 0", want: "Usage: PAUSE [seconds 1-300] (default 30)\r\n", advance: time.Second, seconds: 4},
					{line: "SHOW HOLD", want: "Live spots paused for 4s more. Suppressed spots: 0.\r\n", seconds: 4},
					{line: "RESUME", want: "Live spots resumed. Suppressed spots: 0.\r\n"},
					{line: "SHOW HOLD", want: "Live spots are not paused.\r\n"},
					{line: "RESUME", want: "Live spots were not paused.\r\n"},
					{line: "PAUSE", want: "Missed spots are not replayed.\r\n", seconds: 30},
				} {
					clock.Add(int64(command.advance))
					if _, err := io.WriteString(conn, command.line+"\r\n"); err != nil {
						t.Fatal(err)
					}
					readUntilContains(t, conn, command.want, 2*time.Second)
					active, remaining, _ := client.readPauseStatus(server.now())
					if active != (command.seconds > 0) || remaining != time.Duration(command.seconds)*time.Second {
						t.Fatalf("%s changed pause to active=%t remaining=%s, want %ds", command.line, active, remaining, command.seconds)
					}
				}
			})
		}
	}
}

func TestReadPauseConcurrentSpotSuppression(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	server := &Server{nowFn: func() time.Time { return now }}
	client := &Client{}
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 2 {
		workers.Go(func() {
			<-start
			for range 2000 {
				client.suppressSpotForReadPause(&spotEnvelope{enqueueAt: now}, now)
				client.readPauseStatus(now)
			}
		})
	}
	close(start)
	for range 200 {
		server.handleReadPauseCommand(client, "PAUSE 60")
		server.handleReadPauseCommand(client, "SHOW HOLD")
		server.handleReadPauseCommand(client, "RESUME")
	}
	workers.Wait()
	server.handleReadPauseCommand(client, "PAUSE")
	if active, remaining, _ := client.readPauseStatus(now); !active || remaining != 30*time.Second {
		t.Fatalf("final pause = active:%t remaining:%s", active, remaining)
	}
}

func FuzzReadPauseCommand(f *testing.F) {
	for _, seed := range []string{"PAUSE", "pause\t60", "PAUSE +60", "PAUSE 00030", "PAUSE 1", "PAUSE 300", "PAUSE 0", "PAUSE -1", "PAUSE 301", "PAUSE 1.5", "PAUSE 9999999999999999999999", "PAUSE 60 extra", "PAUSES", "RESUME", "SHOW HOLD", "HELP PAUSE"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, line string) {
		if len(line) > defaultCommandLineLimit {
			return // Bound fuzz inputs to the default command-line limit.
		}
		now := time.Unix(1700000000, 0).UTC()
		server := &Server{nowFn: func() time.Time { return now }}
		client := &Client{}
		deadline := now.Add(time.Minute).UnixNano()
		client.readPauseUntilUnixNano.Store(deadline)
		client.readPauseDiscardBefore.Store(deadline)
		client.readPauseSuppressed.Store(9)
		unchanged := func() {
			if client.readPauseUntilUnixNano.Load() != deadline || client.readPauseDiscardBefore.Load() != deadline || client.readPauseSuppressed.Load() != 9 {
				t.Fatalf("command changed protected pause state: %q", line)
			}
		}
		response, handled := server.handleReadPauseCommand(client, line)
		upper := strings.ToUpper(strings.TrimSpace(line))
		fields := strings.Fields(upper)
		if len(fields) > 0 && fields[0] == "PAUSE" {
			seconds, valid := 30, len(fields) <= 2
			if len(fields) == 2 {
				// Derive the bounded decimal value without the production integer parser.
				number := strings.TrimPrefix(fields[1], "+")
				seconds, valid = 0, number != ""
				for _, digit := range number {
					if digit < '0' || digit > '9' || seconds > 300 {
						valid = false
						break
					}
					seconds = seconds*10 + int(digit-'0')
				}
				valid = valid && seconds >= 1 && seconds <= 300
			}
			if !handled {
				t.Fatalf("PAUSE was not routed: %q", line)
			}
			if !valid {
				if response != "Usage: PAUSE [seconds 1-300] (default 30)\n" {
					t.Fatalf("invalid duration did not return usage: %q -> %q", line, response)
				}
				unchanged()
				return
			}
			wantDeadline := now.Add(time.Duration(seconds) * time.Second).UnixNano()
			if !strings.HasPrefix(response, fmt.Sprintf("Live spots paused for %ds.", seconds)) || client.readPauseUntilUnixNano.Load() != wantDeadline || client.readPauseDiscardBefore.Load() != wantDeadline || client.readPauseSuppressed.Load() != 9 {
				t.Fatalf("valid PAUSE violated duration or counter contract: %q -> %q", line, response)
			}
			return
		}
		if upper == "RESUME" {
			if !handled || client.readPauseUntilUnixNano.Load() != 0 || client.readPauseDiscardBefore.Load() != now.UnixNano() || client.readPauseSuppressed.Load() != 0 {
				t.Fatalf("RESUME did not clear pause state: %q", line)
			}
			return
		}
		unchanged()
		if handled != (upper == "SHOW HOLD") || (!handled && response != "") {
			t.Fatalf("incorrect command routing: %q -> %q, handled=%t", line, response, handled)
		}
	})
}
