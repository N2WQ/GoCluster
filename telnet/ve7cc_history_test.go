package telnet

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"dxcluster/archive"
	"dxcluster/cty"
	"dxcluster/spot"
)

const ve7ccHistoryWire = "CC11^14000.0^K1ABC^ 1-Jan-2000^0000Z^CW hello^VE7XYZ-12^291^1^^8^5^2^3^MA^BC^United States^Canada^FN42^cn89\r\n"

func runVE7CCHistory(t testing.TB, s *Server, c *Client, line string) controlMessage {
	t.Helper()
	if handled, proceed := s.handleHistoryCommand(c, line); !handled || !proceed {
		t.Fatalf("history failed: %q handled=%t proceed=%t", line, handled, proceed)
	}
	return ve7ccTakeControl(t, c)
}

func discardVE7CCHistory(message *controlMessage) {
	message.raw, message.line = nil, ""
	message.configurationReservation.Release()
	message.configurationReservation = nil
}

func TestVE7CCHistoryWireAndContinuation(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			input, output := configurationTransferBudgetStats()
			s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
				return archive.HistoryPage{Spots: []*spot.Spot{ve7ccTestSpot()}, Before: []byte("position"), End: archive.HistoryCountReached}, nil
			})
			c.dialect, s.clusterCall = dialect, "LIVE-NODE"
			ve7ccSelect(t, s, c, "SET/VE7CC")
			command := "SHOW DX 1"
			if dialect == DialectCC {
				command = "SH/DX 1"
			}
			message := runVE7CCHistory(t, s, c, command)
			defer discardVE7CCHistory(&message)
			if !bytes.HasPrefix(message.raw, []byte(ve7ccHistoryWire)) || strings.Contains(string(message.raw), "LIVE-NODE") ||
				message.configurationReservation == nil || cap(message.raw) != 65_536 || c.historySearch == nil {
				t.Fatalf("native history lost framing, historical origin or ownership: %q", message.raw)
			}
			assertConfigurationTransferStats(t, input, output+1<<20)
			token := c.historySearch.token
			discardVE7CCHistory(&message)
			assertConfigurationTransferStats(t, input, output)
			ve7ccSelect(t, s, c, "UNSET/VE7CC")
			if c.historySearch == nil || c.historySearch.token != token {
				t.Fatal("presentation change invalidated continuation")
			}
			text := runVE7CCHistory(t, s, c, "SHOW DX NEXT "+token)
			if text.raw != nil || !strings.HasPrefix(text.line, "Older retained history page:\n") ||
				!strings.Contains(text.line, "DX de ") || strings.Contains(text.line, "CC11^") {
				t.Fatalf("ordinary continuation changed: %q %q", text.raw, text.line)
			}
		})
	}
}

func TestVE7CCHistoryRefusalDoesNotPublishPartialPage(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "oversized", true: "invalid"}[invalid], func(t *testing.T) {
			input, output := configurationTransferBudgetStats()
			bad := false
			s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
				newer := ve7ccTestSpot()
				if bad {
					if invalid {
						newer.Frequency = 0
					} else {
						newer.Comment = strings.Repeat("X", 65_536)
					}
				}
				// Oldest is rendered first; failure follows an already prepared row.
				return archive.HistoryPage{Spots: []*spot.Spot{newer, ve7ccTestSpot()}, Before: []byte("position"), End: archive.HistoryCountReached}, nil
			})
			ve7ccSelect(t, s, c, "SET/VE7CC")
			first := runVE7CCHistory(t, s, c, "SHOW DX 2")
			discardVE7CCHistory(&first)
			token := c.historySearch.token
			bad = true
			reply := runVE7CCHistory(t, s, c, "SHOW DX NEXT "+token)
			if reply.raw != nil || reply.line != ve7ccHistoryFailure || reply.configurationReservation != nil ||
				c.historySearch == nil || c.historySearch.token != token || !bytes.Equal(c.historySearch.before, []byte("position")) {
				t.Fatal("failed continuation exposed a partial page or advanced its cursor")
			}
			fresh := runVE7CCHistory(t, s, c, "SHOW DX 2")
			if fresh.raw != nil || fresh.line != ve7ccHistoryFailure || c.historySearch != nil ||
				c.ve7ccDiagnostics.refused.Load() != 2 {
				t.Fatal("fresh failure resurrected prior search or lost refusal diagnostics")
			}
			assertConfigurationTransferStats(t, input, output)
		})
	}
}

func TestVE7CCHistoryFinalWireLimitAndFooter(t *testing.T) {
	for _, footer := range []bool{false, true} {
		t.Run(map[bool]string{false: "exact_wire_limit", true: "footer_headroom"}[footer], func(t *testing.T) {
			input, output := configurationTransferBudgetStats()
			sp := ve7ccTestSpot()
			const status = "End of retained history search.\r\n"
			bodyBudget := 65_536
			s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
				return archive.HistoryPage{Spots: []*spot.Spot{sp}, End: archive.HistoryExhausted}, nil
			})
			if footer {
				s.autoReadPauseMinRows, s.autoReadPauseDuration = 1, 30*time.Second
				bodyBudget -= dedupeReadPauseFooterBytes
			}
			sp.Comment += strings.Repeat("X", bodyBudget-len(ve7ccHistoryWire)-len(status))
			ve7ccSelect(t, s, c, "SET/VE7CC")
			message := runVE7CCHistory(t, s, c, "SHOW DX 1")
			defer discardVE7CCHistory(&message)
			if len(message.raw) > 65_536 || cap(message.raw) != 65_536 || message.configurationReservation == nil ||
				(!footer && len(message.raw) != 65_536) {
				t.Fatalf("wrong complete-frame bound: len=%d cap=%d", len(message.raw), cap(message.raw))
			}
			if footer && (!bytes.Contains(message.raw, []byte("after 2 output rows.")) ||
				!bytes.Contains(message.raw, []byte("Missed spots are not replayed.\r\n")) ||
				c.readPausePending.Load() || c.readPauseUntilUnixNano.Load() == 0) {
				t.Fatal("native history lost generic automatic pause semantics")
			}
			for i, ch := range message.raw {
				if (ch == '\n' && (i == 0 || message.raw[i-1] != '\r')) ||
					(ch == '\r' && (i+1 == len(message.raw) || message.raw[i+1] != '\n')) {
					t.Fatal("complete frame contains a bare or doubled line ending")
				}
			}
			discardVE7CCHistory(&message)
			sp.Comment += "X"
			over := runVE7CCHistory(t, s, c, "SHOW DX 1")
			if over.raw != nil || !strings.HasPrefix(over.line, ve7ccHistoryFailure) || over.configurationReservation != nil {
				t.Fatal("one byte over the admitted body budget was partially published")
			}
			assertConfigurationTransferStats(t, input, output)
		})
	}
}

func TestVE7CCHistoryChoosesFormatAfterScan(t *testing.T) {
	entered, releaseScan := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
		close(entered)
		<-releaseScan
		return archive.HistoryPage{Spots: []*spot.Spot{ve7ccTestSpot()}, End: archive.HistoryExhausted}, nil
	})
	finished := make(chan bool, 1)
	go func() { _, proceed := s.handleHistoryCommand(c, "SHOW DX 1"); finished <- proceed }()
	t.Cleanup(func() { unblock.Do(func() { close(releaseScan) }) })
	writerV15Wait(t, entered, "archive scan")
	ve7ccSelect(t, s, c, "SET/VE7CC")
	unblock.Do(func() { close(releaseScan) })
	select {
	case proceed := <-finished:
		if !proceed {
			t.Fatal("history publication failed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("history publication stalled")
	}
	message := ve7ccTakeControl(t, c)
	defer discardVE7CCHistory(&message)
	if !bytes.HasPrefix(message.raw, []byte(ve7ccHistoryWire)) {
		t.Fatalf("used pre-scan output mode: %q", message.raw)
	}
}

func TestVE7CCHistoryRevalidatesAfterEncoding(t *testing.T) {
	input, output := configurationTransferBudgetStats()
	entered, releaseEncoding := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
		return archive.HistoryPage{Spots: []*spot.Spot{ve7ccTestSpot()}, End: archive.HistoryCountReached}, nil
	})
	s.ctyLookup = func() *cty.CTYDatabase { close(entered); <-releaseEncoding; return nil }
	ve7ccSelect(t, s, c, "SET/VE7CC")
	finished := make(chan bool, 1)
	go func() { _, proceed := s.handleHistoryCommand(c, "SHOW DX 1"); finished <- proceed }()
	t.Cleanup(func() { unblock.Do(func() { close(releaseEncoding) }) })
	writerV15Wait(t, entered, "native encoding")
	assertConfigurationTransferStats(t, input, output+1<<20)
	c.setDiagMode(diagModeSource)
	c.setDiagMode(diagModeOff)
	unblock.Do(func() { close(releaseEncoding) })
	select {
	case proceed := <-finished:
		if !proceed {
			t.Fatal("restart response failed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("generation revalidation deadlocked")
	}
	message := ve7ccTakeControl(t, c)
	if message.raw != nil || message.line != historyRestart || message.configurationReservation != nil || c.historySearch != nil {
		t.Fatal("generation change-and-restore published stale native history")
	}
	assertConfigurationTransferStats(t, input, output)
}

func TestVE7CCHistoryCloseDuringPreparationBeforeFinalOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		input, output := configurationTransferBudgetStats()
		entered, releaseEncoding := make(chan struct{}), make(chan struct{})
		s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
			return archive.HistoryPage{Spots: []*spot.Spot{ve7ccTestSpot()}, End: archive.HistoryExhausted}, nil
		})
		s.ctyLookup = func() *cty.CTYDatabase { close(entered); <-releaseEncoding; return nil }
		ve7ccSelect(t, s, c, "SET/VE7CC")
		publication := make(chan bool, 1)
		go func() { _, proceed := s.handleHistoryCommand(c, "SHOW DX 1"); publication <- proceed }()
		<-entered
		c.interrupt()
		assertConfigurationTransferStats(t, input, output+1<<20)
		final := make(chan error, 1)
		go func() {
			release, err := s.acquireConfiguration(c, true, false, time.Time{})
			if err == nil {
				var owner *configurationOutputReservation
				owner, err = c.reserveConfigurationPreparation()
				owner.Release()
				release()
			}
			final <- err
		}()
		synctest.Wait()
		select {
		case <-final:
			t.Fatal("final owner overlapped the unqueued native producer")
		default:
		}
		close(releaseEncoding)
		if <-publication || <-final != nil || len(c.controlChan) != 0 || c.historySearch != nil {
			t.Fatal("closed producer published or retained its owner into final preparation")
		}
		assertConfigurationTransferStats(t, input, output)
	})
}

func TestVE7CCHistorySharesOutputAdmissionAcrossServers(t *testing.T) {
	input, output := configurationTransferBudgetStats()
	s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
		return archive.HistoryPage{Spots: []*spot.Spot{ve7ccTestSpot()}, Before: []byte("position"), End: archive.HistoryCountReached}, nil
	})
	servers := [2]*Server{s, {}}
	var owners []*configurationOutputReservation
	t.Cleanup(func() {
		for _, owner := range owners {
			owner.Release()
		}
		assertConfigurationTransferStats(t, input, output)
	})
	for i := range 7 {
		owner, err := (&Client{server: servers[i%2], done: make(chan struct{})}).reserveConfigurationOutput()
		if err != nil {
			t.Fatal(err)
		}
		owners = append(owners, owner)
	}
	lookups := 0
	s.ctyLookup = func() *cty.CTYDatabase { lookups++; return nil }
	ve7ccSelect(t, s, c, "SET/VE7CC")
	message := runVE7CCHistory(t, s, c, "SHOW DX 1")
	defer discardVE7CCHistory(&message)
	if message.configurationReservation == nil || lookups != 1 {
		t.Fatal("native page did not take one output owner and one CTY snapshot")
	}
	owners = append(owners, message.configurationReservation)
	assertConfigurationTransferStats(t, input, output+8<<20)
	extra := &Client{server: servers[1], done: make(chan struct{})}
	if owner, err := extra.reserveConfigurationOutput(); owner != nil || !errors.Is(err, errConfigurationTransferBusy) {
		t.Fatal("native history did not exhaust shared configuration capacity")
	}
	owners[0].Release()
	token := c.historySearch.token
	busy := runVE7CCHistory(t, s, c, "SHOW DX NEXT "+token)
	if busy.raw != nil || busy.line != ve7ccHistoryBusy || busy.configurationReservation != nil ||
		lookups != 1 || c.historySearch == nil || c.historySearch.token != token {
		t.Fatal("second client owner was admitted, encoded before admission, or advanced continuation")
	}
	discardVE7CCHistory(&message)
	reused, err := extra.reserveConfigurationOutput()
	if err != nil {
		t.Fatalf("released native capacity was not reusable by another Server: %v", err)
	}
	reused.Release()
}

func TestVE7CCHistoryWriterRetiresNativeAndBulletinOwners(t *testing.T) {
	for _, outcome := range []string{"flush", "write_failure", "closed", "shutdown"} {
		t.Run(outcome, func(t *testing.T) {
			input, output := configurationTransferBudgetStats()
			sp := ve7ccTestSpot()
			sp.Comment = strings.Repeat("X", 40<<10)
			s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
				return archive.HistoryPage{Spots: []*spot.Spot{sp}, End: archive.HistoryExhausted}, nil
			})
			s.writerBatchMaxBytes = 32 << 10
			c.writerStopped, c.spotChan = make(chan struct{}), make(chan *spotEnvelope, 1)
			ve7ccSelect(t, s, c, "SET/VE7CC")
			message := runVE7CCHistory(t, s, c, "SHOW DX 1")
			owner := message.configurationReservation
			t.Cleanup(owner.Release)
			want := append([]byte("bulletin-history\r\n"), message.raw...)
			if !s.reserveBulletinResponse() {
				t.Fatal("bulletin admission failed")
			}
			c.controlChan <- controlMessage{line: "bulletin-history\n", bulletinReservation: bulletinResponseReservation}
			c.controlChan <- message
			message.raw, message.configurationReservation = nil, nil
			c.controlChan <- controlMessage{closeAfter: true}

			conn := &readbackDeliveryConn{entered: make(chan struct{}), release: make(chan struct{})}
			if outcome == "write_failure" {
				conn.err = errors.New("native history write refused")
			}
			c.conn, c.writer = conn, bufio.NewWriterSize(conn, 32<<10)
			var closedReader net.Conn
			if outcome == "closed" || outcome == "shutdown" {
				writerConn, readerConn := net.Pipe()
				if err := readerConn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
				c.conn, c.writer = writerConn, bufio.NewWriterSize(writerConn, 32<<10)
				closedReader = readerConn
				t.Cleanup(func() { _ = writerConn.Close(); _ = readerConn.Close() })
			}
			switch outcome {
			case "closed":
				c.interrupt()
			case "shutdown":
				close(s.shutdown)
			}
			var unblock sync.Once
			done := writerV15Start(c)
			t.Cleanup(func() {
				unblock.Do(func() { close(conn.release) })
				writerV15Wait(t, done, "native history cleanup")
			})
			if outcome == "flush" || outcome == "write_failure" {
				writerV15Wait(t, conn.entered, "native history flush")
				assertConfigurationTransferStats(t, input, output+1<<20)
				if owner.released.Load() || s.bulletinResponseBytes.Load() != bulletinResponseReservation {
					t.Fatal("dequeue or in-flight write released native/bulletin ownership")
				}
				unblock.Do(func() { close(conn.release) })
			}
			writerV15Wait(t, done, "native history retirement")
			assertMixedConfigurationOwnersRetired(t, s, c, owner, input, output)
			if outcome == "flush" && !bytes.Equal(conn.written, want) {
				t.Fatal("writer altered mixed native and ordinary control bytes")
			}
			if closedReader != nil {
				var data [1]byte
				if n, err := closedReader.Read(data[:]); n != 0 || !errors.Is(err, io.EOF) {
					t.Fatal("closed writer emitted or retained a native frame")
				}
			}
		})
	}
}

func TestVE7CCHistoryQueueFullReleasesBeforeReporter(t *testing.T) {
	input, output := configurationTransferBudgetStats()
	s, c := historyTestSession(func(archive.HistoryRequest) (archive.HistoryPage, error) {
		return archive.HistoryPage{Spots: []*spot.Spot{ve7ccTestSpot()}, End: archive.HistoryCountReached}, nil
	})
	ve7ccSelect(t, s, c, "SET/VE7CC")
	c.controlChan = make(chan controlMessage, 1)
	c.controlChan <- controlMessage{line: "occupied"}
	reporterEntered, reporterRelease := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	s.connectionReporter = func(ConnectionEvent) { close(reporterEntered); <-reporterRelease }
	t.Cleanup(func() { unblock.Do(func() { close(reporterRelease) }) })
	finished := make(chan bool, 1)
	go func() { _, proceed := s.handleHistoryCommand(c, "SHOW DX 1"); finished <- proceed }()
	writerV15Wait(t, reporterEntered, "native queue-full reporter")
	assertConfigurationTransferStats(t, input, output)
	select {
	case <-c.done:
	default:
		t.Fatal("optional reporting preceded mandatory close")
	}
	if c.configurationOutputHeld || !c.historyClosed || c.historySearch != nil {
		t.Fatal("queue-full reporting retained native backing ownership or cursor")
	}
	unblock.Do(func() { close(reporterRelease) })
	select {
	case proceed := <-finished:
		if proceed {
			t.Fatal("full queue allowed command processing to continue")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("native queue-full publication deadlocked")
	}
}
