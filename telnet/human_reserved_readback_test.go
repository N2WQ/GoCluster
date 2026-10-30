package telnet

import (
	"bufio"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func nativeReadbackSession(t *testing.T) (*Server, *Client) {
	t.Helper()
	c, _, _ := nativeCCCommandFixture(t)
	c.done, c.controlChan = make(chan struct{}), make(chan controlMessage, 8)
	c.server.nowFn = func() time.Time { return time.Unix(1700000000, 0) }
	return c.server, c
}

func TestNativeCCReadbackReservationPrecedesCaptureAndRetainsPause(t *testing.T) {
	input, output := configurationTransferBudgetStats()
	s, c := nativeReadbackSession(t)
	release, err := s.acquireConfiguration(c, false, false, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan bool, 1)
	go func() { finished <- s.handleHumanReadback(c, "SHOW/FILTER FULL") }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, used := configurationTransferBudgetStats()
		if c.readPausePending.Load() && used == output+configurationOutputReservationBytes {
			break
		}
		runtime.Gosched()
	}
	_, used := configurationTransferBudgetStats()
	if !c.readPausePending.Load() || used != output+configurationOutputReservationBytes || len(c.controlChan) != 0 {
		release()
		t.Fatal("native capture waited without first reserving output and suppressing spots")
	}
	release()
	select {
	case handled := <-finished:
		if !handled {
			t.Fatal("native readback was not recognized")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("native readback did not leave the transaction wait")
	}
	message := <-c.controlChan
	if message.configurationReservation == nil {
		t.Fatal("native readback did not transfer its reservation")
	}
	t.Cleanup(message.configurationReservation.Release)
	if !strings.Contains(string(message.raw), "Active filter bank: CC") || message.readback.epoch == 0 || !c.readPausePending.Load() {
		t.Fatal("native body or flush-owned pause completion was lost")
	}
	assertConfigurationTransferStats(t, input, output+configurationOutputReservationBytes)
	message.raw = nil
	message.configurationReservation.Release()
	assertConfigurationTransferStats(t, input, output)
}

func TestNativeCCReadbackWriterOwnsReservationThroughFlushAndFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "flush", true: "failure"}[fail], func(t *testing.T) {
			input, output := configurationTransferBudgetStats()
			s, c := nativeReadbackSession(t)
			s.writerBatchMaxBytes, s.writerBatchWait = 2048, time.Millisecond
			conn := &readbackDeliveryConn{entered: make(chan struct{}), release: make(chan struct{})}
			if fail {
				conn.err = errors.New("injected native response write failure")
			}
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(conn.release) }) })
			c.conn, c.writer, c.spotChan = conn, bufio.NewWriterSize(conn, 128), make(chan *spotEnvelope, 1)
			if !s.handleHumanReadback(c, "SHOW/FILTER FULL") {
				t.Fatal("native response not handled")
			}
			c.controlChan <- controlMessage{closeAfter: true}
			finished := writerV15Start(c)
			writerV15Wait(t, conn.entered, "native delivery entered")
			assertConfigurationTransferStats(t, input, output+configurationOutputReservationBytes)
			if !c.readPausePending.Load() || c.readPauseUntilUnixNano.Load() != 0 {
				t.Fatal("native reading interval started before successful flush")
			}
			once.Do(func() { close(conn.release) })
			writerV15Wait(t, finished, "native writer completion")
			assertConfigurationTransferStats(t, input, output)
			if c.readPausePending.Load() || (!fail && c.readPauseUntilUnixNano.Load() != 1700000030000000000) || (fail && c.readPauseUntilUnixNano.Load() != 0) {
				t.Fatal("native writer completion used queued bytes as a successful flush")
			}
		})
	}
}

func TestNativeCCReadbackBusyDoesNotCaptureAndSettingsAliasesRemainExact(t *testing.T) {
	input, output := configurationTransferBudgetStats()
	s, c := nativeReadbackSession(t)
	held, err := c.reserveConfigurationOutput()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(held.Release)
	current := c.filter
	c.filter = nil // A busy readback must fail before attempting this capture.
	if !s.handleHumanReadback(c, "SHOW/FILTER") {
		t.Fatal("busy native readback was not recognized")
	}
	busy := <-c.controlChan
	if busy.configurationReservation != nil || !strings.Contains(string(busy.raw), "busy") || busy.readback.epoch == 0 {
		t.Fatal("busy response captured native state or lost the human pause")
	}
	c.filter = current
	held.Release()
	for _, alias := range []string{"SHOW SETTINGS", "SHOW/SETTINGS", "SH/SETTINGS"} {
		if !s.handleHumanReadback(c, alias) {
			t.Fatalf("native settings alias not recognized: %s", alias)
		}
		message := <-c.controlChan
		if message.configurationReservation == nil {
			t.Fatal("native settings was captured without a reservation")
		}
		if !strings.Contains(string(message.raw), "Dialect") || strings.Contains(string(message.raw), "Numbered personal") || message.readback.epoch == 0 {
			message.configurationReservation.Release()
			t.Fatal("settings alias exposed Boolean guidance or lost completion")
		}
		message.raw = nil
		message.configurationReservation.Release()
	}
	if s.handleHumanReadback(c, "SHOW CONFIG") {
		t.Fatal("internal CONFIG rendering created a new human CLI command")
	}
	assertConfigurationTransferStats(t, input, output)
}
