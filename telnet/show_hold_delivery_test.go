package telnet

import (
	"bufio"
	"bytes"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestShowHoldUsesActualWriterCompletionAndSuppression(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1700000000000000000)
	s := &Server{writerBatchMaxBytes: 2048, writerBatchWait: time.Millisecond,
		nowFn: func() time.Time { return time.Unix(0, clock.Load()) }}
	conn := &readbackDeliveryConn{entered: make(chan struct{}), release: make(chan struct{})}
	c := &Client{server: s, conn: conn, writer: bufio.NewWriterSize(conn, 256),
		done: make(chan struct{}), controlChan: make(chan controlMessage, 1), spotChan: make(chan *spotEnvelope, 2)}
	completion := c.beginHumanReadback(s.now(), 30*time.Second)
	c.enqueueSpot(&spotEnvelope{spot: writerV15Spot(), enqueueAt: s.now()})
	if c.readPauseSuppressed.Load() != 1 || len(c.spotChan) != 0 {
		t.Fatal("actual enqueue did not suppress pending-delivery traffic")
	}
	// The second envelope bypasses enqueue to exercise writer consumption, as
	// an envelope already queued before a readback would. Neither may be written.
	c.spotChan <- &spotEnvelope{spot: writerV15Spot(), enqueueAt: time.Unix(1699999999, 0)}
	const payload = "one complete readback\r\n"
	c.controlChan <- controlMessage{raw: []byte(payload), readback: completion}
	writerDone := writerV15Start(c)
	var releaseOnce sync.Once
	// No test path can leave Write blocked or the explicitly owned writer alive.
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(conn.release) })
		c.close("")
		writerV15Wait(t, writerDone, "HOLD delivery cleanup")
	})
	writerV15Wait(t, conn.entered, "HOLD delivery entered")
	assertReadbackPauseState(t, c, readbackPauseTestState{count: 2, epoch: 1, pending: true})
	clock.Store(1700000120000000000)
	requireShowHoldReply(t, s, c, "SHOW  HOLD", "Live spots paused while a response is being delivered. Suppressed spots: 2.\n", true)
	requireShowHoldReply(t, s, c, "SHOW HOLD EXTRA", "Usage: SHOW HOLD\n", true)
	select {
	case <-writerDone:
		t.Fatal("writer completed while its actual Write remained gated")
	default:
	}
	releaseOnce.Do(func() { close(conn.release) })
	// Only this successful writer completion can clear pending. Keep the clock
	// fixed: polling waits for a barrier; it never supplies the reading interval.
	deadline := time.Now().Add(5 * time.Second)
	for c.readPausePending.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.readPausePending.Load() {
		t.Fatal("successful Write/Flush did not publish its completion")
	}
	want := readbackPauseTestState{until: 1700000150000000000, cutoff: 1700000150000000000, count: 2, epoch: 1}
	assertReadbackPauseState(t, c, want)
	// The gate permits exactly one Write; HOLD is queried through its real
	// read-only owner, not sent as a second output through this test connection.
	if !bytes.Equal(conn.written, []byte(payload)) {
		t.Fatalf("writer emitted a suppressed spot or changed the readback: %q", conn.written)
	}
	requireShowHoldReply(t, s, c, "show hold", "Live spots paused for 30s more. Suppressed spots: 2.\n", true)
	clock.Store(1700000149999999999)
	requireShowHoldReply(t, s, c, "SHOW HOLD", "Live spots paused for 1s more. Suppressed spots: 2.\n", true)
	clock.Store(1700000150000000000)
	requireShowHoldReply(t, s, c, "SHOW HOLD", "Live spots are not paused. Suppressed spots in last pause: 2.\n", true)
	clock.Store(1700000150000000001)
	c.enqueueSpot(&spotEnvelope{spot: writerV15Spot(), enqueueAt: time.Unix(1700000149, 0)})
	if c.readPauseSuppressed.Load() != 3 || len(c.spotChan) != 0 || atomic.LoadUint64(&c.dropCount) != 0 {
		t.Fatal("stale suppression was replayed or counted as a slow-client drop")
	}
	if c.suppressSpotForReadPause(&spotEnvelope{spot: writerV15Spot(), enqueueAt: s.now()}, s.now()) || c.readPauseSuppressed.Load() != 3 {
		t.Fatal("fresh traffic remained suppressed after the delivery-based deadline")
	}
	requireShowHoldReply(t, s, c, "SHOW HOLD", "Live spots are not paused. Suppressed spots in last pause: 3.\n", true)
}
