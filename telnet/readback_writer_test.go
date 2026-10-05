package telnet

import (
	"bufio"
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type readbackDeliveryConn struct {
	discardConn
	entered chan struct{}
	release chan struct{}
	err     error
	written []byte
}

func (c *readbackDeliveryConn) Write(p []byte) (int, error) {
	c.written = append(c.written, p...)
	close(c.entered)
	<-c.release
	if c.err != nil {
		return 0, c.err
	}
	return len(p), nil
}

func TestReadbackWriterCompletionRequiresSuccessfulWriteAndFlush(t *testing.T) {
	for _, behavior := range []string{"mixed_success", "later_resume", "later_pause", "write_failure", "flush_failure"} {
		t.Run(behavior, func(t *testing.T) {
			initial := time.Unix(1700000000, 0)
			var clock atomic.Int64
			clock.Store(initial.UnixNano())
			s := &Server{writerBatchMaxBytes: 2048, writerBatchWait: time.Millisecond,
				nowFn: func() time.Time { return time.Unix(0, clock.Load()) }}
			conn := &readbackDeliveryConn{entered: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(conn.release) }) })
			c := &Client{server: s, conn: conn, writer: bufio.NewWriterSize(conn, 256),
				done: make(chan struct{}), controlChan: make(chan controlMessage, 8), spotChan: make(chan *spotEnvelope, 1)}
			old := c.beginHumanReadback(initial, 10*time.Second)
			latest := c.beginHumanReadback(initial.Add(time.Second), 30*time.Second)
			c.readPauseSuppressed.Store(3)
			want := "first\r\nsecond\r\n---\r\nresult: ok\r\n...\r\n"
			if behavior == "write_failure" {
				want = strings.Repeat("X", 1024)
				conn.err = errors.New("direct write failed")
				c.controlChan <- controlMessage{raw: []byte(want), readback: latest}
			} else {
				c.controlChan <- controlMessage{raw: []byte("first\r\n"), readback: old}
				c.controlChan <- controlMessage{raw: []byte("second\r\n"), readback: latest}
				c.controlChan <- controlMessage{raw: []byte("---\r\nresult: ok\r\n...\r\n")}
				if behavior == "flush_failure" {
					conn.err = errors.New("buffer flush failed")
				}
			}
			c.controlChan <- controlMessage{closeAfter: true}
			loopDone := writerV15Start(c)
			writerV15Wait(t, conn.entered, "delivery entered")
			if !c.readPausePending.Load() || c.readPauseUntilUnixNano.Load() != 0 {
				t.Fatal("reading interval began before write and flush completed")
			}
			completedAt := initial.Add(2 * time.Minute)
			clock.Store(completedAt.UnixNano())
			switch behavior {
			case "later_resume":
				_, _ = s.handleReadPauseCommand(c, "RESUME")
			case "later_pause":
				_, _ = s.handleReadPauseCommand(c, "PAUSE 5")
			}
			releaseOnce.Do(func() { close(conn.release) })
			writerV15Wait(t, loopDone, "completion")
			if !bytes.Equal(conn.written, []byte(want)) {
				t.Fatal("mixed batch changed or interleaved the complete YAML frame")
			}
			wantUntil, wantCount := int64(0), uint64(3)
			switch behavior {
			case "mixed_success":
				wantUntil = completedAt.Add(30 * time.Second).UnixNano()
			case "later_pause":
				wantUntil = completedAt.Add(5 * time.Second).UnixNano()
			case "later_resume":
				wantCount = 0
			}
			if c.readPausePending.Load() || c.readPauseUntilUnixNano.Load() != wantUntil || c.readPauseSuppressed.Load() != wantCount {
				t.Fatalf("completion state: pending=%v deadline=%d count=%d; want deadline=%d count=%d",
					c.readPausePending.Load(), c.readPauseUntilUnixNano.Load(), c.readPauseSuppressed.Load(), wantUntil, wantCount)
			}
		})
	}
}
