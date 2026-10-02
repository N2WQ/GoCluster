package peer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"testing"
	"time"
)

type retryReceiptWriterFunc func([]byte) (int, error)

func (write retryReceiptWriterFunc) Write(data []byte) (int, error) { return write(data) }

func TestPC92V14RecoveryReceiptFastWriter(t *testing.T) {
	s, _ := newTransportTestSession(t)
	s.writer = bufio.NewWriter(io.Discard)
	if err := s.sendControlLine("C"); err != nil || !s.writeQueuedLine(<-s.priorityLineCh, true) {
		t.Fatalf("C did not flush: %v", err)
	}
	s.writer = bufio.NewWriter(retryReceiptWriterFunc(func(data []byte) (int, error) {
		_, flushed, target := retryWriterProgress(s)
		if flushed != 1 || target != 2 {
			return 0, fmt.Errorf("A reached Flush before registration: flushed=%d target=%d", flushed, target)
		}
		return len(data), nil
	}))
	admitted := make(chan error, 1)
	allowReturn := make(chan struct{})
	returned := make(chan struct{})
	// Exercise the production lock-owned operation, holding its admitting
	// caller's continuation until the immediate writer finishes. This forces
	// the hazardous order without a runtime callback, field, or timing guess.
	go func() {
		s.queueMu.Lock()
		err := s.enqueueControlLineLocked("A", true, time.Time{}, true)
		s.queueMu.Unlock()
		admitted <- err
		<-allowReturn
		close(returned)
	}()
	t.Cleanup(func() { close(allowReturn); <-returned })
	if err := <-admitted; err != nil {
		t.Fatal(err)
	}
	if !s.writeQueuedLine(<-s.priorityLineCh, true) {
		t.Fatal("immediate A Flush failed")
	}
	if _, flushed, target := retryWriterProgress(s); flushed != 2 || target != 0 {
		t.Fatalf("fast writer lost receipt: flushed=%d target=%d", flushed, target)
	}
	select {
	case <-returned:
		t.Fatal("admitting caller returned before forced writer completion")
	default:
	}
}

func retryWriterProgress(s *session) (enqueued, flushed, target uint64) {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	return s.controlLineEnqueued, s.controlLineFlushed, s.recoveryFlushTarget
}

func waitRetryWriterProgress(t *testing.T, s *session, want uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_, flushed, _ := retryWriterProgress(s)
		if flushed == want {
			return
		}
		runtime.Gosched()
	}
	_, flushed, target := retryWriterProgress(s)
	t.Fatalf("writer did not reach flush %d: flushed=%d target=%d", want, flushed, target)
}

func TestPC92V14RecoveryReceiptFIFO(t *testing.T) {
	s, _ := newTransportTestSession(t)
	// Socket deadlines still run, but each successful Flush completes locally.
	s.writer = bufio.NewWriter(io.Discard)
	if err := s.sendControlLine("C"); err != nil {
		t.Fatal(err)
	}
	if err := s.enqueueRecoveryMetadata("A", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := s.enqueueRecoveryMetadata("later A", time.Time{}); err != nil {
		t.Fatal(err)
	}
	if enqueued, flushed, target := retryWriterProgress(s); enqueued != 3 || flushed != 0 || target != 2 {
		t.Fatalf("later recovery replaced first target: enqueued=%d flushed=%d target=%d", enqueued, flushed, target)
	}
	if err := s.sendLine("spot"); err != nil {
		t.Fatal(err)
	}
	if !s.sendPriorityRaw([]byte{255, 254, 1}) || !s.writeQueuedRaw(<-s.priorityRawCh) || !s.writeQueuedLine(<-s.writeCh, false) {
		t.Fatal("non-control write failed")
	}
	if _, flushed, target := retryWriterProgress(s); flushed != 0 || target != 2 {
		t.Fatalf("other lane completed recovery: flushed=%d target=%d", flushed, target)
	}
	for index, want := range []string{"C", "A", "later A"} {
		line := <-s.priorityLineCh
		if line != want || !s.writeQueuedLine(line, true) {
			t.Fatalf("FIFO position %d: got=%q want=%q", index, line, want)
		}
		_, flushed, target := retryWriterProgress(s)
		wantTarget := uint64(2)
		if index >= 1 {
			wantTarget = 0
		}
		if flushed != uint64(index+1) || target != wantTarget {
			t.Fatalf("FIFO position %d: flushed=%d target=%d", index, flushed, target)
		}
	}
}

func TestPC92V14RecoveryReceiptFailedOrCancelledA(t *testing.T) {
	for _, cancelSession := range []bool{false, true} {
		name := "failed"
		if cancelSession {
			name = "canceled"
		}
		t.Run(name, func(t *testing.T) {
			s, remote := newTransportTestSession(t)
			if err := s.sendControlLine("C"); err != nil {
				t.Fatal(err)
			}
			if err := s.enqueueRecoveryMetadata("A", time.Time{}); err != nil {
				t.Fatal(err)
			}
			s.startWorker(s.writerLoop)
			if got := readSessionWire(t, bufio.NewReader(remote), remote); got != "C" {
				t.Fatalf("recovery membership=%q", got)
			}
			waitRetryWriterProgress(t, s, 1)
			// A cannot Flush: its peer has not read it. C success must not be
			// promoted to pair success, whether A then fails or is canceled.
			if _, flushed, target := retryWriterProgress(s); flushed != 1 || target != 2 {
				t.Fatalf("stalled A falsely completed: flushed=%d target=%d", flushed, target)
			}
			if cancelSession {
				s.close()
			} else if err := remote.Close(); err != nil {
				t.Fatal(err)
			}
			s.workers.Wait()
			if _, flushed, target := retryWriterProgress(s); flushed != 1 || target != 2 {
				t.Fatalf("failed A falsely completed: flushed=%d target=%d", flushed, target)
			}
			if !errors.Is(s.ctx.Err(), context.Canceled) {
				t.Fatal("failed writer did not retire session")
			}
			s.discardQueuedOutput()
			if enqueued, flushed, target := retryWriterProgress(s); enqueued != 0 || flushed != 0 || target != 0 {
				t.Fatalf("retired receipt retained: enqueued=%d flushed=%d target=%d", enqueued, flushed, target)
			}
		})
	}
}

func TestPC92V14RecoveryReceiptAdmissionFailure(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "deadline"
		if canceled {
			name = "canceled"
		}
		t.Run(name, func(t *testing.T) {
			s, _ := newTransportTestSession(t)
			deadline := time.Now().Add(-time.Second)
			want := context.DeadlineExceeded
			if canceled {
				s.close()
				deadline, want = time.Time{}, context.Canceled
			}
			if err := s.enqueueRecoveryMetadata("A", deadline); !errors.Is(err, want) {
				t.Fatalf("admission error=%v want=%v", err, want)
			}
			if enqueued, flushed, target := retryWriterProgress(s); enqueued != 0 || flushed != 0 || target != 0 {
				t.Fatalf("refused A retained receipt: enqueued=%d flushed=%d target=%d", enqueued, flushed, target)
			}
		})
	}
}
