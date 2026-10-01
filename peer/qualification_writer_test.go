//go:build qualification

package peer

import (
	"bufio"
	"context"
	"errors"
	"testing"
	"time"
)

func TestQualificationWriterLatchReleaseAndCancel(t *testing.T) {
	for _, cancelWait := range []bool{false, true} {
		s, remote := newTransportTestSession(t)
		m := &Manager{sessions: newFixedIndex[string, *session](64)}
		m.sessions.Set("peer", s)
		release, err := m.QualificationHoldWrites(t.Context(), []string{s.remoteCall})
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		s.startWorker(s.writerLoop)
		if err := s.sendControlLine("PC51^N1REM^N0CALL^1^"); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(time.Second)
		for m.QualificationTransports()[0].ActiveBytes == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if state := m.QualificationTransports()[0]; state.ActiveBytes == 0 || state.ControlCount != 0 {
			t.Fatalf("fault did not follow ordinary dequeue ownership: %+v", state)
		}
		if cancelWait {
			s.close()
			s.workers.Wait()
		} else {
			release()
			if got := readSessionWire(t, bufio.NewReader(remote), remote); got != "PC51^N1REM^N0CALL^1^" {
				t.Fatal(got)
			}
		}
	}
}

func TestQualificationWriterLatchKeepsOriginalDeadline(t *testing.T) {
	q := qualificationWriterState{hold: make(chan struct{})}
	deadline := time.Now().Add(20 * time.Millisecond)
	if err := q.wait(context.Background(), deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("writer scheduling fault bypassed original deadline: %v", err)
	}
	if time.Since(deadline) > time.Second {
		t.Fatal("writer scheduling fault extended the original deadline")
	}
}
