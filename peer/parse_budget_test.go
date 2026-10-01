package peer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFrameParseBudgetConcurrentFloodAndRelease(t *testing.T) {
	budget := newFrameParseBudget()
	line := "PC99^" + strings.Repeat("^", MaxPeerFrameBytes-5)
	charge, err := frameParseCharge(line)
	if err != nil || charge > peerParseScratchBytes {
		t.Fatalf("charge %d %v", charge, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 192; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := budget.acquire(ctx, time.Time{}, line)
			if err != nil {
				t.Error(err)
				return
			}
			defer lease.release()
			frame, err := ParseFrame(line)
			if err != nil || frame == nil {
				t.Errorf("frame rejected: %v", err)
				return
			}
			used, _ := budget.usage()
			if used > peerParseScratchBytes {
				t.Errorf("scratch cap exceeded: %d", used)
			}
		}()
	}
	wg.Wait()
	used, peak := budget.usage()
	if used != 0 || peak < charge || peak > peerParseScratchBytes {
		t.Fatalf("leaked or unbounded scratch used=%d peak=%d", used, peak)
	}
}

func TestFrameParseBudgetCancellationAndFixedDeadline(t *testing.T) {
	budget := newFrameParseBudget()
	line := "PC99^" + strings.Repeat("^", MaxPeerFrameBytes-5)
	lease, err := budget.acquire(context.Background(), time.Time{}, line)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.release()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := budget.acquire(ctx, time.Time{}, line); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not release waiter")
	}
	deadline := time.Now().Add(20 * time.Millisecond)
	if _, err := budget.acquire(context.Background(), deadline, line); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline not honored: %v", err)
	}
	if time.Since(deadline) > time.Second {
		t.Fatal("phase deadline extended")
	}
	used, _ := budget.usage()
	if used != lease.bytes {
		t.Fatalf("canceled waiters retained charge: %d", used)
	}
}

func TestSessionParseLeaseEveryExit(t *testing.T) {
	budget := newFrameParseBudget()
	s := &session{manager: &Manager{parseBudget: budget}, ctx: context.Background()}
	for _, line := range []string{"not a frame", "PC92^" + strings.Repeat("^", 10000), "PC51^K1ABC^K2ABC^0^"} {
		_, _ = s.withParsedFrame(line, time.Time{}, func(*Frame) (bool, error) { return true, errors.New("handler failure") })
		if used, _ := budget.usage(); used != 0 {
			t.Fatalf("lease survived %q: %d", line[:min(len(line), 16)], used)
		}
	}
}

func BenchmarkFrameParseLease(b *testing.B) {
	budget := newFrameParseBudget()
	ctx := context.Background()
	line := "PC61^14000^K1ABC^01-Oct-2026^1200Z^CQ^K2ABC^K3ABC^192.0.2.1^H99^"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lease, err := budget.acquire(ctx, time.Time{}, line)
		if err != nil {
			b.Fatal(err)
		}
		lease.release()
	}
}
