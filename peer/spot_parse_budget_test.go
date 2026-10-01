package peer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSpotFrameParseChargeExactFieldAndASCIIShape(t *testing.T) {
	for _, kind := range []string{"PC11", "pc61", "Pc26"} {
		for _, comment := range []string{"", "A\t B  C", "A\u00a0B", "\u00a0 A\tB\u00a0", "\xff \xff", "H95"} {
			line := " \t" + kind + "^14000^K1ABC^01-Oct-2026^1200Z^" + comment + "^W1ABC^N1REM^192.0.2.1^^^H2^\r\n"
			// This independent oracle deliberately inspects the known comment
			// fixture, not the production pre-split field locator.
			tokens := 0
			for _, group := range strings.Split(strings.ReplaceAll(comment, "\t", " "), " ") {
				if group != "" {
					tokens++
				}
			}
			want := int64(65536 + 64*(strings.Count(line, "^")+1) + 32*len(line) + 24*len(comment) + 128*tokens)
			got, err := frameParseCharge(line)
			if err != nil || got != want {
				t.Fatalf("%s comment%q: got%d want%d err%v", kind, comment, got, want, err)
			}
		}
	}
	for _, line := range []string{"PC11^", "PC61^1^2^3^4^", "PC26^1^2^3^4^A A", " PC61^" + strings.Repeat("^", 65529)} {
		charge, err := frameParseCharge(line)
		if err != nil || charge > 7929984 {
			t.Fatalf("malformed shape exceeded its bounded conservative charge: %d %v", charge, err)
		}
	}
}

func maximumSpotBudgetWire(kind, dx, de string) string {
	prefix := kind + "^14000^" + dx + "^01-Oct-2026^1200Z^"
	suffix := "^" + de + "^N1REM^192.0.2.1^H1^"
	n := MaxPeerFrameBytes - len(prefix) - len(suffix)
	return prefix + strings.Repeat("X ", n/2) + strings.Repeat("X", n%2) + suffix
}

func TestSpotLeaseRealParserConcurrentAndReaderHeadroom(t *testing.T) {
	b := newFrameParseBudget()
	line := maximumSpotBudgetWire("PC61", "K1ABC", "W1ABC")
	charge, err := frameParseCharge(line)
	if err != nil || charge > 7929984 || peerParseScratchBytes-charge < readerScratchBytes {
		t.Fatalf("maximum spot lease leaves no reader progress: %d %v", charge, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	spotLease, err := b.acquire(ctx, time.Time{}, line)
	if err != nil {
		t.Fatal(err)
	}
	readerLease, err := b.acquireCharge(ctx, time.Time{}, readerScratchBytes)
	if err != nil {
		t.Fatal(err)
	}
	readerLease.release()
	spotLease.release()
	s := &session{manager: &Manager{parseBudget: b}, ctx: ctx}
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := s.withParsedFrame(line, time.Time{}, func(frame *Frame) (bool, error) {
				if _, err := parseSpotFromFrame(frame, "N1REM"); err != nil {
					return false, err
				}
				return true, nil
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	if used, peak := b.usage(); used != 0 || peak > peerParseScratchBytes || peak < charge+readerScratchBytes {
		t.Fatalf("spot parser ownership or reader progress failed: used%d peak%d", used, peak)
	}
}

func TestSpotLeaseInvalidDXAndDEDiagnosticConsumers(t *testing.T) {
	for _, role := range []string{"DX", "DE"} {
		for _, kind := range []string{"PC11", "PC61", "PC26"} {
			b := newFrameParseBudget()
			ctx := t.Context()
			m := &Manager{parseBudget: b, dropReporter: func(string) {}}
			s := &session{manager: m, ctx: ctx, remoteCall: "N1REM"}
			called := 0
			m.badCallReporter = func(source, gotRole, reason, call, deCall, dxCall, mode, detail string) {
				called++
				if gotRole != role || reason != "invalid_callsign" {
					t.Errorf("incorrect diagnostic branch: %s %s", gotRole, reason)
				}
				// Installed reporting normalizes these temporary fields before
				// handing them to its separately owned persistent dedupe/logger.
				for _, value := range []string{source, call, deCall, dxCall} {
					_ = strings.Join(strings.Fields(strings.ToUpper(value)), " ")
				}
				if used, _ := b.usage(); used == 0 {
					t.Error("diagnostic callback ran after its parser lease ended")
				}
			}
			dx, de := "K1ABC", "W1ABC"
			if role == "DX" {
				dx = "INVALID CALL"
			} else {
				de = "INVALID CALL"
			}
			line := maximumSpotBudgetWire(kind, dx, de)
			_, err := s.withParsedFrame(line, time.Time{}, func(frame *Frame) (bool, error) {
				m.HandleFrame(frame, s)
				return false, errors.New("intentional handler failure")
			})
			if err == nil || called != 1 {
				t.Fatalf("diagnostic consumer not exercised exactly once: callbacks%d err%v", called, err)
			}
			if used, _ := b.usage(); used != 0 {
				t.Fatalf("error path leaked%d bytes", used)
			}
		}
	}
}

func TestSpotLeaseManagerStopCancelsActualParserAndQueuedWaiter(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	m := &Manager{parseBudget: newFrameParseBudget(), ctx: ctx, cancel: cancel}
	m.protocol = newProtocolController(m)
	line := maximumSpotBudgetWire("PC61", "K1ABC", "W1ABC")
	entered := make(chan struct{})
	results := make(chan error, 2)
	s := &session{manager: m, ctx: ctx}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		_, err := s.withParsedFrame(line, time.Time{}, func(frame *Frame) (bool, error) {
			if _, err := parseSpotFromFrame(frame, "N1REM"); err != nil {
				return false, err
			}
			close(entered)
			<-ctx.Done()
			return false, ctx.Err()
		})
		results <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("actual parser never acquired its lease")
	}
	deadline := time.Now().Add(20 * time.Millisecond)
	if _, err := m.parseBudget.acquire(ctx, deadline, line); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fixed parser wait deadline changed: %v", err)
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		_, err := s.withParsedFrame(line, time.Time{}, func(*Frame) (bool, error) {
			return false, errors.New("waiting parser unexpectedly acquired scratch before Stop")
		})
		results <- err
	}()
	// The fixture owner acknowledges the ordinary bounded withdrawal. Stop
	// itself then cancels and joins the two real parser scopes above.
	go func() {
		request := <-m.protocol.lifecycle
		request.done <- nil
	}()
	m.Stop()
	for range 2 {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Errorf("Stop parser outcome: %v", err)
		}
	}
	if used, peak := m.parseBudget.usage(); used != 0 || peak > peerParseScratchBytes {
		t.Fatalf("Stop leaked parser ownership: used%d peak%d", used, peak)
	}
}
