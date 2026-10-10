package main

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/peer"
)

func FuzzProbePC92Publication(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0})
	f.Add([]byte{1})
	f.Add([]byte{0, 1})
	f.Add([]byte{1, 0})
	f.Add(make([]byte, 32))
	f.Add(bytes.Repeat([]byte{1}, 32))
	alternating := make([]byte, 32)
	for i := range alternating {
		alternating[i] = byte(i % 2)
	}
	f.Add(alternating)
	f.Fuzz(func(t *testing.T, input []byte) { fuzzTimestampPublishers(t, input) })
}

// Cap before starting callers. There is no reader, interruptor, keeper or BYE:
// <=32 actual Flush Write entry/exit events fit each existing 128-entry channel.
func fuzzTimestampPublishers(t *testing.T, input []byte) {
	t.Helper()
	if len(input) > 32 {
		input = input[:32]
	}
	guard, endGuard := context.WithTimeout(context.Background(), probeFixtureBound)
	conn := newFixtureProbeConn(phaseMemoryConn{})
	var clockCalls atomic.Int32
	var missingClockLock atomic.Bool
	var owner *probeSession
	now := func() time.Time {
		clockCalls.Add(1)
		if owner.wireMu.TryLock() {
			missingClockLock.Store(true)
			owner.wireMu.Unlock()
		}
		return probeNoonUTC
	}
	owner = newProbeSession(guard, fixtureProbeConfig(), conn, peer.NewTimestampGenerator(), now, newProbeTicker)
	start := make(chan struct{})
	results := make(chan error, len(input))
	var workers sync.WaitGroup
	t.Cleanup(func() {
		defer endGuard()
		releaseProbeBarrier(start)
		owner.cancel(context.Canceled)
		workers.Wait()
		if err := conn.Close(); err != nil {
			t.Errorf("close publication fuzz transport: %v", err)
		}
	})
	wantAdds := 0
	for _, value := range input {
		add := value&1 == 0
		if add {
			wantAdds++
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, err := owner.sendPC92(add)
			results <- err
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	// Collect every caller result after every worker joined, before assertions
	// or successful-case cancellation. Failure cleanup also joins all callers.
	errorsSeen := make([]error, 0, len(input))
	for err := range results {
		errorsSeen = append(errorsSeen, err)
	}
	for _, err := range errorsSeen {
		if err != nil {
			t.Fatalf("actual publisher error = %v", err)
		}
	}
	if guard.Err() != nil || context.Cause(owner.ctx) != nil {
		t.Fatalf("publication fuzz guard/cause = %v / %v", guard.Err(), context.Cause(owner.ctx))
	}
	if len(errorsSeen) != len(input) || int(clockCalls.Load()) != len(input) || missingClockLock.Load() {
		t.Fatalf("publication admission/result cardinality = %d/%d/%d, missing gate %v", len(input), clockCalls.Load(), len(errorsSeen), missingClockLock.Load())
	}
	requireFuzzTimestampWire(t, conn, len(input), wantAdds)
	owner.cancel(context.Canceled)
}

func requireFuzzTimestampWire(t *testing.T, conn *fixtureProbeConn, count, wantAdds int) {
	t.Helper()
	writes := conn.writes()
	if len(writes) != count || conn.writers.Load() != 0 || conn.overlap.Load() {
		t.Fatalf("publication wire count/active/overlap = %d/%d/%v", len(writes), conn.writers.Load(), conn.overlap.Load())
	}
	adds := 0
	for i, line := range writes {
		fields := strings.Split(line, "^")
		if len(fields) < 4 || (fields[3] != "A" && fields[3] != "K") {
			t.Fatalf("actual publication action/payload = %q", line)
		}
		add := fields[3] == "A"
		if add {
			adds++
		}
		if line != literalProbePC92(noonProbeStamp(i), add) {
			t.Fatalf("publication %d changed stamp/full payload/CRLF or added trailing bytes: %q", i, line)
		}
	}
	if adds != wantAdds || count-adds != count-wantAdds {
		t.Fatalf("publication action multiset = %d A/%d K, want %d A/%d K", adds, count-adds, wantAdds, count-wantAdds)
	}
	if len(conn.writeExited) != count {
		t.Fatalf("accepted Flush exit count = %d, want %d", len(conn.writeExited), count)
	}
	for len(conn.writeExited) > 0 {
		exit := <-conn.writeExited
		if exit.err != nil || exit.closed || exit.interrupted {
			t.Fatalf("publication Flush was not accepted: %+v", exit)
		}
	}
}
