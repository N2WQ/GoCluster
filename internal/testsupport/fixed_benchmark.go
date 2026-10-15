package testsupport

import (
	"context"
	"flag"
	"fmt"
	"testing"
	"time"
)

// FixedBenchmarkProtocol identifies the complete-once S4 measurement contract.
const FixedBenchmarkProtocol = "s4-fixed-benchmark-v3"

type fixedBenchmarkAuthority struct {
	ctx         context.Context
	deadline    time.Time
	hasDeadline bool
}

type fixedBenchmarkState struct {
	callbacks  int
	iterations int
	completed  bool
	benchmark  *testing.B
	refusal    error
}

// RunFixedBenchmark runs exactly one complete fixed-work callback. The caller
// must explicitly select -test.benchtime=1x and supply a live outer test deadline.
// Work owns its goroutines and must cancel, release and join all of them in its
// own defers, including Fatal/Skip/panic exits, before the callback can return.
// The helper's deferred cancellation cannot replace cancel-before-join ordering.
//
// All readers share the supplied primed clock until work and its defers finish.
// Work must not mutate the benchmark timer, call B.Run or nest Benchmark, and
// must return true only after its full population and correctness checks pass.
// Capture the outer test name before entry: programmatic benchmarks are unnamed.
func RunFixedBenchmark(t *testing.T, work func(*testing.B, context.Context, context.CancelFunc, *Clock) bool) {
	t.Helper()
	deadline, ok := t.Deadline()
	authority := fixedBenchmarkAuthority{ctx: t.Context(), deadline: deadline, hasDeadline: ok}
	runFixedBenchmark(t, authority, work, testing.Benchmark)
}

// The runner parameter is private and specialized: public calls always use the
// genuine testing lifecycle. Child tests can fault dispatch or result state
// without changing process-global testing flags or relying on watchdog expiry.
func runFixedBenchmark(t *testing.T, authority fixedBenchmarkAuthority, work func(*testing.B, context.Context, context.CancelFunc, *Clock) bool, run func(func(*testing.B)) testing.BenchmarkResult) {
	t.Helper()
	benchtime := flag.Lookup("test.benchtime")
	if benchtime == nil || benchtime.Value.String() != "1x" {
		t.Fatal("fixed-benchmark-refused: entry-benchtime requires explicit -test.benchtime=1x")
	}
	if err := authority.failure("entry"); err != nil {
		t.Fatal(err)
	}
	if work == nil {
		t.Fatal("fixed-benchmark-refused: work-missing")
	}
	var state fixedBenchmarkState
	result := run(func(b *testing.B) {
		state.callbacks++
		state.benchmark, state.iterations = b, b.N
		if state.callbacks != 1 || b.N != 1 {
			b.Fatal("fixed-benchmark-refused: callback-count-or-iterations")
		}
		if err := authority.failure("dispatch"); err != nil {
			state.refusal = err
			b.Fatal(err)
		}
		ctx, cancel := context.WithDeadline(authority.ctx, authority.deadline)
		defer cancel()
		clock := NewClock(b)
		clock.benchmark = b
		// Windows testing lazily initializes its shared QPC frequency. Prime on
		// this owner before publishing the immutable clock to any goroutine.
		clock.NowNS()
		if err := authority.failure("dispatch"); err != nil {
			state.refusal = err
			b.Fatal(err)
		}
		state.completed = work(b, ctx, cancel, clock)
	})
	// Benchmark has joined its callback and run B.Cleanup here. Checking only a
	// callback defer would miss cleanup failures and could emit a false receipt.
	if err := state.failure(result); err != nil {
		t.Fatal(err)
	}
}

func (a fixedBenchmarkAuthority) failure(stage string) error {
	if !a.hasDeadline {
		return fmt.Errorf("fixed-benchmark-refused: %s-deadline-missing", stage)
	}
	if !time.Now().Before(a.deadline) {
		return fmt.Errorf("fixed-benchmark-refused: %s-deadline-expired", stage)
	}
	if a.ctx == nil || a.ctx.Err() != nil {
		return fmt.Errorf("fixed-benchmark-refused: %s-outer-canceled", stage)
	}
	return nil
}

func (s fixedBenchmarkState) failure(result testing.BenchmarkResult) error {
	reason := ""
	switch {
	case s.callbacks != 1:
		reason = "callback-count"
	case s.benchmark == nil:
		reason = "callback-missing"
	case s.iterations != 1:
		reason = "callback-iterations"
	case s.refusal != nil:
		return s.refusal
	case s.benchmark.Skipped():
		reason = "callback-skipped"
	case s.benchmark.Failed():
		reason = "callback-failed"
	case !s.completed:
		reason = "incomplete"
	case result.N != 1:
		reason = "result-iterations"
	}
	if reason != "" {
		return fmt.Errorf("fixed-benchmark-refused: %s", reason)
	}
	return nil
}
