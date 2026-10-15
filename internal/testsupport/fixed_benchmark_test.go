package testsupport

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const fixedBenchmarkChildEnv = "GOCLUSTER_FIXED_BENCHMARK_CHILD"

func TestFixedBenchmarkLifecycle(t *testing.T) {
	for _, test := range []struct {
		name       string
		refusal    string
		active     bool
		beforeWork bool
	}{
		{"missing_benchtime", "entry-benchtime", false, true},
		{"wrong_benchtime", "entry-benchtime", false, true},
		{"no_deadline", "entry-deadline-missing", false, true},
		{"expired_deadline", "entry-deadline-expired", false, true},
		{"canceled_outer", "entry-outer-canceled", false, true},
		{"dispatch_expired", "dispatch-deadline-expired", false, true},
		{"dispatch_canceled", "dispatch-outer-canceled", false, true},
		{"fatal", "callback-failed", true, false},
		{"skip", "callback-skipped", true, false},
		{"worker_error", "callback-failed", true, false},
		{"cleanup_failure", "callback-failed", true, false},
		{"panic", "process-panic", true, false},
		{"incomplete", "incomplete", true, false},
		{"extra_callback_count", "callback-count", false, false},
		{"missing_callback_count", "callback-count", false, false},
		{"missing_callback", "callback-missing", false, false},
		{"wrong_callback_n", "callback-iterations", false, false},
		{"wrong_result_n", "result-iterations", false, false},
		{"success", "", true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			output := runFixedBenchmarkChild(t, test.name, test.refusal != "")
			if test.refusal != "" {
				if test.name != "panic" && !strings.Contains(output, "fixed-benchmark-refused: "+test.refusal) {
					t.Fatalf("missing exact refusal %q:\n%s", test.refusal, output)
				}
				if strings.Contains(output, "fixed-benchmark-valid-receipt:") {
					t.Fatalf("failed child emitted a valid receipt:\n%s", output)
				}
			} else if strings.Count(output, "fixed-benchmark-valid-receipt:"+test.name) != 1 {
				t.Fatalf("success requires exactly one receipt:\n%s", output)
			}
			if test.beforeWork && strings.Contains(output, "fixed-benchmark-work-entered:") {
				t.Fatalf("refused authority started work:\n%s", output)
			}
			if strings.HasPrefix(test.name, "dispatch_") && !strings.Contains(output, "fixed-benchmark-entry-validated:") {
				t.Fatalf("dispatch fault never passed initial validation:\n%s", output)
			}
			if test.active {
				retired := "fixed-benchmark-readers-retired:" + test.name + " canceled=true readers=4"
				retirement, cleanup := strings.Index(output, retired), strings.Index(output, "fixed-benchmark-domain-cleanup:"+test.name)
				if retirement < 0 || cleanup <= retirement || strings.Count(output, retired) != 1 {
					t.Fatalf("readers did not join with canceled context before domain cleanup:\n%s", output)
				}
			}
		})
	}
}

// Each child is the same saved executable, never a go-run/go-test subprocess.
// Parent bounds are an unexpected-failure watchdog, never a rejection oracle.
func runFixedBenchmarkChild(t *testing.T, scenario string, wantFailure bool) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	timeout := "10s"
	if scenario == "no_deadline" {
		timeout = "0"
	}
	arguments := []string{"-test.run=^TestFixedBenchmarkChild$", "-test.v", "-test.count=1", "-test.timeout=" + timeout}
	if scenario != "missing_benchtime" {
		benchtime := "1x"
		if scenario == "wrong_benchtime" {
			benchtime = "2x"
		}
		arguments = append(arguments, "-test.benchtime="+benchtime)
	}
	command := exec.CommandContext(ctx, executable, arguments...)
	command.Env = append(os.Environ(), fixedBenchmarkChildEnv+"="+scenario)
	command.WaitDelay = 2 * time.Second
	outputBytes, err := command.CombinedOutput()
	output := string(outputBytes)
	if ctx.Err() != nil {
		t.Fatalf("child failed execution/join watchdog, not an expected refusal: %v\n%s", ctx.Err(), output)
	}
	if !strings.Contains(output, "=== RUN   TestFixedBenchmarkChild") || strings.Contains(output, "test timed out") {
		t.Fatalf("child crashed/timed out or did not run its exact leaf:\n%s", output)
	}
	if scenario == "panic" {
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() <= 0 || !strings.Contains(output, "panic: injected active-reader panic") {
			t.Fatalf("panic child requires its deliberate process-fatal identity and nonzero exit: %v\n%s", err, output)
		}
	} else if strings.Contains(output, "\npanic:") || strings.HasPrefix(output, "panic:") {
		t.Fatalf("unexpected child panic:\n%s", output)
	} else if wantFailure {
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(output, "--- FAIL: TestFixedBenchmarkChild") {
			t.Fatalf("child requires normal test-failure exit 1; err=%v:\n%s", err, output)
		}
	} else if err != nil || !strings.Contains(output, "--- PASS: TestFixedBenchmarkChild") {
		t.Fatalf("child did not pass: %v\n%s", err, output)
	}
	return output
}

func TestFixedBenchmarkChild(t *testing.T) {
	scenario := os.Getenv(fixedBenchmarkChildEnv)
	if scenario == "" {
		return // The ordinary parent owns all required negative/success leaves.
	}
	if strings.HasPrefix(scenario, "clock_") {
		runFixedBenchmarkClockChild(t, scenario)
		return
	}
	if strings.HasPrefix(scenario, "extra_") || strings.HasPrefix(scenario, "wrong_callback_") || scenario == "wrong_result_n" || strings.HasPrefix(scenario, "missing_callback") {
		runFixedBenchmarkStateChild(t, scenario)
		return
	}
	work := func(b *testing.B, ctx context.Context, cancel context.CancelFunc, clock *Clock) bool {
		fmt.Println("fixed-benchmark-work-entered:" + scenario)
		return fixedBenchmarkReaderFault(ctx, b, cancel, clock, scenario)
	}
	if scenario == "expired_deadline" || scenario == "canceled_outer" || strings.HasPrefix(scenario, "dispatch_") {
		runFixedBenchmarkAuthorityChild(t, scenario, work)
	} else {
		RunFixedBenchmark(t, work)
	}
	fmt.Println("fixed-benchmark-valid-receipt:" + scenario)
}

func runFixedBenchmarkAuthorityChild(t *testing.T, scenario string, work func(*testing.B, context.Context, context.CancelFunc, *Clock) bool) {
	deadline, ok := t.Deadline()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	authority := fixedBenchmarkAuthority{ctx: ctx, deadline: deadline, hasDeadline: ok}
	runner := testing.Benchmark
	switch scenario {
	case "expired_deadline":
		authority.deadline = time.Now().Add(-time.Second)
	case "canceled_outer":
		cancel()
	case "dispatch_canceled":
		runner = func(callback func(*testing.B)) testing.BenchmarkResult {
			fmt.Println("fixed-benchmark-entry-validated:" + scenario)
			cancel()
			return testing.Benchmark(callback)
		}
	case "dispatch_expired":
		// The runner is reached only after real entry validation. Waiting for
		// this finite injected deadline makes actual dispatch stale, without
		// risking the child's ten-second framework watchdog.
		authority.deadline = time.Now().Add(time.Second)
		deadlineCtx, deadlineCancel := context.WithDeadline(ctx, authority.deadline)
		defer deadlineCancel()
		authority.ctx = deadlineCtx
		runner = func(callback func(*testing.B)) testing.BenchmarkResult {
			fmt.Println("fixed-benchmark-entry-validated:" + scenario)
			<-deadlineCtx.Done()
			return testing.Benchmark(callback)
		}
	}
	runFixedBenchmark(t, authority, work, runner)
}

func runFixedBenchmarkStateChild(t *testing.T, scenario string) {
	// Obtain a genuine completed benchmark, then alter only the otherwise
	// unreachable receipt state. No zero B or global flag mutation is involved.
	var benchmark *testing.B
	result := testing.Benchmark(func(b *testing.B) { benchmark = b })
	if benchmark == nil || benchmark.N != 1 || result.N != 1 || benchmark.Failed() || benchmark.Skipped() {
		t.Fatal("state-fault setup did not complete a genuine one-iteration benchmark")
	}
	state := fixedBenchmarkState{callbacks: 1, iterations: 1, completed: true, benchmark: benchmark}
	switch scenario {
	case "extra_callback_count":
		state.callbacks = 2
	case "missing_callback_count":
		state.callbacks = 0
	case "missing_callback":
		state.benchmark = nil
	case "wrong_callback_n":
		state.iterations = 2
	case "wrong_result_n":
		result.N = 2
	}
	if err := state.failure(result); err != nil {
		t.Fatal(err)
	}
	t.Fatal("state-fault checker accepted injected invalid state")
}
