package pathreliability

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"dxcluster/internal/testsupport"
)

var pathShardAllocationWitness []byte

// Each required mode gets a fresh copy of this same saved package executable.
// AllocsPerRun counts the entire process: prior test owners must not fund or
// contaminate the exact warmed-operation witness. The zero limit is unchanged.
func TestPathShardSteadyAllocations(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		t.Run(mode, func(t *testing.T) {
			output, err := runPathShardAllocationChild(t, mode, false)
			marker := "path-shard-allocation-accepted mode=" + mode + " operations=2000"
			if err != nil || strings.Count(output, marker) != 1 || !strings.Contains(output, "--- PASS: TestPathShardSteadyAllocationChild") || strings.Contains(output, "SKIP") {
				t.Fatalf("exact warmed allocation child did not complete: %v\n%s", err, output)
			}
		})
	}
}

func TestPathShardSteadyAllocationWitness(t *testing.T) {
	output, err := runPathShardAllocationChild(t, ReceiverContributionOff, true)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 ||
		!strings.Contains(output, "beacon_reader warmed batch allocations=1 valid observed prediction=true operations=2000") ||
		!strings.Contains(output, "--- FAIL: TestPathShardSteadyAllocationChild") ||
		strings.Contains(output, "path-shard-allocation-accepted") || strings.Contains(output, "SKIP") ||
		strings.Contains(output, "panic:") || strings.Contains(output, "test timed out") {
		t.Fatalf("one escaping measured allocation did not reach the actual zero-allocation rejection: %v\n%s", err, output)
	}
}

func runPathShardAllocationChild(t *testing.T, mode string, injectAllocation bool) (string, error) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestPathShardSteadyAllocationChild$", "-test.count=1", "-test.timeout=20s", "-test.v")
	for _, setting := range os.Environ() {
		if !strings.HasPrefix(setting, "GOCLUSTER_SHARD_ALLOCATION_MODE=") && !strings.HasPrefix(setting, "GOCLUSTER_SHARD_ALLOCATION_FAULT=") {
			command.Env = append(command.Env, setting)
		}
	}
	command.Env = append(command.Env, "GOCLUSTER_SHARD_ALLOCATION_MODE="+mode, fmt.Sprintf("GOCLUSTER_SHARD_ALLOCATION_FAULT=%t", injectAllocation))
	command.WaitDelay = 2 * time.Second
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("allocation child exceeded its bounded lifetime: %v\n%s", ctx.Err(), output)
	}
	return string(output), err
}

func TestPathShardSteadyAllocationChild(t *testing.T) {
	mode := os.Getenv("GOCLUSTER_SHARD_ALLOCATION_MODE")
	if mode == "" {
		return
	}
	if mode != ReceiverContributionOff && mode != ReceiverContributionShadow && mode != ReceiverContributionEnforce {
		t.Fatalf("unknown allocation child mode %q", mode)
	}
	fault := os.Getenv("GOCLUSTER_SHARD_ALLOCATION_FAULT")
	if fault != "false" && fault != "true" {
		t.Fatalf("unknown allocation child fault %q", fault)
	}
	checkPathShardSteadyAllocations(t, mode, fault == "true")
	if !t.Failed() {
		fmt.Printf("path-shard-allocation-accepted mode=%s operations=2000\n", mode)
	}
}

func startPathShardProfiles(t testing.TB, dir string) func() {
	t.Helper()
	if dir == "" {
		return func() {}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, profile string) {
		file, err := os.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := pprof.Lookup(profile).WriteTo(file, 0); err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	runtime.GC()
	write("heap-before.pprof", "heap")
	write("allocs-before.pprof", "allocs")
	cpuFile, err := os.Create(filepath.Join(dir, "cpu.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	previousMutexFraction := runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		_ = cpuFile.Close()
		runtime.SetMutexProfileFraction(previousMutexFraction)
		runtime.SetBlockProfileRate(0)
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		pprof.StopCPUProfile()
		runtime.SetMutexProfileFraction(previousMutexFraction)
		runtime.SetBlockProfileRate(0)
		if err := cpuFile.Close(); err != nil {
			t.Fatal(err)
		}
		write("mutex.pprof", "mutex")
		write("block.pprof", "block")
		runtime.GC()
		runtime.GC()
		write("heap-after.pprof", "heap")
		write("allocs-after.pprof", "allocs")
	}
	t.Cleanup(stop)
	return stop
}

// Re-execute the existing stable package binary so intentional benchmark failure
// must cross the real outer-test bridge. A timeout, crash, or late valid receipt
// cannot stand in for the expected joined-owner rejection.
func TestPathShardRepairedLifecycle(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, fault := range []string{"cancellation", "worker_error"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			receipt := filepath.Join(t.TempDir(), "invalid-receipt.json")
			command := exec.CommandContext(ctx, executable, "-test.run=^TestPathShardRepairedLifecycleChild$", "-test.benchtime=1x", "-test.count=1", "-test.timeout=20s", "-test.v")
			command.Env = append(os.Environ(), "GOCLUSTER_SHARD_LIFECYCLE_FAULT="+fault, "GOCLUSTER_SHARD_LIFECYCLE_RECEIPT="+receipt)
			command.WaitDelay = 2 * time.Second
			output, runErr := command.CombinedOutput()
			var exit *exec.ExitError
			if ctx.Err() != nil || !errors.As(runErr, &exit) || exit.ExitCode() != 1 {
				t.Fatalf("lifecycle child did not make the expected ordinary failure: err=%v timeout=%v\n%s", runErr, ctx.Err(), output)
			}
			marker := "path-shard-lifecycle " + fault + ": all four roles and maintenance retired; rejecting actual work failure"
			if !strings.Contains(string(output), marker) || strings.Contains(string(output), "panic:") || strings.Contains(string(output), "test timed out") {
				t.Fatalf("missing exact real-owner rejection or unexpected crash:\n%s", output)
			}
			if _, err := os.Stat(receipt); !os.IsNotExist(err) {
				t.Fatalf("failed benchmark produced a receipt or could not certify its absence: %v", err)
			}
		})
	}
}

func TestPathShardRepairedLifecycleChild(t *testing.T) {
	fault := os.Getenv("GOCLUSTER_SHARD_LIFECYCLE_FAULT")
	if fault == "" {
		return
	}
	if fault != "cancellation" && fault != "worker_error" {
		t.Fatalf("unknown lifecycle child fault %q", fault)
	}
	testsupport.RunFixedBenchmark(t, func(b *testing.B, ctx context.Context, cancel context.CancelFunc, clock *testsupport.Clock) bool {
		f := newPathShardFixture(b, ReceiverContributionOff, true)
		f.prepareMaintenance(b)
		var result pathShardRunResult
		var clocks [4]*testsupport.Clock
		for role := range result.Roles {
			result.Roles[role] = pathShardRoleResult{Role: pathShardRoleNames[role], latencies: make([]int64, pathShardCampaignOperations)}
			clocks[role] = clock
		}
		if fault == "worker_error" {
			// A missing predictor is an isolated fixture fault. The real public
			// update/read guards reject it; no campaign input or policy changes.
			f.predictor = nil
		}
		execution := f.prepareExecution(ctx, cancel, "ContendedMaintenance", pathShardCampaignOperations, clocks, clock, &result)
		defer execution.stop()
		if fault == "cancellation" {
			observerDone := make(chan struct{})
			go func() {
				defer close(observerDone)
				select {
				case <-execution.allActive:
					cancel()
				case <-ctx.Done():
				}
			}()
			defer func() { cancel(); <-observerDone }()
		}
		execution.run(clock.NowNS())
		err := execution.err()
		for role, retired := range execution.rolesRetired {
			if !retired || fault == "cancellation" && result.Roles[role].Operations == 0 {
				b.Fatalf("real owner %d did not complete active-work retirement: %+v", role, result.Roles[role])
			}
		}
		if !execution.maintenanceRetired || err == nil || fault == "cancellation" && !errors.Is(err, context.Canceled) || fault == "worker_error" && errors.Is(err, context.Canceled) {
			b.Fatalf("wrong real execution outcome: maintenance-retired=%t err=%v", execution.maintenanceRetired, err)
		}
		if fault == "worker_error" && !strings.Contains(err.Error(), "rejected input") && !strings.Contains(err.Error(), "lost observed") {
			b.Fatalf("worker-error witness lost its actual admission/prediction failure: %v", err)
		}
		fmt.Printf("path-shard-lifecycle %s: all four roles and maintenance retired; rejecting actual work failure\n", fault)
		b.Fatalf("actual core execution failure: %v", err)
		return false
	})
	// If the bridge ever accepts a failed callback, leave a receipt that makes
	// the parent fail even though this child's final Fatal also exits nonzero.
	if err := os.WriteFile(os.Getenv("GOCLUSTER_SHARD_LIFECYCLE_RECEIPT"), []byte("invalid accepted result\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Fatal("fixed benchmark bridge accepted actual core failure")
}
