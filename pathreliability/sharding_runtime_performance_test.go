package pathreliability

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"dxcluster/internal/testsupport"
)

var pathShardRoleNames = [4]string{"normal_writer", "path_only_writer", "ordinary_reader", "beacon_reader"}

type pathShardRoleResult struct {
	Role       string `json:"role"`
	Operations int    `json:"operations"`
	Samples    int    `json:"samples"`
	StartNS    int64  `json:"start_ns"`
	EndNS      int64  `json:"end_ns"`
	ElapsedNS  int64  `json:"elapsed_ns"`
	P999NS     int64  `json:"p999_ns"`
	MaximumNS  int64  `json:"maximum_ns"`
	ZeroNS     int    `json:"zero_ns_samples"`
	Checksum   uint64 `json:"checksum"`
	latencies  []int64
	err        error
}

type pathShardRunResult struct {
	TestName                string                     `json:"test"`
	MeasurementProtocol     string                     `json:"measurement_protocol"`
	Scored                  bool                       `json:"scored"`
	Scenario                string                     `json:"scenario"`
	Mode                    string                     `json:"mode"`
	InputHash               string                     `json:"input_hash"`
	ConfigHash              string                     `json:"config_hash"`
	H3Hashes                [2]string                  `json:"h3_hashes_fine_coarse"`
	GOOS                    string                     `json:"goos"`
	GOARCH                  string                     `json:"goarch"`
	GOGC                    string                     `json:"gogc"`
	GoVersion               string                     `json:"go_version"`
	GOMAXPROCS              int                        `json:"gomaxprocs"`
	ClockSource             string                     `json:"clock_source"`
	ClockFrequencyHz        int64                      `json:"clock_frequency_hz"`
	Operations              int                        `json:"operations_per_role"`
	ElapsedNS               int64                      `json:"elapsed_ns"`
	OverlapNS               int64                      `json:"all_role_overlap_ns"`
	Profiled                bool                       `json:"profiled"`
	TimedMaintenanceOracles bool                       `json:"timed_maintenance_oracles"`
	AllocatedBytes          uint64                     `json:"workload_allocated_bytes"`
	AllocatedObjects        uint64                     `json:"workload_allocated_objects"`
	HotBuckets              int                        `json:"hot_buckets"`
	GrownBuckets            int                        `json:"grown_buckets"`
	SeedReports             int                        `json:"seed_reports"`
	RawMassBefore           float64                    `json:"raw_mass_before"`
	RawMassAfter            float64                    `json:"raw_mass_after"`
	CapMassBefore           float64                    `json:"capped_mass_before"`
	CapMassAfter            float64                    `json:"capped_mass_after"`
	CapUnitsBefore          float64                    `json:"capped_units_before"`
	CapUnitsAfter           float64                    `json:"capped_units_after"`
	RawCountBefore          uint64                     `json:"lifetime_raw_count_before"`
	RawCountAfter           uint64                     `json:"lifetime_raw_count_after"`
	Roles                   [4]pathShardRoleResult     `json:"roles"`
	Maintenance             pathShardMaintenanceResult `json:"maintenance"`
}

func TestPathShardRuntimePreflight(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		for _, scenario := range []string{"Serial", "Contended", "ContendedMaintenance"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				result := runPathShardWorkload(t, mode, scenario, 16384, "")
				t.Logf("fixture=%s hot=%d grown=%d overlap=%d", result.InputHash, result.HotBuckets, result.GrownBuckets, result.OverlapNS)
			})
		}
	}
}

func TestPathShardRepairedPreflight(t *testing.T) {
	if os.Getenv("GOCLUSTER_SHARD_REPAIRED_PREFLIGHT") != "1" {
		t.Skip("opt-in unscored shared-benchmark integration preflight")
	}
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		for _, scenario := range []string{"Serial", "Contended", "ContendedMaintenance"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				result := runFixedPathShardWorkload(t, mode, scenario, 262144, "")
				if result.Scored || result.MeasurementProtocol != testsupport.FixedBenchmarkProtocol || result.ClockSource != "testing.B.Elapsed(shared live benchmark)" {
					t.Fatalf("repaired preflight lost its unscored benchmark identity: %+v", result)
				}
				t.Logf("unscored fixture=%s operations=%d overlap=%d protocol=%s", result.InputHash, result.Operations, result.OverlapNS, result.MeasurementProtocol)
			})
		}
	}
}

func checkPathShardSteadyAllocations(t *testing.T, mode string, injectAllocation bool) {
	t.Helper()
	f := newPathShardFixture(t, mode, false)
	const batchOperations = 1000
	const totalOperations = 2 * batchOperations
	var beforeMeasured map[uint64]pathShardMassState
	iteration := 0
	batches := 0
	updateFailed := false
	// AllocsPerRun truncates its average. One measured batch makes
	// its result an exact allocation count, after one full warm batch.
	updates := testing.AllocsPerRun(1, func() {
		for range batchOperations {
			if !f.update(0, iteration, totalOperations) {
				updateFailed = true
			}
			iteration++
		}
		batches++
		if batches == 1 {
			// The warm reports must not fund the measured-work admission
			// witness. Snapshot allocation finishes before measurement.
			beforeMeasured = f.captureMassStates()
		}
	})
	if updateFailed || updates != 0 || batches != 2 || iteration != totalOperations {
		t.Fatalf("warmed update batch allocations=%g admission failure=%t batches=%d operations=%d", updates, updateFailed, batches, iteration)
	}
	for operation := 0; operation < iteration; operation++ {
		for _, key := range f.reportFor(0, operation).keys() {
			f.wantCounts[key]++
		}
	}
	f.assertState(t, f.hotKeys)
	added, valid := pathShardNewMass(beforeMeasured, f.captureMassStates())
	if !valid || !pathShardHasNewMass(added, batchOperations, mode) {
		t.Fatalf("measured allocation batch added raw signal=%g capped signal=%g admitted units=%g for %d actual updates; valid baseline=%t", added.raw, added.capped, added.count, batchOperations, valid)
	}
	for _, role := range []int{2, 3} {
		iteration := 0
		failed := false
		allocations := testing.AllocsPerRun(1, func() {
			for range batchOperations {
				if injectAllocation && role == 3 && iteration == batchOperations {
					// This package-owned sink forces one heap escape in the
					// measured batch; every real read still completes.
					pathShardAllocationWitness = make([]byte, 64)
				}
				if !pathShardUsable(f.read(role, iteration), role == 3) {
					failed = true
				}
				iteration++
			}
		})
		if failed || allocations != 0 || iteration != totalOperations {
			t.Fatalf("%s warmed batch allocations=%g valid observed prediction=%t operations=%d", pathShardRoleNames[role], allocations, !failed, iteration)
		}
	}
	f.assertPredictions(t)
}

func TestPathShardAdmissionOracleRejectsWarmSeed(t *testing.T) {
	seed := pathShardMassState{raw: 10000, capped: 10000, count: 10000, reference: 100, halfLife: 360}
	for _, test := range []struct {
		name  string
		final pathShardMassState
		want  bool
	}{
		{"zero_new_work", seed, false},
		// One quarter of a half-life leaves this literal residual. It would
		// fund the former 0.7*seed + 5% witness despite admitting no new work.
		{"decay_only", pathShardMassState{8408.964152537145, 8408.964152537145, 8408.964152537145, 190, 360}, false},
		{"raw_only", pathShardMassState{10250, 10000, 10000, 100, 360}, false},
		{"signal_without_units", pathShardMassState{10250, 10250, 10000, 100, 360}, false},
		{"units_without_signal", pathShardMassState{10250, 10000, 10250, 100, 360}, false},
		{"accepted_signal_and_units", pathShardMassState{10250, 10250, 10250, 100, 360}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			added, valid := pathShardNewMass(map[uint64]pathShardMassState{1: seed}, map[uint64]pathShardMassState{1: test.final})
			if got := valid && pathShardHasNewMass(added, 1001, ReceiverContributionEnforce); got != test.want {
				t.Fatalf("causal admission oracle=%t, want=%t; added raw signal=%g capped signal=%g admitted report units=%g", got, test.want, added.raw, added.capped, added.count)
			}
		})
	}
}

func TestPathShardPerformance(t *testing.T) {
	output := os.Getenv("GOCLUSTER_SHARD_PERF_OUTPUT")
	if output == "" {
		t.Skip("opt-in fixed-work performance campaign; preflight and allocation checks remain unconditional")
	}
	mode, scenario := os.Getenv("GOCLUSTER_SHARD_PERF_MODE"), os.Getenv("GOCLUSTER_SHARD_PERF_SCENARIO")
	if mode != ReceiverContributionOff && mode != ReceiverContributionShadow && mode != ReceiverContributionEnforce {
		t.Fatalf("explicit GOCLUSTER_SHARD_PERF_MODE required, got %q", mode)
	}
	if scenario != "Serial" && scenario != "Contended" && scenario != "ContendedMaintenance" {
		t.Fatalf("explicit GOCLUSTER_SHARD_PERF_SCENARIO required, got %q", scenario)
	}
	result := runFixedPathShardWorkload(t, mode, scenario, pathShardCampaignOperations, os.Getenv("GOCLUSTER_SHARD_PROFILE_DIR"))
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, append(encoded, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// Only the outer test may serialize the result, after the fixed benchmark bridge
// has checked callback completion and late cleanup failures.
func runFixedPathShardWorkload(t *testing.T, mode, scenario string, operations int, profileDir string) pathShardRunResult {
	t.Helper()
	name := t.Name()
	var result pathShardRunResult
	testsupport.RunFixedBenchmark(t, func(b *testing.B, ctx context.Context, cancel context.CancelFunc, clock *testsupport.Clock) bool {
		result = runPathShardWorkloadWithClock(ctx, b, cancel, clock, mode, scenario, operations, profileDir)
		return true
	})
	result.TestName = name
	return result
}

func runPathShardWorkload(t *testing.T, mode, scenario string, operations int, profileDir string) pathShardRunResult {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	return runPathShardWorkloadWithClock(ctx, t, cancel, nil, mode, scenario, operations, profileDir)
}

func runPathShardWorkloadWithClock(ctx context.Context, t testing.TB, cancel context.CancelFunc, shared *testsupport.Clock, mode, scenario string, operations int, profileDir string) pathShardRunResult {
	t.Helper()
	defer cancel()
	// Materialize the cancellation channel before timed work. Each owner caches
	// it once; cancellation checks perform no per-operation allocation or lock.
	_ = ctx.Done()
	f := newPathShardFixture(t, mode, scenario == "ContendedMaintenance")
	beforeRaw, beforeCap, beforeCount := f.signalTotals()
	result := pathShardRunResult{Scenario: scenario, Mode: mode, InputHash: f.inputHash, ConfigHash: f.configHash, GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GOGC: os.Getenv("GOGC"),
		H3Hashes: f.h3Hashes, RawCountBefore: f.lifetimeRawCount(),
		GoVersion: runtime.Version(), GOMAXPROCS: runtime.GOMAXPROCS(0), Operations: operations, Profiled: profileDir != "",
		HotBuckets: len(f.hotKeys), GrownBuckets: len(f.allKeys), SeedReports: f.seedReports,
		RawMassBefore: beforeRaw, CapMassBefore: beforeCap, CapUnitsBefore: beforeCount}
	if shared != nil {
		result.MeasurementProtocol = testsupport.FixedBenchmarkProtocol
		result.Scored = operations == pathShardCampaignOperations && profileDir == ""
	}
	for role := range result.Roles {
		result.Roles[role] = pathShardRoleResult{Role: pathShardRoleNames[role], latencies: make([]int64, operations)}
		for i := range result.Roles[role].latencies {
			result.Roles[role].latencies[i] = -1
		}
	}
	clock := shared
	if clock == nil {
		clock = testsupport.NewClock(t)
	}
	var roleClocks [4]*testsupport.Clock
	for role := range roleClocks {
		roleClocks[role] = shared
		if shared == nil {
			roleClocks[role] = testsupport.NewClock(t)
		}
	}
	maintenanceClock := shared
	if maintenanceClock == nil {
		maintenanceClock = testsupport.NewClock(t)
	}
	result.ClockSource, result.ClockFrequencyHz = clock.Source(), clock.FrequencyHz()
	if f.maintenance {
		f.prepareMaintenance(t)
	}
	execution := f.prepareExecution(ctx, cancel, scenario, operations, roleClocks, maintenanceClock, &result)
	stopProfiles := func() {}
	defer func() {
		// Fatal/Skip/panic unwinding must retire every live clock reader before
		// profiling cleanup or the benchmark framework can end its timer domain.
		execution.stop()
		stopProfiles()
	}()
	if shared != nil {
		runtime.GC()
	}
	stopProfiles = startPathShardProfiles(t, profileDir)
	var memoryBefore, memoryAfter runtime.MemStats
	runtime.ReadMemStats(&memoryBefore)
	epoch := clock.NowNS()
	execution.run(epoch)
	result.ElapsedNS = clock.NowNS() - epoch
	runtime.ReadMemStats(&memoryAfter)
	result.AllocatedBytes = memoryAfter.TotalAlloc - memoryBefore.TotalAlloc
	result.AllocatedObjects = memoryAfter.Mallocs - memoryBefore.Mallocs
	stopProfiles()
	if workErr := execution.err(); workErr != nil {
		t.Fatal(workErr)
	}
	for role := range result.Roles {
		entry := &result.Roles[role]
		if entry.err != nil {
			t.Fatal(entry.err)
		}
		if entry.Operations != operations || len(entry.latencies) != operations {
			t.Fatalf("%s completed=%d recorded=%d expected=%d", entry.Role, entry.Operations, len(entry.latencies), operations)
		}
		for index, elapsed := range entry.latencies {
			if elapsed < 0 {
				t.Fatalf("%s missing/negative sample %d: %d ns", entry.Role, index, elapsed)
			}
			if elapsed == 0 {
				entry.ZeroNS++
			}
		}
		slices.Sort(entry.latencies)
		entry.Samples = len(entry.latencies)
		entry.P999NS = entry.latencies[(999*operations+999)/1000-1]
		entry.MaximumNS = entry.latencies[operations-1]
		entry.latencies = nil
	}
	if scenario != "Serial" {
		latestStart, earliestEnd := int64(0), int64(1<<63-1)
		for _, entry := range result.Roles {
			latestStart, earliestEnd = max(latestStart, entry.StartNS), min(earliestEnd, entry.EndNS)
		}
		result.OverlapNS = earliestEnd - latestStart
		if result.OverlapNS <= 0 {
			t.Fatalf("four roles did not overlap: %+v", result.Roles)
		}
	}
	f.accountWork(operations)
	want := f.allKeys
	if f.maintenance {
		want = f.hotKeys
		f.verifyMaintenance(t, result.Roles)
		result.Maintenance = f.maintenanceRun
	}
	f.assertState(t, want)
	f.assertPredictions(t)
	f.assertActiveAdmission(t, operations, beforeRaw, beforeCap, beforeCount)
	result.RawMassAfter, result.CapMassAfter, result.CapUnitsAfter = f.signalTotals()
	result.RawCountAfter = f.lifetimeRawCount()
	return result
}

func (f *pathShardFixture) runRole(ctx context.Context, cancel context.CancelFunc, role, operations int, epoch int64, clock *testsupport.Clock, result pathShardRoleResult, active chan<- struct{}) pathShardRoleResult {
	done := ctx.Done()
	start := clock.NowNS()
	result.StartNS = start - epoch
	for iteration := 0; iteration < operations; iteration++ {
		select {
		case <-done:
			result.err = ctx.Err()
			result.EndNS = clock.NowNS() - epoch
			result.ElapsedNS = result.EndNS - result.StartNS
			return result
		default:
		}
		operationStart := clock.NowNS()
		if role < 2 {
			if !f.update(role, iteration, operations) {
				result.err = pathShardError("%s rejected input at operation %d", pathShardRoleNames[role], iteration)
			}
		} else {
			prediction := f.read(role, iteration)
			if !pathShardUsable(prediction, role == 3) {
				result.err = pathShardError("%s operation %d lost observed -10.5 dB HIGH: %+v", pathShardRoleNames[role], iteration, prediction)
			}
			result.Checksum += uint64(prediction.ObservationCount) + uint64(prediction.ReceiverCount) + 1
		}
		result.latencies[iteration] = clock.NowNS() - operationStart
		result.Operations++
		if result.err != nil {
			cancel()
			break
		}
		if iteration == 0 && active != nil {
			select {
			case active <- struct{}{}:
			case <-done:
			}
		}
	}
	result.EndNS = clock.NowNS() - epoch
	result.ElapsedNS = result.EndNS - result.StartNS
	return result
}
