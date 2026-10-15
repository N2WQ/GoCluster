// File role: Candidate-only, opt-in S4 cadence/lifecycle witness. The production
// minute maintenance and five-minute logger run without fake clocks or shorter
// intervals. Fixed input cohorts, counters and rings bound retained test state.
package cluster

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/internal/testsupport"
)

const pathShardingSoakDuration = 66 * time.Minute

type pathShardingSoakCompaction struct {
	AtUTC  time.Time `json:"at_utc"`
	AtNS   int64     `json:"at_ns"`
	Shards int       `json:"shards"`
}

type pathShardingSoakReport struct {
	EmittedUTC time.Time `json:"emitted_utc"`
	Available  bool      `json:"available"`
	AsOfUnix   int64     `json:"asof_unix"`
}

// At most eight compactions and sixteen bucket emissions can occur in this
// fixed 66-minute witness. Overflow fails rather than silently dropping proof.
type pathShardingSoakSink struct {
	mu              sync.Mutex
	file            *os.File
	clock           *testsupport.Clock
	compactions     [8]pathShardingSoakCompaction
	compactionCount int
	reports         [16]pathShardingSoakReport
	reportCount     int
	sources         [2]uint64
	problem         string
}

func (s *pathShardingSoakSink) failLocked(err error) {
	if s.problem == "" {
		s.problem = err.Error()
	}
}

func (s *pathShardingSoakSink) Write(value []byte) (int, error) {
	text := string(value)
	position := strings.Index(text, "Path predictor compacted ")
	if position < 0 {
		return len(value), nil
	}
	var shards int
	if _, err := fmt.Sscanf(text[position:], "Path predictor compacted %d shards", &shards); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.compactionCount >= len(s.compactions) {
		s.failLocked(fmt.Errorf("compaction event ring overflow"))
		return len(value), nil
	}
	event := pathShardingSoakCompaction{AtUTC: time.Now().UTC(), AtNS: s.clock.NowNS(), Shards: shards}
	s.compactions[s.compactionCount] = event
	s.compactionCount++
	if _, err := fmt.Fprintf(s.file, "%s Path predictor compacted %d shards\n", event.AtUTC.Format(time.RFC3339Nano), shards); err != nil {
		s.failLocked(err)
	}
	return len(value), nil
}

func (s *pathShardingSoakSink) WriteLine(line string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(line) > 8192 {
		s.failLocked(fmt.Errorf("unexpected unbounded propagation line"))
		return
	}
	if _, err := fmt.Fprintf(s.file, "%s %s\n", at.UTC().Format(time.RFC3339Nano), line); err != nil {
		s.failLocked(err)
	}
	if strings.HasPrefix(line, "Path buckets (5m):") {
		if s.reportCount >= len(s.reports) {
			s.failLocked(fmt.Errorf("dated report ring overflow"))
			return
		}
		report := pathShardingSoakReport{EmittedUTC: at.UTC()}
		for _, field := range strings.Fields(line) {
			if field == "available=true" {
				report.Available = true
			}
			if strings.HasPrefix(field, "asof_unix=") {
				value, err := strconv.ParseInt(strings.TrimPrefix(field, "asof_unix="), 10, 64)
				if err != nil {
					s.failLocked(err)
				} else {
					report.AsOfUnix = value
				}
			}
		}
		s.reports[s.reportCount] = report
		s.reportCount++
	}
	if strings.HasPrefix(line, "Path source mix (5m):") {
		for _, field := range strings.Fields(line) {
			for role, label := range []string{"RBN=", "PSK="} {
				if strings.HasPrefix(field, label) {
					value, err := strconv.ParseUint(strings.ReplaceAll(strings.TrimPrefix(field, label), ",", ""), 10, 64)
					if err != nil {
						s.failLocked(err)
					} else {
						s.sources[role] += value
					}
				}
			}
		}
	}
}

func (s *pathShardingSoakSink) Close() error { return s.file.Close() }

func (s *pathShardingSoakSink) snapshot() ([]pathShardingSoakCompaction, []pathShardingSoakReport, [2]uint64, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pathShardingSoakCompaction(nil), s.compactions[:s.compactionCount]...), append([]pathShardingSoakReport(nil), s.reports[:s.reportCount]...), s.sources, s.problem
}

func TestPathShardingHourlySoak(t *testing.T) {
	if os.Getenv("GOCLUSTER_PATH_SHARDING_SOAK") != "1" {
		t.Skip("opt-in fixed 66-minute real-cadence S4 soak")
	}
	pathShardingRequireExecutable(t)
	pathShardingCheckDeadline(t, pathShardingSoakDuration+time.Minute)
	fixture := newPathShardingConnectedFixture(t, "enforce")
	fixture.channelWitness(t)
	cohort := fixture.coldCohort(t)
	growth := fixture.seedCold(t, cohort)
	growthHeap := pathShardingSoakHeap(fixture)
	dir := pathShardingArtifactDir(t, "hourly-soak-")
	file, err := os.Create(filepath.Join(dir, "propagation-and-maintenance.log"))
	if err != nil {
		t.Fatal(err)
	}
	sink := &pathShardingSoakSink{file: file, clock: testsupport.NewClock(t)}
	originalLog := log.Writer()
	log.SetOutput(io.MultiWriter(originalLog, sink))
	ctx, cancel := context.WithCancel(t.Context())
	loggerDone := make(chan struct{})
	stackBuffer := make([]byte, 1<<20)
	before := pathShardingMaintenanceIDs(t, stackBuffer)
	var maintenanceID uint64
	r := &clusterRuntime{ctx: ctx, pathCfg: fixture.cfg, pathPredictor: fixture.predictor}
	started := time.Now()
	r.startPathPredictorMaintenance()
	go func() {
		defer close(loggerDone)
		startPathPredictionLogger(ctx, sink, nil, fixture.predictor, fixture.metrics)
	}()
	t.Cleanup(func() {
		defer func() {
			log.SetOutput(originalLog)
			if err := sink.Close(); err != nil {
				t.Error(err)
			}
		}()
		cancel()
		select {
		case <-loggerDone:
		case <-time.After(5 * time.Second):
			t.Error("real prediction logger failed to join")
		}
		if maintenanceID != 0 {
			pathShardingAwaitMaintenanceExit(t, stackBuffer, maintenanceID)
		}
	})
	maintenanceID = pathShardingAwaitMaintenanceStart(t, stackBuffer, before)
	base := [2]uint64{fixture.completed[0], fixture.completed[1]}
	state := pathShardingRunSoak(ctx, t, fixture, sink, cohort, started, growthHeap)
	cancel()
	select {
	case <-loggerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("real prediction logger failed to join after cancellation")
	}
	pathShardingAwaitMaintenanceExit(t, stackBuffer, maintenanceID)
	fixture.guardPopulation(t, state.FinalBuckets, false)
	events, reports, sources, problem := sink.snapshot()
	if problem != "" {
		t.Fatal(problem)
	}
	remaining := fixture.metrics.SnapshotSources()
	for role, label := range []string{"RBN", "PSK"} {
		if got, want := sources[role]+remaining[label], fixture.completed[role]-base[role]; got != want {
			t.Fatalf("actual 5m source accounting %s=%d want %d", label, got, want)
		}
	}
	if len(events) != 2 || events[0].Shards <= 0 || events[1].Shards <= 0 {
		t.Fatalf("need first-minute and later real rebuilds: %+v", events)
	}
	if events[0].AtUTC.Sub(started.UTC()) < time.Minute-time.Second || events[1].AtUTC.Sub(events[0].AtUTC) < time.Hour-time.Second {
		t.Fatalf("maintenance cadence was shortened: %+v", events)
	}
	for role, successes := range state.PostSecondSuccess {
		if successes.Completed == 0 || successes.FirstNS < events[1].AtNS || successes.LastNS-successes.FirstNS < int64(3*time.Minute) {
			t.Fatalf("%s lacks a three-minute span of successful operations after later compaction: %+v event=%+v", pathShardingRoleNames[role], successes, events[1])
		}
	}
	if len(reports) < 13 {
		t.Fatalf("real 5-minute logger emitted %d bucket reports; want at least 13", len(reports))
	}
	available := 0
	for i, report := range reports {
		if report.EmittedUTC.IsZero() || report.AsOfUnix <= 0 || (i > 0 && !report.EmittedUTC.After(reports[i-1].EmittedUTC)) {
			t.Fatalf("undated/nonadvancing actual propagation report: %+v", report)
		}
		if report.Available {
			available++
		}
	}
	if available == 0 {
		t.Fatal("all actual bucket publications were unavailable")
	}
	pathShardingWriteJSON(t, filepath.Join(dir, "manifest.json"), struct {
		Boundary             string                       `json:"evidence_boundary"`
		StartedUTC           time.Time                    `json:"started_utc"`
		FinishedUTC          time.Time                    `json:"finished_utc"`
		DurationSeconds      int                          `json:"fixed_duration_seconds"`
		ConfigSHA256         string                       `json:"config_sha256"`
		GrowthBuckets        int                          `json:"growth_buckets"`
		State                pathShardingSoakState        `json:"state"`
		Compactions          []pathShardingSoakCompaction `json:"actual_compactions"`
		Reports              []pathShardingSoakReport     `json:"actual_reports"`
		MaintenanceGoroutine uint64                       `json:"joined_maintenance_goroutine"`
	}{"Candidate-only real-cadence stability/lifecycle evidence: actual normal/path-only item consumers and queued public Predictor readers. Separate telnet-package wire witness. No comparative hourly performance/memory or full-chain wire-latency claim.", started.UTC(), time.Now().UTC(), int(pathShardingSoakDuration / time.Second), pathShardingFileHash(t, filepath.Join(pathShardingRepoRoot(), "data", "config", "path_reliability.yaml")), growth, state, events, reports, maintenanceID})
	t.Logf("66-minute S4 real-cadence soak complete: artifacts=%s updates=%d/%d rebuilds=%+v reports=%d", dir, fixture.completed[0]-base[0], fixture.completed[1]-base[1], events, len(reports))
}

// Only bounded first/last success times and exact counts are retained. A batch
// that straddles the actual event is excluded from this post-event witness;
// its complete latency population remains present in the batch result.
type pathShardingSoakSuccess struct {
	FirstUTC  time.Time `json:"first_success_utc"`
	LastUTC   time.Time `json:"last_success_utc"`
	FirstNS   int64     `json:"first_success_ns"`
	LastNS    int64     `json:"last_success_ns"`
	Completed uint64    `json:"completed"`
}

type pathShardingSoakState struct {
	Iterations            int                        `json:"iterations"`
	RegrownUTC            time.Time                  `json:"regrown_utc"`
	RegrownBuckets        int                        `json:"regrown_buckets"`
	ColdPurgedUTC         [2]time.Time               `json:"cold_purged_utc"`
	RefilledUTC           time.Time                  `json:"post_compaction_refill_utc"`
	FinalBuckets          int                        `json:"final_buckets"`
	Completed             [4]uint64                  `json:"completed"`
	MaximumLatencyNS      [4]int64                   `json:"maximum_observed_completion_ns"`
	PostSecondSuccess     [4]pathShardingSoakSuccess `json:"successful_work_after_second_compaction"`
	InitialGrowthHeap     uint64                     `json:"initial_growth_heap_bytes"`
	FirstCompactedHeap    uint64                     `json:"first_compacted_heap_bytes"`
	RegrownHeap           uint64                     `json:"regrown_heap_bytes"`
	PurgedBeforeLaterHeap uint64                     `json:"purged_before_later_compaction_heap_bytes"`
	LaterCompactedHeap    uint64                     `json:"later_compacted_heap_bytes"`
	RefilledHeap          uint64                     `json:"refilled_heap_bytes"`
}

func pathShardingRunSoak(ctx context.Context, t *testing.T, f *pathShardingConnectedFixture, sink *pathShardingSoakSink, cohort []pathShardingPath, started time.Time, growthHeap uint64) pathShardingSoakState {
	t.Helper()
	state := pathShardingSoakState{FinalBuckets: f.hotBuckets, InitialGrowthHeap: growthHeap}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	end := time.NewTimer(time.Until(started.Add(pathShardingSoakDuration)))
	defer end.Stop()
	coldPending, coldPhase := true, 0
	for {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-end.C:
			if state.RegrownUTC.IsZero() || state.RefilledUTC.IsZero() || state.ColdPurgedUTC[0].IsZero() || state.ColdPurgedUTC[1].IsZero() {
				t.Fatalf("missing real maintenance phase: %+v", state)
			}
			state.RefilledHeap = pathShardingSoakHeap(f)
			if state.FirstCompactedHeap >= state.InitialGrowthHeap || state.LaterCompactedHeap >= state.PurgedBeforeLaterHeap {
				t.Fatalf("actual compaction did not release retained growth/map capacity: %+v", state)
			}
			for role := range state.Completed {
				state.Completed[role] = f.completed[role]
			}
			return state
		case <-tick.C:
			elapsed := time.Since(started)
			if elapsed >= 55*time.Minute+5*time.Second && state.RegrownUTC.IsZero() {
				state.RegrownBuckets = f.seedCold(t, cohort)
				state.RegrownHeap = pathShardingSoakHeap(f)
				state.RegrownUTC, coldPending, coldPhase = time.Now().UTC(), true, 1
			}
			run, err := f.run(ctx, t, [4]int{32, 32, 16, 16})
			if err != nil {
				t.Fatal(err)
			}
			state.Iterations++
			for role, completed := range run.Roles {
				for _, duration := range completed.Samples {
					state.MaximumLatencyNS[role] = max(state.MaximumLatencyNS[role], duration)
				}
			}
			events, _, _, problem := sink.snapshot()
			if problem != "" {
				t.Fatal(problem)
			}
			if len(events) >= 2 {
				for role, completed := range run.Roles {
					if completed.Completed == 0 || completed.StartedNS < events[1].AtNS {
						continue
					}
					success := &state.PostSecondSuccess[role]
					if success.Completed == 0 {
						success.FirstUTC, success.FirstNS = completed.FirstCompletedUTC, completed.firstCompletedNS
					}
					success.LastUTC, success.LastNS = completed.FinishedUTC, completed.FinishedNS
					success.Completed += completed.Completed
				}
			}
			if coldPending && f.predictor.TotalBuckets() == f.hotBuckets {
				state.ColdPurgedUTC[coldPhase], coldPending = time.Now().UTC(), false
				if coldPhase == 1 {
					state.PurgedBeforeLaterHeap = pathShardingSoakHeap(f)
				}
			}
			if !coldPending {
				f.guardPopulation(t, state.FinalBuckets, false)
			}
			if elapsed > 2*time.Minute && len(events) == 0 {
				t.Fatal("first real purge/compaction did not rebuild")
			}
			if !state.RegrownUTC.IsZero() && coldPending && time.Since(state.RegrownUTC) > 2*time.Minute {
				t.Fatal("regrown expired cohort survived two real minute purges")
			}
			if elapsed >= 63*time.Minute && len(events) < 2 {
				t.Fatal("later real hourly compaction did not rebuild by minute 63")
			}
			if len(events) >= 1 && state.FirstCompactedHeap == 0 {
				state.FirstCompactedHeap = pathShardingSoakHeap(f)
			}
			if len(events) >= 2 && state.RefilledUTC.IsZero() {
				if coldPending {
					t.Fatal("later compaction left old cold owners")
				}
				state.LaterCompactedHeap = pathShardingSoakHeap(f)
				state.FinalBuckets = f.seedCohort(t, cohort[:64], true)
				state.RefilledUTC = time.Now().UTC()
				f.guardPopulation(t, state.FinalBuckets, false)
			}
			if state.Iterations%60 == 0 {
				t.Logf("real S4 soak elapsed=%s buckets=%d compactions=%d", elapsed.Round(time.Second), f.predictor.TotalBuckets(), len(events))
			}
		}
	}
}

// These are descriptive live-heap readings at known population/maintenance
// phases, not a comparison against another implementation or bytes per bucket.
func pathShardingSoakHeap(f *pathShardingConnectedFixture) uint64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	runtime.KeepAlive(f)
	return stats.HeapAlloc
}

func pathShardingMaintenanceIDs(t *testing.T, buffer []byte) []uint64 {
	t.Helper()
	length := runtime.Stack(buffer, true)
	if length == len(buffer) {
		t.Fatal("bounded stack buffer cannot certify maintenance lifecycle")
	}
	var ids []uint64
	for _, block := range strings.Split(string(buffer[:length]), "\n\n") {
		if !strings.Contains(block, "startPathPredictorMaintenance.func1") {
			continue
		}
		fields := strings.Fields(block)
		if len(fields) < 2 {
			t.Fatal("malformed maintenance goroutine header")
		}
		id, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

func pathShardingAwaitMaintenanceStart(t *testing.T, buffer []byte, before []uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range pathShardingMaintenanceIDs(t, buffer) {
			found := false
			for _, old := range before {
				if id == old {
					found = true
				}
			}
			if !found {
				return id
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual runtime maintenance goroutine did not start")
	return 0
}

func pathShardingAwaitMaintenanceExit(t *testing.T, buffer []byte, id uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		found := false
		for _, live := range pathShardingMaintenanceIDs(t, buffer) {
			if live == id {
				found = true
			}
		}
		if !found {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual runtime maintenance goroutine survived cancellation")
}
