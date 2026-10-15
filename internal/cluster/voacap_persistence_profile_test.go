// File role: Opt-in controlled profiles of actual VOACAP fallback-worker
// completions after a 50,000-entry restart hydration. The same fixture runs in
// the S1 baseline and S2 candidate; it uses no incremental-index interfaces.
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/pathreliability"

	"github.com/cockroachdb/pebble"
)

const (
	voacapProfileSeedCount       = 50000
	voacapProfileCompletionCount = 256
	voacapProfileExecutable      = `C:\src\gocluster\.tmp\windows-tests\dxcluster\internal\cluster\package.test.exe`
)

func TestVOACAPPersistenceWorkerProfile(t *testing.T) {
	outputDir := os.Getenv("GOCLUSTER_VOACAP_PROFILE_DIR")
	if outputDir == "" {
		t.Skip("opt-in controlled VOACAP worker profiling")
	}
	if !filepath.IsAbs(outputDir) {
		t.Fatal("GOCLUSTER_VOACAP_PROFILE_DIR must be an absolute artifact directory")
	}
	executable, err := os.Executable()
	if err != nil || !strings.EqualFold(filepath.Clean(executable), voacapProfileExecutable) {
		t.Fatalf("controlled profiles require executable %s; got %q error=%v", voacapProfileExecutable, executable, err)
	}
	if err := os.MkdirAll(outputDir, 0700); err != nil {
		t.Fatal(err)
	}
	runDir, err := os.MkdirTemp(outputDir, "voacap-worker-")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("controlled worker artifacts: %s", runDir)
	seedAt := time.Now().UTC()
	dbPath := filepath.Join(runDir, "cache")
	seedVOACAPPersistenceProfile(t, dbPath, seedAt)
	logger := &voacapPersistenceProfileLogger{}
	forecaster := &voacapPersistenceProfileForecaster{}
	fallback, restored := newVOACAPPersistenceProfileFallback(t, dbPath, runDir, seedAt, forecaster, logger)
	ctx, cancel := context.WithCancel(t.Context())
	fallback.Start(ctx)
	t.Cleanup(func() {
		cancel()
		fallback.Wait()
		if err := fallback.CloseForecastCacheStore(); err != nil {
			t.Errorf("close forecast store: %v", err)
		}
	})

	// Setup, hydration and the collections that settle their allocations are
	// outside both CPU sampling and the cumulative-allocation difference.
	runtime.GC()
	runtime.GC()
	writeVOACAPPersistenceProfile(t, filepath.Join(runDir, "allocs-before.pprof"), "allocs")
	previousMutexRate := runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)
	defer func() {
		runtime.SetMutexProfileFraction(previousMutexRate)
		runtime.SetBlockProfileRate(0)
	}()
	stopCPU := startVOACAPPersistenceCPUProfile(t, filepath.Join(runDir, "cpu.pprof"))
	defer func() {
		cancel()
		fallback.Wait()
		stopCPU()
	}()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now().UTC()
	runVOACAPPersistenceProfileCompletions(t, fallback)
	cancel()
	fallback.Wait()
	finished := time.Now().UTC()
	runtime.ReadMemStats(&after)
	runtime.SetMutexProfileFraction(previousMutexRate)
	runtime.SetBlockProfileRate(0)
	stopCPU()
	writeVOACAPPersistenceProfile(t, filepath.Join(runDir, "mutex.pprof"), "mutex")
	writeVOACAPPersistenceProfile(t, filepath.Join(runDir, "block.pprof"), "block")
	stats := fallback.StatsSnapshot()
	snapshot := fallback.Snapshot()
	if forecaster.calls.Load() != voacapProfileCompletionCount || stats.Queued != voacapProfileCompletionCount || stats.RunSuccess != voacapProfileCompletionCount || stats.RunFailure != 0 || logger.failures.Load() != 0 {
		t.Fatalf("worker completion failure: calls=%d stats=%+v warnings=%d first=%q", forecaster.calls.Load(), stats, logger.failures.Load(), logger.firstWarning())
	}
	if snapshot.CacheEntries != voacapProfileSeedCount || snapshot.InflightEntries != 0 || snapshot.QueueDepth != 0 {
		t.Fatalf("worker did not finish with bounded cache and empty work queues: %+v", snapshot)
	}
	if seedAt.Year() != finished.Year() || seedAt.Month() != finished.Month() {
		t.Fatal("UTC month changed during fixture; validation-generation change confounds the controlled profile")
	}
	runtime.GC()
	runtime.GC()
	writeVOACAPPersistenceProfile(t, filepath.Join(runDir, "allocs-after.pprof"), "allocs")
	runtime.GC()
	runtime.GC()
	writeVOACAPPersistenceProfile(t, filepath.Join(runDir, "heap.pprof"), "heap")
	runtime.KeepAlive(fallback)
	if err := fallback.CloseForecastCacheStore(); err != nil {
		t.Fatal(err)
	}
	verifyVOACAPPersistenceProfileDisk(t, dbPath, seedAt, finished)
	report := voacapPersistenceProfileManifest{
		Test: t.Name(), Executable: executable, GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH,
		GOMAXPROCS: runtime.GOMAXPROCS(0), GOGC: os.Getenv("GOGC"), MemProfileRate: runtime.MemProfileRate,
		SeedCount: voacapProfileSeedCount, Completions: voacapProfileCompletionCount, WorkerCount: 1, ForecastHours: 24,
		SeedAtUTC: seedAt, StartedUTC: started, FinishedUTC: finished, CrossedUTCHour: !started.Truncate(time.Hour).Equal(finished.Truncate(time.Hour)),
		ElapsedSeconds: finished.Sub(started).Seconds(), TotalAllocBytes: after.TotalAlloc - before.TotalAlloc, AllocObjects: after.Mallocs - before.Mallocs,
		Restored: restored, WorkerStats: stats, FinalCacheEntries: snapshot.CacheEntries, PersistedEntries: voacapProfileSeedCount,
		CPU: "cpu.pprof", AllocationsBefore: "allocs-before.pprof", AllocationsAfter: "allocs-after.pprof", Heap: "heap.pprof", Mutex: "mutex.pprof", Block: "block.pprof", Database: "cache",
	}
	value, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "manifest.json"), append(value, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("completed %d worker persistence operations in %.3fs; profiles and manifest: %s", voacapProfileCompletionCount, report.ElapsedSeconds, runDir)
}

func newVOACAPPersistenceProfileFallback(t *testing.T, dbPath, runDir string, now time.Time, forecaster *voacapPersistenceProfileForecaster, logger *voacapPersistenceProfileLogger) (*pathreliability.VOACAPClosedFallback, pathreliability.VOACAPForecastCacheRestoreStats) {
	t.Helper()
	cfg := pathreliability.DefaultConfig()
	cfg.VOACAPFallback.Enabled = true
	cfg.VOACAPFallback.VOACAPHome = runDir
	cfg.VOACAPFallback.DelaySeconds = 0
	cfg.VOACAPFallback.ForecastHours = 24
	cfg.VOACAPFallback.CacheTTLSeconds = 86400
	cfg.VOACAPFallback.MaxCacheEntries = voacapProfileSeedCount
	cfg.VOACAPFallback.MaxDelayEntries = voacapProfileSeedCount
	cfg.VOACAPFallback.MaxQueueDepth = 2
	cfg.VOACAPFallback.WorkerCount = 1
	cfg.VOACAPFallback.CenterFrequenciesMHz = []float64{14.1}
	cfg.MergeReceiveWeight = 0.6
	cfg.MergeTransmitWeight = 0.4
	store, err := pathreliability.OpenVOACAPForecastCacheStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close profile store: %v", err)
		}
	})
	fallback, err := pathreliability.NewVOACAPClosedFallback(cfg, forecaster, voacapPersistenceProfileSSN{}, logger)
	if err != nil {
		t.Fatal(err)
	}
	stats, err := fallback.AttachForecastCacheStore(store, now)
	if err != nil || stats.Loaded != voacapProfileSeedCount || stats.Pruned != 0 || stats.SSNUnavailable || fallback.Snapshot().CacheEntries != voacapProfileSeedCount {
		t.Fatalf("seed hydration: stats=%+v error=%v", stats, err)
	}
	return fallback, stats
}

func runVOACAPPersistenceProfileCompletions(t *testing.T, fallback *pathreliability.VOACAPClosedFallback) {
	t.Helper()
	for i := 0; i < voacapProfileCompletionCount; i++ {
		now := time.Now().UTC()
		req := pathreliability.VOACAPClosedRequest{
			UserCell: 1, DXCell: pathreliability.CellID(voacapProfileSeedCount + i + 1),
			UserGrid: "FN31", DXGrid: "FN32", Band: "20m", Mode: "FT8",
		}
		window, status := fallback.CheckForecastWindowWait(req, now, 2*time.Minute)
		if status != pathreliability.VOACAPForecastWindowReady || len(window.Records) != 24 || !window.WindowStartUTC.Equal(now.Truncate(time.Hour)) {
			t.Fatalf("completion %d: status=%v window=%+v", i, status, window)
		}
		cached, ok := fallback.CheckCachedForecast(req, now)
		if !ok || cached.SSN != 112 || cached.Record.ReceiveFT8SNRDB != -14 || cached.Record.TransmitFT8SNRDB != -15 || math.Abs(cached.EffectiveDB()-(-14.4)) > 1e-12 {
			t.Fatalf("completion %d: wrong published forecast ok=%v result=%+v", i, ok, cached)
		}
		if (i+1)%64 == 0 {
			t.Logf("controlled worker waiters served: %d/%d", i+1, voacapProfileCompletionCount)
		}
	}
}

type voacapPersistenceProfileDiskKey struct {
	UserCell     uint16 `json:"user_cell"`
	DXCell       uint16 `json:"dx_cell"`
	Band         string `json:"band"`
	FrequencyKHz int    `json:"frequency_khz"`
	Year         int    `json:"year"`
	Month        int    `json:"month"`
	SSN          int    `json:"ssn"`
	Direction    string `json:"direction"`
}

type voacapPersistenceProfileDiskRecord struct {
	SchemaVersion   int                                    `json:"schema_version"`
	ModelGeneration int                                    `json:"model_generation"`
	Key             voacapPersistenceProfileDiskKey        `json:"key"`
	StoredAtUnixNS  int64                                  `json:"stored_at_unix_ns"`
	WindowUnixNS    int64                                  `json:"window_unix_ns"`
	Records         []pathreliability.VOACAPHourlyForecast `json:"records"`
}

func voacapPersistenceProfileKey(cell int, now time.Time) []byte {
	return []byte(fmt.Sprintf("voacapfc/v1/e/0001/%04x/20m/014100/%04d/%02d/0112/bidirectional", cell, now.Year(), int(now.Month())))
}

func seedVOACAPPersistenceProfile(t *testing.T, path string, now time.Time) {
	t.Helper()
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close seed database: %v", err)
		}
	}()
	record := voacapPersistenceProfileDiskRecord{
		SchemaVersion: 1, ModelGeneration: 1,
		Key:            voacapPersistenceProfileDiskKey{UserCell: 1, Band: "20m", FrequencyKHz: 14100, Year: now.Year(), Month: int(now.Month()), SSN: 112, Direction: "bidirectional"},
		StoredAtUnixNS: now.Add(-time.Minute).UnixNano(), WindowUnixNS: now.Truncate(time.Hour).UnixNano(),
		Records: voacapPersistenceProfileRows(now.Truncate(time.Hour), 14.1),
	}
	for first := 1; first <= voacapProfileSeedCount; first += 256 {
		batch := db.NewBatch()
		for cell := first; cell < first+256 && cell <= voacapProfileSeedCount; cell++ {
			record.Key.DXCell = uint16(cell)
			value, err := json.Marshal(record)
			if err != nil {
				_ = batch.Close()
				t.Fatal(err)
			}
			if err := batch.Set(voacapPersistenceProfileKey(cell, now), value, nil); err != nil {
				_ = batch.Close()
				t.Fatal(err)
			}
		}
		commitErr := batch.Commit(pebble.NoSync)
		closeErr := batch.Close()
		if commitErr != nil || closeErr != nil {
			t.Fatalf("seed batch: commit=%v close=%v", commitErr, closeErr)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := db.Compact([]byte("voacapfc/v1/e/"), []byte("voacapfc/v1/e0"), true); err != nil {
		t.Fatal(err)
	}
}

func verifyVOACAPPersistenceProfileDisk(t *testing.T, path string, seedAt, finished time.Time) {
	t.Helper()
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close verification database: %v", err)
		}
	}()
	iter, err := db.NewIter(&pebble.IterOptions{LowerBound: []byte("voacapfc/v1/e/"), UpperBound: []byte("voacapfc/v1/e0")})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	scanErr := iter.Error()
	closeErr := iter.Close()
	if scanErr != nil || closeErr != nil || count != voacapProfileSeedCount {
		t.Fatalf("raw post-workload cardinality: count=%d scan=%v close=%v", count, scanErr, closeErr)
	}
	for cell := voacapProfileSeedCount + 1; cell <= voacapProfileSeedCount+voacapProfileCompletionCount; cell++ {
		value, closer, err := db.Get(voacapPersistenceProfileKey(cell, seedAt))
		if err != nil {
			t.Fatalf("raw worker forecast cell=%d: %v", cell, err)
		}
		var record voacapPersistenceProfileDiskRecord
		decodeErr := json.Unmarshal(value, &record)
		closeErr := closer.Close()
		wantKey := voacapPersistenceProfileDiskKey{UserCell: 1, DXCell: uint16(cell), Band: "20m", FrequencyKHz: 14100, Year: seedAt.Year(), Month: int(seedAt.Month()), SSN: 112, Direction: "bidirectional"}
		windowStart := time.Unix(0, record.WindowUnixNS).UTC()
		if decodeErr != nil || closeErr != nil || record.SchemaVersion != 1 || record.ModelGeneration != 1 || record.Key != wantKey || record.StoredAtUnixNS < seedAt.UnixNano() || record.StoredAtUnixNS > finished.UnixNano() || windowStart.Before(seedAt.Truncate(time.Hour)) || windowStart.After(finished) || !slices.Equal(record.Records, voacapPersistenceProfileRows(windowStart, 14.1)) {
			t.Fatalf("raw worker forecast cell=%d record=%+v decode=%v close=%v", cell, record, decodeErr, closeErr)
		}
	}
}

type voacapPersistenceProfileForecaster struct{ calls atomic.Int32 }

func (f *voacapPersistenceProfileForecaster) ForecastClosed(ctx context.Context, job pathreliability.VOACAPClosedJob) (pathreliability.VOACAPClosedForecast, error) {
	if err := ctx.Err(); err != nil {
		return pathreliability.VOACAPClosedForecast{}, err
	}
	f.calls.Add(1)
	return pathreliability.VOACAPClosedForecast{WindowStartUTC: job.WindowStartUTC, Records: voacapPersistenceProfileRows(job.WindowStartUTC, job.FrequencyMHz)}, nil
}

func voacapPersistenceProfileRows(start time.Time, frequency float64) []pathreliability.VOACAPHourlyForecast {
	rows := make([]pathreliability.VOACAPHourlyForecast, 24)
	for hour := range rows {
		rows[hour] = pathreliability.VOACAPHourlyForecast{
			HourUTC: start.Add(time.Duration(hour) * time.Hour).Hour(), FrequencyMHz: frequency,
			FT8SNRDB: -14, VOACAPSNRDBHz: 16, ReceiveFT8SNRDB: -14, TransmitFT8SNRDB: -15,
			ReceiveVOACAPSNRDBHz: 16, TransmitVOACAPSNRDBHz: 15, HasDirectionalSNR: true,
			ReqSNRReliability: 0.8, HasReqSNRReliability: true,
			ReceiveReqSNRReliability: 0.8, TransmitReqSNRReliability: 0.7, HasDirectionalReqSNRReliability: true,
		}
	}
	return rows
}

type voacapPersistenceProfileSSN struct{}

func (voacapPersistenceProfileSSN) CurrentSSN(time.Time) (int, bool) { return 112, true }

type voacapPersistenceProfileLogger struct {
	failures atomic.Int32
	first    atomic.Pointer[string]
}

func (l *voacapPersistenceProfileLogger) Printf(format string, args ...any) {
	l.failures.Add(1)
	if l.first.Load() == nil {
		message := fmt.Sprintf(format, args...)
		if len(message) > 1024 {
			message = message[:1024]
		}
		l.first.CompareAndSwap(nil, &message)
	}
}

func (l *voacapPersistenceProfileLogger) firstWarning() string {
	if first := l.first.Load(); first != nil {
		return *first
	}
	return ""
}

func startVOACAPPersistenceCPUProfile(t *testing.T, path string) func() {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			pprof.StopCPUProfile()
			if err := file.Close(); err != nil {
				t.Errorf("close CPU profile: %v", err)
			}
		})
	}
}

func writeVOACAPPersistenceProfile(t *testing.T, path, name string) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	writeErr := pprof.Lookup(name).WriteTo(file, 0)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("write %s profile: write=%v close=%v", name, writeErr, closeErr)
	}
}

type voacapPersistenceProfileManifest struct {
	Test, Executable, GoVersion, GOOS, GOARCH                              string
	GOMAXPROCS                                                             int
	GOGC                                                                   string
	MemProfileRate                                                         int
	SeedCount, Completions, WorkerCount, ForecastHours                     int
	SeedAtUTC, StartedUTC, FinishedUTC                                     time.Time
	CrossedUTCHour                                                         bool
	ElapsedSeconds                                                         float64
	TotalAllocBytes, AllocObjects                                          uint64
	Restored                                                               pathreliability.VOACAPForecastCacheRestoreStats
	WorkerStats                                                            pathreliability.VOACAPFallbackStats
	FinalCacheEntries, PersistedEntries                                    int
	CPU, AllocationsBefore, AllocationsAfter, Heap, Mutex, Block, Database string
}
