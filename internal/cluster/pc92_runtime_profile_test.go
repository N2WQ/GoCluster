//go:build qualification

package cluster

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/peer"
)

type qualificationLoadProfile struct {
	Enabled                                  bool
	CPU, AllocationsBefore, AllocationsAfter string
	ElapsedSeconds                           float64
	TotalAllocBytes, AllocObjects            uint64
	Warm                                     *qualificationWarmProfile `json:",omitempty"`
}

// Profiling is an opt-in diagnostic of the service process (runtime and its
// bounded enqueue observer). Population and drain are excluded from CPU samples. Allocation
// profiles bracket the same load; their cumulative samples require pprof -base.
// GC before either allocation snapshot is outside the measured load interval.
func startQualificationLoadProfile(ctx context.Context, t *testing.T, profile qualificationProfile, manager *peer.Manager, clockNow func() time.Time) func() qualificationLoadProfile {
	t.Helper()
	path := os.Getenv("GOCLUSTER_PC92_RUNTIME_CPU_PROFILE")
	if profile.name == "warm-diagnostic" {
		if path == "" || !filepath.IsAbs(path) {
			t.Fatal("warm diagnostic requires CPU profiling and an absolute output path")
		}
		return startQualificationWarmProfile(ctx, t, path, manager, clockNow)
	}
	if path == "" {
		return func() qualificationLoadProfile { return qualificationLoadProfile{} }
	}
	if profile.name != "preflight" || !filepath.IsAbs(path) {
		t.Fatal("CPU profiling requires preflight and an absolute output path")
	}
	base := strings.TrimSuffix(path, filepath.Ext(path))
	report := qualificationLoadProfile{Enabled: true, CPU: path, AllocationsBefore: base + "-allocs-before.pprof", AllocationsAfter: base + "-allocs-after.pprof"}
	if err := writeQualificationAllocations(report.AllocationsBefore); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.StartCPUProfile(file); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	started := time.Now()
	var once sync.Once
	stop := func() qualificationLoadProfile {
		once.Do(func() {
			report.ElapsedSeconds = time.Since(started).Seconds()
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			report.TotalAllocBytes = after.TotalAlloc - before.TotalAlloc
			report.AllocObjects = after.Mallocs - before.Mallocs
			pprof.StopCPUProfile()
			if err := file.Close(); err != nil {
				t.Errorf("close CPU profile: %v", err)
			}
			if err := writeQualificationAllocations(report.AllocationsAfter); err != nil {
				t.Error(err)
			}
		})
		return report
	}
	t.Cleanup(func() { stop() })
	return stop
}

func writeQualificationAllocations(path string) error {
	// Memory profiling can lag reclamation by two collections. Neither
	// collection is part of the CPU or latency interval.
	runtime.GC()
	runtime.GC()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	writeErr := pprof.Lookup("allocs").WriteTo(file, 0)
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("write allocation profile: %w", writeErr)
	}
	return closeErr
}
