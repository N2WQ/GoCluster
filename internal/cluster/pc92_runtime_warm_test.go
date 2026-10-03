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
	"unsafe"

	"dxcluster/peer"
)

const qualificationWarmSeconds = 15 * 60

// This owner exists only in diagnostic qualification builds. It allocates its
// complete sample/window arrays once. A missing interval cannot be backfilled
// and presented as a factual observation. Counters are sampled views; they do
// not prove simultaneous protocol allocation or replace the token oracle.
type qualificationWarmSample struct {
	Second                                    int
	AtNS                                      int64
	HeapAlloc, HeapInuse, TotalAlloc, Mallocs uint64
	Frees, PauseTotalNS                       uint64
	NumGC                                     uint32
	Goroutines                                int
	State                                     peer.ProtocolStats
}

type qualificationWarmWindow struct {
	Path                        string
	StartSecond, EndSecond      int
	StartedNS, StoppedNS, Bytes int64
}

type qualificationWarmProfile struct {
	EpochCounterNS, EpochUTCNS int64
	Samples                    [qualificationWarmSeconds]qualificationWarmSample
	Windows                    [3]qualificationWarmWindow
	Count                      int
	Complete                   bool
	BackingBytes               uint64
	Failure                    string
}

func qualificationWarmWindows(base string) [3]qualificationWarmWindow {
	return [3]qualificationWarmWindow{
		{Path: base + "-000-020.pprof", StartSecond: 0, EndSecond: 20},
		{Path: base + "-060-180.pprof", StartSecond: 60, EndSecond: 180},
		{Path: base + "-660-780.pprof", StartSecond: 660, EndSecond: 780},
	}
}

func (p *qualificationWarmProfile) validate() error {
	if p.Failure != "" || !p.Complete || p.Count != len(p.Samples) || p.EpochCounterNS <= 0 || p.EpochUTCNS <= 0 {
		return fmt.Errorf("incomplete warm diagnostic: count=%d complete=%t failure=%s", p.Count, p.Complete, p.Failure)
	}
	expected := qualificationWarmWindows("")
	for i, w := range p.Windows {
		if w.StartSecond != expected[i].StartSecond || w.EndSecond != expected[i].EndSecond || w.Path == "" || w.Bytes <= 0 {
			return fmt.Errorf("warm CPU window %d missing or changed", i)
		}
		if !qualificationWarmInterval(w.StartedNS, w.StartSecond) || !qualificationWarmInterval(w.StoppedNS, w.EndSecond) {
			return fmt.Errorf("warm CPU window %d missed scheduled boundary", i)
		}
	}
	for i, sample := range p.Samples {
		if sample.Second != i+1 || !qualificationWarmInterval(sample.AtNS, i+1) {
			return fmt.Errorf("missing or invalid warm sample %d", i+1)
		}
	}
	return nil
}

func qualificationWarmInterval(atNS int64, second int) bool {
	start := time.Duration(second) * time.Second
	return time.Duration(atNS) >= start && time.Duration(atNS) < start+time.Second
}

// CPU profile control and sampling run in their own fixture worker, never in
// the source load generator. Unlike the cold allocation helper, this worker
// never forces collection. Stop joins it before handing the immutable report
// to the RPC encoder. All normal runtime settings and hot-path observers stay
// the same as Q1. Scheduling delay of a whole interval invalidates diagnosis.
func startQualificationWarmProfile(t *testing.T, path string, manager *peer.Manager, clockNow func() time.Time) func() qualificationLoadProfile {
	t.Helper()
	base := strings.TrimSuffix(path, filepath.Ext(path))
	warm := &qualificationWarmProfile{Windows: qualificationWarmWindows(base)}
	warm.BackingBytes = uint64(unsafe.Sizeof(*warm))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	ready := make(chan struct{})
	var report qualificationLoadProfile
	var once sync.Once
	go func() {
		defer close(done)
		report = runQualificationWarmProfile(ctx, warm, manager, clockNow, ready)
	}()
	<-ready // opened initial profile before the driver arms/offers load
	stop := func() qualificationLoadProfile {
		once.Do(func() {
			// Normally the complete 900-second sampling owner has already
			// finished when the driver ends its load. A canceled/shortened
			// driver cannot silently turn a partial report into success.
			select {
			case <-done:
			case <-time.After(time.Second):
				cancel()
				<-done
			}
			cancel()
			if err := warm.validate(); err != nil {
				t.Error(err)
			}
		})
		return report
	}
	t.Cleanup(func() { stop() })
	return stop
}

func runQualificationWarmProfile(ctx context.Context, warm *qualificationWarmProfile, manager *peer.Manager, clockNow func() time.Time, ready chan struct{}) qualificationLoadProfile {
	started := time.Now()
	warm.EpochCounterNS, warm.EpochUTCNS = clockNow().UnixNano(), started.UTC().UnixNano()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	var file *os.File
	window, active := 0, false
	var readyOnce sync.Once
	defer readyOnce.Do(func() { close(ready) })
	fail := func(reason string) { warm.Failure = reason }
	for second := 0; second <= qualificationWarmSeconds; second++ {
		if ctx.Err() != nil {
			fail("canceled before complete warm sampling")
			break
		}
		if err := qualificationWaitContext(ctx, time.Until(started.Add(time.Duration(second)*time.Second))); err != nil {
			fail("canceled before complete warm sampling")
			break
		}
		if !qualificationWarmInterval(time.Since(started).Nanoseconds(), second) {
			fail("missed one-second warm interval")
			break
		}
		if second > 0 {
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			warm.Samples[second-1] = qualificationWarmSample{Second: second, AtNS: time.Since(started).Nanoseconds(),
				HeapAlloc: memory.HeapAlloc, HeapInuse: memory.HeapInuse, TotalAlloc: memory.TotalAlloc, Mallocs: memory.Mallocs,
				Frees: memory.Frees, PauseTotalNS: memory.PauseTotalNs, NumGC: memory.NumGC, Goroutines: runtime.NumGoroutine(), State: manager.ProtocolStats()}
			warm.Count++
		}
		if window < len(warm.Windows) && second == warm.Windows[window].EndSecond && active {
			warm.Windows[window].StoppedNS = time.Since(started).Nanoseconds()
			if err := finishQualificationWarmCPU(file, &warm.Windows[window]); err != nil {
				fail("could not finish warm CPU profile")
			}
			file, active = nil, false
			window++
		}
		if window < len(warm.Windows) && second == warm.Windows[window].StartSecond {
			var err error
			file, err = os.OpenFile(warm.Windows[window].Path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if err == nil {
				err = pprof.StartCPUProfile(file)
				if err != nil {
					_ = file.Close()
					file = nil
				}
			}
			if err != nil {
				fail("could not start warm CPU profile")
				break
			}
			active = true
			warm.Windows[window].StartedNS = time.Since(started).Nanoseconds()
		}
		readyOnce.Do(func() { close(ready) })
		if warm.Failure != "" {
			break
		}
	}
	if active {
		_ = finishQualificationWarmCPU(file, &warm.Windows[window])
	}
	warm.Complete = warm.Failure == "" && warm.Count == len(warm.Samples) && window == len(warm.Windows)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	return qualificationLoadProfile{Enabled: true, Warm: warm, ElapsedSeconds: time.Since(started).Seconds(),
		TotalAllocBytes: after.TotalAlloc - before.TotalAlloc, AllocObjects: after.Mallocs - before.Mallocs}
}

func finishQualificationWarmCPU(file *os.File, window *qualificationWarmWindow) error {
	pprof.StopCPUProfile()
	info, statErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil {
		return statErr
	}
	window.Bytes = info.Size()
	return closeErr
}
