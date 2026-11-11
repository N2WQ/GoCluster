package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/gridstore"
)

func gridRetirementStoreOptions() gridstore.Options {
	return gridstore.Options{CacheSizeBytes: 1 << 20, MemTableSizeBytes: 1 << 20,
		L0CompactionThreshold: 2, L0StopWritesThreshold: 4, WriteQueueDepth: 4}
}

func waitGridRetirement(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("grid owner did not complete")
	}
}

func waitGridCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("grid ownership witness did not arrive")
		}
		runtime.Gosched()
	}
}

func assertGridRetirementBlocked(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("grid retirement abandoned an admitted owner")
	case <-time.After(15 * time.Millisecond):
	}
}

func gridRetirementCTY(t *testing.T) *cty.CTYDatabase {
	t.Helper()
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(`<plist version="1.0"><dict><key>K1</key><dict><key>Country</key><string>Alpha</string><key>Prefix</key><string>K1</string><key>ExactCallsign</key><false/><key>Latitude</key><real>40.0</real><key>Longitude</key><real>75.0</real></dict></dict></plist>`))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestGridRetirementAdmittedCacheHook(t *testing.T) {
	store, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	cache := newCallMetaCache(64, 0)
	cache.UpdateGrid("K1ABC", "FN20", false)
	_, _, metrics, stop, lookup, _ := startGridWriter(newGridStoreHandle(store), time.Hour, cache, 0, true, nil)
	shard := cache.shardFor("K1ABC")
	shard.mu.Lock()
	var release sync.Once
	unlock := func() { release.Do(shard.mu.Unlock) }
	defer func() { unlock(); stop(); _ = store.Close() }()
	result := make(chan gridLookupResult, 1)
	go func() {
		grid, derived, ok := lookup("K1ABC")
		result <- gridLookupResult{grid: grid, derived: derived, ok: ok}
	}()
	// LookupGrid increments before taking the held shard; this positively proves
	// the external hook was admitted before retirement tries its exclusive lock.
	waitGridCondition(t, func() bool { return metrics.cacheLookups.Load() == 1 })
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	assertGridRetirementBlocked(t, done)
	unlock()
	select {
	case got := <-result:
		if got.grid != "FN20" || got.derived || !got.ok {
			t.Fatalf("admitted lookup lost result: %+v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("admitted lookup stuck")
	}
	waitGridRetirement(t, done)
}

func TestGridRetirementDrainsDerivedOwners(t *testing.T) {
	for _, mode := range []string{"async", "timed_out_sync"} {
		t.Run(mode, func(t *testing.T) {
			path := t.TempDir()
			store, err := gridstore.Open(path, gridRetirementStoreOptions())
			if err != nil {
				t.Fatal(err)
			}
			db := gridRetirementCTY(t)
			entered := make(chan struct{}, 8)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			handle := newGridStoreHandle(store)
			update, updateCTY, _, stop, lookup, lookupSync := startGridWriter(handle, time.Hour, newCallMetaCache(128, 0), 0, true, func() *cty.CTYDatabase {
				entered <- struct{}{}
				<-release
				return db
			})
			retire := stop
			runtimeOwnsStore := mode == "async"
			if runtimeOwnsStore {
				r := &clusterRuntime{gridStoreHandle: handle, stopGridWriter: stop}
				retire = r.retireGridStore
			}
			closed := false
			var lookupCallers sync.WaitGroup
			defer func() {
				unblock()
				retire()
				lookupCallers.Wait()
				if !runtimeOwnsStore && !closed {
					_ = store.Close()
				}
			}()
			calls := []string{"K1AAA"}
			if mode == "async" {
				lookup(calls[0])
				waitGridRetirement(t, entered)
			} else {
				calls = []string{"K1AAA", "K1AAB", "K1AAC", "K1AAD", "K1AAE", "K1AAF"}
				for _, call := range calls {
					lookupCallers.Add(1)
					go func() {
						defer lookupCallers.Done()
						if grid, derived, ok := lookupSync(call); grid != "" || derived || ok {
							t.Errorf("blocked lookup should time out: %q/%v/%v", grid, derived, ok)
						}
					}()
				}
				for i := 0; i < gridSyncLookupWorkers; i++ {
					waitGridRetirement(t, entered)
				}
				callersDone := make(chan struct{})
				go func() { lookupCallers.Wait(); close(callersDone) }()
				// Queued work survives the caller's literal tight timeout.
				waitGridRetirement(t, callersDone)
			}
			update("W1XYZ", "EM12")
			updateCTY("W1XYZ", &cty.PrefixInfo{ADIF: 291, CQZone: 5, ITUZone: 8, Country: "United States"})
			done := make(chan struct{})
			returned := make(chan struct{}, 8)
			started := make(chan struct{}, 8)
			go func() {
				var callers sync.WaitGroup
				for i := 0; i < 8; i++ {
					callers.Add(1)
					go func() { defer callers.Done(); started <- struct{}{}; retire(); returned <- struct{}{} }()
				}
				callers.Wait()
				close(done)
			}()
			for i := 0; i < 8; i++ {
				waitGridRetirement(t, started)
			}
			assertGridRetirementBlocked(t, returned)
			unblock()
			waitGridRetirement(t, done)
			if !runtimeOwnsStore {
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			}
			closed = true
			reopened, err := gridstore.Open(path, gridRetirementStoreOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for _, call := range calls {
				rec, err := reopened.Get(call)
				if err != nil || rec == nil || !rec.Grid.Valid || rec.Grid.String != "FN20" || !rec.GridDerived {
					t.Fatalf("derived owner not persisted for %s: %+v %v", call, rec, err)
				}
			}
			rec, err := reopened.Get("W1XYZ")
			if err != nil || rec == nil || rec.Grid.String != "EM12" || rec.GridDerived || !rec.CTYValid || rec.CTYADIF != 291 {
				t.Fatalf("final ordinary/CTY flush lost: %+v %v", rec, err)
			}
		})
	}
}

func TestGridRetirementAsyncOverflowDrains(t *testing.T) {
	store, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var first, releaseOnce sync.Once
	var processed atomic.Int64
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	_, _, metrics, stop, lookup, _ := startGridWriter(newGridStoreHandle(store), time.Hour, nil, 0, true, func() *cty.CTYDatabase { processed.Add(1); first.Do(func() { close(entered) }); <-release; return nil })
	defer func() { unblock(); stop(); _ = store.Close() }()
	lookup("K1FIRST")
	waitGridRetirement(t, entered)
	for i := 0; i < 4106; i++ {
		lookup(fmt.Sprintf("K1%04d", i))
	}
	if got := metrics.asyncDrops.Load(); got != 10 {
		t.Fatalf("overflow drops=%d want 10 beyond fixed 4096 queue", got)
	}
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	assertGridRetirementBlocked(t, done)
	unblock()
	waitGridRetirement(t, done)
	if got := processed.Load(); got != 4097 {
		t.Fatalf("accepted lookup backlog was abandoned: processed=%d want 4097", got)
	}
}

func TestGridRetirementDisabledAndUnavailable(t *testing.T) {
	_, _, _, stopNil, lookupNil, syncNil := startGridWriter(nil, time.Hour, nil, 0, false, nil)
	stopNil()
	stopNil()
	if _, _, ok := lookupNil("K1ABC"); ok {
		t.Fatal("nil store lookup hit")
	}
	if _, _, ok := syncNil("K1ABC"); ok {
		t.Fatal("nil store sync lookup hit")
	}
	cache := newCallMetaCache(64, 0)
	update, _, _, stop, lookup, lookupSync := startGridWriter(newGridStoreHandle(nil), time.Hour, cache, 0, false, nil)
	defer stop()
	if lookupSync != nil {
		t.Fatal("disabled sync lookup changed contract")
	}
	update("K1ABC", "FN20")
	if grid, _, ok := lookup("K1ABC"); !ok || grid != "FN20" {
		t.Fatal("unavailable persistence lost live cache behavior")
	}
	stop()
	update("K1ABC", "EM12")
	if grid, _, ok := lookup("K1ABC"); ok || grid != "" {
		t.Fatal("disabled lookup admitted after terminal")
	}
}

func TestRuntimeGridRetirementMaintenance(t *testing.T) {
	for _, operation := range []string{"checkpoint", "integrity"} {
		t.Run(operation, func(t *testing.T) {
			path := t.TempDir()
			store, err := gridstore.Open(path, gridRetirementStoreOptions())
			if err != nil {
				t.Fatal(err)
			}
			parent, parentCancel := context.WithCancel(context.Background())
			defer parentCancel()
			ctx, cancel := context.WithCancel(parent)
			r := &clusterRuntime{gridStoreHandle: newGridStoreHandle(store), gridCancel: cancel}
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer func() { unblock(); r.retireGridStore() }()
			if operation == "checkpoint" {
				r.gridCheckpointDone = startGridCheckpointSchedulerWithOps(ctx, r.gridStoreHandle, path, func(time.Time) time.Duration { return 0 }, func(s *gridstore.Store, dest string) error { close(entered); <-release; return s.Checkpoint(dest) })
			} else {
				r.gridIntegrityDone = startGridIntegritySchedulerWithOps(ctx, r.gridStoreHandle, func(time.Time) time.Duration { return 0 }, func(scanCtx context.Context, s *gridstore.Store) (gridstore.IntegrityStats, error) {
					close(entered)
					<-release
					if scanCtx.Err() == nil {
						t.Error("active scan did not inherit grid cancellation")
					}
					return s.Verify(scanCtx, time.Minute)
				})
			}
			waitGridRetirement(t, entered)
			done := make(chan struct{})
			returned := make(chan struct{}, 8)
			started := make(chan struct{}, 8)
			go func() {
				var callers sync.WaitGroup
				for i := 0; i < 8; i++ {
					callers.Add(1)
					go func() { defer callers.Done(); started <- struct{}{}; r.retireGridStore(); returned <- struct{}{} }()
				}
				callers.Wait()
				close(done)
			}()
			waitGridRetirement(t, ctx.Done())
			for i := 0; i < 8; i++ {
				waitGridRetirement(t, started)
			}
			assertGridRetirementBlocked(t, returned)
			if parent.Err() != nil {
				t.Fatal("grid retirement canceled global parent")
			}
			unblock()
			waitGridRetirement(t, done)
			r.retireGridStore() // includes concrete non-idempotent store Close exactly once.
			if operation == "checkpoint" {
				entries, err := listGridCheckpoints(gridCheckpointRoot(path))
				if err != nil || len(entries) != 1 {
					t.Fatalf("active checkpoint cleanup did not finish: %v %v", entries, err)
				}
			}
			reopened, err := gridstore.Open(path, gridRetirementStoreOptions())
			if err != nil {
				t.Fatalf("retired store still owned: %v", err)
			}
			_ = reopened.Close()
		})
	}
}

func TestRuntimeGridRetirementRecovery(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(fmt.Sprintf("published_%v", published), func(t *testing.T) {
			path := t.TempDir()
			store, err := gridstore.Open(path, gridRetirementStoreOptions())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			r := &clusterRuntime{gridStoreHandle: newGridStoreHandle(nil), gridCancel: cancel}
			cache := newCallMetaCache(64, 0)
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			if published {
				cache.shards[0].mu.Lock()
			}
			unblock := func() {
				releaseOnce.Do(func() {
					if published {
						cache.shards[0].mu.Unlock()
					}
					close(release)
				})
			}
			defer func() { unblock(); r.retireGridStore() }()
			r.gridRecoveryDone = startGridStoreRecoveryWithOps(ctx, r.gridStoreHandle, path, gridRetirementStoreOptions(), cache, func(context.Context, string, gridstore.Options) (*gridstore.Store, string, error) {
				close(entered)
				if !published {
					<-release
				}
				return store, "literal-checkpoint", nil
			}, time.Hour, func() { t.Error("unexpected warning") })
			waitGridRetirement(t, entered)
			if published {
				waitGridCondition(t, func() bool { return r.gridStoreHandle.Store() == store })
			}
			done := make(chan struct{})
			go func() { r.retireGridStore(); close(done) }()
			waitGridRetirement(t, ctx.Done())
			assertGridRetirementBlocked(t, done)
			unblock()
			waitGridRetirement(t, done)
			if !published && r.gridStoreHandle.Store() != nil {
				t.Fatal("canceled recovery republished its store")
			}
			reopened, err := gridstore.Open(path, gridRetirementStoreOptions())
			if err != nil {
				t.Fatalf("recovery store was not closed: %v", err)
			}
			_ = reopened.Close()
		})
	}
}

func TestGridRecoveryCompletionJoinsWarning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	warningEntered, warningRelease, restoreRelease := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var warningOnce, restoreOnce sync.Once
	var done <-chan struct{}
	defer func() {
		restoreOnce.Do(func() { close(restoreRelease) })
		warningOnce.Do(func() { close(warningRelease) })
		if done != nil {
			waitGridRetirement(t, done)
		}
	}()
	done = startGridStoreRecoveryWithOps(ctx, newGridStoreHandle(nil), t.TempDir(), gridRetirementStoreOptions(), nil,
		func(context.Context, string, gridstore.Options) (*gridstore.Store, string, error) {
			<-restoreRelease
			return nil, "", errors.New("controlled restore failure")
		}, 0,
		func() { close(warningEntered); <-warningRelease })
	waitGridRetirement(t, warningEntered)
	restoreOnce.Do(func() { close(restoreRelease) })
	assertGridRetirementBlocked(t, done)
	warningOnce.Do(func() { close(warningRelease) })
	waitGridRetirement(t, done)
}

func TestRuntimeGridRetirementStartupAndIdle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &clusterRuntime{ctx: ctx, cfg: &config.Config{GridDBPath: t.TempDir(), GridBlockCacheMB: 1, GridMemTableSizeMB: 1, GridL0Compaction: 2, GridL0StopWrites: 4, GridWriteQueueDepth: 4}, gridStoreHandle: newGridStoreHandle(nil), metaCache: newCallMetaCache(64, 0)}
	defer r.retireGridStore()
	if !r.initializeGridStore() {
		t.Fatal("grid startup failed")
	}
	if r.gridCancel == nil || r.gridCheckpointDone == nil || r.gridIntegrityDone == nil || r.stopGridWriter == nil {
		t.Fatal("startup omitted grid ownership handles")
	}
	r.retireGridStore()
	waitGridRetirement(t, r.gridCheckpointDone)
	waitGridRetirement(t, r.gridIntegrityDone)
	if ctx.Err() != nil {
		t.Fatal("grid retirement changed global cancellation order")
	}
	var callers sync.WaitGroup
	for i := 0; i < 8; i++ {
		callers.Add(1)
		go func() { defer callers.Done(); r.retireGridStore() }()
	}
	callers.Wait()
	partial := &clusterRuntime{}
	partial.retireGridStore()
	partial.retireGridStore()
	waitGridRetirement(t, startGridCheckpointScheduler(ctx, nil, ""))
	waitGridRetirement(t, startGridIntegrityScheduler(ctx, nil))
	waitGridRetirement(t, startGridStoreRecovery(ctx, nil, "", gridRetirementStoreOptions(), nil))
}

func TestRuntimeGridRetirementFailedStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, []byte("file"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &clusterRuntime{ctx: ctx, cfg: &config.Config{GridDBPath: path}, gridStoreHandle: newGridStoreHandle(nil)}
	defer r.retireGridStore()
	if r.initializeGridStore() || r.startupErr == nil {
		t.Fatal("non-corruption open failure did not abort startup")
	}
	if r.gridCancel != nil || r.stopGridWriter != nil || r.gridCheckpointDone != nil || r.gridRecoveryDone != nil {
		t.Fatal("failed startup ownership differs from partial-startup contract")
	}
	r.retireGridStore()
	if ctx.Err() != nil {
		t.Fatal("failed grid startup retirement canceled parent")
	}
}

func TestGridRetirementTerminalHooks(t *testing.T) {
	store, err := gridstore.Open(t.TempDir(), gridRetirementStoreOptions())
	if err != nil {
		t.Fatal(err)
	}
	cache := newCallMetaCache(64, 0)
	cache.UpdateGrid("K1ABC", "FN20", false)
	update, updateCTY, metrics, stop, lookup, lookupSync := startGridWriter(newGridStoreHandle(store), time.Hour, cache, 0, true, nil)
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	}()
	stop()
	stopped = true
	lookups, hits := metrics.cacheLookups.Load(), metrics.cacheHits.Load()
	for _, hook := range []func(string) (string, bool, bool){lookup, lookupSync} {
		if grid, derived, ok := hook("K1ABC"); grid != "" || derived || ok {
			t.Fatalf("retired lookup still admitted cached data: %q/%v/%v", grid, derived, ok)
		}
		hook("W1XYZ")
	}
	update("K1ABC", "EM12")
	update("W1XYZ", "EM12")
	updateCTY("K1ABC", &cty.PrefixInfo{ADIF: 1, CQZone: 1, ITUZone: 1})
	if grid, derived, ok := cache.LookupGrid("K1ABC", nil); !ok || grid != "FN20" || derived {
		t.Fatalf("retired update mutated cached authority: %q/%v/%v", grid, derived, ok)
	}
	if _, _, ok := cache.LookupGrid("W1XYZ", nil); ok {
		t.Fatal("retired update created a cache entry")
	}
	if metrics.cacheLookups.Load() != lookups || metrics.cacheHits.Load() != hits {
		t.Fatal("retired hooks changed lookup metrics")
	}
	if count, err := store.Count(); err != nil || count != 0 {
		t.Fatalf("retired hook wrote store: count=%d err=%v", count, err)
	}
}
