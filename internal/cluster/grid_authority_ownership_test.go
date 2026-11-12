package cluster

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/cty"
)

func TestGridAuthorityOwnership(t *testing.T) {
	t.Run("offline_and_terminal", func(t *testing.T) {
		f := newGridAuthorityFixture(t, newCallMetaCache(64, 0), time.Hour, gridWriterOps{}, nil)
		f.handle.Set(nil)
		f.update("K1ABC", "FN31AB")
		if grid, _, ok := f.lookup("K1ABC"); !ok || grid != "FN31AB" {
			t.Fatal("offline cache report lost")
		}
		assertAuthorityDrained(t, f)
		f.update("K1ABC", "DM04AA")
		f.lookup("W6XYZ")
		f.syncLookup("W6XYZ")
		assertAuthorityDrained(t, f)
		if grid, _, _ := f.cache.LookupGrid("K1ABC", nil); grid != "FN31AB" {
			t.Fatal("terminal hook mutated authority")
		}
	})
	t.Run("async_overflow", func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		var once, releaseOnce sync.Once
		var reads atomic.Int64
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		f := newGridAuthorityFixture(t, nil, time.Hour, gridWriterOps{}, func() *cty.CTYDatabase { reads.Add(1); once.Do(func() { close(entered) }); <-release; return nil })
		t.Cleanup(unblock)
		f.lookup("K1FIRST")
		waitGridRetirement(t, entered)
		for i := 0; i < 4106; i++ {
			f.lookup(fmt.Sprintf("K1Q%04d", i))
		}
		if f.metrics.asyncDrops.Load() != 10 {
			t.Fatalf("fixed queue overflow changed: %d", f.metrics.asyncDrops.Load())
		}
		unblock()
		assertAuthorityDrained(t, f)
		if reads.Load() != 4097 {
			t.Fatalf("queued work abandoned: %d", reads.Load())
		}
	})
	t.Run("coalescing_has_one_owner", func(t *testing.T) {
		f := newGridAuthorityFixture(t, nil, time.Hour, gridWriterOps{}, nil)
		queue := make(chan gridWriteRequest, 1)
		pending := make(map[string]gridWriteRequest)
		for i := 0; i < 12000; i++ {
			f.coord.enqueueReport(authorityRecord(fmt.Sprintf("FN%02d", i%100), false, time.Now()), queue)
			f.coord.coalesce(pending, <-queue)
		}
		f.coord.mu.Lock()
		count, writes := len(f.coord.entries), f.coord.pending
		f.coord.mu.Unlock()
		if count != 1 || writes != 1 || len(pending) != 1 {
			t.Fatalf("coalescing retained historical owners: %d/%d/%d", count, writes, len(pending))
		}
		batch, store := f.coord.prepareBatch(pending)
		err := store.UpsertBatch(batch)
		if err != nil {
			t.Fatal(err)
		}
		f.coord.finishBatch(pending, err)
		assertAuthorityDrained(t, f)
	})
}
