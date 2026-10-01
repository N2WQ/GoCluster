package peer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A timestamp map is the behavioral oracle; it does not share the production
// hash, bucket traversal, heap, elapsed-time representation or expiry order.
type dedupeReference struct {
	items     map[string]time.Time
	ttl       time.Duration
	limit     int
	byteLimit int
	refused   uint64
}

func (r *dedupeReference) prune(now time.Time) {
	for key, admitted := range r.items {
		if now.Sub(admitted) > r.ttl {
			delete(r.items, key)
		}
	}
}

func (r *dedupeReference) bytes() int {
	var total int
	for key := range r.items {
		total += len(key)
	}
	return total
}

func (r *dedupeReference) admit(key string, now time.Time) dedupeAdmission {
	if key == "" {
		return dedupeFull
	}
	r.prune(now)
	if _, exists := r.items[key]; exists {
		return dedupeDuplicate
	}
	if len(r.items) == r.limit || len(key)+r.bytes() > r.byteLimit {
		r.refused++
		return dedupeFull
	}
	r.items[key] = now
	return dedupeAccepted
}

func newCollisionDedupe(limit, bytes int) *dedupeCache {
	cache := newBoundedDedupe(600*time.Second, limit, bytes)
	// Unit fixture only: one bucket forces collisions independently of the
	// production hash. All entries still pass ordinary cache admission.
	cache.items.buckets = make([]*dedupeItem, 1)
	return cache
}

func assertDedupeReference(t *testing.T, cache *dedupeCache, reference *dedupeReference) {
	t.Helper()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.items.Len() != len(reference.items) || cache.bytes != reference.bytes() || cache.refused != reference.refused {
		t.Fatalf("cache/reference count=%d/%d bytes=%d/%d refused=%d/%d", cache.items.Len(), len(reference.items), cache.bytes, reference.bytes(), cache.refused, reference.refused)
	}
	if len(cache.expiry) != cache.items.Len() || cap(cache.expiry) != cache.limit || len(cache.items.buckets) != 1 {
		t.Fatal("primary/expiry count or fixed backing changed")
	}
	heapItems := make(map[*dedupeItem]bool, len(cache.expiry))
	for i, entry := range cache.expiry {
		if entry == nil || heapItems[entry] {
			t.Fatal("nil or duplicate expiry entry")
		}
		heapItems[entry] = true
		admitted, ok := reference.items[entry.key]
		if !ok || entry.value != int64(admitted.Sub(cache.epoch)) || cache.items.Entry(entry.key) != entry {
			t.Fatal("expiry and exact-key index are not bijective")
		}
		if i > 0 && cache.expiry[(i-1)/2].value > entry.value {
			t.Fatal("expiry heap ordering changed")
		}
	}
	for key, admitted := range reference.items {
		entry := cache.items.Entry(key)
		if entry == nil || !heapItems[entry] || entry.value != int64(admitted.Sub(cache.epoch)) {
			t.Fatalf("missing exact key %q", key)
		}
	}
	for _, unused := range cache.expiry[len(cache.expiry):cap(cache.expiry)] {
		if unused != nil {
			t.Fatal("unused heap backing retains a removed entry")
		}
	}
}

func TestDedupeCollisionExpiryModel(t *testing.T) {
	cache := newCollisionDedupe(32, 256)
	reference := &dedupeReference{items: make(map[string]time.Time), ttl: 600 * time.Second, limit: 32, byteLimit: 256}
	epoch := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	var random uint64 = 1
	for operation := 0; operation < 12000; operation++ {
		random = random*6364136223846793005 + 1
		key := fmt.Sprintf("k:%02d", (random>>32)%70)
		if operation%113 == 0 {
			key = "\x00\xff"
		}
		if operation%229 == 0 {
			key = ""
		}
		// Captured arrival times can reach the mutex out of order. The map
		// oracle prunes by absolute timestamps without assuming FIFO arrival.
		now := epoch.Add(time.Duration(operation/3-int(random%31)) * time.Second)
		switch random % 10 {
		case 0:
			cache.prune(now)
			reference.prune(now)
		case 1, 2:
			reference.prune(now)
			_, want := reference.items[key]
			if got := cache.contains(key, now); got != want {
				t.Fatalf("operation%d contains(%q)=%v want%v", operation, key, got, want)
			}
		default:
			if got, want := cache.admit(key, now), reference.admit(key, now); got != want {
				t.Fatalf("operation%d admit(%q)=%d want%d", operation, key, got, want)
			}
		}
		assertDedupeReference(t, cache, reference)
	}
}

func TestDedupeExactExpiryAndCapacity(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	cache := newCollisionDedupe(3, 6)
	for _, key := range []string{"aa", "bb", "cc"} {
		if cache.admit(key, now) != dedupeAccepted {
			t.Fatal("exact capacity refused")
		}
	}
	original := cache.items.Entry("bb")
	for _, offset := range []time.Duration{599 * time.Second, 600*time.Second - time.Nanosecond, 600 * time.Second} {
		if cache.admit("bb", now.Add(offset)) != dedupeDuplicate || cache.items.Entry("bb") != original || original.value != 0 {
			t.Fatal("duplicate renewed age or replaced the entry")
		}
	}
	if cache.admit("dd", now.Add(600*time.Second)) != dedupeFull || cache.items.Len() != 3 {
		t.Fatal("unexpired key evicted at full capacity")
	}
	if cache.admit("bb", now.Add(600*time.Second+time.Nanosecond)) != dedupeAccepted {
		t.Fatal("expired capacity not reclaimed before admission")
	}
	if original.key != "" || original.value != 0 || original.next != nil {
		t.Fatal("popped item retains removed ownership")
	}
	if cache.items.Len() != 1 || cache.bytes != 2 || len(cache.expiry) != 1 {
		t.Fatal("expired collision chain was only partially removed")
	}
	byteBound := newCollisionDedupe(8, 3)
	if byteBound.admit("abc", now) != dedupeAccepted || byteBound.admit("d", now) != dedupeFull || byteBound.admit("abc", now) != dedupeDuplicate {
		t.Fatal("byte capacity or full-cache duplicate behavior changed")
	}
}

func TestDedupeConcurrentCollisionExpiry(t *testing.T) {
	const keyCount, workerCount = 64, 16
	cache := newCollisionDedupe(keyCount, 4096)
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	keys := make([]string, keyCount)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%02d", i)
	}
	run := func(at time.Time, wantAccept uint32) {
		t.Helper()
		var admitted [keyCount]atomic.Uint32
		start := make(chan struct{})
		var workers sync.WaitGroup
		for worker := 0; worker < workerCount; worker++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				<-start
				for i, key := range keys {
					cache.prune(at)
					switch cache.admit(key, at) {
					case dedupeAccepted:
						admitted[i].Add(1)
					case dedupeDuplicate:
					case dedupeFull:
						t.Error("concurrent admission refused within limits")
					}
					if !cache.contains(key, at) {
						t.Error("concurrent exact key disappeared")
					}
					cache.occupancy()
				}
			}()
		}
		close(start)
		workers.Wait()
		for i := range admitted {
			if got := admitted[i].Load(); got != wantAccept {
				t.Fatalf("key%d accepted%d times, want%d", i, got, wantAccept)
			}
		}
	}
	run(now, 1)
	run(now.Add(600*time.Second), 0)
	run(now.Add(601*time.Second), 1)
	reference := &dedupeReference{items: make(map[string]time.Time), ttl: 600 * time.Second, limit: keyCount, byteLimit: 4096}
	for _, key := range keys {
		reference.items[key] = now.Add(601 * time.Second)
	}
	assertDedupeReference(t, cache, reference)
	cache.prune(now.Add(1201 * time.Second))
	assertDedupeReference(t, cache, reference)
	cache.prune(now.Add(1201*time.Second + time.Nanosecond))
	reference.prune(now.Add(1201*time.Second + time.Nanosecond))
	assertDedupeReference(t, cache, reference)
}
