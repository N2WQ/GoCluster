package reputation

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
)

var cacheTestNow = time.Unix(1700000000, 0).UTC()

func cacheTestViews(shards, limit int) (*ttlCache, *ttlCache) {
	store := newLookupCacheStore(shards, limit)
	return &ttlCache{store: store, namespace: cacheMain, ttl: time.Minute, negativeTT: time.Second},
		&ttlCache{store: store, namespace: cacheCymru, ttl: time.Hour, negativeTT: 2 * time.Second}
}

func cacheTestValue(tag string) LookupResult {
	return LookupResult{ASN: tag, CountryCode: "US", CountryName: "United States", ContinentCode: "NA", Source: "fixture", FetchedAt: cacheTestNow}
}

func cacheTestStrings(key string, value LookupResult) []string {
	return []string{key, value.ASN, value.CountryCode, value.CountryName, value.ContinentCode, value.Source}
}

func cacheTestBytes(key string, value LookupResult) int {
	n := 0
	for _, field := range cacheTestStrings(key, value) {
		n += len(field)
	}
	return n
}

// Walk independently of snapshot counters: every used slot must belong exactly
// once to the FIFO or free chain, and live slots must match map membership.
func cacheTestInvariant(store *lookupCacheStore) error {
	for i := range store.shards {
		store.shards[i].mu.Lock()
	}
	defer func() {
		for i := len(store.shards) - 1; i >= 0; i-- {
			store.shards[i].mu.Unlock()
		}
	}()
	total, quotas := 0, 0
	for i := range store.shards {
		s := &store.shards[i]
		quotas += s.quota
		total += len(s.items)
		if s.quota < 1 || len(s.items) > s.quota || s.used > len(s.slots) || cap(s.slots) > s.quota {
			return fmt.Errorf("shard %d invalid quota/items/used/backing: %d/%d/%d/%d", i, s.quota, len(s.items), s.used, cap(s.slots))
		}
		if len(s.slots) != 0 && len(s.slots) != s.quota {
			return fmt.Errorf("shard %d allocated %d slots for quota %d", i, len(s.slots), s.quota)
		}
		seen := make([]bool, s.used)
		live, bytes, previous := 0, 0, cacheNoSlot
		var counts [cacheNamespaceCount]int
		for at := s.head; at != cacheNoSlot; at = s.slots[at].next {
			if at < 0 || at >= s.used || seen[at] {
				return fmt.Errorf("shard %d FIFO invalid/cyclic index %d", i, at)
			}
			seen[at] = true
			slot := s.slots[at]
			mapped, ok := s.items[slot.key]
			if !ok || mapped != at || slot.prev != previous || slot.key.namespace >= cacheNamespaceCount {
				return fmt.Errorf("shard %d FIFO/map/previous mismatch at %d", i, at)
			}
			counts[slot.key.namespace]++
			bytes += cacheTestBytes(slot.key.address, slot.entry.value)
			if cacheTestBytes(slot.key.address, slot.entry.value) > 1024 {
				return fmt.Errorf("shard %d overlarge retained record", i)
			}
			live++
			previous = at
		}
		if previous != s.tail || live != len(s.items) || counts != s.counts || bytes != s.ownedBytes {
			return fmt.Errorf("shard %d FIFO tail/count/bytes mismatch", i)
		}
		for at := s.free; at != cacheNoSlot; at = s.slots[at].next {
			if at < 0 || at >= s.used || seen[at] {
				return fmt.Errorf("shard %d free invalid/cyclic index %d", i, at)
			}
			seen[at] = true
			slot := s.slots[at]
			if slot.key != (cacheKey{}) || slot.entry != (cacheEntry{}) || slot.prev != cacheNoSlot {
				return fmt.Errorf("shard %d free slot %d retains key/value/previous", i, at)
			}
		}
		for at, visited := range seen {
			if !visited {
				return fmt.Errorf("shard %d orphan slot %d", i, at)
			}
		}
	}
	if quotas != store.maxEntries || total > store.maxEntries {
		return fmt.Errorf("aggregate quotas=%d occupancy=%d max=%d", quotas, total, store.maxEntries)
	}
	return nil
}

func cacheTestCheck(t *testing.T, c *ttlCache) {
	t.Helper()
	if err := cacheTestInvariant(c.store); err != nil {
		t.Fatal(err)
	}
}

func TestLookupCacheQuotasAndSkew(t *testing.T) {
	for _, limit := range []int{1, 2, 31, 32, 33, 50000} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			main, dns := cacheTestViews(32, limit)
			if len(main.store.shards) != min(32, limit) {
				t.Fatal("wrong shard count")
			}
			cacheTestCheck(t, main)
			quota := main.store.shards[0].quota
			admitted := 0
			for candidate := 0; admitted < quota+3; candidate++ {
				key := fmt.Sprintf("skew-%d", candidate)
				if main.shardIndex(key) != 0 {
					continue
				}
				view := main
				if admitted%2 != 0 {
					view = dns
				}
				view.set(key, cacheTestValue(key), cacheTestNow, false)
				admitted++
			}
			stats := main.store.snapshot()
			if stats.Entries != quota || stats.Evictions != 3 || stats.AllocatedSlots != quota {
				t.Fatalf("skew stats: %+v", stats)
			}
			cacheTestCheck(t, main)
		})
	}
	for _, args := range [][2]int{{0, 0}, {-1, -1}} {
		store := newLookupCacheStore(args[0], args[1])
		if len(store.shards) != 16 || store.maxEntries != 100000 {
			t.Fatal("constructor fallback changed")
		}
	}
}

func TestLookupCacheNamespacesAndWriteFIFO(t *testing.T) {
	main, dns := cacheTestViews(1, 3)
	main.set("same", cacheTestValue("main"), cacheTestNow, false)
	dns.set("same", cacheTestValue("dns"), cacheTestNow, true)
	main.set("third", cacheTestValue("third"), cacheTestNow, false)
	if got, ok, negative := dns.get("same", cacheTestNow); !ok || !negative || got.ASN != "dns" {
		t.Fatal("namespace value/negative flag lost")
	}
	if got, ok, negative := main.get("same", cacheTestNow); !ok || negative || got.ASN != "main" {
		t.Fatal("main value/negative flag lost")
	}
	main.set("fourth", cacheTestValue("fourth"), cacheTestNow, false)
	if _, ok, _ := main.get("same", cacheTestNow); ok {
		t.Fatal("read refreshed write FIFO")
	}
	// A write refreshes the DNS entry; third becomes oldest across namespaces.
	dns.set("same", cacheTestValue("updated"), cacheTestNow, false)
	main.set("fifth", cacheTestValue("fifth"), cacheTestNow, false)
	if _, ok, _ := main.get("third", cacheTestNow); ok {
		t.Fatal("write failed to refresh FIFO")
	}
	if got, ok, negative := dns.get("same", cacheTestNow); !ok || negative || got.ASN != "updated" {
		t.Fatal("replacement did not update result/flag")
	}
	cacheTestCheck(t, main)
}

func TestLookupCacheTTLContract(t *testing.T) {
	main, dns := cacheTestViews(1, 8)
	for _, tc := range []struct {
		view     *ttlCache
		negative bool
		ttl      time.Duration
	}{{main, false, time.Minute}, {main, true, time.Second}, {dns, false, time.Hour}, {dns, true, 2 * time.Second}} {
		name := fmt.Sprintf("namespace%d/negative%v", tc.view.namespace, tc.negative)
		t.Run(name, func(t *testing.T) {
			want := cacheTestValue(name)
			tc.view.set(name, want, cacheTestNow, tc.negative)
			for _, delta := range []time.Duration{tc.ttl - time.Nanosecond, tc.ttl} {
				if got, hit, negative := tc.view.get(name, cacheTestNow.Add(delta)); !hit || negative != tc.negative || got != want {
					t.Fatal("entry must remain valid before and at deadline")
				}
			}
			if got, hit, negative := tc.view.get(name, cacheTestNow.Add(tc.ttl+time.Nanosecond)); hit || negative || got != (LookupResult{}) {
				t.Fatal("expired result not removed")
			}
			cacheTestCheck(t, tc.view)
		})
	}
	main.ttl, main.negativeTT = 0, -time.Second
	for _, negative := range []bool{false, true} {
		main.set("no-expiry", cacheTestValue("forever"), cacheTestNow, negative)
		if _, hit, gotNegative := main.get("no-expiry", cacheTestNow.AddDate(100, 0, 0)); !hit || gotNegative != negative {
			t.Fatal("nonpositive TTL must remain nonexpiring")
		}
	}
	var nilCache *ttlCache
	nilCache.set("x", LookupResult{}, cacheTestNow, false)
	for _, c := range []*ttlCache{nilCache, main} {
		for _, key := range []string{"", " \t\n"} {
			c.set(key, cacheTestValue("blank"), cacheTestNow, false)
			if _, hit, negative := c.get(key, cacheTestNow); hit || negative {
				t.Fatal("nil or blank key must miss")
			}
		}
	}
}

func TestLookupCacheSlotChurn(t *testing.T) {
	main, dns := cacheTestViews(1, 7)
	for i := 0; i < 700; i++ {
		view := main
		if i%2 != 0 {
			view = dns
		}
		key := fmt.Sprintf("key-%d", i%11)
		now := cacheTestNow.Add(time.Duration(i) * time.Second)
		view.set(key, cacheTestValue(key), now, i%3 == 0)
		if i%4 == 0 {
			view.get(key, now.Add(2*time.Hour))
		}
		if i%5 == 0 {
			view.set(key, cacheTestValue("refresh"), now, false)
		}
		cacheTestCheck(t, main)
	}
}

func TestLookupCacheExpiryUnlinksEveryPosition(t *testing.T) {
	for _, expired := range []string{"head", "middle", "tail"} {
		t.Run(expired, func(t *testing.T) {
			main, _ := cacheTestViews(1, 3)
			for _, key := range []string{"head", "middle", "tail"} {
				main.set(key, cacheTestValue(key), cacheTestNow, key == expired)
			}
			main.get(expired, cacheTestNow.Add(2*time.Second))
			cacheTestCheck(t, main)
			main.set(expired, cacheTestValue("reinserted"), cacheTestNow, false)
			cacheTestCheck(t, main)
			if got, hit, negative := main.get(expired, cacheTestNow.Add(2*time.Second)); !hit || negative || got.ASN != "reinserted" {
				t.Fatal("reinserted expiry slot has wrong value")
			}
			if stats := main.store.snapshot(); stats.Entries != 3 || stats.Evictions != 0 || stats.Expirations != 1 {
				t.Fatalf("expiry/reuse stats: %+v", stats)
			}
		})
	}
}

func TestLookupCacheOwnedStrings(t *testing.T) {
	main, _ := cacheTestViews(1, 2)
	backing := strings.Repeat("x", 1<<20)
	part := backing[100:108]
	value := LookupResult{ASN: part, CountryCode: part, CountryName: part, ContinentCode: part, Source: part, FetchedAt: cacheTestNow}
	main.set(part, value, cacheTestNow, false)
	got, hit, _ := main.get(part, cacheTestNow)
	if !hit || got != value {
		t.Fatal("owned copy changed value")
	}
	shard := &main.store.shards[0]
	ownedKey := shard.slots[shard.items[cacheKey{namespace: cacheMain, address: part}]].key.address
	fields := cacheTestStrings(ownedKey, got)
	start := uintptr(unsafe.Pointer(unsafe.StringData(backing)))
	for i, field := range fields {
		pointer := uintptr(unsafe.Pointer(unsafe.StringData(field)))
		if pointer >= start && pointer < start+uintptr(len(backing)) {
			t.Fatalf("field %d retains source backing", i)
		}
		for j := 0; j < i; j++ {
			if unsafe.StringData(field) == unsafe.StringData(fields[j]) {
				t.Fatalf("fields %d and %d share owned allocation", i, j)
			}
		}
	}
	if allocations := testing.AllocsPerRun(100, func() { main.set(part, value, cacheTestNow, false) }); allocations != 0 {
		t.Fatalf("unchanged replacement allocated: %v", allocations)
	}
	changedBacking := strings.Repeat("y", 1<<20)
	value.ASN = changedBacking[100:108]
	main.set(part, value, cacheTestNow, false)
	updated, _, _ := main.get(part, cacheTestNow)
	if updated != value || unsafe.StringData(updated.ASN) == unsafe.StringData(value.ASN) {
		t.Fatal("changed field not copied exactly")
	}
	if unsafe.StringData(updated.Source) != unsafe.StringData(got.Source) {
		t.Fatal("unchanged owned field unnecessarily replaced")
	}
	runtime.KeepAlive(backing)
	runtime.KeepAlive(changedBacking)
	runtime.KeepAlive(fields)
	cacheTestCheck(t, main)
}

func TestLookupCacheOversizeBypass(t *testing.T) {
	t.Run("cumulative", func(t *testing.T) {
		main, _ := cacheTestViews(1, 2)
		for _, total := range []int{1023, 1024, 1025} {
			// Neither individual field is large. Admission must budget all
			// fields together, including the three address-key bytes.
			value := LookupResult{ASN: strings.Repeat("a", 500), CountryName: strings.Repeat("b", total-503)}
			main.set("key", value, cacheTestNow, false)
			got, hit, _ := main.get("key", cacheTestNow)
			if hit != (total <= 1024) || hit && got != value {
				t.Fatalf("cumulative %d-byte admission: hit=%v", total, hit)
			}
			cacheTestCheck(t, main)
		}
	})
	for field := 0; field < 6; field++ {
		t.Run(fmt.Sprint(field), func(t *testing.T) {
			main, _ := cacheTestViews(1, 2)
			key := "key"
			value := LookupResult{}
			fields := []*string{&key, &value.ASN, &value.CountryCode, &value.CountryName, &value.ContinentCode, &value.Source}
			length := 1024 - len(key)
			if field == 0 {
				length = 1024
			}
			*fields[field] = strings.Repeat("x", length)
			main.set(key, value, cacheTestNow, false)
			if got, hit, _ := main.get(key, cacheTestNow); !hit || got != value {
				t.Fatal("exact 1024-byte record bypassed")
			}
			*fields[field] += "x"
			main.set(key, value, cacheTestNow, false)
			if _, hit, _ := main.get(key, cacheTestNow); hit {
				t.Fatal("oversized record cached")
			}
			if main.store.snapshot().Bypasses != 1 {
				t.Fatal("bypass not counted")
			}
			cacheTestCheck(t, main)
		})
	}
	main, dns := cacheTestViews(1, 2)
	main.set("same", cacheTestValue("main"), cacheTestNow, false)
	dns.set("same", cacheTestValue("dns"), cacheTestNow, true)
	large := LookupResult{ASN: strings.Repeat("z", 1025)}
	main.set("new", large, cacheTestNow, false)
	if main.store.snapshot().Entries != 2 {
		t.Fatal("oversized new input evicted existing data")
	}
	main.set("same", large, cacheTestNow, false)
	if _, hit, _ := main.get("same", cacheTestNow); hit {
		t.Fatal("oversized update retained stale own value")
	}
	if got, hit, negative := dns.get("same", cacheTestNow); !hit || !negative || got.ASN != "dns" {
		t.Fatal("oversized update removed namespace sibling")
	}
	stats := main.store.snapshot()
	if stats.Entries != 1 || stats.Bypasses != 2 || stats.Evictions != 0 || stats.Expirations != 0 {
		t.Fatalf("oversize stats: %+v", stats)
	}
	main.set("reuse", cacheTestValue("reuse"), cacheTestNow, false)
	cacheTestCheck(t, main)
}

func TestLookupCacheConcurrentInvariants(t *testing.T) {
	main, dns := cacheTestViews(32, 33)
	start := make(chan struct{})
	errors := make(chan error, 1)
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Go(func() {
			<-start
			for j := 0; j < 500; j++ {
				view := main
				if (worker+j)%2 != 0 {
					view = dns
				}
				key := fmt.Sprintf("198.18.%d.%d", worker, j%256)
				now := cacheTestNow.Add(time.Duration(j) * time.Second)
				view.set(key, cacheTestValue(key), now, j%3 == 0)
				view.get(key, now.Add(2*time.Second))
				if j%17 == 0 {
					view.set(strings.Repeat("b", 1025), LookupResult{}, now, false)
				}
			}
		})
	}
	workers.Go(func() {
		<-start
		for i := 0; i < 200; i++ {
			stats := main.store.snapshot()
			entries, mainEntries, dnsEntries, slots, bytes := 0, 0, 0, 0, 0
			for _, shard := range stats.Shards {
				entries += shard.Entries
				mainEntries += shard.MainEntries
				dnsEntries += shard.CymruEntries
				slots += shard.AllocatedSlots
				bytes += shard.OwnedStringBytes
				if shard.Entries > shard.MaxEntries || shard.AllocatedSlots > shard.MaxEntries {
					select {
					case errors <- fmt.Errorf("shard snapshot exceeded quota"):
					default:
					}
					return
				}
			}
			if stats.Entries != entries || stats.MainEntries != mainEntries || stats.CymruEntries != dnsEntries || stats.AllocatedSlots != slots || stats.OwnedStringBytes != bytes || entries > 33 || entries != mainEntries+dnsEntries {
				select {
				case errors <- fmt.Errorf("incoherent snapshot %+v", stats):
				default:
				}
				return
			}
			if err := cacheTestInvariant(main.store); err != nil {
				select {
				case errors <- err:
				default:
				}
				return
			}
		}
	})
	close(start)
	workers.Wait()
	select {
	case err := <-errors:
		t.Fatal(err)
	default:
	}
	cacheTestCheck(t, main)
}

func TestLookupCacheSnapshotCounters(t *testing.T) {
	main, _ := cacheTestViews(1, 2)
	main.set("a", cacheTestValue("a"), cacheTestNow, false)
	main.set("b", cacheTestValue("b"), cacheTestNow, false)
	main.set("c", cacheTestValue("c"), cacheTestNow.Add(2*time.Minute), false)
	// An expired FIFO victim is still a capacity eviction; only get expires.
	main.get("b", cacheTestNow.Add(2*time.Minute))
	main.set("c", LookupResult{ASN: strings.Repeat("z", 1025)}, cacheTestNow, false)
	before := main.store.snapshot()
	if before.Entries != 0 || before.Evictions != 1 || before.Expirations != 1 || before.Bypasses != 1 || before.AllocatedSlots != 2 || before.OwnedStringBytes != 0 {
		t.Fatalf("counter attribution: %+v", before)
	}
	before.Shards[0].Entries = 999
	if after := main.store.snapshot(); after.Shards[0].Entries != 0 {
		t.Fatal("snapshot exposes mutable owner data")
	}
	main.set("read-only", cacheTestValue("read-only"), cacheTestNow, false)
	main.store.snapshot()
	if _, hit, _ := main.get("read-only", cacheTestNow.Add(time.Minute)); !hit {
		t.Fatal("snapshot changed cache expiry")
	}
	cacheTestCheck(t, main)
}
