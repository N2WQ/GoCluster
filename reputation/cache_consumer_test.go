package reputation

import (
	"context"
	"fmt"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
)

func cacheConsumerGate(t *testing.T, limit int) *Gate {
	t.Helper()
	gate, err := NewGate(config.ReputationConfig{
		Enabled: true, FallbackTeamCymru: true, LookupCacheMaxEntries: limit,
		LookupCacheTTLSeconds: 60, CymruCacheTTLSeconds: 3600,
		CymruNegativeTTLSeconds: 3600, ReputationDir: t.TempDir(),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Tests below supply their own fixed timestamps; the age-specific case
	// enables that independent consumer rule explicitly. Worker completion uses
	// wall time, so keep its TTL long; short exact boundaries belong in owner tests.
	gate.cfg.ipinfoSnapshotMaxAge = 0
	t.Cleanup(gate.Close)
	return gate
}

func TestLookupCacheGateSharedOwner(t *testing.T) {
	gate := cacheConsumerGate(t, 64)
	if gate.lookupCache.store != gate.cymru.cache.store || gate.lookupCache.namespace != cacheMain || gate.cymru.cache.namespace != cacheCymru {
		t.Fatal("NewGate cache views do not share namespaced owner")
	}
	filled := make([]bool, 32)
	for candidate, count := 1, 0; count < len(filled); candidate++ {
		key := cymruTestAddr(candidate).String()
		shard := gate.lookupCache.shardIndex(key)
		if filled[shard] {
			continue
		}
		filled[shard], count = true, count+1
		gate.lookupCache.set(key, cacheTestValue("main"), cacheTestNow, false)
		gate.cymru.cache.set(key, cacheTestValue("dns"), cacheTestNow, true)
		if got, hit, negative := gate.lookupCache.get(key, cacheTestNow); !hit || negative || got.ASN != "main" {
			t.Fatal("main namespace conflated")
		}
		if got, hit, negative := gate.cymru.cache.get(key, cacheTestNow); !hit || !negative || got.ASN != "dns" {
			t.Fatal("DNS namespace conflated")
		}
	}
	stats := gate.LookupCacheStats()
	if stats.MaxEntries != 64 || stats.Entries != 64 || stats.MainEntries != 32 || stats.CymruEntries != 32 || stats.AllocatedSlots != 64 {
		t.Fatalf("NewGate aggregate stats: %+v", stats)
	}
	cacheTestCheck(t, gate.lookupCache)
	defaults := cacheConsumerGate(t, 0)
	if defaults.LookupCacheStats().MaxEntries != 200000 {
		t.Fatal("gate normalization default changed")
	}
	var absent *Gate
	if absent.LookupCacheStats().Entries != 0 {
		t.Fatal("nil gate stats must be empty")
	}
}

func TestLookupCacheWorkerAndConsumerCompatibility(t *testing.T) {
	for _, success := range []bool{true, false} {
		t.Run(fmt.Sprint(success), func(t *testing.T) {
			gate := cacheConsumerGate(t, 64)
			addr := cymruTestAddr(1)
			want := LookupResult{}
			if success {
				want = LookupResult{ASN: "64500", CountryCode: "US", Source: cymruSource, FetchedAt: cacheTestNow}
			}
			gate.cymru.enqueue(addr, addr.String(), cacheTestNow)
			worker := startCymruTestWorker(t, gate.cymru, want, success)
			worker.await(t)
			worker.finish(t)
			fallbacks := 0
			gate.lookupIPInfoFunc = func(netip.Addr, time.Time) LookupResult { fallbacks++; return LookupResult{} }
			now := time.Now().UTC()
			ipinfo, cymru, country, continent, known := gate.lookupIP(addr, now)
			if success {
				if known != true || ipinfo != (LookupResult{}) || cymru != want || country != "US" || continent != "" || fallbacks != 0 {
					t.Fatalf("worker result tuple changed: %+v %+v %q %q %v", ipinfo, cymru, country, continent, known)
				}
			} else if known || ipinfo != (LookupResult{}) || cymru != (LookupResult{}) || country != "" || continent != "" || fallbacks != 1 {
				t.Fatal("negative worker fallback behavior changed")
			}
			// Positive main-cache hits retain the cached tuple. A DNS negative
			// remains in the DNS namespace while unavailable enrichment stays unknown.
			ipinfo, cymru, country, continent, known = gate.lookupIP(addr, now)
			expectedFallbacks := 0
			if !success {
				expectedFallbacks = 2
			}
			if known != success || ipinfo != want || cymru != (LookupResult{}) || continent != "" || fallbacks != expectedFallbacks {
				t.Fatal("positive main-hit tuple or negative DNS fallback changed")
			}
			if (success && country != "US") || (!success && country != "") {
				t.Fatal("main-hit country changed")
			}
			if pending := cymruPendingCount(gate.cymru); pending != 0 {
				t.Fatalf("worker retained pending ownership: %d", pending)
			}
			gate.lookupCymruFunc = func(netip.Addr, time.Time) (LookupResult, bool, bool) {
				t.Fatal("admission invoked Cymru")
				return LookupResult{}, false, false
			}
			gate.lookupIPInfoFunc = func(netip.Addr, time.Time) LookupResult { t.Fatal("admission invoked API"); return LookupResult{} }
			admission, found := gate.LookupIPForAdmission(addr.String(), now)
			if found != success || admission != want {
				t.Fatal("admission main-hit behavior changed")
			}
			// Evict this main entry using the actual shared shard, then admission
			// must miss rather than introducing network work or reading DNS view.
			shard := gate.lookupCache.shardIndex(addr.String())
			for i, writes := 2, 0; writes < 2; i++ {
				key := cymruTestAddr(i).String()
				if gate.lookupCache.shardIndex(key) == shard {
					gate.lookupCache.set(key, cacheTestValue(key), now, false)
					writes++
				}
			}
			if _, found := gate.LookupIPForAdmission(addr.String(), now); found {
				t.Fatal("admission retained evicted metadata")
			}
			if len(gate.cymru.queue) != 0 {
				t.Fatal("admission enqueued network work")
			}
			cacheTestCheck(t, gate.lookupCache)
		})
	}
}

func TestLookupCacheOversizeConsumers(t *testing.T) {
	gate := cacheConsumerGate(t, 64)
	addr := cymruTestAddr(3)
	want := cacheTestValue(strings.Repeat("1", 1025))
	gate.lookupCymruFunc = func(netip.Addr, time.Time) (LookupResult, bool, bool) { return LookupResult{}, false, false }
	calls := 0
	gate.lookupIPInfoFunc = func(netip.Addr, time.Time) LookupResult { calls++; return want }
	for i := 0; i < 2; i++ {
		ipinfo, cymru, country, continent, known := gate.lookupIP(addr, cacheTestNow)
		if !known || ipinfo != want || cymru != (LookupResult{}) || country != "US" || continent != "NA" {
			t.Fatal("oversized fallback result was changed")
		}
	}
	if stats := gate.LookupCacheStats(); calls != 2 || stats.Entries != 0 || stats.Bypasses != 2 {
		t.Fatalf("oversize fallback cached: calls=%d stats=%+v", calls, stats)
	}
	gate.cymru.enqueue(addr, addr.String(), cacheTestNow)
	worker := startCymruTestWorker(t, gate.cymru, want, true)
	worker.await(t)
	worker.finish(t)
	if _, hit, _ := gate.cymru.cache.get(addr.String(), time.Now().UTC()); hit {
		t.Fatal("oversized worker result cached")
	}
	if stats := gate.LookupCacheStats(); stats.Bypasses != 3 || stats.Entries != 0 {
		t.Fatalf("oversize worker stats: %+v", stats)
	}
}

func TestLookupCacheOversizeAdmissionStoreResult(t *testing.T) {
	directory := t.TempDir()
	csvPath := filepath.Join(directory, "location.csv")
	largeASN := strings.Repeat("1", 1025)
	writeSnapshot(t, csvPath, []string{"start_ip,end_ip,country_code,asn", "203.0.113.0,203.0.113.255,US," + largeASN})
	dbPath, err := buildIPInfoPebble(context.Background(), csvPath, filepath.Join(directory, "pebble"), false)
	if err != nil {
		t.Fatal(err)
	}
	gate := cacheConsumerGate(t, 64)
	store, err := openIPInfoStore(dbPath, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	gate.ipinfoStore = store
	gate.lookupCymruFunc = func(netip.Addr, time.Time) (LookupResult, bool, bool) {
		t.Fatal("admission invoked Cymru")
		return LookupResult{}, false, false
	}
	gate.lookupIPInfoFunc = func(netip.Addr, time.Time) LookupResult { t.Fatal("admission invoked API"); return LookupResult{} }
	want, _ := gate.lookupIPInfoStore(netip.MustParseAddr("203.0.113.10"))
	if want.Source == "" || want.ASN != largeASN {
		t.Fatal("oversized store fixture did not preserve input")
	}
	for i := 0; i < 2; i++ {
		if got, found := gate.LookupIPForAdmission("203.0.113.10", cacheTestNow); !found || got != want {
			t.Fatal("oversized admission result changed")
		}
	}
	if stats := gate.LookupCacheStats(); stats.Entries != 0 || stats.Bypasses != 2 || stats.OwnedStringBytes != 0 {
		t.Fatalf("oversized admission result cached: %+v", stats)
	}
	if len(gate.cymru.queue) != 0 {
		t.Fatal("admission enqueued network work")
	}
}

func TestLookupCacheSnapshotAgeStillRejectsMainHit(t *testing.T) {
	gate := cacheConsumerGate(t, 64)
	gate.cfg.ipinfoSnapshotMaxAge = time.Minute
	addr := cymruTestAddr(9)
	stale := cacheTestValue("stale")
	stale.FetchedAt = cacheTestNow.Add(-time.Minute - time.Nanosecond)
	gate.lookupCache.set(addr.String(), stale, cacheTestNow, false)
	fresh := cacheTestValue("fresh")
	gate.lookupCymruFunc = func(netip.Addr, time.Time) (LookupResult, bool, bool) { return LookupResult{}, false, false }
	gate.lookupIPInfoFunc = func(netip.Addr, time.Time) LookupResult { return fresh }
	if result, _, _, _, known := gate.lookupIP(addr, cacheTestNow); !known || result != fresh {
		t.Fatal("stale snapshot main hit bypassed age check")
	}
}

func TestLookupCacheIPInfoEpochStaysWithinSharedCap(t *testing.T) {
	const cap = 8
	gate := cacheConsumerGate(t, cap)
	for epoch := uint64(1); epoch <= 128; epoch++ {
		key := cymruTestAddr(int(epoch)).String()
		current := LookupResult{Source: ipinfoSource, ASN: "64500"}
		older := LookupResult{Source: ipinfoSource, ASN: "64501"}
		gate.lookupCache.setEntry(key, cacheTestNow, cacheEntry{value: current, ipinfoEpoch: epoch, localIPInfo: true})
		gate.lookupCache.setEntry(key, cacheTestNow, cacheEntry{value: older, ipinfoEpoch: epoch - 1, localIPInfo: true})
		var origin ipinfoCacheOrigin
		if value, ok, _ := gate.lookupCache.getWithIPInfo(key, cacheTestNow, &origin); !ok || value != current || origin.epoch != epoch || !origin.local {
			t.Fatalf("retired generation displaced the current entry: value=%+v provenance=%+v", value, origin)
		}
		gate.cymru.cache.set(key, current, cacheTestNow, false)
	}
	cacheTestCheck(t, gate.lookupCache)
}
