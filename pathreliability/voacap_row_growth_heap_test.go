package pathreliability

import (
	"encoding/json"
	"os"
	"runtime"
	"testing"
	"time"
	"unsafe"
)

// This opt-in measurement isolates the live forecast owner's eight-to-24-row
// growth. The identical 24-row paired hydration fixture separately measures the
// complete disk-index owner against the frozen baseline. Neither is a process
// memory ceiling, and a capped slice is not evidence of compact backing storage.
func TestVOACAPForecastRowRetainedHeapGrowth(t *testing.T) {
	output := os.Getenv("GOCLUSTER_VOACAP_ROW_GROWTH_OUTPUT")
	if output == "" {
		t.Skip("physical eight-to-24-row retained heap measurement is opt-in")
	}
	type observation struct {
		Hours        int   `json:"hours"`
		Entries      int   `json:"entries"`
		RowBytes     int   `json:"row_bytes"`
		LogicalBytes int64 `json:"logical_payload_bytes"`
		Retained     int64 `json:"retained_live_owner_bytes"`
	}
	var results []observation
	for _, hours := range []int{8, 24} {
		const entries = 50000
		now := time.Date(2026, 6, 8, 20, 0, 0, 0, time.UTC)
		cfg := testVOACAPFallbackConfig()
		cfg.VOACAPFallback.MaxCacheEntries = entries
		cfg.VOACAPFallback.ForecastHours = hours
		store := openTestVOACAPForecastCacheStore(t)
		f := newTestClosedFallback(t, cfg, fakeClosedForecaster{}, fixedSSNProvider{ssn: 112})
		if _, err := f.AttachForecastCacheStore(store, now); err != nil {
			t.Fatal(err)
		}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := 0; i < entries; i++ {
			// Allocate the actual owned backing array; slicing a 24-row array
			// down to eight would falsify the physical-growth comparison.
			rows := make([]VOACAPHourlyForecast, hours)
			for h := range rows {
				rows[h] = VOACAPHourlyForecast{HourUTC: (now.Hour() + h) % 24, FrequencyMHz: 14.1}
			}
			entry := testFrozenCacheEntry(voacapCacheEntry{storedAt: now, forecast: VOACAPClosedForecast{WindowStartUTC: now, Records: rows}}, hours, 86400)
			key, _ := liveQualificationKeys(i)
			f.addCacheLocked(key, entry)
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(f)
		rowBytes := int(unsafe.Sizeof(VOACAPHourlyForecast{}))
		results = append(results, observation{hours, entries, rowBytes, int64(entries * hours * rowBytes), int64(after.HeapAlloc) - int64(before.HeapAlloc)})
		closeTestVOACAPForecastCacheStore(t, store)
	}
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, data, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("50000-key actual live owner: 8 rows=%d bytes; 24 rows=%d bytes; growth=%d bytes", results[0].Retained, results[1].Retained, results[1].Retained-results[0].Retained)
}
