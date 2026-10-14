package uls

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDXLicenseRejectionPolicy(t *testing.T) {
	allowlistPath := filepath.Join(t.TempDir(), "allowlist.txt")
	if err := os.WriteFile(allowlistPath, []byte("US:K1ALLOW\nADIF1:VE3ALLOW\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	SetAllowlistPath(allowlistPath)
	t.Cleanup(func() { SetAllowlistPath(""); SetLicenseChecksEnabled(true); SetCanadianLicenseChecksEnabled(true) })
	for _, tc := range []struct {
		name, call      string
		adif            int
		enabled, beacon bool
		result          LookupResult
		want            bool
	}{
		{"us_absent", "K1ABC", 291, true, false, LookupResult{Available: true}, true},
		{"ca_absent", "VE3ABC", 1, true, false, LookupResult{Available: true}, true},
		{"us_disabled", "K1ABC", 291, false, false, LookupResult{Available: true}, false},
		{"ca_disabled", "VE3ABC", 1, false, false, LookupResult{Available: true}, false},
		{"unavailable", "K1ABC", 291, true, false, LookupResult{}, false},
		{"found", "K1ABC", 291, true, false, LookupResult{Available: true, Found: true}, false},
		{"beacon", "K1ABC/B", 291, true, true, LookupResult{Available: true}, false},
		{"other_entity", "G1ABC", 223, true, false, LookupResult{Available: true}, false},
		{"us_portable_allowlist", "VE3/K1ALLOW-#", 291, true, false, LookupResult{Available: true}, false},
		{"ca_portable_allowlist", "W1/VE3ALLOW/P", 1, true, false, LookupResult{Available: true}, false},
		{"wrong_jurisdiction", "K1ALLOW", 1, true, false, LookupResult{Available: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			SetLicenseChecksEnabled(tc.enabled)
			SetCanadianLicenseChecksEnabled(tc.enabled)
			if got := ShouldRejectDXLicense(tc.adif, tc.call, tc.beacon, tc.result); got != tc.want {
				t.Fatalf("reject=%t want %t", got, tc.want)
			}
		})
	}
}

func TestHistoricalLookupSeparatesDateAndCacheLifetime(t *testing.T) {
	SetCanadianLicenseDBPath(canadianLookupFixture(t))
	t.Cleanup(func() { SetCanadianLicenseDBPath("") })
	processAt := time.Date(2028, 1, 15, 12, 0, 0, 0, time.UTC)
	activeAt := time.Date(2026, 10, 7, 23, 59, 59, 0, time.UTC)
	cacheClock := func() time.Time { return processAt }
	eligibilityClock := func() time.Time { return activeAt }
	if got := lookupSourceWithClocks(canadianSnapshot, "CG3ABC", cacheClock, eligibilityClock); !got.Available || !got.Found {
		t.Fatalf("historical membership: %+v", got)
	}
	cache := licenseCache.Load()
	key := licenseCacheKey{source: canadianSource, call: "CG3ABC"}
	cache.mu.Lock()
	entry := *cache.entries[key]
	cache.mu.Unlock()
	if !entry.at.Equal(processAt) || entry.day != canadianLookupDay(activeAt) {
		t.Fatalf("clocks conflated: %+v", entry)
	}
	if _, ok := cache.get(key, canadianSnapshot.generation.Load(), entry.day, processAt.Add(cache.ttl+time.Nanosecond)); ok {
		t.Fatal("historical eligibility bypassed process TTL")
	}
	// A date boundary and reversal must invalidate positive/negative facts but
	// replace one bounded key, rather than retaining one authority per date.
	for _, at := range []time.Time{activeAt.Add(time.Second), activeAt, activeAt.Add(time.Second), activeAt} {
		got := LookupForADIFAt(1, "CG3ABC", at)
		wantFound := at.Equal(activeAt)
		if !got.Available || got.Found != wantFound {
			t.Fatalf("at=%s fact=%+v wantFound=%t", at, got, wantFound)
		}
	}
	cache.mu.Lock()
	count := 0
	for k := range cache.entries {
		if k.source == canadianSource {
			count++
		}
	}
	cache.mu.Unlock()
	if count != 1 {
		t.Fatalf("historical date churn retained %d Canadian keys", count)
	}
	if got := LookupForADIFAt(1, "CG3ABC", time.Time{}); got.Available {
		t.Fatal("zero explicit historical time invented authority")
	}
}

func TestHistoricalLookupProcessMidnightAndAuthorityBarriers(t *testing.T) {
	SetCanadianLicenseDBPath(canadianLookupFixture(t))
	t.Cleanup(func() { SetCanadianLicenseDBPath("") })
	historical := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	process := time.Date(2028, 1, 1, 23, 59, 59, 0, time.UTC)
	eligibilityClock := func() time.Time { return historical }
	for _, delta := range []time.Duration{0, 2 * time.Second} {
		if got := lookupSourceWithClocks(canadianSnapshot, "CG3ABC", func() time.Time { return process.Add(delta) }, eligibilityClock); !got.Found {
			t.Fatalf("process midnight changed historical authority: %+v", got)
		}
	}
	cache, generation := licenseCache.Load(), canadianSnapshot.generation.Load()
	day := canadianLookupDay(historical)
	ResetCanadianLicenseDB()
	if got := finishLookup(canadianSnapshot, cache, licenseCacheKey{source: canadianSource, call: "CG3DETACHED"}, generation, day,
		LookupResult{Available: true, Found: true}, process, eligibilityClock); got.Available {
		t.Fatal("detached historical query escaped generation barrier")
	}
	// The warm-result barrier must still reject a changing eligibility day;
	// process midnight was deliberately harmless in the control above.
	calls := 0
	if got := lookupSourceWithClocks(canadianSnapshot, "CG3ABC", func() time.Time { return process }, func() time.Time {
		calls++
		if calls == 1 {
			return historical
		}
		return historical.Add(24 * time.Hour)
	}); got.Available {
		t.Fatal("in-flight historical date change returned old-day authority")
	}
}

func TestHistoricalLookupInclusiveUTCDateBoundaries(t *testing.T) {
	SetCanadianLicenseDBPath(canadianLookupFixture(t))
	t.Cleanup(func() { SetCanadianLicenseDBPath("") })
	start := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		at    time.Time
		found bool
	}{
		{start.Add(-time.Nanosecond), false},
		{start, true},
		{start.Add(24*time.Hour - time.Nanosecond), true},
		{start.Add(24 * time.Hour), false},
		{start.In(time.FixedZone("west", -4*60*60)), true},
	} {
		got := LookupForADIFAt(1, "CG3ABC", tc.at)
		if !got.Available || got.Found != tc.found {
			t.Fatalf("at=%s fact=%+v wantFound=%t", tc.at, got, tc.found)
		}
	}
}
