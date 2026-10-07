package uls

import "testing"

func BenchmarkLookupWarm(b *testing.B) {
	defer SetLicenseDBPath("")
	SetLicenseDBPath(fixtureDB(b, false))
	LookupUS("K1ABC")
	b.ReportAllocs()
	b.ResetTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		LookupUS("K1ABC")
	}
}
func BenchmarkLookupCold(b *testing.B) {
	defer SetLicenseDBPath("")
	SetLicenseDBPath(fixtureDB(b, false))
	LookupUS("K1ABC")
	b.ReportAllocs()
	b.ResetTimer()
	defer b.StopTimer()
	for i := 0; i < b.N; i++ {
		cache := licenseCache.Load()
		cache.mu.Lock()
		entry := cache.entries[licenseCacheKey{call: "K1ABC"}]
		if entry != nil {
			cache.deleteEntryLocked(licenseCacheKey{call: "K1ABC"}, entry)
		}
		cache.mu.Unlock()
		LookupUS("K1ABC")
	}
}
