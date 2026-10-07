// File role: Owns FCC lookup snapshots and their bounded result cache. Admission
// and address enrichment share factual results; enforcement is a separate flag.
package uls

import (
	"context"
	"database/sql"
	"dxcluster/spot"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

// CurrentSchemaVersion marks the FCC projection that includes mailing state.
const CurrentSchemaVersion = 1

// LookupResult separates confirmed membership from reference-data unavailability.
// State is a registered mailing-address code, never an inferred operating site.
type LookupResult struct {
	Available, Found bool
	State            string
}

// LookupStatsSnapshot describes the current bounded cache and schema capability.
type LookupStatsSnapshot struct {
	Entries, Slots, Capacity int
	TTL                      time.Duration
	Generation               uint64
	StateCapable, Refreshing bool
}

var (
	licenseDBPath    string
	licenseDB        *sql.DB
	licenseMu        sync.Mutex
	licenseEnabled   atomic.Bool
	refreshActive    atomic.Bool
	loggedDBError    atomic.Bool
	licenseCacheTTL  atomic.Int64
	licenseCache     atomic.Pointer[ttlCache]
	lookupGeneration atomic.Uint64
	stateCapable     atomic.Bool
)

func init() {
	licenseEnabled.Store(true)
	licenseCacheTTL.Store(int64(defaultLicenseCacheTTL))
	resetLicenseCache()
}

// SetLicenseChecksEnabled changes admission enforcement, not reference lookup.
func SetLicenseChecksEnabled(enabled bool) { licenseEnabled.Store(enabled) }

// LicenseChecksEnabled reports admission enforcement independently of enrichment.
func LicenseChecksEnabled() bool { return licenseEnabled.Load() }

// SetLicenseCacheTTL configures the existing bounded lookup cache expiry.
func SetLicenseCacheTTL(ttl time.Duration) {
	if ttl <= 0 {
		ttl = defaultLicenseCacheTTL
	}
	licenseCacheTTL.Store(int64(ttl))
	ResetLicenseDB()
}

// SetRefreshInProgress marks the fail-open publication/build interval.
func SetRefreshInProgress(active bool) { refreshActive.Store(active) }

// RefreshInProgress lets diagnostics avoid opening the DB during replacement.
func RefreshInProgress() bool { return refreshActive.Load() }

// SetLicenseDBPath retains missing paths so first-build publication can rearm
// lookup. Handle/path transitions share one mutex; SQL work uses a captured DB.
func SetLicenseDBPath(path string) {
	clean := strings.TrimSpace(path)
	if clean != "" {
		if abs, err := filepath.Abs(clean); err == nil {
			clean = abs
		}
	}
	licenseMu.Lock()
	old := licenseDB
	licenseDB = nil
	licenseDBPath = clean
	stateCapable.Store(false)
	resetLicenseCache()
	licenseMu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	loggedDBError.Store(false)
}

// IsLicensedUS preserves the existing fail-open admission API.
func IsLicensedUS(call string) bool {
	if !LicenseChecksEnabled() {
		return true
	}
	result := LookupUS(call)
	return !result.Available || result.Found
}

// LookupUS performs one indexed query on a cold lookup. Only definitive
// results enter the existing capped cache; outages cannot invent a license.
func LookupUS(call string) LookupResult {
	if refreshActive.Load() {
		return LookupResult{}
	}
	canonical := NormalizeForLicense(call)
	if canonical == "" {
		return LookupResult{}
	}
	now := time.Now().UTC()
	cache := licenseCache.Load()

	if cache != nil {
		if result, ok := cache.get(canonical, now); ok {
			if cache != licenseCache.Load() || refreshActive.Load() {
				return LookupResult{}
			}
			return result
		}
	}
	db, hasState := getLicenseDB()
	if db == nil {
		return LookupResult{}
	}
	query := "SELECT '' FROM AM WHERE call_sign = ? LIMIT 1;"
	if hasState {
		query = "SELECT state FROM AM WHERE call_sign = ? LIMIT 1;"
	}
	result := queryLicense(db, query, canonical)
	return finishLookup(cache, canonical, result, now)
}

// finishLookup is the generation publication barrier shared by cold lookups.
// An old owner is harmless after reset, but its result must not become current.
func finishLookup(cache *ttlCache, canonical string, result LookupResult, now time.Time) LookupResult {
	if cache != licenseCache.Load() || refreshActive.Load() {
		return LookupResult{}
	}
	if result.Available && cache != nil {
		cache.set(canonical, result, now)
	}
	return result
}

func queryLicense(db *sql.DB, query, canonical string) LookupResult {
	delay := 100 * time.Millisecond
	for attempt := 0; attempt < 5; attempt++ {
		var rawState sql.NullString
		err := db.QueryRowContext(context.Background(), query, canonical).Scan(&rawState)
		if err == nil {
			state := strings.ToUpper(strings.TrimSpace(rawState.String))
			if !spot.IsFCCState(state) {
				state = ""
			}
			return LookupResult{Available: true, Found: true, State: state}
		}
		if errors.Is(err, sql.ErrNoRows) {
			return LookupResult{Available: true}
		}
		if strings.Contains(strings.ToLower(err.Error()), "database is locked") && attempt < 4 {
			time.Sleep(delay)
			delay *= 2
			continue
		}
		logLookupError(err)
		return LookupResult{}
	}
	return LookupResult{}
}

// logLookupError shares one diagnostic across cold-open/probe/query failures.
// Resetting the generation rearms it; there is no per-spot logging or error cache.
func logLookupError(err error) {
	if loggedDBError.CompareAndSwap(false, true) {
		log.Printf("FCC ULS lookup unavailable: %v", err)
	}
}

// getLicenseDB probes schema once per handle generation. Legacy databases remain
// membership authorities while an attempted state-schema rebuild is unavailable.
func getLicenseDB() (*sql.DB, bool) {
	licenseMu.Lock()
	defer licenseMu.Unlock()
	if refreshActive.Load() || licenseDBPath == "" {
		return nil, false
	}
	if licenseDB != nil {
		return licenseDB, stateCapable.Load()
	}
	if _, err := os.Stat(licenseDBPath); err != nil {
		logLookupError(err)
		return nil, false
	}
	dsn := fmt.Sprintf("file:%s?mode=ro&_busy_timeout=5000&_pragma=query_only(1)&_pragma=immutable(1)", licenseDBPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		logLookupError(err)
		return nil, false
	}
	var version int
	if err = db.QueryRowContext(context.Background(), "PRAGMA user_version;").Scan(&version); err != nil {
		_ = db.Close()
		logLookupError(err)
		return nil, false
	}
	var dummy string
	if err = db.QueryRowContext(context.Background(), "SELECT call_sign FROM AM LIMIT 1;").Scan(&dummy); err != nil && !errors.Is(err, sql.ErrNoRows) {
		_ = db.Close()
		logLookupError(err)
		return nil, false
	}
	licenseDB = db
	stateCapable.Store(version == CurrentSchemaVersion)
	return db, stateCapable.Load()
}

// ResetLicenseDB detaches the old owner before closing it. In-flight queries
// may complete, but cannot publish into or return data from the new generation.
func ResetLicenseDB() {
	licenseMu.Lock()
	old := licenseDB
	licenseDB = nil
	stateCapable.Store(false)
	resetLicenseCache()
	licenseMu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	loggedDBError.Store(false)
}
func resetLicenseCache() {
	ttl := time.Duration(licenseCacheTTL.Load())
	if ttl <= 0 {
		ttl = defaultLicenseCacheTTL
	}
	licenseCache.Store(newLicenseCache(ttl, defaultLicenseCacheMaxEntries))
	lookupGeneration.Add(1)
}

// LookupStats reads bounded ownership without taking the DB/refresh mutex.
func LookupStats() LookupStatsSnapshot {
	s := LookupStatsSnapshot{Generation: lookupGeneration.Load(), StateCapable: stateCapable.Load(), Refreshing: refreshActive.Load()}
	if cache := licenseCache.Load(); cache != nil {
		cache.mu.Lock()
		s.Entries, s.Slots, s.Capacity, s.TTL = len(cache.entries), len(cache.slots), cache.max, cache.ttl
		cache.mu.Unlock()
	}
	return s
}

// NormalizeForLicense normalizes callsigns for FCC ULS lookup.
// Key aspects: Strips SSIDs/skimmer suffixes and chooses the most call-like slash segment.
// Upstream: IsLicensedUS.
// Downstream: spot.NormalizeCallsign, unicode digit checks.
func NormalizeForLicense(call string) string {
	normalized := spot.NormalizeCallsign(call)
	if normalized == "" {
		return ""
	}
	normalized = strings.TrimSuffix(normalized, "-#") // RBN skimmer indicator

	// When slashes are present, pick the most callsign-like segment (longest slice that contains a digit)
	// so base calls like W1VF/VE3 resolve to the licensed call rather than the location suffix.
	if strings.Contains(normalized, "/") {
		segments := strings.Split(normalized, "/")
		var candidate string
		var candidateLen int
		for _, seg := range segments {
			seg = strings.TrimSpace(seg)
			if seg == "" {
				continue
			}
			if idx := strings.IndexFunc(seg, unicode.IsDigit); idx >= 0 {
				if len(seg) > candidateLen {
					candidate = seg
					candidateLen = len(seg)
				}
			}
		}
		if candidate != "" {
			normalized = candidate
		} else if len(segments) > 0 {
			normalized = segments[0]
		}
	}

	// Drop SSID or other hyphen suffixes for license lookup.
	if idx := strings.Index(normalized, "-"); idx > 0 {
		normalized = normalized[:idx]
	}

	return strings.TrimSpace(normalized)
}
