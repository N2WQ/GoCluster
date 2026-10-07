// File role: FCC and ISED snapshots share factual lookup and one bounded cache.
// Each source owns its handle, generation and refresh state independently;
// admission enforcement never changes the factual address evidence.
package uls

import (
	"context"
	"database/sql"
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

	"dxcluster/spot"
)

// CurrentSchemaVersion marks the FCC projection that includes mailing state.
const CurrentSchemaVersion = 1

// LookupResult separates assigned membership from unavailable reference data.
// State is a registered address code, never an inferred operating location.
type LookupResult struct {
	Available, Found bool
	State            string
}

// LookupStatsSnapshot reports source readiness and the aggregate bounded cache.
type LookupStatsSnapshot struct {
	Entries, Slots, Capacity int
	TTL                      time.Duration
	Generation               uint64
	StateCapable, Refreshing bool
}

type licenseSourceID uint8

const (
	fccSource licenseSourceID = iota
	canadianSource
)

// Exactly two owners exist. Closing/replacing one cannot close the other's
// database or invalidate its generation. SQL work uses a captured handle and
// must pass the generation barrier before returning or caching any result.
type licenseSnapshot struct {
	mu           sync.Mutex
	path         string
	db           *sql.DB
	id           licenseSourceID
	label        string
	enabled      atomic.Bool
	refreshing   atomic.Bool
	loggedError  atomic.Bool
	generation   atomic.Uint64
	stateCapable atomic.Bool
}

var (
	fccSnapshot      = &licenseSnapshot{id: fccSource, label: "FCC ULS"}
	canadianSnapshot = &licenseSnapshot{id: canadianSource, label: "ISED"}
	licenseCache     atomic.Pointer[ttlCache]
)

func init() {
	fccSnapshot.enabled.Store(true)
	canadianSnapshot.enabled.Store(true)
	licenseCache.Store(newLicenseCache(defaultLicenseCacheTTL, defaultLicenseCacheMaxEntries))
}

// SetLicenseChecksEnabled controls FCC rejection only.
func SetLicenseChecksEnabled(enabled bool) { fccSnapshot.enabled.Store(enabled) }

// LicenseChecksEnabled reports FCC enforcement independently of enrichment.
func LicenseChecksEnabled() bool { return fccSnapshot.enabled.Load() }

// SetCanadianLicenseChecksEnabled controls Canadian rejection only.
func SetCanadianLicenseChecksEnabled(enabled bool) { canadianSnapshot.enabled.Store(enabled) }

// CanadianLicenseChecksEnabled reports Canadian enforcement.
func CanadianLicenseChecksEnabled() bool { return canadianSnapshot.enabled.Load() }

// SetLicenseCacheTTL sets the common TTL without increasing the cache budget.
// Canadian entries also revalidate whenever their UTC calendar date changes.
func SetLicenseCacheTTL(ttl time.Duration) {
	if ttl <= 0 {
		ttl = defaultLicenseCacheTTL
	}
	licenseCache.Store(newLicenseCache(ttl, defaultLicenseCacheMaxEntries))
	ResetLicenseDB()
	ResetCanadianLicenseDB()
}

// SetRefreshInProgress suppresses FCC facts during publication.
func SetRefreshInProgress(active bool) { fccSnapshot.refreshing.Store(active) }

// RefreshInProgress reports FCC refresh state.
func RefreshInProgress() bool { return fccSnapshot.refreshing.Load() }

// SetCanadianRefreshInProgress suppresses only Canadian publication facts.
func SetCanadianRefreshInProgress(active bool) { canadianSnapshot.refreshing.Store(active) }

// CanadianRefreshInProgress reports Canadian refresh state.
func CanadianRefreshInProgress() bool { return canadianSnapshot.refreshing.Load() }

// SetLicenseDBPath retains missing paths for successful first-build activation.
func SetLicenseDBPath(path string) { fccSnapshot.setPath(path) }

// SetCanadianLicenseDBPath configures the independently published ISED snapshot.
func SetCanadianLicenseDBPath(path string) { canadianSnapshot.setPath(path) }

func (s *licenseSnapshot) setPath(path string) {
	clean := strings.TrimSpace(path)
	if clean != "" {
		if abs, err := filepath.Abs(clean); err == nil {
			clean = abs
		}
	}
	s.mu.Lock()
	old := s.db
	s.db, s.path = nil, clean
	s.stateCapable.Store(false)
	s.generation.Add(1)
	s.loggedError.Store(false)
	if cache := licenseCache.Load(); cache != nil {
		cache.removeSource(s.id)
	}
	s.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

// ResetLicenseDB invalidates only FCC facts and closes its old handle.
func ResetLicenseDB() { fccSnapshot.reset() }

// ResetCanadianLicenseDB invalidates only ISED facts and closes its old handle.
func ResetCanadianLicenseDB() { canadianSnapshot.reset() }

func (s *licenseSnapshot) reset() {
	s.mu.Lock()
	old := s.db
	s.db = nil
	s.stateCapable.Store(false)
	s.generation.Add(1)
	s.loggedError.Store(false)
	if cache := licenseCache.Load(); cache != nil {
		cache.removeSource(s.id)
	}
	s.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

// CloseLicenseDatabases is called after refresh workers and consumers stop.
// Empty paths prevent a racing late consumer from reopening a released owner.
func CloseLicenseDatabases() {
	SetLicenseDBPath("")
	SetCanadianLicenseDBPath("")
}

// IsLicensedUS preserves the existing fail-open FCC admission API.
func IsLicensedUS(call string) bool {
	if !LicenseChecksEnabled() {
		return true
	}
	result := LookupUS(call)
	return !result.Available || result.Found
}

// IsLicensedCanadian applies the same fail-open admission rule to ISED facts.
func IsLicensedCanadian(call string) bool {
	if !CanadianLicenseChecksEnabled() {
		return true
	}
	result := LookupCanadian(call)
	return !result.Available || result.Found
}

// LookupUS returns FCC facts without consulting the enforcement flag.
func LookupUS(call string) LookupResult { return lookupSource(fccSnapshot, call, time.Now) }

// LookupCanadian returns ISED assignment/plausibility and registered province.
func LookupCanadian(call string) LookupResult { return lookupSource(canadianSnapshot, call, time.Now) }

// LookupForADIF routes by the base identity's CTY entity, not portable location.
func LookupForADIF(adif int, call string) LookupResult {
	if spot.IsFCCJurisdiction(adif) {
		return LookupUS(call)
	}
	if spot.IsCanadianJurisdiction(adif) {
		return LookupCanadian(call)
	}
	return LookupResult{}
}

// LicenseChecksEnabledForADIF keeps source-specific rejection independent.
func LicenseChecksEnabledForADIF(adif int) bool {
	if spot.IsFCCJurisdiction(adif) {
		return LicenseChecksEnabled()
	}
	if spot.IsCanadianJurisdiction(adif) {
		return CanadianLicenseChecksEnabled()
	}
	return false
}

func canadianLookupDay(now time.Time) int64 { return now.UTC().Truncate(24 * time.Hour).Unix() }

func lookupSource(source *licenseSnapshot, call string, clock func() time.Time) LookupResult {
	if source.refreshing.Load() {
		return LookupResult{}
	}
	canonical := NormalizeForLicense(call)
	if canonical == "" {
		return LookupResult{}
	}
	now := clock().UTC()
	day := int64(0)
	if source.id == canadianSource {
		day = canadianLookupDay(now)
	}
	generation := source.generation.Load()
	cache := licenseCache.Load()
	key := licenseCacheKey{source: source.id, call: canonical}
	if cache != nil {
		if result, ok := cache.get(key, generation, day, now); ok {
			if !lookupCurrent(source, cache, generation, day, clock) {
				return LookupResult{}
			}
			return result
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, hasState := source.getDatabase(ctx)
	if db == nil {
		return LookupResult{}
	}
	var result LookupResult
	if source.id == canadianSource {
		var err error
		result, err = queryCanadianLicense(ctx, db, canonical, now)
		if err != nil {
			source.logGenerationError(err, generation)
			return LookupResult{}
		}
	} else {
		query := "SELECT '' FROM AM WHERE call_sign = ? LIMIT 1;"
		if hasState {
			query = "SELECT state FROM AM WHERE call_sign = ? LIMIT 1;"
		}
		result = queryLicenseContext(ctx, db, query, canonical, generation)
	}
	return finishLookup(source, cache, key, generation, day, result, now, clock)
}

func lookupCurrent(source *licenseSnapshot, cache *ttlCache, generation uint64, day int64, clock func() time.Time) bool {
	return cache == licenseCache.Load() && generation == source.generation.Load() && !source.refreshing.Load() &&
		(source.id != canadianSource || day == canadianLookupDay(clock()))
}

// finishLookup also rejects a Canadian query that straddles UTC midnight.
func finishLookup(source *licenseSnapshot, cache *ttlCache, key licenseCacheKey, generation uint64, day int64, result LookupResult, now time.Time, clock func() time.Time) LookupResult {
	if !lookupCurrent(source, cache, generation, day, clock) {
		return LookupResult{}
	}
	if result.Available && cache != nil {
		cache.set(key, generation, day, result, now)
	}
	return result
}

func queryLicense(db *sql.DB, query, canonical string) LookupResult {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return queryLicenseContext(ctx, db, query, canonical, fccSnapshot.generation.Load())
}

// The query budget includes pool wait and lock retries; no cold lookup may
// remain queued indefinitely behind the finite connection pool.
func queryLicenseContext(ctx context.Context, db *sql.DB, query, canonical string, generation uint64) LookupResult {
	delay := 100 * time.Millisecond
	for attempt := 0; attempt < 5; attempt++ {
		var rawState sql.NullString
		err := db.QueryRowContext(ctx, query, canonical).Scan(&rawState)
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
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return LookupResult{}
			case <-timer.C:
			}
			delay *= 2
			continue
		}
		fccSnapshot.logGenerationError(err, generation)
		return LookupResult{}
	}
	return LookupResult{}
}

func (s *licenseSnapshot) logError(err error) {
	if s.loggedError.CompareAndSwap(false, true) {
		log.Printf("%s lookup unavailable: %v", s.label, err)
	}
}

// Reservation shares the owner lock with reset, so a detached query cannot
// consume the replacement generation's single diagnostic. Logging holds no lock.
func (s *licenseSnapshot) logGenerationError(err error, generation uint64) {
	s.mu.Lock()
	report := s.generation.Load() == generation && s.loggedError.CompareAndSwap(false, true)
	s.mu.Unlock()
	if report {
		log.Printf("%s lookup unavailable: %v", s.label, err)
	}
}

func getLicenseDB() (*sql.DB, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return fccSnapshot.getDatabase(ctx)
}

func (s *licenseSnapshot) getDatabase(ctx context.Context) (*sql.DB, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refreshing.Load() || s.path == "" {
		return nil, false
	}
	if s.db != nil {
		return s.db, s.stateCapable.Load()
	}
	if _, err := os.Stat(s.path); err != nil {
		s.logError(err)
		return nil, false
	}
	dsn := fmt.Sprintf("file:%s?mode=ro&_busy_timeout=5000&_pragma=query_only(1)&_pragma=immutable(1)", s.path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		s.logError(err)
		return nil, false
	}
	// Both owners have a finite reader pool; no connection is held by the cache.
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	var version int
	if err = db.QueryRowContext(ctx, "PRAGMA user_version;").Scan(&version); err == nil {
		if s.id == canadianSource {
			err = probeCanadianDatabase(ctx, db)
		} else {
			var dummy string
			err = db.QueryRowContext(ctx, "SELECT call_sign FROM AM LIMIT 1;").Scan(&dummy)
			if errors.Is(err, sql.ErrNoRows) {
				err = nil
			}
		}
	}
	if err != nil {
		_ = db.Close()
		s.logError(err)
		return nil, false
	}
	s.db = db
	s.stateCapable.Store(s.id == canadianSource || version == CurrentSchemaVersion)
	return db, s.stateCapable.Load()
}

// LookupStats reports actual retained cache cardinality without acquiring a DB lock.
func LookupStats() LookupStatsSnapshot { return sourceLookupStats(fccSnapshot) }

// CanadianLookupStats reports ISED generation/readiness with the same aggregate cache.
func CanadianLookupStats() LookupStatsSnapshot { return sourceLookupStats(canadianSnapshot) }
func sourceLookupStats(source *licenseSnapshot) LookupStatsSnapshot {
	s := LookupStatsSnapshot{Generation: source.generation.Load(), StateCapable: source.stateCapable.Load(), Refreshing: source.refreshing.Load()}
	if cache := licenseCache.Load(); cache != nil {
		cache.mu.Lock()
		s.Entries, s.Slots, s.Capacity, s.TTL = len(cache.entries), len(cache.slots), cache.max, cache.ttl
		cache.mu.Unlock()
	}
	return s
}

// NormalizeForLicense selects the station identity for FCC/ISED lookup and
// qualified allowlist matching. Existing station-identity syntax outranks a
// bare prefix lacking that shape, even when the prefix ties or exceeds its length.
// SSIDs/skimmer markers are removed; equally call-like segments retain their
// existing longest/first ordering, as does the fallback without an identity.
func NormalizeForLicense(call string) string {
	normalized := spot.NormalizeCallsign(call)
	if normalized == "" {
		return ""
	}
	normalized = strings.TrimSuffix(normalized, "-#") // RBN skimmer indicator

	// A bare prefix like VE3 can tie W1A or outlength a short base call. Prefer
	// station syntax before length, using the same identity gate as admission.
	if strings.Contains(normalized, "/") {
		segments := strings.Split(normalized, "/")
		var candidate string
		var candidateLen int
		var candidateIdentity bool
		for _, seg := range segments {
			seg = strings.TrimSpace(seg)
			if seg == "" {
				continue
			}
			if idx := strings.IndexFunc(seg, unicode.IsDigit); idx >= 0 {
				identity := spot.IsValidNormalizedCallsign(seg)
				if identity && !candidateIdentity || identity == candidateIdentity && len(seg) > candidateLen {
					candidate = seg
					candidateLen = len(seg)
					candidateIdentity = identity
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
