package uls

import (
	"database/sql"
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

var (
	licenseDBPath  string
	licenseDB      *sql.DB
	licenseOnce    sync.Once
	licenseCache   sync.Map // call (uppercased) -> bool
	licenseMu      sync.Mutex
	licenseDead    atomic.Bool
	licenseEnabled atomic.Bool
	loggedDBError  atomic.Bool
)

func init() {
	licenseEnabled.Store(true)
}

// SetLicenseChecksEnabled controls whether FCC ULS checks are performed.
// When disabled, IsLicensedUS always returns true without touching the DB.
func SetLicenseChecksEnabled(enabled bool) {
	licenseEnabled.Store(enabled)
}

// SetLicenseDBPath configures the path to the FCC ULS SQLite database used for license lookups.
// If the path is empty or the DB cannot be opened, lookups will be skipped and treated as allowed.
func SetLicenseDBPath(path string) {
	clean := strings.TrimSpace(path)
	if clean == "" {
		licenseDBPath = ""
		return
	}
	if abs, err := filepath.Abs(clean); err == nil {
		clean = abs
	}
	if _, err := os.Stat(clean); err != nil {
		log.Printf("FCC ULS: database not found at %s (%v); license checks will be skipped", clean, err)
		licenseDBPath = ""
		licenseDead.Store(true)
		return
	}
	licenseDBPath = clean
}

// IsLicensedUS reports whether the callsign appears in the FCC ULS AM table (active licenses only).
// If no DB is configured or the DB cannot be opened, the call is treated as licensed (allowed).
// Results are cached for the lifetime of the process.
func IsLicensedUS(call string) bool {
	if !licenseEnabled.Load() {
		return true
	}
	if licenseDead.Load() {
		return true
	}
	canonical := NormalizeForLicense(call)
	if canonical == "" {
		return true
	}

	if cached, ok := licenseCache.Load(canonical); ok {
		return cached.(bool)
	}

	allow := true

	db := getLicenseDB()
	if db != nil {
		const retries = 5
		delay := 100 * time.Millisecond
		for attempt := 0; attempt < retries; attempt++ {
			var dummy int
			err := db.QueryRow("SELECT 1 FROM AM WHERE call_sign = ? LIMIT 1;", canonical).Scan(&dummy)
			if err == nil {
				allow = true
				break
			}
			if err == sql.ErrNoRows {
				allow = false
				break
			}
			if strings.Contains(strings.ToLower(err.Error()), "database is locked") && attempt < retries-1 {
				time.Sleep(delay)
				delay *= 2
				continue
			}
			if strings.Contains(strings.ToLower(err.Error()), "unable to open database file") ||
				strings.Contains(strings.ToLower(err.Error()), "out of memory") {
				if !loggedDBError.Load() {
					loggedDBError.Store(true)
					log.Printf("FCC ULS lookup failed for %s: %v (disabling license checks)", call, err)
				}
				licenseDead.Store(true)
				ResetLicenseDB()
				allow = true
				break
			}
			// On other query errors, default to allow and log once.
			if !loggedDBError.Load() {
				loggedDBError.Store(true)
				log.Printf("FCC ULS lookup failed for %s: %v", call, err)
			}
			allow = true
			break
		}
	}

	licenseCache.Store(canonical, allow)
	return allow
}

func getLicenseDB() *sql.DB {
	licenseOnce.Do(func() {
		if licenseDBPath == "" {
			return
		}
		// Open read-only with query_only/immutable pragmas and a short busy timeout so refresh
		// swaps don't trigger write attempts (WAL needs write access, so avoid it here).
		dsn := fmt.Sprintf("file:%s?mode=ro&_busy_timeout=5000&_pragma=query_only(1)&_pragma=immutable(1)", licenseDBPath)
		db, err := sql.Open("sqlite", dsn)
		if err != nil {
			log.Printf("FCC ULS: unable to open license DB at %s: %v (skipping license checks)", licenseDBPath, err)
			return
		}
		licenseMu.Lock()
		licenseDB = db
		licenseMu.Unlock()
	})
	licenseMu.Lock()
	defer licenseMu.Unlock()
	return licenseDB
}

// ResetLicenseDB closes any open license DB, clears caches, and allows reopening.
func ResetLicenseDB() {
	licenseMu.Lock()
	defer licenseMu.Unlock()
	if licenseDB != nil {
		_ = licenseDB.Close()
		licenseDB = nil
	}
	licenseOnce = sync.Once{}
	licenseCache = sync.Map{}
	licenseDead.Store(false)
	loggedDBError.Store(false)
}

// NormalizeForLicense strips adornments (SSID, skimmer suffix, portable prefixes)
// so FCC lookups use the underlying base callsign.
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
