// File role: Owns persisted per-callsign user state.
// Crawler notes: Start here for filter persistence, dialect selection, recent
// login IP tracking, grid/noise/PATHSAMPLES settings, and solar summary opt-in
// values loaded by telnet sessions.
// Related docs: telnet/README.md, data/config/README.md.
// Related tests: filter/user_record_test.go, telnet/*filter*_test.go.
package filter

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const maxRecentIPs = 5

// UserRecord stores per-callsign metadata that should survive across sessions.
// Native fields are transport-only; YAML is explicitly bound to the v6 DTO.
type UserRecord struct {
	ConfigurationVersion int `yaml:"configuration_version,omitempty"`
	Filter               `yaml:",inline"`
	Preset               *PresetReference `yaml:"preset,omitempty"`
	RecentIPs            []string         `yaml:"recent_ips,omitempty"`
	Dialect              string           `yaml:"dialect,omitempty"`
	DedupePolicy         string           `yaml:"dedupe_policy,omitempty"`
	// LastLoginUTC records the timestamp of the previous successful login (UTC).
	LastLoginUTC time.Time `yaml:"last_login_utc,omitempty"`
	Grid         string    `yaml:"grid,omitempty"`        // Optional user-supplied grid (uppercased)
	NoiseClass   string    `yaml:"noise_class,omitempty"` // Optional noise class token (uppercased)
	// PathMinObservationCount stores a per-user stricter path sample floor (0=cluster default).
	PathMinObservationCount int `yaml:"path_min_observation_count,omitempty"`
	// SolarSummaryMinutes controls opt-in solar summary cadence (0=off).
	SolarSummaryMinutes int `yaml:"solar_summary_minutes,omitempty"`
}

// LoadUserRecord loads a persisted user record by callsign.
// Key aspects: Exact v6 values only. Missing files return os.ErrNotExist;
// unversioned, legacy, malformed and future files fail without rewriting.
// Upstream: LoadUserFilter, TouchUserRecordIP, telnet login flows.
// Downstream: strict v6 loading and runtime preparation.
func LoadUserRecord(callsign string) (*UserRecord, error) {
	record, err := LoadV6UserRecord(callsign)
	if err != nil {
		return nil, err
	}
	return RuntimeUserRecordFromV6(record)
}

// UnmarshalYAML admits only the strict v6 DTO, without migration or defaults.
func (record *UserRecord) UnmarshalYAML(node *yaml.Node) error {
	var value V6UserRecord
	if err := decodeV6StorageNode(node, storageDecodeLimits{work: maxUserRecordWork}, &value); err != nil {
		return err
	}
	prepared, _, err := prepareV6UserRecord(&value)
	if err != nil {
		return err
	}
	runtime, err := RuntimeUserRecordFromV6(prepared)
	if err == nil {
		*record = *runtime
	}
	return err
}

// TouchUserRecordIP updates recent IP history for a callsign and persists it.
// Key aspects: Creates a new record with defaults if none exists.
// Upstream: Telnet login handling.
// Downstream: LoadUserRecord, UpdateRecentIPs, SaveUserRecord.
func TouchUserRecordIP(callsign, ip string) (*UserRecord, bool, error) {
	return TouchUserRecordIPWithDefaultDedupe(callsign, ip, DedupePolicyMed)
}

// TouchUserRecordIPWithDefaultDedupe updates recent IP history using the
// caller-supplied default policy for new records.
// Key aspects: Creates a new record with config-aware defaults if none exists.
// Upstream: Telnet login handling.
// Downstream: LoadUserRecord, UpdateRecentIPs, SaveUserRecord.
func TouchUserRecordIPWithDefaultDedupe(callsign, ip, defaultDedupePolicy string) (*UserRecord, bool, error) {
	defaultDedupePolicy = NormalizeDedupePolicy(defaultDedupePolicy)
	record, err := LoadUserRecord(callsign)
	created := false
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			record = &UserRecord{Filter: *NewFilter(), Dialect: "go", DedupePolicy: defaultDedupePolicy}
			created = true
		} else {
			return nil, false, err
		}
	}
	record.RecentIPs = UpdateRecentIPs(record.RecentIPs, ip)
	if err := SaveUserRecord(callsign, record); err != nil {
		return nil, created, err
	}
	return record, created, nil
}

// TouchUserRecordLogin updates login metadata (timestamp + IP) while returning the prior values.
// Key aspects: Persists the new state and provides previous login/IP for templates.
// Upstream: Telnet login handling.
// Downstream: SaveUserRecord.
func TouchUserRecordLogin(callsign, ip string, loginTime time.Time) (record *UserRecord, created bool, prevLogin time.Time, prevIP string, err error) {
	return TouchUserRecordLoginWithDefaultDedupe(callsign, ip, loginTime, DedupePolicyMed)
}

// TouchUserRecordLoginWithDefaultDedupe updates login metadata using the
// caller-supplied default policy for new records.
// Key aspects: Persists the new state and provides previous login/IP for templates.
// Upstream: Telnet login handling.
// Downstream: LoadUserRecord, SaveUserRecord.
func TouchUserRecordLoginWithDefaultDedupe(callsign, ip string, loginTime time.Time, defaultDedupePolicy string) (record *UserRecord, created bool, prevLogin time.Time, prevIP string, err error) {
	callsign = strings.TrimSpace(callsign)
	if callsign == "" {
		return nil, false, time.Time{}, "", errors.New("empty callsign")
	}
	defaultDedupePolicy = NormalizeDedupePolicy(defaultDedupePolicy)
	record, err = LoadUserRecord(callsign)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			record = &UserRecord{Filter: *NewFilter(), Dialect: "go", DedupePolicy: defaultDedupePolicy}
			created = true
		} else {
			return nil, false, time.Time{}, "", err
		}
	}
	if len(record.RecentIPs) > 0 {
		prevIP = strings.TrimSpace(record.RecentIPs[0])
	}
	prevLogin = record.LastLoginUTC
	record.RecentIPs = UpdateRecentIPs(record.RecentIPs, ip)
	record.LastLoginUTC = loginTime
	if err := SaveUserRecord(callsign, record); err != nil {
		return record, created, prevLogin, prevIP, err
	}
	return record, created, prevLogin, prevIP, nil
}

// SaveUserRecord persists a user record to disk.
// Key aspects: Ensures data dir exists; trims recent IP list.
// Upstream: SaveUserFilter, TouchUserRecordIP.
// Downstream: yaml.Marshal, atomic replacement, userRecordPath.
func SaveUserRecord(callsign string, record *UserRecord) error {
	return saveUserRecord(callsign, record, writeAtomicUserFile)
}

func saveUserRecord(callsign string, record *UserRecord, write func(string, []byte) error) error {
	prepared, err := PrepareUserRecord(callsign, record)
	if err != nil {
		return err
	}
	return prepared.commitWith(write)
}

// UpdateRecentIPs updates recent IP history with a new address.
// Key aspects: Most-recent-first order; removes duplicates; enforces cap.
// Upstream: TouchUserRecordIP.
// Downstream: trimRecentIPs.
func UpdateRecentIPs(recent []string, ip string) []string {
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return trimRecentIPs(recent)
	}
	updated := make([]string, 0, len(recent)+1)
	updated = append(updated, ip)
	for _, existing := range recent {
		if existing == ip {
			continue
		}
		updated = append(updated, existing)
		if len(updated) >= maxRecentIPs {
			break
		}
	}
	return trimRecentIPs(updated)
}

// MergeRecentIPs merges two recent IP lists while preserving primary order.
// Key aspects: De-duplicates and caps at maxRecentIPs.
// Upstream: Client filter save flows.
// Downstream: None.
func MergeRecentIPs(primary, fallback []string) []string {
	merged := make([]string, 0, maxRecentIPs)
	for _, ip := range primary {
		if ip = strings.TrimSpace(ip); ip == "" {
			continue
		}
		merged = append(merged, ip)
		if len(merged) >= maxRecentIPs {
			return merged
		}
	}
	for _, ip := range fallback {
		if ip = strings.TrimSpace(ip); ip == "" {
			continue
		}
		already := false
		for _, existing := range merged {
			if existing == ip {
				already = true
				break
			}
		}
		if already {
			continue
		}
		merged = append(merged, ip)
		if len(merged) >= maxRecentIPs {
			break
		}
	}
	return merged
}

// Purpose: Trim a list of IPs to the provided limit.
// Key aspects: Returns nil on non-positive limit; preserves order.
// Upstream: UpdateRecentIPs, LoadUserRecord, SaveUserRecord.
// Downstream: None.
func trimRecentIPs(recent []string) []string {
	limit := maxRecentIPs
	if limit <= 0 {
		return nil
	}
	if len(recent) <= limit {
		return recent
	}
	return recent[:limit]
}

// Purpose: Build the on-disk path for a user's record.
// Key aspects: Uppercases callsign for stable filenames.
// Upstream: LoadUserRecord, SaveUserRecord.
// Downstream: filepath.Join, strings.ToUpper.
func userRecordPath(callsign string) string {
	return filepath.Join(UserDataDir, fmt.Sprintf("%s.yaml", strings.ToUpper(callsign)))
}
