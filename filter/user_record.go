// File role: Owns persisted per-callsign user state.
// Crawler notes: Start here for filter persistence, dialect selection, recent
// login IP tracking, grid/noise/PATHSAMPLES settings, and solar summary opt-in
// values loaded by telnet sessions.
// Related docs: telnet/README.md, data/config/README.md.
// Related tests: filter/user_record_test.go, telnet/*filter*_test.go.
package filter

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"dxcluster/strutil"
	"gopkg.in/yaml.v3"
)

const maxRecentIPs = 5

// UserRecord stores per-callsign metadata that should survive across sessions.
// The Filter fields are inline so legacy files that only contain filters still load.
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
// Key aspects: Migrates only legacy defaults and trims recent IPs; marked values
// remain exact. Missing files return os.ErrNotExist; malformed/future files fail.
// Upstream: LoadUserFilter, TouchUserRecordIP, telnet login flows.
// Downstream: yaml.Unmarshal, trimRecentIPs, Filter normalization helpers.
func LoadUserRecord(callsign string) (*UserRecord, error) {
	callsign = strings.TrimSpace(callsign)
	if callsign == "" {
		return nil, errors.New("empty callsign")
	}
	path := userRecordPath(callsign)
	bs, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(bs))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	if len(document.Content) != 1 {
		return nil, errors.New("user record must contain one YAML mapping")
	}
	var record UserRecord
	if err := record.UnmarshalYAML(document.Content[0]); err != nil {
		return nil, err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("user record must contain one YAML document")
	}
	record.RecentIPs = trimRecentIPs(record.RecentIPs)
	return &record, nil
}

// UnmarshalYAML preserves marked records exactly. Only an absent marker invokes
// the historic migrations/defaults; explicit zero/null/future markers fail.
func (record *UserRecord) UnmarshalYAML(node *yaml.Node) error {
	version, err := storedConfigurationVersion(node)
	if err != nil {
		return err
	}
	if version != 0 {
		if err := validateStoredMapping(node, reflect.TypeFor[UserRecord]()); err != nil {
			return err
		}
		if err := validateCurrentStoredValues(node, reflect.TypeFor[UserRecord]()); err != nil {
			return err
		}
	}
	type plain UserRecord
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	if decoded.ConfigurationVersion != version {
		return fmt.Errorf("%w: marker must be an explicit field", ErrUnsupportedConfigurationVersion)
	}
	*record = UserRecord(decoded)
	if version == 0 {
		record.migrateLegacyConfidence()
		record.normalizeDefaults()
		if strings.TrimSpace(record.Dialect) == "" {
			record.Dialect = "go"
		}
		record.DedupePolicy = NormalizeDedupePolicy(record.DedupePolicy)
		record.Grid = strutil.NormalizeUpper(record.Grid)
		record.NoiseClass = strutil.NormalizeUpper(record.NoiseClass)
		record.PathMinObservationCount = normalizePathMinObservationCount(record.PathMinObservationCount)
		record.SolarSummaryMinutes = normalizeSolarSummaryMinutes(record.SolarSummaryMinutes)
	}
	record.ConfigurationVersion = CurrentConfigurationVersion
	return nil
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
	if record == nil {
		return errors.New("nil user record")
	}
	callsign = strings.TrimSpace(callsign)
	if callsign == "" {
		return errors.New("empty callsign")
	}
	if record.ConfigurationVersion != 0 && record.ConfigurationVersion != CurrentConfigurationVersion {
		return ErrUnsupportedConfigurationVersion
	}
	// Preserve the caller's captured preferences and baseline. Metadata trimming
	// applies to this local copy; a failed write cannot alter the live reference.
	snapshot := *record
	snapshot.ConfigurationVersion = CurrentConfigurationVersion
	snapshot.RecentIPs = trimRecentIPs(snapshot.RecentIPs)
	var err error
	snapshot.Preset, err = snapshot.Preset.Clone()
	if err != nil {
		return err
	}
	bs, err := yaml.Marshal(&snapshot)
	if err != nil {
		return err
	}
	path := userRecordPath(callsign)
	return write(path, bs)
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

func normalizeSolarSummaryMinutes(minutes int) int {
	switch minutes {
	case 15, 30, 60:
		return minutes
	default:
		return 0
	}
}

func normalizePathMinObservationCount(count int) int {
	if count <= 0 {
		return 0
	}
	return count
}
