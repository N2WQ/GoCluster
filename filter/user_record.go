package filter

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const maxRecentIPs = 5

// UserRecord stores per-callsign metadata that should survive across sessions.
// The Filter fields are inline so legacy files that only contain filters still load.
type UserRecord struct {
	Filter    `yaml:",inline"`
	RecentIPs []string `yaml:"recent_ips,omitempty"`
}

// Purpose: Load a persisted user record by callsign.
// Key aspects: Normalizes defaults and trims recent IPs; returns os.ErrNotExist if missing.
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
	var record UserRecord
	if err := yaml.Unmarshal(bs, &record); err != nil {
		return nil, err
	}
	record.Filter.migrateLegacyConfidence()
	record.Filter.normalizeDefaults()
	record.RecentIPs = trimRecentIPs(record.RecentIPs, maxRecentIPs)
	return &record, nil
}

// Purpose: Update recent IP history for a callsign and persist it.
// Key aspects: Creates a new record with defaults if none exists.
// Upstream: Telnet login handling.
// Downstream: LoadUserRecord, UpdateRecentIPs, SaveUserRecord.
func TouchUserRecordIP(callsign, ip string) (*UserRecord, bool, error) {
	record, err := LoadUserRecord(callsign)
	created := false
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			record = &UserRecord{Filter: *NewFilter()}
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

// Purpose: Persist a user record to disk.
// Key aspects: Ensures data dir exists; trims recent IP list.
// Upstream: SaveUserFilter, TouchUserRecordIP.
// Downstream: yaml.Marshal, os.WriteFile, userRecordPath.
func SaveUserRecord(callsign string, record *UserRecord) error {
	if record == nil {
		return errors.New("nil user record")
	}
	callsign = strings.TrimSpace(callsign)
	if callsign == "" {
		return errors.New("empty callsign")
	}
	if err := os.MkdirAll(UserDataDir, 0o755); err != nil {
		return err
	}
	record.RecentIPs = trimRecentIPs(record.RecentIPs, maxRecentIPs)
	bs, err := yaml.Marshal(record)
	if err != nil {
		return err
	}
	path := userRecordPath(callsign)
	return os.WriteFile(path, bs, 0o644)
}

// Purpose: Update recent IP history with a new address.
// Key aspects: Most-recent-first order; removes duplicates; enforces cap.
// Upstream: TouchUserRecordIP.
// Downstream: trimRecentIPs.
func UpdateRecentIPs(recent []string, ip string) []string {
	ip = strings.TrimSpace(ip)
	if ip == "" {
		return trimRecentIPs(recent, maxRecentIPs)
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
	return trimRecentIPs(updated, maxRecentIPs)
}

// Purpose: Merge two recent IP lists while preserving primary order.
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
func trimRecentIPs(recent []string, limit int) []string {
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
