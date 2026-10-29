// File role: Authoritative v6 record codecs and captured replacement payloads.
package filter

import (
	"errors"
	"slices"
	"time"

	"gopkg.in/yaml.v3"
)

// V6UserRecord separates exact preferences from login history and one baseline.
type V6UserRecord struct {
	ConfigurationVersion int                `yaml:"configuration_version"`
	Configuration        V6Configuration    `yaml:"configuration"`
	Preset               *V6PresetReference `yaml:"preset,omitempty"`
	RecentIPs            []string           `yaml:"recent_ips,omitempty"`
	LastLoginUTC         time.Time          `yaml:"last_login_utc,omitempty"`
}

func v6UserRecordMinimumFits(record *V6UserRecord) bool {
	remaining := MaxUserRecordBytes
	for _, ip := range trimRecentIPs(record.RecentIPs) {
		if len(ip) > remaining || remaining-len(ip) < 3 {
			return false
		}
		remaining -= len(ip) + 3
	}
	var fits bool
	remaining, fits = v6ConfigurationMinimumRemaining(record.Configuration, remaining)
	if !fits {
		return false
	}
	if record.Preset != nil {
		if len(record.Preset.Name) > remaining {
			return false
		}
		remaining -= len(record.Preset.Name)
		if record.Preset.Baseline != nil {
			_, fits = v6ConfigurationMinimumRemaining(record.Preset.Baseline.Configuration, remaining)
		}
	}
	return fits
}

func prepareV6UserRecord(record *V6UserRecord) (*V6UserRecord, []byte, error) {
	if record == nil {
		return nil, nil, errors.New("nil v6 user record")
	}
	if err := validateV6EncodingVersion(record.ConfigurationVersion); err != nil {
		return nil, nil, err
	}
	if !v6UserRecordMinimumFits(record) {
		return nil, nil, storageAdmissionf("user record exceeds %d bytes", MaxUserRecordBytes)
	}
	cfg, err := prepareV6StoredConfiguration(record.Configuration)
	if err != nil {
		return nil, nil, err
	}
	ref, err := record.Preset.Clone()
	if err != nil {
		return nil, nil, err
	}
	snapshot := &V6UserRecord{
		ConfigurationVersion: CurrentConfigurationVersion, Configuration: cfg, Preset: ref,
		RecentIPs: slices.Clone(trimRecentIPs(record.RecentIPs)), LastLoginUTC: record.LastLoginUTC,
	}
	data, err := yaml.Marshal(snapshot)
	if err != nil {
		return nil, nil, err
	}
	if len(data) > MaxUserRecordBytes {
		return nil, nil, storageAdmissionf("user record exceeds %d bytes", MaxUserRecordBytes)
	}
	return snapshot, data, nil
}

// EncodeV6UserRecord is pure: new DTOs may use version zero, stamped only on an
// owned canonical snapshot. Existing disk input must instead carry explicit v6.
func EncodeV6UserRecord(record *V6UserRecord) ([]byte, error) {
	_, data, err := prepareV6UserRecord(record)
	return data, err
}

// DecodeV6UserRecord rejects missing/legacy/future markers and malformed presence
// before materializing preferences. It never installs new-user defaults or writes.
func DecodeV6UserRecord(data []byte) (*V6UserRecord, error) {
	var record V6UserRecord
	if err := decodeV6StorageDocument(data, MaxUserRecordBytes,
		storageDecodeLimits{work: maxUserRecordWork}, &record); err != nil {
		return nil, err
	}
	snapshot, _, err := prepareV6UserRecord(&record)
	return snapshot, err
}

// PrepareV6UserRecord captures the existing atomic writer's path/bytes without IO.
// The caller must own the transaction lease and validate/load the authoritative
// destination before replacement; prospective admission cannot identify its version.
// Caller-owned slices must remain synchronized for the duration of preparation.
func PrepareV6UserRecord(callsign string, record *V6UserRecord) (*PreparedUserRecord, error) {
	path, err := v6UserRecordPath(callsign)
	if err != nil {
		return nil, err
	}
	data, err := EncodeV6UserRecord(record)
	if err != nil {
		return nil, err
	}
	return &PreparedUserRecord{path: path, data: data}, nil
}
