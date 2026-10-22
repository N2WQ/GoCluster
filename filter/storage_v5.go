// File role: Authoritative v5 record codecs and captured replacement payloads.
package filter

import (
	"errors"
	"slices"
	"time"

	"gopkg.in/yaml.v3"
)

// V5UserRecord separates exact preferences from login history and one baseline.
type V5UserRecord struct {
	ConfigurationVersion int                `yaml:"configuration_version"`
	Configuration        V5Configuration    `yaml:"configuration"`
	Preset               *V5PresetReference `yaml:"preset,omitempty"`
	RecentIPs            []string           `yaml:"recent_ips,omitempty"`
	LastLoginUTC         time.Time          `yaml:"last_login_utc,omitempty"`
}

func v5UserRecordMinimumFits(record *V5UserRecord) bool {
	remaining := MaxUserRecordBytes
	for _, ip := range trimRecentIPs(record.RecentIPs) {
		if len(ip) > remaining || remaining-len(ip) < 3 {
			return false
		}
		remaining -= len(ip) + 3
	}
	var fits bool
	remaining, fits = v5ConfigurationMinimumRemaining(record.Configuration, remaining)
	if !fits {
		return false
	}
	if record.Preset != nil {
		if len(record.Preset.Name) > remaining {
			return false
		}
		remaining -= len(record.Preset.Name)
		if record.Preset.Baseline != nil {
			_, fits = v5ConfigurationMinimumRemaining(record.Preset.Baseline.Configuration, remaining)
		}
	}
	return fits
}

func prepareV5UserRecord(record *V5UserRecord) (*V5UserRecord, []byte, error) {
	if record == nil {
		return nil, nil, errors.New("nil v5 user record")
	}
	if err := validateV5EncodingVersion(record.ConfigurationVersion); err != nil {
		return nil, nil, err
	}
	if !v5UserRecordMinimumFits(record) {
		return nil, nil, storageAdmissionf("user record exceeds %d bytes", MaxUserRecordBytes)
	}
	cfg, err := prepareV5StoredConfiguration(record.Configuration)
	if err != nil {
		return nil, nil, err
	}
	ref, err := record.Preset.Clone()
	if err != nil {
		return nil, nil, err
	}
	snapshot := &V5UserRecord{
		ConfigurationVersion: MVPConfigurationVersion, Configuration: cfg, Preset: ref,
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

// EncodeV5UserRecord is pure: new DTOs may use version zero, stamped only on an
// owned canonical snapshot. Existing disk input must instead carry explicit v5.
func EncodeV5UserRecord(record *V5UserRecord) ([]byte, error) {
	_, data, err := prepareV5UserRecord(record)
	return data, err
}

// DecodeV5UserRecord rejects missing/legacy/future markers and malformed presence
// before materializing preferences. It never installs new-user defaults or writes.
func DecodeV5UserRecord(data []byte) (*V5UserRecord, error) {
	var record V5UserRecord
	if err := decodeV5StorageDocument(data, MaxUserRecordBytes,
		storageDecodeLimits{work: maxUserRecordWork}, &record); err != nil {
		return nil, err
	}
	snapshot, _, err := prepareV5UserRecord(&record)
	return snapshot, err
}

// PrepareV5UserRecord captures the existing atomic writer's path/bytes without IO.
// The caller must own the transaction lease and validate/load the authoritative
// destination before replacement; prospective admission cannot identify its version.
// Caller-owned slices must remain synchronized for the duration of preparation.
func PrepareV5UserRecord(callsign string, record *V5UserRecord) (*PreparedUserRecord, error) {
	path, err := v5UserRecordPath(callsign)
	if err != nil {
		return nil, err
	}
	data, err := EncodeV5UserRecord(record)
	if err != nil {
		return nil, err
	}
	return &PreparedUserRecord{path: path, data: data}, nil
}
