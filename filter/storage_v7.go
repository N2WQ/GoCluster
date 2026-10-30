// File role: Exact generation-seven profiles and captured atomic replacements.
package filter

import (
	"errors"
	"os"
	"slices"
	"strings"

	"time"
)

type V7NoticeState struct {
	ProfileResetGeneration        int `yaml:"profile_reset_generation"`
	ProfileAcknowledgedGeneration int `yaml:"profile_acknowledged_generation"`
	LibraryAcknowledgedGeneration int `yaml:"library_acknowledged_generation"`
}

type V7UserRecord struct {
	ConfigurationVersion int                `yaml:"configuration_version"`
	Configuration        V7Configuration    `yaml:"configuration"`
	Presets              V7PresetReferences `yaml:"presets"`
	RecentIPs            []string           `yaml:"recent_ips,omitempty"`
	LastLoginUTC         time.Time          `yaml:"last_login_utc,omitempty"`
	Notice               V7NoticeState      `yaml:"notice"`
}

func prepareV7UserRecord(record *V7UserRecord) (*V7UserRecord, []byte, error) {
	if record == nil {
		return nil, nil, errors.New("nil v7 user record")
	}
	if err := validateV7EncodingVersion(record.ConfigurationVersion); err != nil {
		return nil, nil, err
	}
	for _, generation := range []int{record.Notice.ProfileResetGeneration, record.Notice.ProfileAcknowledgedGeneration, record.Notice.LibraryAcknowledgedGeneration} {
		if err := validateV7Generation(generation); err != nil {
			return nil, nil, err
		}
	}
	if record.Notice.ProfileAcknowledgedGeneration > record.Notice.ProfileResetGeneration {
		return nil, nil, errors.New("profile acknowledgement exceeds reset generation")
	}
	remaining := MaxUserRecordBytes
	for _, ip := range trimRecentIPs(record.RecentIPs) {
		if len(ip) > remaining {
			return nil, nil, storageAdmissionf("user metadata exceeds profile byte limit")
		}
		remaining -= len(ip)
	}
	cfg, _, _, err := PrepareV7Configuration(record.Configuration)
	if err != nil {
		return nil, nil, err
	}
	refs, err := record.Presets.Clone()
	if err != nil {
		return nil, nil, err
	}
	result := &V7UserRecord{ConfigurationVersion: V7ConfigurationVersion, Configuration: cfg, Presets: refs, RecentIPs: slices.Clone(trimRecentIPs(record.RecentIPs)), LastLoginUTC: record.LastLoginUTC, Notice: record.Notice}
	for i, ip := range result.RecentIPs {
		result.RecentIPs[i] = strings.Clone(ip)
	}
	data, err := marshalV7Admitted(result, MaxUserRecordBytes, maxUserRecordWork, v7ProfileNodes)
	return result, data, err
}

func EncodeV7UserRecord(record *V7UserRecord) ([]byte, error) {
	_, data, err := prepareV7UserRecord(record)
	return data, err
}

func DecodeV7UserRecord(data []byte) (*V7UserRecord, error) {
	var record V7UserRecord
	if err := decodeV7StorageDocument(data, MaxUserRecordBytes, maxUserRecordWork, v7ProfileNodes, &record); err != nil {
		return nil, err
	}
	canonical, _, err := prepareV7UserRecord(&record)
	return canonical, err
}

func PrepareV7UserRecord(callsign string, record *V7UserRecord) (*PreparedUserRecord, error) {
	path, err := v6UserRecordPath(callsign)
	if err != nil {
		return nil, err
	}
	data, err := EncodeV7UserRecord(record)
	if err != nil {
		return nil, err
	}
	return &PreparedUserRecord{path: path, data: data}, nil
}

func LoadV7UserRecord(callsign string) (*V7UserRecord, error) {
	path, err := v6UserRecordPath(callsign)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	data, err := readUserRecordBytes(file)
	if err != nil {
		return nil, err
	}
	return DecodeV7UserRecord(data)
}

// AdmitV7UserRecord includes both bank-owned baselines in one shared work budget.
func AdmitV7UserRecord(record *V7UserRecord) error { _, err := EncodeV7UserRecord(record); return err }

func EncodeV7Configuration(configuration V7Configuration) ([]byte, error) {
	canonical, _, _, err := PrepareV7Configuration(configuration)
	if err != nil {
		return nil, err
	}
	return marshalV7Admitted(canonical, MaxV7ConfigurationBytes, maxUserRecordWork, v7ConfigurationNodes)
}

func DecodeV7Configuration(data []byte) (V7Configuration, error) {
	var configuration V7Configuration
	if err := decodeV7StorageDocument(data, MaxV7ConfigurationBytes, maxUserRecordWork, v7ConfigurationNodes, &configuration); err != nil {
		return V7Configuration{}, err
	}
	canonical, _, _, err := PrepareV7Configuration(configuration)
	return canonical, err
}
