package filter

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

type storageCountedReader struct {
	reader io.Reader
	read   int
}

func (r *storageCountedReader) Read(data []byte) (int, error) {
	n, err := r.reader.Read(data)
	r.read += n
	return n, err
}

func TestStorageRecordRawReadBound(t *testing.T) {
	usePresetTestDir(t)
	source := strings.NewReader(strings.Repeat(" ", MaxUserRecordBytes+73))
	counted := &storageCountedReader{reader: source}
	if data, err := readUserRecordBytes(counted); data != nil || !errors.Is(err, ErrStorageAdmission) ||
		counted.read != MaxUserRecordBytes+1 || source.Len() != 72 {
		t.Fatal("physical read did not stop at limit+1", err)
	}
	raw := v5StorageMinimalRecord + strings.Repeat(" ", MaxUserRecordBytes-len(v5StorageMinimalRecord))
	for _, extra := range []string{"", " "} {
		path := userRecordPath("N2WQ")
		if err := os.WriteFile(path, []byte(raw+extra), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadUserRecord("N2WQ")
		if extra == "" && err != nil || extra != "" && !errors.Is(err, ErrStorageAdmission) {
			t.Fatal("native physical byte boundary changed", err)
		}
		after, err := os.ReadFile(path)
		if err != nil || string(after) != raw+extra {
			t.Fatal("read modified bytes")
		}
	}
}

func TestStorageConfigurationStandaloneOracle(t *testing.T) {
	usePresetTestDir(t)
	dto := v5StorageExactPreset(t)
	set, err := RuntimeSavedPresetFromV5(dto)
	if err != nil {
		t.Fatal(err)
	}
	cfg := ConfigurationFromPreset(set)
	record := &UserRecord{}
	applyRecordConfiguration(record, cfg)
	prepared, err := PrepareUserRecord("N2WQ-1", record)
	if err != nil {
		t.Fatal("exact standalone native configuration refused", err)
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadUserRecord("N2WQ-1"); err != nil {
		t.Fatal(err)
	}
	record.Grid += "A"
	if _, err := PrepareUserRecord("N2WQ-1", record); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("oversized native configuration admitted", err)
	}
	dto.Configuration.Settings.Grid += "A"
	raw, err := yaml.Marshal(dto)
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(raw, &SavedPreset{}); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("native direct callback bypassed standalone bound", err)
	}
}

func TestStorageCanonicalAndProspectiveRecordBounds(t *testing.T) {
	usePresetTestDir(t)
	raw := v5StorageMinimalRecord + "recent_ips: [&ip " + strings.Repeat("A", 220_000) + ", *ip, *ip, *ip, *ip]\n"
	if err := os.WriteFile(userRecordPath("N2WQ"), []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadUserRecord("N2WQ"); !errors.Is(err, ErrStorageAdmission) || strings.Contains(err.Error(), "work") {
		t.Fatal("canonical native record bound bypassed", err)
	}
	if err := yaml.Unmarshal([]byte(raw), &UserRecord{}); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("native direct callback bypassed canonical bound", err)
	}
	record := &UserRecord{Filter: *NewFilter(), RecentIPs: []string{"A"}, LastLoginUTC: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
	base, err := yaml.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	record.RecentIPs[0] = strings.Repeat("A", MaxUserRecordBytes-len(base)+1)
	prepared, err := PrepareUserRecord("N2WQ", record)
	if err != nil || prepared.ByteLen() != MaxUserRecordBytes {
		t.Fatal("exact native prospective boundary refused", err)
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	record.RecentIPs[0] += "A"
	if _, err := PrepareUserRecord("N2WQ", record); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("native prospective bound bypassed", err)
	}
}

func TestStorageCurrentRecordFailureCausality(t *testing.T) {
	usePresetTestDir(t)
	path := userRecordPath("N2WQ")
	for _, raw := range []string{"configuration_version: 99\n", strings.Repeat(" ", MaxUserRecordBytes+1)} {
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		for _, prepare := range []func() (*PreparedUserRecord, error){
			func() (*PreparedUserRecord, error) { return PrepareConfiguration("N2WQ", Configuration{}, nil, nil) },
			func() (*PreparedUserRecord, error) { return PrepareUserPreferences("N2WQ", &SavedPreset{}, nil) },
		} {
			_, err := prepare()
			cause := ErrStorageAdmission
			if strings.HasPrefix(raw, "configuration_version") {
				cause = ErrUnsupportedConfigurationVersion
			}
			if !errors.Is(err, ErrCurrentUserRecord) || !errors.Is(err, cause) {
				t.Fatal("authoritative failure lost its cause", err)
			}
		}
	}
	old := &UserRecord{Filter: *NewFilter()}
	for i := range 5 {
		old.RecentIPs = append(old.RecentIPs, strings.Repeat(string(rune('A'+i)), 160_000))
	}
	if err := SaveUserRecord("N2WQ", old); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	cfg := Configuration{Settings: SettingsConfiguration{Grid: strings.Repeat("A", 250_000)}}
	if _, err := cfg.Preset(); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareConfiguration("N2WQ", cfg, nil, nil); !errors.Is(err, ErrStorageAdmission) || errors.Is(err, ErrCurrentUserRecord) {
		t.Fatal("prospective refusal confused with current-record refusal", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("prospective refusal changed file")
	}
}

func TestStoragePureAdmissionAndExactBaseline(t *testing.T) {
	usePresetTestDir(t)
	UserDataDir = filepath.Join(UserDataDir, "not-created")
	set, err := RuntimeSavedPresetFromV5(v5StorageExactPreset(t))
	if err != nil {
		t.Fatal(err)
	}
	record := &UserRecord{Preset: &PresetReference{Name: "BASE", Baseline: set},
		RecentIPs: []string{"a", "b", "c", "d", "e", strings.Repeat("X", MaxUserRecordBytes+1)}}
	if err := AdmitUserRecord(record); err != nil || len(record.RecentIPs) != 6 {
		t.Fatal("pure native admission changed input or rejected exact baseline", err)
	}
	if _, err := os.Stat(UserDataDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pure admission acquired filesystem authority")
	}
	set.Grid += "A"
	if err := AdmitUserRecord(record); !errors.Is(err, ErrStorageAdmission) {
		t.Fatal("oversized baseline admitted", err)
	}
}
