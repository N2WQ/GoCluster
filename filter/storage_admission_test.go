package filter

import (
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

type storageCountedReader struct {
	reader io.Reader
	read   int
}

func (reader *storageCountedReader) Read(data []byte) (int, error) {
	n, err := reader.reader.Read(data)
	reader.read += n
	return n, err
}

func TestStorageRecordRawReadBound(t *testing.T) {
	usePresetTestDir(t)
	source := strings.NewReader(strings.Repeat(" ", MaxUserRecordBytes+73))
	counted := &storageCountedReader{reader: source}
	if data, err := readUserRecordBytes(counted); data != nil || !errors.Is(err, ErrStorageAdmission) || counted.read != MaxUserRecordBytes+1 || source.Len() != 72 {
		t.Fatalf("read was not bounded at limit+1: bytes=%d unread=%d err=%v", counted.read, source.Len(), err)
	}
	raw := "{}\n" + strings.Repeat(" ", MaxUserRecordBytes-3)
	path := userRecordPath("N2WQ")
	for _, extra := range []string{"", " "} {
		data := []byte(raw + extra)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		record, err := LoadUserRecord("N2WQ")
		if extra == "" && (err != nil || record == nil) || extra != "" && !errors.Is(err, ErrStorageAdmission) {
			t.Fatalf("raw boundary extra=%d: %v", len(extra), err)
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(after, data) {
			t.Fatal("raw read changed stored bytes")
		}
	}
}

func TestStorageConfigurationStandaloneOracle(t *testing.T) {
	usePresetTestDir(t)
	set := &SavedPreset{ConfigurationVersion: CurrentConfigurationVersion}
	set.DXCallsigns = []string{"A"}
	base, err := yaml.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	set.DXCallsigns[0] = strings.Repeat("A", MaxPresetBytes-len(base)+1)
	encoded, err := yaml.Marshal(set)
	if err != nil || len(encoded) != MaxPresetBytes {
		t.Fatalf("standalone fixture=%d: %v", len(encoded), err)
	}
	cfg := ConfigurationFromPreset(set)
	configurationBytes, err := yaml.Marshal(cfg)
	if err != nil || len(configurationBytes) <= MaxPresetBytes {
		t.Fatalf("serializer disagreement absent: %d %v", len(configurationBytes), err)
	}
	record := &UserRecord{}
	applyRecordConfiguration(record, cfg)
	prepared, err := PrepareUserRecord("not a login", record)
	if err != nil || prepared == nil {
		t.Fatalf("Configuration serializer incorrectly determined admission: %v", err)
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadUserRecord("not a login"); err != nil {
		t.Fatalf("canonical load used wrong serializer: %v", err)
	}
	record.DXCallsigns[0] += "A"
	if _, err := PrepareUserRecord("N2WQ", record); !errors.Is(err, ErrStorageAdmission) {
		t.Fatalf("262145-byte configuration admitted: %v", err)
	}
	if err := yaml.Unmarshal(encoded, &SavedPreset{}); err != nil {
		t.Fatalf("exact callback boundary rejected: %v", err)
	}
	oversized, err := yaml.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(oversized, &SavedPreset{}); !errors.Is(err, ErrStorageAdmission) {
		t.Fatalf("direct preset callback bypassed bound: %v", err)
	}
}

func TestStorageCanonicalAndProspectiveRecordBounds(t *testing.T) {
	usePresetTestDir(t)
	path := userRecordPath("N2WQ")
	// Five aliases expand metadata while staying below raw/work/config limits.
	raw := []byte("legacy: &ip " + strings.Repeat("A", 220_000) + "\nrecent_ips: [*ip,*ip,*ip,*ip,*ip]\n")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadUserRecord("N2WQ"); !errors.Is(err, ErrStorageAdmission) || strings.Contains(err.Error(), "work") {
		t.Fatalf("canonical refusal not isolated: %v", err)
	}
	if err := yaml.Unmarshal(raw, &UserRecord{}); !errors.Is(err, ErrStorageAdmission) {
		t.Fatalf("direct record callback bypassed canonical bound: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, raw) {
		t.Fatal("canonical refusal changed file")
	}

	record := &UserRecord{ConfigurationVersion: CurrentConfigurationVersion, Filter: *NewFilter(), RecentIPs: []string{"A"}, LastLoginUTC: time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)}
	base, err := yaml.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	record.RecentIPs[0] = strings.Repeat("A", MaxUserRecordBytes-len(base)+1)
	prepared, err := PrepareUserRecord("N2WQ", record)
	if err != nil || prepared.ByteLen() != MaxUserRecordBytes {
		t.Fatalf("exact prospective boundary: bytes=%d err=%v", prepared.ByteLen(), err)
	}
	if err := prepared.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadUserRecord("N2WQ"); err != nil {
		t.Fatalf("exact canonical boundary: %v", err)
	}
	record.RecentIPs[0] += "A"
	if _, err := PrepareUserRecord("N2WQ", record); !errors.Is(err, ErrStorageAdmission) {
		t.Fatalf("prospective boundary bypassed: %v", err)
	}
	if _, err := PrepareUserRecord("N2WQ", &UserRecord{RecentIPs: []string{strings.Repeat("A", MaxUserRecordBytes+1)}}); !errors.Is(err, ErrStorageAdmission) {
		t.Fatalf("metadata lower bound bypassed: %v", err)
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
			if !errors.Is(err, ErrCurrentUserRecord) || !errors.Is(err, cause) || !strings.HasPrefix(err.Error(), "load current user record: ") {
				t.Fatalf("load boundary/cause lost: %v", err)
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
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Configuration{Settings: SettingsConfiguration{Grid: strings.Repeat("A", 250_000)}}
	if _, err := cfg.Preset(); err != nil {
		t.Fatal("replacement configuration does not independently fit", err)
	}
	_, err = PrepareConfiguration("N2WQ", cfg, nil, nil)
	if !errors.Is(err, ErrStorageAdmission) || errors.Is(err, ErrCurrentUserRecord) {
		t.Fatalf("prospective refusal confused with existing refusal: %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(after, before) {
		t.Fatal("prospective refusal changed stored bytes")
	}
}

func TestStorageWorkExactSharedBoundary(t *testing.T) {
	fixtures := []struct {
		raw  string
		work uint64
	}{
		{"preset: {name: TEST, baseline: {bands: {20m: true}}}\n", 93},
		{"presets: {TEST: {bands: {20m: true}}}\n", 67},
		{"legacy: \"\\u00e9\"\n", 20},
	}
	for _, fixture := range fixtures {
		var document yaml.Node
		if err := yaml.Unmarshal([]byte(fixture.raw), &document); err != nil {
			t.Fatal(err)
		}
		for _, budget := range []uint64{fixture.work, fixture.work - 1} {
			var err error
			if strings.HasPrefix(fixture.raw, "presets:") {
				_, _, err = decodeStoredPresetCollectionDocumentWithLimits(&document, storageDecodeLimits{work: budget, presets: MaxPresets})
			} else {
				_, _, err = decodeStoredUserRecordDocumentWithLimits(&document, storageDecodeLimits{work: budget})
			}
			if budget == fixture.work && err != nil || budget < fixture.work && !errors.Is(err, ErrStorageAdmission) {
				t.Fatalf("hand counted boundary %d/%d: %v", budget, fixture.work, err)
			}
		}
	}
	// Each direct callback shares its nested owner's work and retains stock resets.
	text := strings.Repeat("A", 32<<10)
	patterns := strings.TrimSuffix(strings.Repeat("*text,", 70), ",")
	for _, raw := range []string{
		"legacy: &text " + text + "\nrecent_ips: [" + patterns + "]\n",
		"grid: &text " + text + "\ncallsigns: [" + patterns + "]\n",
		"name: TEST\nbaseline: {grid: &text " + text + ", callsigns: [" + patterns + "]}\n",
	} {
		var target any = &UserRecord{}
		if strings.HasPrefix(raw, "grid:") {
			target = &SavedPreset{}
		}
		if strings.HasPrefix(raw, "name:") {
			target = &PresetReference{}
		}
		err := yaml.Unmarshal([]byte(raw), target)
		if !errors.Is(err, ErrStorageAdmission) || !strings.Contains(err.Error(), "decoding work") {
			t.Fatalf("direct owner callback bypassed shared work: %v", err)
		}
	}
}

func TestStoragePureAdmissionAndExactBaseline(t *testing.T) {
	usePresetTestDir(t)
	UserDataDir += "/not-created"
	if err := AdmitUserRecord(nil); err == nil || err.Error() != "nil user record" {
		t.Fatalf("pure nil validation: %v", err)
	}
	set := testSavedPreset()
	set.DXCallsigns = []string{"é"}
	base, err := yaml.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	padding := MaxPresetBytes - len(base)
	set.DXCallsigns[0] += strings.Repeat("é", padding/2) + strings.Repeat("A", padding%2)
	encoded, err := yaml.Marshal(set)
	if err != nil || len(encoded) != MaxPresetBytes {
		t.Fatalf("UTF8 baseline fixture: %d %v", len(encoded), err)
	}
	record := &UserRecord{Filter: *NewFilter(), Preset: &PresetReference{Name: "BASE", Baseline: set}, RecentIPs: []string{"a", "b", "c", "d", "e", strings.Repeat("X", MaxUserRecordBytes+1)}}
	if err := AdmitUserRecord(record); err != nil {
		t.Fatalf("exact baseline pure admission: %v", err)
	}
	if len(record.RecentIPs) != 6 || record.Preset.Baseline.ConfigurationVersion != set.ConfigurationVersion {
		t.Fatal("pure admission changed borrowed data")
	}
	if _, err := os.Stat(UserDataDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pure admission acquired filesystem authority")
	}
	set.DXCallsigns[0] += "A"
	if err := AdmitUserRecord(record); !errors.Is(err, ErrStorageAdmission) {
		t.Fatalf("oversized baseline admitted: %v", err)
	}
}
