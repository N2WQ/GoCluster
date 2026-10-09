package telnet

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

// Multiline metadata makes the exact canonical byte boundary test independent
// of decoding work. It is fixture data, not a claim that these are real IPs.
func metadataRecordAt(t *testing.T, size int) *filter.UserRecord {
	t.Helper()
	record := &filter.UserRecord{ConfigurationVersion: 4, Filter: *filter.NewFilter(), Dialect: "go", DedupePolicy: "MED"}
	encoded := func(n, tail int) []byte {
		record.RecentIPs = []string{strings.Repeat("A\n", n) + strings.Repeat("Z", tail+1)}
		bs, err := yaml.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		return bs
	}
	first, second := len(encoded(1, 0)), len(encoded(2, 0))
	slope := second - first
	if slope <= 0 || size < first {
		t.Fatal("invalid canonical boundary fixture")
	}
	n := 1 + (size-first)/slope
	tail := size - len(encoded(n, 0))
	if got := len(encoded(n, tail)); got != size {
		t.Fatalf("canonical fixture has %d bytes, want %d", got, size)
	}
	return record
}

func writeAdmissionRecord(t *testing.T, call string, record *filter.UserRecord) []byte {
	t.Helper()
	bs, err := yaml.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filter.UserDataDir, call+".yaml"), bs, 0o600); err != nil {
		t.Fatal(err)
	}
	return bs
}

func refusedRecordFixtures(t *testing.T) map[string][]byte {
	t.Helper()
	expanded := metadataRecordAt(t, filter.MaxUserRecordBytes+1)
	ip := expanded.RecentIPs[0]
	expanded.RecentIPs = nil
	compact, err := yaml.Marshal(expanded)
	if err != nil {
		t.Fatal(err)
	}
	compact = append(compact, []byte("recent_ips:\n  - "+strconv.Quote(ip)+"\n")...)
	work := "configuration_version: 4\nrecent_ips:\n  - &ip " + strings.Repeat("A", 125000) + "\n" + strings.Repeat("  - *ip\n", 19)
	return map[string][]byte{
		"raw":       []byte("#" + strings.Repeat("A", filter.MaxUserRecordBytes) + "\n"),
		"canonical": compact,
		"work":      []byte(work),
	}
}

func TestAdmissionRefusedLoginAndWarmSessionPreserveFiles(t *testing.T) {
	for name, refused := range refusedRecordFixtures(t) {
		for _, login := range []bool{false, true} {
			t.Run(name+"/login="+strconv.FormatBool(login), func(t *testing.T) {
				s := presetTestServer(t)
				c := configurationTestClient(s, "W1ABC-1")
				if err := c.saveFilter(); err != nil {
					t.Fatal(err)
				}
				s.registerClient(c)
				path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
				if err := os.WriteFile(path, refused, 0o600); err != nil {
					t.Fatal(err)
				}
				if _, err := filter.LoadUserRecord(c.callsign); !errors.Is(err, filter.ErrStorageAdmission) {
					t.Fatalf("fixture did not reach production resource admission: %v", err)
				}
				commits := 0
				s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
				s.commitLoginRecordFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
				if login {
					restored := s.restoreClientRecordOwned(c, time.Now().UTC())
					if !c.recordProtected || !strings.Contains(restored.warning, "temporary defaults") {
						t.Fatalf("refused login was not protected: %+v", restored)
					}
				}
				response, handled := s.handlePathSettingsCommand(c, "SET NOISE URBAN")
				if !handled || !strings.Contains(response, "warning: failed to persist") || c.noiseClass != "URBAN" || commits != 0 || c.recordProtected != login {
					t.Fatalf("fitting temporary change: %q, commits=%d protected=%v", response, commits, c.recordProtected)
				}
				response, _ = s.handlePresetCommand(c, "SAVE PRESET NEW")
				if !strings.HasPrefix(response, "SAVE PRESET failed:") {
					t.Fatalf("SAVE gave partial success for admission failure: %q", response)
				}
				if _, err := os.Stat(presetLibraryPathFor(c.callsign)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("SAVE created a library before record admission: %v", err)
				}
				s.unregisterClient(c)
				if commits != 0 || !bytes.Equal(refused, presetDiskBytes(t, path)) {
					t.Fatal("temporary command or logout overwrote refused bytes")
				}
			})
		}
	}
}

func presetLibraryPathFor(call string) string {
	return filepath.Join(filter.UserDataDir, "presets", hex.EncodeToString([]byte(spot.NormalizeOwnCallsign(call)))+".yaml")
}

func TestAdmissionHumanCandidateRejectedWithoutPublication(t *testing.T) {
	for _, protected := range []bool{false, true} {
		t.Run(strconv.FormatBool(protected), func(t *testing.T) {
			s := presetTestServer(t)
			s.filterEngine = newFilterCommandEngine()
			c := configurationTestClient(s, "W1ABC-1")
			cfg, err := c.captureConfiguration(0)
			if err != nil {
				t.Fatal(err)
			}
			set, err := cfg.Preset()
			if err != nil {
				t.Fatal(err)
			}
			set.DXCallsigns = []string{"A"}
			bs, err := yaml.Marshal(set)
			if err != nil {
				t.Fatal(err)
			}
			c.filter.DXCallsigns = []string{strings.Repeat("A", filter.MaxPresetBytes-len(bs)+1)}
			if _, err := c.presetSnapshot(); err != nil {
				t.Fatalf("fitting control failed: %v", err)
			}
			c.recordProtected = protected
			before, _ := c.captureConfiguration(0)
			revision, _ := c.configurationRevisionToken()
			commits := 0
			s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
			response, _ := s.filterEngine.Handle(c, "PASS DXCALL W1*")
			after, _ := c.captureConfiguration(0)
			afterRevision, _ := c.configurationRevisionToken()
			if !strings.HasPrefix(response, "Configuration command failed:") || !before.Equal(after) || revision != afterRevision || commits != 0 {
				t.Fatalf("oversized temporary candidate was published: %q, commits=%d", response, commits)
			}
		})
	}
}

func TestAdmissionProspectiveHumanRecordRejected(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	before := writeAdmissionRecord(t, c.callsign, metadataRecordAt(t, filter.MaxUserRecordBytes-2))
	if _, err := filter.LoadUserRecord(c.callsign); err != nil {
		t.Fatalf("fitting authoritative record was refused: %v", err)
	}
	commits := 0
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
	cfg, _ := c.captureConfiguration(0)
	revision, _ := c.configurationRevisionToken()
	response, _ := s.handlePathSettingsCommand(c, "SET NOISE URBAN")
	after, _ := c.captureConfiguration(0)
	afterRevision, _ := c.configurationRevisionToken()
	if !strings.HasPrefix(response, "Configuration command failed:") || !cfg.Equal(after) || revision != afterRevision || commits != 0 || !bytes.Equal(before, presetDiskBytes(t, path)) {
		t.Fatalf("prospective quota failure had effects: %q, commits=%d", response, commits)
	}
}

func TestAdmissionTemporaryBaselineAndMetadataRefuseRuntimeEffects(t *testing.T) {
	for _, bad := range []string{"baseline-size", "baseline-version", "temporary-record"} {
		t.Run(bad, func(t *testing.T) {
			s := presetTestServer(t)
			c := configurationTestClient(s, "W1ABC-1")
			if err := c.saveFilter(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
			refused := []byte("[broken")
			if err := os.WriteFile(path, refused, 0o600); err != nil {
				t.Fatal(err)
			}
			c.setSolarSummaryMinutes(30, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
			c.configuredSettings.SolarSummaryMinutes = 30
			c.filter.NearbyEnabled = true
			c.filter.NearbyUserFine, c.filter.NearbyUserCoarse = 1, 2
			c.filter.NearbySnapshot = &filter.NearbyLocationSnapshot{}
			c.presetReference = &filter.PresetReference{Name: "OLD", Baseline: &filter.SavedPreset{Filter: *filter.NewFilter()}}
			switch bad {
			case "baseline-size":
				c.presetReference.Baseline.DXCallsigns = []string{strings.Repeat("A", filter.MaxPresetBytes+1)}
			case "baseline-version":
				c.presetReference.Baseline.ConfigurationVersion = 99
			case "temporary-record":
				c.recentIPs = []string{strings.Repeat("A", filter.MaxUserRecordBytes+1)}
			}
			cfg, _ := c.captureConfiguration(0)
			revision, _ := c.configurationRevisionToken()
			cursor := &historySearch{token: "older"}
			c.historySearch = cursor
			generation := c.historyGeneration
			clock, snapshot := c.solarNextSummaryAt, c.filter.NearbySnapshot
			policy, pause := c.getDedupePolicy(), c.readPauseUntilUnixNano.Load()
			commits := 0
			s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
			// An accepted same-cadence SET would deliberately reschedule; refusal
			// must occur before that runtime effect or history invalidation.
			response, _ := s.handleSolarCommand(c, "SET SOLAR 30")
			after, _ := c.captureConfiguration(0)
			afterRevision, _ := c.configurationRevisionToken()
			if !strings.HasPrefix(response, "Configuration command failed:") || !cfg.Equal(after) || revision != afterRevision || commits != 0 || !bytes.Equal(refused, presetDiskBytes(t, path)) {
				t.Fatalf("temporary refusal had configuration/durable effects: %s => %q", bad, response)
			}
			if c.solarNextSummaryAt != clock || c.filter.NearbySnapshot != snapshot || c.filter.NearbyUserFine != 1 || c.filter.NearbyUserCoarse != 2 || c.getDedupePolicy() != policy || c.readPauseUntilUnixNano.Load() != pause || c.historySearch != cursor || c.historyGeneration != generation {
				t.Fatalf("temporary refusal changed scheduling/location/history: %s", bad)
			}
		})
	}
}

func TestAdmissionWarmRepairRetriesFreshMetadata(t *testing.T) {
	for _, logoutOnly := range []bool{false, true} {
		t.Run(strconv.FormatBool(logoutOnly), func(t *testing.T) {
			s := presetTestServer(t)
			c := configurationTestClient(s, "W1ABC-1")
			s.registerClient(c)
			path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
			if err := os.WriteFile(path, []byte("[broken"), 0o600); err != nil {
				t.Fatal(err)
			}
			response, _ := s.handlePathSettingsCommand(c, "SET NOISE URBAN")
			if !strings.Contains(response, "warning: failed to persist") || c.recordProtected {
				t.Fatalf("warm failure became sticky: %q", response)
			}
			stamp := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
			repaired := &filter.UserRecord{Filter: *filter.NewFilter(), Dialect: "go", LastLoginUTC: stamp, RecentIPs: []string{"192.0.2.99"}}
			if err := filter.SaveUserRecord(c.callsign, repaired); err != nil {
				t.Fatal(err)
			}
			if !logoutOnly {
				response, _ = s.handlePathSettingsCommand(c, "SET PATHSAMPLES 30")
				if strings.Contains(response, "failed") {
					t.Fatal(response)
				}
			}
			s.unregisterClient(c)
			record, err := filter.LoadUserRecord(c.callsign)
			if err != nil || record.NoiseClass != "URBAN" || !record.LastLoginUTC.Equal(stamp) || len(record.RecentIPs) != 1 || record.RecentIPs[0] != "192.0.2.99" {
				t.Fatalf("repair retry lost fresh metadata or temporary settings: %+v, %v", record, err)
			}
		})
	}
}

func TestAdmissionLoginMetadataOverflowDoesNotProtectFittingRecord(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	before := writeAdmissionRecord(t, c.callsign, metadataRecordAt(t, filter.MaxUserRecordBytes-2))
	commits := 0
	s.commitLoginRecordFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
	restored := s.restoreClientRecordOwned(c, time.Now().UTC())
	if c.recordProtected || restored.loadError != nil || !strings.Contains(restored.warning, "could not save this login's timestamp/IP") || commits != 0 || c.lastLoginUTC.IsZero() || !bytes.Equal(before, presetDiskBytes(t, path)) {
		t.Fatalf("prospective login failure: %+v, protected=%v commits=%d", restored, c.recordProtected, commits)
	}
}

func TestAdmissionSaveAndLoadRejectBeforeLibraryOrPublication(t *testing.T) {
	s := presetTestServer(t)
	c := presetTransactionClient(s)
	if response, _ := s.handlePresetCommand(c, "SAVE PRESET OLD"); response != "Saved preset OLD for N2WQ.\n" {
		t.Fatal(response)
	}
	if err := filter.SavePreset(c.callsign, "LOADABLE", &filter.SavedPreset{Filter: *filter.NewFilter(), NoiseClass: "URBAN"}); err != nil {
		t.Fatal(err)
	}
	old := c.presetReference
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	before := writeAdmissionRecord(t, c.callsign, metadataRecordAt(t, filter.MaxUserRecordBytes-200))
	library := presetDiskBytes(t, presetLibraryPath())
	commits := 0
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
	for _, command := range []string{"SAVE PRESET OLD", "SAVE PRESET NEW", "LOAD PRESET LOADABLE"} {
		cfg, _ := c.captureConfiguration(0)
		response, _ := s.handlePresetCommand(c, command)
		after, _ := c.captureConfiguration(0)
		if !strings.Contains(response, "PRESET failed:") || !cfg.Equal(after) || c.presetReference != old || commits != 0 || !bytes.Equal(before, presetDiskBytes(t, path)) || !bytes.Equal(library, presetDiskBytes(t, presetLibraryPath())) {
			t.Fatalf("admission failure had effects: %s => %q, commits=%d", command, response, commits)
		}
	}
}

func TestAdmissionMachineQuotaFailureAndValidateAreNonmutating(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	before := writeAdmissionRecord(t, c.callsign, metadataRecordAt(t, filter.MaxUserRecordBytes-2))
	commits := 0
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { commits++; return nil }
	for _, verb := range []string{"VALIDATE", "PATCH"} {
		var command machineCommand
		var request machineRequest
		if verb == "VALIDATE" {
			command = machineCommand{Verb: verb, Resource: "CONFIG"}
			var err error
			request, err = decodeMachineRequest(machineRequestFixture(completeValidationConfigurationTest(), false), command)
			if err != nil {
				t.Fatal(err)
			}
		} else {
			command, request = decodeProposalTest(t, c, verb, "SETTINGS", "  noise_class: URBAN\n")
		}
		cfg, _ := c.captureConfiguration(0)
		response := s.applyMachineRequest(c, command, request)
		after, _ := c.captureConfiguration(0)
		if verb == "PATCH" && !strings.Contains(response, "code: persistence_failed") || verb == "VALIDATE" && strings.Contains(response, "error:") || !cfg.Equal(after) || commits != 0 || !bytes.Equal(before, presetDiskBytes(t, path)) {
			t.Fatalf("machine admission/validation effects: %s => %q", verb, response)
		}
	}
}
