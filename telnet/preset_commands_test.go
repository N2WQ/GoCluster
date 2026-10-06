package telnet

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"gopkg.in/yaml.v3"
)

func presetTestServer(t *testing.T) *Server {
	t.Helper()
	previous := filter.UserDataDir
	filter.UserDataDir = t.TempDir()
	t.Cleanup(func() { filter.UserDataDir = previous })
	cfg := pathreliability.DefaultConfig()
	cfg.MinObservationCount = 19
	return &Server{dedupeFastEnabled: true, dedupeMedEnabled: true, dedupeSlowEnabled: true, pathPredictor: pathreliability.NewPredictor(cfg, []string{"20m"}), noiseModel: cfg.NoiseModel(), nearbyLoginWarning: nearbyLoginWarningMsg}
}

func TestParsePresetCommand(t *testing.T) {
	for _, tc := range []struct {
		line, verb, name string
		handled, invalid bool
	}{
		{"save preset Contest", "SAVE", "CONTEST", true, false},
		{" LIST\tpreset ", "LIST", "", true, false},
		{"load preset 1-main", "LOAD", "1-MAIN", true, false},
		{"DELETE PRESET CON", "DELETE", "CON", true, false},
		{"SAVE", "SAVE", "", true, true},
		{"SAVE PRESET", "SAVE", "", true, true},
		{"LIST PRESET extra", "LIST", "", true, true},
		{"LOAD PRESET one two", "LOAD", "", true, true},
		{"DELETE PRESET ../bad", "DELETE", "", true, true},
		{"SAVE PRESET good_bad", "SAVE", "", true, true},
		{"SAVE PRESET _bad", "SAVE", "", true, true},
		{"SAVE PRESET Å", "SAVE", "", true, true},
		{"SAVE OTHER one", "", "", false, false},
		{"SAVE FILTER one", "", "", false, false},
		{"LIST FILTER", "", "", false, false},
		{"LOAD FILTER one", "", "", false, false},
		{"DELETE FILTER one", "", "", false, false},
		{"SHOW FILTER", "", "", false, false},
		{"RESET FILTER", "", "", false, false},
		{"", "", "", false, false},
	} {
		command, handled, usage := parsePresetCommand(tc.line)
		if handled != tc.handled || (usage != "") != tc.invalid || command.verb != tc.verb || command.name != tc.name {
			t.Fatalf("%q: %+v handled=%v usage=%q", tc.line, command, handled, usage)
		}
	}
}

func TestPresetCommands(t *testing.T) {
	s := presetTestServer(t)
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			origin := newTestClient()
			origin.callsign, origin.dialect = "N2WQ-1", dialect
			origin.filter.SetBand("20m", true)
			origin.filter.AddDXCallsignPattern("K1*")
			origin.filter.SetWWVEnabled(false)
			origin.grid, origin.noiseClass = "FN31", "URBAN"
			origin.pathMinObservationCount = 30
			origin.setSolarSummaryMinutes(30, time.Now())
			origin.setDedupePolicy(dedupePolicySlow)
			response, handled := s.handlePresetCommand(origin, "save preset contest-live-1")
			if !handled || !strings.Contains(response, "Saved preset CONTEST-LIVE-1") {
				t.Fatalf("SAVE: %q", response)
			}
			origin.filter.SetBand("40m", true)
			target := newTestClient()
			target.callsign, target.dialect = "N2WQ-2", dialect
			target.setDiagMode(diagModeSource)
			target.readPauseUntilUnixNano.Store(12345)
			login := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
			if err := filter.SaveUserRecord(target.callsign, &filter.UserRecord{Filter: *target.filter, LastLoginUTC: login, RecentIPs: []string{"192.0.2.2"}}); err != nil {
				t.Fatal(err)
			}
			pointer := target.filter
			response, handled = s.handlePresetCommand(target, "LIST PRESET")
			if !handled || !strings.Contains(response, "N2WQ (1/20):\nCONTEST-LIVE-1\n") {
				t.Fatalf("LIST: %q", response)
			}
			response, handled = s.handlePresetCommand(target, "LOAD PRESET contest-live-1")
			if !handled || !strings.Contains(response, "Loaded preset CONTEST-LIVE-1") {
				t.Fatalf("LOAD: %q", response)
			}
			if target.filter != pointer || !target.filter.Bands["20m"] || target.filter.Bands["40m"] || target.filter.WWVEnabled() || target.grid != "FN31" || target.noiseClass != "URBAN" || target.pathMinObservationCount != 30 || target.getSolarSummaryMinutes() != 30 || target.getDedupePolicy() != dedupePolicySlow || target.dialect != dialect {
				t.Fatal("LOAD failed to restore all preferences independently")
			}
			if target.getDiagMode() != diagModeSource || target.readPauseUntilUnixNano.Load() != 12345 {
				t.Fatal("LOAD changed temporary session controls")
			}
			record, err := filter.LoadUserRecord("N2WQ-2")
			if err != nil || record.Grid != "FN31" || record.PathMinObservationCount != 30 || record.SolarSummaryMinutes != 30 || record.Dialect != string(dialect) {
				t.Fatalf("reconnect defaults=%+v err=%v", record, err)
			}
			if !record.LastLoginUTC.Equal(login) || strings.Join(record.RecentIPs, ",") != "192.0.2.2" || !origin.filter.Bands["40m"] {
				t.Fatal("LOAD changed login metadata or another connected SSID")
			}
			response, handled = s.handlePresetCommand(target, "DELETE PRESET contest-live-1")
			if !handled || !strings.Contains(response, "Deleted preset CONTEST-LIVE-1") || target.grid != "FN31" || !target.filter.Bands["20m"] {
				t.Fatalf("DELETE changed active settings: %q", response)
			}
			response, _ = s.handlePresetCommand(origin, "LOAD PRESET contest-live-1")
			if response != "Saved preset CONTEST-LIVE-1 not found.\n" {
				t.Fatalf("missing: %q", response)
			}
		})
	}
}

func TestPresetLoadFailure(t *testing.T) {
	s := presetTestServer(t)
	client := newTestClient()
	client.callsign, client.grid, client.noiseClass = "N2WQ-2", "IO91", "QUIET"
	client.setDedupePolicy(dedupePolicyFast)
	client.setSolarSummaryMinutes(15, time.Now())
	if err := client.saveFilter(); err != nil {
		t.Fatal(err)
	}
	set := &filter.SavedPreset{Filter: *filter.NewFilter(), Grid: "FN31", NoiseClass: "URBAN", Dialect: "cc", DedupePolicy: "SLOW", SolarSummaryMinutes: 30, PathMinObservationCount: 40}
	if err := filter.SavePreset("N2WQ-1", "new", set); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filter.UserDataDir, "N2WQ-2.yaml")
	beforeDisk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	before, err := client.presetSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	beforeBytes, err := yaml.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	beforePath, pointer, beforeTick := client.pathSnapshot(), client.filter, client.solarNextSummaryAt
	s.saveConfigurationFn = func(_ string, _ filter.Configuration, _ *filter.PresetReference, _ []string) error {
		if client.grid != "IO91" || client.dialect != DialectGo {
			t.Fatal("live settings changed before disk commit")
		}
		return os.ErrNotExist // A filesystem failure must not be reported as a missing preset.
	}
	response, _ := s.handlePresetCommand(client, "LOAD PRESET NEW")
	s.saveConfigurationFn = nil
	if !strings.Contains(response, "LOAD PRESET failed") || strings.Contains(response, "Loaded preset") {
		t.Fatalf("failure reported success: %q", response)
	}
	after, err := client.presetSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	afterBytes, err := yaml.Marshal(after)
	if err != nil || !bytes.Equal(beforeBytes, afterBytes) || client.pathSnapshot() != beforePath || client.filter != pointer || client.solarNextSummaryAt != beforeTick {
		t.Fatal("failed LOAD changed live settings")
	}
	afterDisk, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(beforeDisk, afterDisk) {
		t.Fatal("failed LOAD changed saved default")
	}
	if err := os.WriteFile(path, []byte("[broken"), 0o644); err != nil {
		t.Fatal(err)
	}
	response, _ = s.handlePresetCommand(client, "LOAD PRESET NEW")
	if !strings.Contains(response, "LOAD PRESET failed") || client.grid != "IO91" {
		t.Fatalf("corrupt current record silently replaced: %q", response)
	}
}

func TestPresetLoadRuntimeState(t *testing.T) {
	s := presetTestServer(t)
	requireH3Mappings(t)
	s.dedupeSlowEnabled = false
	origin := newTestClient()
	origin.callsign, origin.grid, origin.noiseClass = "N2WQ-1", "FN31", "URBAN"
	origin.gridCell, origin.gridCoarseCell = pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")
	origin.pathMinObservationCount = 40
	origin.setDedupePolicy(dedupePolicySlow)
	origin.setSolarSummaryMinutes(30, time.Now())
	origin.filter.SetDXContinent("EU", true)
	if err := origin.filter.EnableNearby(origin.gridCell, origin.gridCoarseCell); err != nil {
		t.Fatal(err)
	}
	if response, _ := s.handlePresetCommand(origin, "SAVE PRESET nearby"); !strings.Contains(response, "Saved preset") {
		t.Fatal(response)
	}
	target := newTestClient()
	target.callsign, target.grid, target.noiseClass = "N2WQ-2", "IO91", "QUIET"
	start := time.Now().UTC()
	if response, _ := s.handlePresetCommand(target, "LOAD PRESET NEARBY"); !strings.Contains(response, "dedupe SLOW unavailable; using FAST") {
		t.Fatal(response)
	}
	if !target.filter.NearbyEnabled || target.filter.NearbySnapshot == nil || target.filter.NearbyUserFine != pathreliability.EncodeCell("FN31") || target.gridCell != pathreliability.EncodeCell("FN31") || target.gridCoarseCell != pathreliability.EncodeCoarseCell("FN31") || target.getDedupePolicy() != dedupePolicyFast {
		t.Fatal("derived runtime state was not rebuilt")
	}
	if tick := target.solarNextSummaryAt; tick.Before(start) || tick.Sub(start) > 30*time.Minute || tick.Minute()%30 != 0 || tick.Second() != 0 {
		t.Fatalf("wrong solar schedule: %v", tick)
	}
	target.filter.DisableNearby()
	if !target.filter.DXContinents["EU"] || target.filter.AllDXContinents {
		t.Fatal("NEARBY OFF lost saved location filters")
	}
	set, err := filter.LoadPreset("N2WQ", "NEARBY")
	if err != nil {
		t.Fatal(err)
	}
	set.NearbyEnabled, set.Grid, set.NoiseClass = false, "", ""
	set.PathMinObservationCount, set.SolarSummaryMinutes = 10, 0
	if err := filter.SavePreset("N2WQ", "DEFAULTS", set); err != nil {
		t.Fatal(err)
	}
	s.gridLookup = func(_ string) (string, bool, bool) { return "IO91", true, true }
	if response, _ := s.handlePresetCommand(target, "LOAD PRESET DEFAULTS"); !strings.Contains(response, "Loaded preset") {
		t.Fatal(response)
	}
	if target.filter.NearbyEnabled || target.grid != "IO91" || !target.gridDerived || target.pathMinObservationCount != 0 || target.noiseClass != "QUIET" || target.getSolarSummaryMinutes() != 0 || !target.solarNextSummaryAt.IsZero() {
		t.Fatal("LOAD did not reset inactive/default settings")
	}
	set.NearbyEnabled, set.Grid = true, "INVALID"
	if err := filter.SavePreset("N2WQ", "UNLOCATED", set); err != nil {
		t.Fatal(err)
	}
	response, _ := s.handlePresetCommand(target, "LOAD PRESET UNLOCATED")
	if !strings.Contains(response, nearbyLoginInactiveMsg) || !target.filter.NearbyEnabled || target.filter.NearbySnapshot != nil || target.filter.NearbyUserFine != pathreliability.InvalidCell {
		t.Fatalf("NEARBY without usable cells: %q", response)
	}
}

func TestPresetLoadConcurrentReaders(t *testing.T) {
	s := presetTestServer(t)
	client := newTestClient()
	client.callsign = "N2WQ-2"
	if err := filter.SavePreset("N2WQ-1", "ONE", &filter.SavedPreset{Filter: *filter.NewFilter(), Grid: "FN31", DedupePolicy: "MED"}); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			client.filterMu.RLock()
			_ = client.filter.String()
			client.filterMu.RUnlock()
			_ = client.pathSnapshot()
			_ = client.getSolarSummaryMinutes()
			_ = client.getDedupePolicy()
		}
	})
	for range 10 {
		if response, _ := s.handlePresetCommand(client, "LOAD PRESET ONE"); !strings.Contains(response, "Loaded preset") {
			t.Error(response)
		}
	}
	close(stop)
	wg.Wait()
}

func TestPresetSessionTranscriptAndReconnect(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			s := newHandshakeTranscriptServer(t)
			s.noiseModel = pathreliability.DefaultConfig().NoiseModel()
			for i, call := range []string{"N0CALL-1", "N0CALL-2", "N0CALL-2"} {
				serverConn, conn, done := startHandshakeTranscriptSession(t, s)
				t.Cleanup(func() { closeHandshakeTranscriptSession(t, conn, done) })
				readUntilContains(t, conn, "login: ", 2*time.Second)
				if _, err := io.WriteString(conn, call+"\r\n"); err != nil {
					t.Fatal(err)
				}
				greeting := readUntilContains(t, conn, "UTC>", 2*time.Second)
				if i == 2 && !strings.Contains(greeting, "Noise: URBAN") {
					t.Fatalf("reconnect lost loaded preferences: %q", greeting)
				}
				commands := []struct{ line, want string }{{"SET NOISE URBAN", "Noise class set to URBAN"}, {"SAVE PRESET contest-live-1", "Saved preset CONTEST-LIVE-1"}}
				if i == 1 {
					commands = []struct{ line, want string }{{"LOAD PRESET contest-live-1", "Loaded preset CONTEST-LIVE-1"}, {"LIST PRESET", "CONTEST-LIVE-1"}}
				}
				if i == 2 {
					commands = []struct{ line, want string }{{"DELETE PRESET contest-live-1", "Deleted preset CONTEST-LIVE-1"}}
				}
				if dialect == DialectCC && i < 2 {
					commands = append([]struct{ line, want string }{{"DIALECT cc", "Dialect set to CC"}}, commands...)
				}
				if i == 0 {
					for _, obsolete := range []string{"SAVE FILTER contest", "LIST FILTER", "LOAD FILTER contest", "DELETE FILTER contest"} {
						commands = append(commands, struct{ line, want string }{obsolete, "Unknown command: " + strings.Fields(obsolete)[0]})
					}
					show := "SHOW FILTER"
					if dialect == DialectCC {
						show = "SHOW/FILTER"
					}
					commands = append(commands, struct{ line, want string }{show, "Type RESUME when ready. Missed spots are not replayed."}, struct{ line, want string }{"RESET FILTER", "Filters reset to defaults"})
				}
				for _, command := range commands {
					if _, err := io.WriteString(conn, command.line+"\r\n"); err != nil {
						t.Fatal(err)
					}
					readUntilContains(t, conn, command.want, 2*time.Second)
				}
				closeHandshakeTranscriptSession(t, conn, done)
				if err := serverConn.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func FuzzParsePresetCommand(f *testing.F) {
	for _, seed := range []string{"SAVE PRESET contest-live-1", "SAVE PRESET good_bad", "LIST PRESET", "LOAD PRESET two words", "DELETE PRESET ../bad", "SAVE", "SHOW FILTER", "save\tpreset\t1-main", "SAVE FILTER one", "LIST FILTER", "LOAD FILTER one", "DELETE FILTER one"} {
		f.Add(seed)
	}
	grammar := regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,31}$`)
	f.Fuzz(func(t *testing.T, line string) {
		command, handled, usage := parsePresetCommand(line)
		tokens := strings.Fields(line)
		verb := ""
		if len(tokens) > 0 {
			verb = strings.ToUpper(tokens[0])
		}
		supported := verb == "SAVE" || verb == "LIST" || verb == "LOAD" || verb == "DELETE"
		wantHandled := supported && (len(tokens) == 1 || strings.EqualFold(tokens[1], "PRESET"))
		if handled != wantHandled {
			t.Fatalf("wrong command routing: %q handled=%v", line, handled)
		}
		if !wantHandled {
			return
		}
		valid := verb == "LIST" && len(tokens) == 2 || verb != "LIST" && len(tokens) == 3 && grammar.MatchString(tokens[2])
		if valid != (usage == "") || command.verb != verb {
			t.Fatalf("grammar mismatch: %q command=%+v usage=%q", line, command, usage)
		}
		if valid && verb != "LIST" && command.name != strings.ToUpper(tokens[2]) {
			t.Fatalf("incorrect canonical name: %q", line)
		}
	})
}
