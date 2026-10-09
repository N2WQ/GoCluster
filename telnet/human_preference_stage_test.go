package telnet

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/commands"
	"dxcluster/filter"
)

func TestHumanPreferenceCandidateRawIsolation(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.grid, c.gridDerived, c.noiseClass, c.pathMinObservationCount = "FN31", true, "URBAN", 40
	c.gridCell, c.gridCoarseCell = 1, 2
	c.filter.Bands, c.filter.DXCallsigns, c.filter.Comments = map[string]bool{"20m": false}, []string{"K1*"}, []string{"POTA"}
	c.filter.AllowWWV = filter.DefaultBoolFalse.Pointer()
	c.filter.LegacyMinConfidence = 17
	if err := c.filter.EnableNearby(1, 2); err != nil {
		t.Fatal(err)
	}
	c.recordProtected, c.configurationRevision, c.historyGeneration = true, 7, 9
	c.configurationDigest, c.historyDigest = [32]byte{1}, [32]byte{2}
	processor := commands.NewProcessor(nil, nil, nil, nil, nil, nil)
	query, handled, response := processor.ParseHistoryCommand("SHOW DX 2 COMMENT POTA", "go")
	if !handled || response != "" {
		t.Fatal("history query control was not valid")
	}
	expectedQuery, _, _ := processor.ParseHistoryCommand("SHOW DX 2 COMMENT POTA", "go")
	c.historySearch = &historySearch{query: query.Query, before: []byte{1, 2, 3}, token: "raw-cursor", unreadable: true, hadMatches: true}
	c.controlChan <- controlMessage{line: "control-sentinel"}
	c.readPauseEpoch = 8
	c.readPausePending.Store(true)
	c.setDiagMode(diagModeSource)
	c.setDedupePolicy(dedupePolicySlow)
	c.setSolarSummaryMinutes(30, time.Date(2026, 10, 9, 12, 1, 0, 0, time.UTC))
	// Capture RAW state before any capture/revision helper. Those helpers would
	// initialize settings and mask a constructor that mutates legacy live input.
	settings, initialized := c.configuredSettings, c.configurationInitialized
	digest, digestSet, epoch, revision := c.configurationDigest, c.configurationDigestSet, c.configurationEpoch, c.configurationRevision
	history, historyDigest, historySet, generation := c.historySearch, c.historyDigest, c.historyDigestSet, c.historyGeneration
	state, tick, pointer, snapshot := c.pathSnapshot(), c.solarNextSummaryAt, c.filter, c.filter.NearbySnapshot
	before := filter.ConfigurationFromFilter(c.filter, settings).Clone()
	staged := newHumanPreferenceCandidate(c)
	if staged.filter == c.filter || staged.done != nil || staged.conn != nil || staged.reader != nil || staged.writer != nil || staged.controlChan != nil || staged.spotChan != nil || staged.historySearch != nil || staged.recentIPs != nil || staged.presetReference != nil {
		t.Fatal("candidate copied writable preferences or connection/lifecycle ownership")
	}
	if staged.configuredSettings.Grid != "" || staged.grid != "FN31" || !staged.gridDerived || staged.getDedupePolicy() != dedupePolicySlow || staged.filter.LegacyMinConfidence != 17 || staged.filter.NearbySnapshot != snapshot || staged.filter.NearbyUserFine != 1 || staged.filter.NearbyUserCoarse != 2 {
		t.Fatal("candidate lost configured/effective or NEARBY runtime distinction")
	}
	staged.filter.Bands["20m"], staged.filter.DXCallsigns[0], staged.filter.Comments[0], *staged.filter.AllowWWV = true, "CHANGED", "CHANGED", true
	staged.grid, staged.configuredSettings.Grid = "CHANGED", "CHANGED"
	staged.filter.DisableNearby()
	if !before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) || c.filter != pointer || c.filter.NearbySnapshot != snapshot || !c.filter.NearbyActive() || c.pathSnapshot() != state || c.solarNextSummaryAt != tick {
		t.Fatal("discarded candidate changed live preferences/runtime")
	}
	if c.configuredSettings != settings || c.configurationInitialized != initialized || c.configurationDigest != digest || c.configurationDigestSet != digestSet || c.configurationEpoch != epoch || c.configurationRevision != revision || c.historySearch != history || c.historyDigest != historyDigest || c.historyDigestSet != historySet || c.historyGeneration != generation || c.readPauseEpoch != 8 || !c.readPausePending.Load() || c.getDiagMode() != diagModeSource || !c.recordProtected {
		t.Fatal("constructor initialized or changed raw live metadata/session state")
	}
	if !reflect.DeepEqual(c.historySearch.query, expectedQuery.Query) || !bytes.Equal(c.historySearch.before, []byte{1, 2, 3}) || c.historySearch.token != "raw-cursor" || !c.historySearch.unreadable || !c.historySearch.hadMatches {
		t.Fatal("candidate changed cursor contents through the retained live pointer")
	}
	if len(c.controlChan) != 1 || (<-c.controlChan).line != "control-sentinel" {
		t.Fatal("staging sent external output or changed queued control contents")
	}
}

func TestHumanPreferenceExplicitRefusalAndSaveIntent(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			s := presetTestServer(t)
			s.filterEngine = newFilterCommandEngine()
			c := configurationTestClient(s, "W1ABC-1")
			c.dialect = dialect
			c.filter.Comments = []string{"POTA"}
			if err := c.saveFilter(); err != nil {
				t.Fatal(err)
			}
			before, err := c.captureConfiguration(0)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
			disk, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			attempts := 0
			s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { attempts++; return nil }
			for _, line := range []string{"PASS BAND 20 NOTABAND", "PASS MODE CW NOTAMODE", "PASS DXCONT EU INVALID", "PASS COMMENT é", "PASS COMMENT pota", "REMOVE PASS COMMENT absent"} {
				staged := newHumanPreferenceCandidate(c)
				outcome := s.filterEngine.stageOwned(staged, line)
				if !outcome.handled || outcome.publish || outcome.forceSolar {
					t.Fatalf("refused/unchanged command acquired publish intent: %s => %+v", line, outcome)
				}
				if _, handled := s.filterEngine.Handle(c, line); !handled || attempts != 0 {
					t.Fatalf("refused/unchanged command attempted a save: %s", line)
				}
			}
			// A deliberately faulty handler models partial mutation followed by
			// refusal. The coordinator must discard the candidate even then.
			s.filterEngine.registerDomain(&domainHandler{name: "FAULT", apply: func(staged *Client, _ filterAction, _ []string) (string, bool) {
				staged.updateFilter(func(f *filter.Filter) { f.BlockAllBands = true })
				return "Refused after candidate mutation\n", false
			}})
			if _, handled := s.filterEngine.Handle(c, "PASS FAULT"); !handled || attempts != 0 {
				t.Fatal("faulty refusing handler saved its partial mutation")
			}
			if !before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) || c.filter.BlockAllBands {
				t.Fatal("refused stage changed live selections")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(disk, after) {
				t.Fatal("refused stage changed saved bytes")
			}
			for _, line := range []string{"DIALECT", "DIALECT LIST", "DIALECT " + string(dialect), "SHOW DEDUPE"} {
				if strings.HasPrefix(line, "DIALECT") {
					s.handleDialectCommand(c, line)
				} else {
					s.handleDedupeCommand(c, line)
				}
			}
			if attempts != 0 {
				t.Fatal("read-only/unchanged setting saved")
			}
			outcome := s.filterEngine.stageOwned(newHumanPreferenceCandidate(c), "PASS BAND 20")
			if !outcome.handled || !outcome.publish {
				t.Fatal("ordinary PASS lost explicit save intent")
			}
			s.filterEngine.Handle(c, "PASS BAND 20")
			s.filterEngine.Handle(c, "PASS BAND 20")
			if attempts != 2 {
				t.Fatal("ordinary repeated PASS stopped saving")
			}
		})
	}
}

func TestHumanPreferenceSettingsRefusalAndFilterlessCallers(t *testing.T) {
	s := presetTestServer(t)
	c := &Client{}
	before := c.pathSnapshot()
	for _, line := range []string{"SET GRID X", "SET NOISE INVALID", "SET PATHSAMPLES 1"} {
		outcome := s.stagePathSettingsCommand(newHumanPreferenceCandidate(c), line)
		if !outcome.handled || outcome.publish || c.pathSnapshot() != before || c.configurationInitialized {
			t.Fatalf("invalid setting changed raw live input: %s", line)
		}
	}
	if outcome := s.stageSolarCommand(newHumanPreferenceCandidate(c), "SET SOLAR INVALID"); !outcome.handled || outcome.publish || outcome.forceSolar {
		t.Fatal("invalid SOLAR acquired publication intent")
	}
	staged := newHumanPreferenceCandidate(c)
	if outcome := s.stageSolarCommand(staged, "SET SOLAR 30"); !outcome.publish || !outcome.forceSolar || staged.getSolarSummaryMinutes() != 30 || !staged.solarNextSummaryAt.IsZero() || c.getSolarSummaryMinutes() != 0 {
		t.Fatal("candidate SOLAR cadence/deadline/live isolation changed")
	}
	response, handled := s.handlePathSettingsCommand(c, "SET NOISE INDUSTRIAL")
	if !handled || response != "Noise class set to INDUSTRIAL\n" || c.noiseClass != "INDUSTRIAL" || c.filter != nil || c.callsign != "" {
		t.Fatal("filterless lightweight settings behavior changed")
	}
}

func TestHumanPreferenceWarningArgumentsAreLiteral(t *testing.T) {
	outcome := humanMutation("", "Grid set to %s (warning: failed to persist: %v)\n", "FN31PR%V")
	response := outcome.persistenceFailure(&Client{}, errors.New("denied"))
	if response != "Grid set to FN31PR%V (warning: failed to persist: denied)\n" {
		t.Fatalf("warning text was reinterpreted as a format: %q", response)
	}
}

func TestHumanPreferenceOversizedSavedRecordUsesProtectedDefaults(t *testing.T) {
	s := presetTestServer(t)
	path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
	before := []byte("#" + strings.Repeat("A", filter.MaxUserRecordBytes) + "\n")
	if err := os.WriteFile(path, before, 0o600); err != nil {
		t.Fatal(err)
	}
	c := configurationTestClient(s, "W1ABC-1")
	restored, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(time.Minute))
	if err != nil || !c.recordProtected || !strings.Contains(restored.warning, "temporary defaults") {
		t.Fatalf("oversized login: %+v, %v", restored, err)
	}
	response, handled := s.handlePathSettingsCommand(c, "SET NOISE URBAN")
	if !handled || !strings.Contains(response, "warning: failed to persist") || c.noiseClass != "URBAN" {
		t.Fatalf("fitting protected change was not temporary: %q", response)
	}
	s.unregisterClient(c)
	if !bytes.Equal(before, presetDiskBytes(t, path)) {
		t.Fatal("human change or logout overwrote the refused file")
	}
}

func TestHumanPreferenceFilterPreflightRoutes(t *testing.T) {
	s := presetTestServer(t)
	s.filterEngine = newFilterCommandEngine()
	c := configurationTestClient(s, "W1ABC-1")
	attempts := 0
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { attempts++; return nil }
	for _, test := range []struct {
		line, response string
		handled        bool
	}{
		{"DX 14074 K1ABC CW", "", false},
		{"", "", false},
		{"SET/NOFILTER", "Command matches the cc dialect. Switch with DIALECT CC or use the GO syntax.\n", true},
		{"RESET WRONG", "Usage: RESET FILTER\nType HELP for usage.\n", true},
		{"PASS COMMENT é", commentCommandUsage, true},
	} {
		outcome, needsStage := s.filterEngine.preflightHumanFilterOwned(c, test.line)
		if needsStage || outcome.publish || outcome.handled != test.handled || outcome.response != test.response {
			t.Fatalf("preflight route/output changed: %s => %+v, stage=%v", test.line, outcome, needsStage)
		}
		response, handled := s.filterEngine.Handle(c, test.line)
		if handled != test.handled || response != test.response || attempts != 0 {
			t.Fatalf("nonmutation route output/save changed: %s => %q", test.line, response)
		}
	}
	if outcome, needsStage := s.filterEngine.preflightHumanFilterOwned(c, "SHOW FILTER"); needsStage || !outcome.handled || outcome.publish || outcome.response != formatFilterSnapshot(c.filter, s.filterEngine.ctyLookup) {
		t.Fatal("live filter summary entered detached staging")
	}
	if _, needsStage := s.filterEngine.preflightHumanFilterOwned(c, "PASS BAND 20"); !needsStage {
		t.Fatal("recognized mutation bypassed staging")
	}
}
