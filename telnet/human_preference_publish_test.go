package telnet

import (
	"reflect"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
)

func TestHumanPreferenceNearbyPublication(t *testing.T) {
	requireH3Mappings(t)
	for _, loadedWithoutSnapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "snapshot", true: "loaded without snapshot"}[loadedWithoutSnapshot], func(t *testing.T) {
			s := presetTestServer(t)
			s.filterEngine = newFilterCommandEngine()
			c := configurationTestClient(s, "W1ABC-1")
			c.grid, c.gridCell, c.gridCoarseCell = "FN31", pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")
			c.configuredSettings, c.configurationInitialized = filter.SettingsConfiguration{Dialect: "go", Grid: "FN31", DedupePolicy: "FAST"}, true
			c.filter.SetDXContinent("EU", true)
			c.filter.SetDEZone(5, true)
			c.filter.SetDXState("NY", true)
			c.filter.LegacyMinConfidence = 17
			pointer := c.filter
			staged := newHumanPreferenceCandidate(c)
			outcome := s.filterEngine.stageOwned(staged, "PASS NEARBY ON")
			if !outcome.publish || c.filter.NearbyEnabled || c.filter.NearbySnapshot != nil {
				t.Fatal("NEARBY ON changed live state before publication")
			}
			s.publishHumanPreference(c, staged, outcome.forceSolar)
			if c.filter != pointer || !c.filter.NearbyActive() || c.filter.NearbyUserFine != c.gridCell || c.filter.NearbyUserCoarse != c.gridCoarseCell || c.filter.NearbySnapshot == nil || c.filter.LegacyMinConfidence != 17 {
				t.Fatal("ON publication lost runtime/filter ownership")
			}
			if loadedWithoutSnapshot {
				c.filter.NearbySnapshot = nil
			}
			snapshot := c.filter.NearbySnapshot
			staged = newHumanPreferenceCandidate(c)
			outcome = s.stagePathSettingsCommand(staged, "SET NOISE URBAN")
			s.publishHumanPreference(c, staged, outcome.forceSolar)
			if c.filter.NearbySnapshot != snapshot || c.filter.NearbyUserFine != c.gridCell || c.filter.NearbyUserCoarse != c.gridCoarseCell || c.filter.LegacyMinConfidence != 17 {
				t.Fatal("unrelated setting reconstructed NEARBY")
			}
			staged = newHumanPreferenceCandidate(c)
			outcome = s.stagePathSettingsCommand(staged, "SET GRID IO91")
			if !outcome.publish || c.grid != "FN31" || c.filter.NearbySnapshot != snapshot {
				t.Fatal("GRID stage changed live state")
			}
			s.publishHumanPreference(c, staged, outcome.forceSolar)
			if c.grid != "IO91" || c.filter.NearbyUserFine != pathreliability.EncodeCell("IO91") || c.filter.NearbyUserCoarse != pathreliability.EncodeCoarseCell("IO91") || c.filter.NearbySnapshot != snapshot {
				t.Fatal("active GRID publication lost cells/restoration snapshot")
			}
			staged = newHumanPreferenceCandidate(c)
			outcome = s.filterEngine.stageOwned(staged, "PASS NEARBY OFF")
			if !outcome.publish || !c.filter.NearbyEnabled || c.filter.NearbySnapshot != snapshot {
				t.Fatal("OFF stage changed live state")
			}
			s.publishHumanPreference(c, staged, outcome.forceSolar)
			if c.filter != pointer || c.filter.NearbyEnabled || c.filter.NearbySnapshot != nil || c.filter.NearbyUserFine != pathreliability.InvalidCell || c.filter.NearbyUserCoarse != pathreliability.InvalidCell || c.grid != "IO91" || c.filter.AllDXContinents || !c.filter.DXContinents["EU"] || c.filter.AllDEZones || !c.filter.DEZones[5] || c.filter.AllDXStates || !c.filter.DXStates["NY"] {
				t.Fatal("OFF failed literal prior-rule restoration/filter ownership")
			}
			for _, command := range []string{"PASS NOFILTER", "RESET FILTER"} {
				if err := c.filter.EnableNearby(c.gridCell, c.gridCoarseCell); err != nil {
					t.Fatal(err)
				}
				staged = newHumanPreferenceCandidate(c)
				outcome = s.filterEngine.stageOwned(staged, command)
				s.publishHumanPreference(c, staged, outcome.forceSolar)
				if !outcome.publish || c.filter != pointer || c.filter.NearbyEnabled || c.filter.NearbySnapshot != nil || c.grid != "IO91" {
					t.Fatalf("%s publication retained NEARBY or changed GRID ownership", command)
				}
				if command == "PASS NOFILTER" && (!c.filter.AllDXContinents || !c.filter.AllDEZones || !c.filter.AllDXStates || len(c.filter.DXContinents) != 0 || len(c.filter.DEZones) != 0 || len(c.filter.DXStates) != 0) {
					t.Fatal("NOFILTER restored restrictions after clearing")
				}
				if command == "RESET FILTER" && !filter.ConfigurationFromFilter(c.filter, filter.SettingsConfiguration{}).Equal(filter.ConfigurationFromFilter(filter.NewFilter(), filter.SettingsConfiguration{})) {
					t.Fatal("RESET publication lost configured new-user defaults")
				}
			}
		})
	}
}

func TestHumanPreferenceSolarPublicationClockAndBroadcasterProgress(t *testing.T) {
	for _, solarCommand := range []bool{false, true} {
		t.Run(map[bool]string{false: "unrelated edit preserves tick", true: "same cadence SET reschedules"}[solarCommand], func(t *testing.T) {
			s := presetTestServer(t)
			c := configurationTestClient(s, "W1ABC-1")
			c.filter.SetBand("20m", true)
			c.configuredSettings = filter.SettingsConfiguration{Dialect: "go", DedupePolicy: "FAST", SolarSummaryMinutes: 15}
			c.configurationInitialized = true
			clock := time.Date(2026, 10, 9, 12, 14, 59, 0, time.UTC)
			s.nowFn = func() time.Time { return clock }
			c.setSolarSummaryMinutes(15, clock)
			c.refreshConfigurationRevision()
			c.historySearch = &historySearch{token: "unchanged matching cursor"}
			cursor, generation, revision, pointer := c.historySearch, c.historyGeneration, c.configurationRevision, c.filter
			c.setDiagMode(diagModeSource)
			c.readPauseEpoch = 7
			c.readPausePending.Store(true)
			c.readPauseUntilUnixNano.Store(1234)
			c.readPauseSuppressed.Store(9)
			staged := newHumanPreferenceCandidate(c)
			var outcome humanCommandOutcome
			if solarCommand {
				outcome = s.stageSolarCommand(staged, "SET SOLAR 15")
			} else {
				outcome = newFilterCommandEngine().stageOwned(staged, "PASS BAND 20")
			}
			if !outcome.publish || outcome.forceSolar != solarCommand {
				t.Fatal("handler did not declare its scheduling intent")
			}
			// A real concurrent owner advances the live tick after staging. Join
			// its barrier before publication so the expected progress is literal.
			advanced := make(chan struct{})
			go func() { c.advanceSolarSummaryAt(time.Date(2026, 10, 9, 12, 31, 0, 0, time.UTC)); close(advanced) }()
			waitConfigurationTest(t, advanced)
			clock = time.Date(2026, 10, 9, 12, 15, 1, 0, time.UTC)
			s.publishHumanPreference(c, staged, outcome.forceSolar)
			expected := time.Date(2026, 10, 9, 12, 45, 0, 0, time.UTC)
			if solarCommand {
				expected = time.Date(2026, 10, 9, 12, 30, 0, 0, time.UTC)
			}
			if !c.solarNextSummaryAt.Equal(expected) || c.getSolarSummaryMinutes() != 15 {
				t.Fatalf("stage clock or stale tick reached publication: %v, want %v", c.solarNextSummaryAt, expected)
			}
			if c.filter != pointer || c.getDiagMode() != diagModeSource || c.readPauseEpoch != 7 || !c.readPausePending.Load() || c.readPauseUntilUnixNano.Load() != 1234 || c.readPauseSuppressed.Load() != 9 || c.historySearch != cursor || c.historyGeneration != generation || c.configurationRevision != revision {
				t.Fatal("publication changed stable identity/unrelated diagnostics/pause/history state")
			}
			// Matching edits still invalidate an existing cursor as before.
			staged = newHumanPreferenceCandidate(c)
			staged.filter.BlockBands = map[string]bool{"20m": true}
			s.publishHumanPreference(c, staged, false)
			if c.historySearch != nil || c.historyGeneration != generation+1 || c.configurationRevision != revision+1 || !reflect.DeepEqual(c.filter.BlockBands, map[string]bool{"20m": true}) {
				t.Fatal("publication failed existing matching/revision invalidation")
			}
		})
	}
}
