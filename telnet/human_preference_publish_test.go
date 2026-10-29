package telnet

import (
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
)

func TestHumanPreferenceNearbyPublication(t *testing.T) {
	requireH3Mappings(t)
	for _, restored := range []bool{false, true} {
		s := presetTestServer(t)
		s.filterEngine = newFilterCommandEngine()
		c := configurationTestClient(s, "W1ABC-1")
		c.grid, c.gridCell, c.gridCoarseCell = "FN31", pathreliability.EncodeCell("FN31"), pathreliability.EncodeCoarseCell("FN31")
		c.configuredSettings, c.configurationInitialized = filter.SettingsConfiguration{Dialect: "go", Grid: "FN31", DedupePolicy: "FAST"}, true
		c.filter = passRuleTestFilter(t, "MODE=CW")
		previous, pointer := c.filter.PreparedRules(), c.filter
		staged := newHumanPreferenceCandidate(c)
		outcome := s.filterEngine.stageOwned(staged, "PASS RULE 0 NEARBY AND DXCONT=EU")
		if !outcome.publish || c.filter.PreparedRules() != previous {
			t.Fatal("staging changed live rules")
		}
		s.publishHumanPreference(c, staged, outcome.forceSolar)
		original := c.filter.PreparedRules()
		if c.filter != pointer || !c.filter.MatchSnapshot().NearbyFilterActive() || c.gridCell != pathreliability.EncodeCell("FN31") {
			t.Fatal("publication lost location/rule ownership")
		}
		if restored {
			_, value, err := filter.PrepareV6Configuration(c.captureV6Configuration())
			if err != nil {
				t.Fatal(err)
			}
			*c.filter = value
			original = c.filter.PreparedRules()
		}
		staged = newHumanPreferenceCandidate(c)
		outcome = s.stagePathSettingsCommand(staged, "SET NOISE URBAN")
		s.publishHumanPreference(c, staged, outcome.forceSolar)
		if c.gridCell != pathreliability.EncodeCell("FN31") || c.filter.PreparedRules() != original {
			t.Fatal("unrelated edit erased geometry")
		}
		staged = newHumanPreferenceCandidate(c)
		outcome = s.stagePathSettingsCommand(staged, "SET GRID IO91")
		if !outcome.publish || c.grid != "FN31" {
			t.Fatal("GRID stage changed live state")
		}
		s.publishHumanPreference(c, staged, outcome.forceSolar)
		if c.grid != "IO91" || c.gridCell != pathreliability.EncodeCell("IO91") || c.gridCoarseCell != pathreliability.EncodeCoarseCell("IO91") || c.filter.PreparedRules() != original {
			t.Fatal("GRID publication lost geometry/rules")
		}
		staged = newHumanPreferenceCandidate(c)
		outcome = s.filterEngine.stageOwned(staged, "RESET FILTER")
		s.publishHumanPreference(c, staged, outcome.forceSolar)
		if !outcome.publish || c.filter != pointer || c.grid != "IO91" || c.gridCell != pathreliability.EncodeCell("IO91") || len(c.filter.PreparedRules().Definitions()) != 0 || original.Definitions()[0].Expr != "NEARBY AND DXCONT=EU" {
			t.Fatal("reset altered location or retained immutable state")
		}
	}
}

func TestHumanPreferenceSolarPublicationClockAndBroadcasterProgress(t *testing.T) {
	for _, solarCommand := range []bool{false, true} {
		t.Run(map[bool]string{false: "unrelated edit preserves tick", true: "same cadence SET reschedules"}[solarCommand], func(t *testing.T) {
			s := presetTestServer(t)
			c := configurationTestClient(s, "W1ABC-1")
			c.filter = passRuleTestFilter(t, "BAND=20M")
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
				outcome = newFilterCommandEngine().stageOwned(staged, "PASS RULE 0 BAND=20M")
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
			*staged.filter = *passRuleTestFilter(t, "BAND=40M")
			s.publishHumanPreference(c, staged, false)
			if c.historySearch != nil || c.historyGeneration != generation+1 || c.configurationRevision != revision+1 || c.filter.PreparedRules().Definitions()[0].Expr != "BAND=40M" {
				t.Fatal("publication failed existing matching/revision invalidation")
			}
		})
	}
}
