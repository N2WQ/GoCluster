// Keep literal setting controls beside their complete live/durable assertions:
// the table-driven positive matrix is clearer than separating each value from
// the shared exact-state check. No production parser supplies expected values.
package telnet

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
)

func TestFixedSettingsArityPreservesOwners(t *testing.T) {
	requireH3Mappings(t)
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			s := presetTestServer(t)
			s.filterEngine = newFilterCommandEngine()
			c := configurationTestClient(s, "W1ABC-1")
			c.dialect = dialect
			now := time.Date(2026, 10, 9, 12, 1, 0, 0, time.UTC)
			s.nowFn = func() time.Time { return now }
			for _, control := range []struct {
				command string
				handle  func(*Server, *Client, string) (string, bool)
			}{
				{"SET GRID FN31", (*Server).handlePathSettingsCommand},
				{"SET NOISE RURAL", (*Server).handlePathSettingsCommand},
				{"SET PATHSAMPLES 30", (*Server).handlePathSettingsCommand},
				{"SET SOLAR 30", (*Server).handleSolarCommand},
				{"SET DEDUPE FAST", (*Server).handleDedupeCommand},
			} {
				response, handled := control.handle(s, c, control.command)
				if !handled || strings.Contains(response, "Usage:") || strings.Contains(response, "warning") {
					t.Fatalf("setup failed: %s -> %q", control.command, response)
				}
			}
			baseline, err := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Preset()
			if err != nil {
				t.Fatal(err)
			}
			c.presetReference = &filter.PresetReference{Name: "CONTROL", Baseline: baseline}
			c.setDiagMode(diagModeSource)
			if err := c.saveFilter(); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				command, usage string
				handle         func(*Server, *Client, string) (string, bool)
			}{
				{"DIALECT GO EXTRA", "Usage: DIALECT [LIST|go|cc]\n", (*Server).handleDialectCommand},
				{"DIALECT CC EXTRA", "Usage: DIALECT [LIST|go|cc]\n", (*Server).handleDialectCommand},
				{"DIALECT LIST EXTRA", "Usage: DIALECT [LIST|go|cc]\n", (*Server).handleDialectCommand},
				{"DIALECT UNKNOWN EXTRA", "Usage: DIALECT [LIST|go|cc]\n", (*Server).handleDialectCommand},
				{"SET GRID", "Usage: SET GRID <4 or 6 character Maidenhead locator>\n", (*Server).handlePathSettingsCommand},
				{"SET GRID FN31PR EXTRA", "Usage: SET GRID <4 or 6 character Maidenhead locator>\n", (*Server).handlePathSettingsCommand},
				{"SET NOISE", "Usage: SET NOISE <QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL>\n", (*Server).handlePathSettingsCommand},
				{"SET NOISE URBAN EXTRA", "Usage: SET NOISE <QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL>\n", (*Server).handlePathSettingsCommand},
				{"SET PATHSAMPLES", "Usage: SET PATHSAMPLES <count|DEFAULT>\n", (*Server).handlePathSettingsCommand},
				{"SET PATHSAMPLES 40 EXTRA", "Usage: SET PATHSAMPLES <count|DEFAULT>\n", (*Server).handlePathSettingsCommand},
				{"SET PATHSAMPLES DEFAULT EXTRA", "Usage: SET PATHSAMPLES <count|DEFAULT>\n", (*Server).handlePathSettingsCommand},
				{"SET SOLAR", "Usage: SET SOLAR <15|30|60|OFF>\n", (*Server).handleSolarCommand},
				{"SET SOLAR 15 EXTRA", "Usage: SET SOLAR <15|30|60|OFF>\n", (*Server).handleSolarCommand},
				{"SET SOLAR 30 EXTRA", "Usage: SET SOLAR <15|30|60|OFF>\n", (*Server).handleSolarCommand},
				{"SET SOLAR OFF EXTRA", "Usage: SET SOLAR <15|30|60|OFF>\n", (*Server).handleSolarCommand},
				{"SET DEDUPE", "Usage: SET DEDUPE <FAST|MED|SLOW>\n", (*Server).handleDedupeCommand},
				{"SET DEDUPE SLOW EXTRA", "Usage: SET DEDUPE <FAST|MED|SLOW>\n", (*Server).handleDedupeCommand},
				{"SHOW DEDUPE EXTRA", "Usage: SHOW DEDUPE\n", (*Server).handleDedupeCommand},
				{"SET DIAG", diagUsage, (*Server).handleDiagCommand},
				{"SET DIAG OFF EXTRA", diagUsage, (*Server).handleDiagCommand},
				{"SET DIAG CONFIDENCE EXTRA", diagUsage, (*Server).handleDiagCommand},
			} {
				before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
				pathState, tick, mode, revision := c.pathSnapshot(), c.solarNextSummaryAt, c.getDiagMode(), c.configurationRevision
				ref, err := c.presetReference.Clone()
				if err != nil {
					t.Fatal(err)
				}
				file := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
				saved, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				attempts := 0
				s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { attempts++; return nil }
				now = now.Add(31 * time.Minute) // An accidental SOLAR reapply must be observable.
				response, handled := tc.handle(s, c, " \t"+strings.ToLower(tc.command)+"\t ")
				s.commitConfigurationFn = nil
				after, err := os.ReadFile(file)
				if !handled || response != tc.usage || attempts != 0 || err != nil || !bytes.Equal(saved, after) ||
					!before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) ||
					c.pathSnapshot() != pathState || c.solarNextSummaryAt != tick || c.getDiagMode() != mode ||
					c.configurationRevision != revision || !reflect.DeepEqual(ref, c.presetReference) {
					t.Fatalf("wrong arity changed an owner: %q -> %q, commits=%d, err=%v", tc.command, response, attempts, err)
				}
			}
			// Argument admission precedes feature availability too.
			s.pathPredictor = nil
			if response, _ := s.handlePathSettingsCommand(c, "SET PATHSAMPLES 40 EXTRA"); response != "Usage: SET PATHSAMPLES <count|DEFAULT>\n" {
				t.Fatalf("disabled feature hid wrong arity: %q", response)
			}
		})
	}
}

func TestFixedSettingsPositiveControls(t *testing.T) {
	requireH3Mappings(t)
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			s := presetTestServer(t)
			s.filterEngine = newFilterCommandEngine()
			c := configurationTestClient(s, "W1ABC-1")
			c.dialect = dialect
			now := time.Date(2026, 10, 9, 12, 1, 0, 0, time.UTC)
			s.nowFn = func() time.Time { return now }
			for _, tc := range []struct {
				command, response string
				handle            func(*Server, *Client, string) (string, bool)
				apply             func(*filter.SettingsConfiguration)
				live              func() bool
			}{
				{"SET GRID FN31PR", "Grid set to FN31PR\n", (*Server).handlePathSettingsCommand,
					func(v *filter.SettingsConfiguration) { v.Grid = "FN31PR" }, func() bool { return c.pathSnapshot().grid == "FN31PR" && c.pathSnapshot().gridCell != 0 }},
				{"SET NOISE QUIET", "Noise class set to QUIET\n", (*Server).handlePathSettingsCommand,
					func(v *filter.SettingsConfiguration) { v.NoiseClass = "QUIET" }, func() bool { return c.pathSnapshot().noiseClass == "QUIET" }},
				{"SET NOISE RURAL", "Noise class set to RURAL\n", (*Server).handlePathSettingsCommand,
					func(v *filter.SettingsConfiguration) { v.NoiseClass = "RURAL" }, func() bool { return c.pathSnapshot().noiseClass == "RURAL" }},
				{"SET NOISE SUBURBAN", "Noise class set to SUBURBAN\n", (*Server).handlePathSettingsCommand,
					func(v *filter.SettingsConfiguration) { v.NoiseClass = "SUBURBAN" }, func() bool { return c.pathSnapshot().noiseClass == "SUBURBAN" }},
				{"SET NOISE URBAN", "Noise class set to URBAN\n", (*Server).handlePathSettingsCommand,
					func(v *filter.SettingsConfiguration) { v.NoiseClass = "URBAN" }, func() bool { return c.pathSnapshot().noiseClass == "URBAN" }},
				{"SET NOISE INDUSTRIAL", "Noise class set to INDUSTRIAL\n", (*Server).handlePathSettingsCommand,
					func(v *filter.SettingsConfiguration) { v.NoiseClass = "INDUSTRIAL" }, func() bool { return c.pathSnapshot().noiseClass == "INDUSTRIAL" }},
				{"SET PATHSAMPLES 30", "Path sample minimum set to 30 (cluster default 19)\n", (*Server).handlePathSettingsCommand,
					func(v *filter.SettingsConfiguration) { v.PathMinObservationCount = 30 }, func() bool { return c.pathSnapshot().pathMinObservationCount == 30 }},
				{"SET PATHSAMPLES DEFAULT", "Path sample minimum reset to cluster default 19\n", (*Server).handlePathSettingsCommand,
					func(v *filter.SettingsConfiguration) { v.PathMinObservationCount = 0 }, func() bool { return c.pathSnapshot().pathMinObservationCount == 0 }},
				{"SET DEDUPE FAST", "Dedupe policy set to FAST\n", (*Server).handleDedupeCommand,
					func(v *filter.SettingsConfiguration) { v.DedupePolicy = "FAST" }, func() bool { return c.getDedupePolicy() == dedupePolicyFast }},
				{"SET DEDUPE MED", "Dedupe policy set to MED\n", (*Server).handleDedupeCommand,
					func(v *filter.SettingsConfiguration) { v.DedupePolicy = "MED" }, func() bool { return c.getDedupePolicy() == dedupePolicyMed }},
				{"SET DEDUPE SLOW", "Dedupe policy set to SLOW\n", (*Server).handleDedupeCommand,
					func(v *filter.SettingsConfiguration) { v.DedupePolicy = "SLOW" }, func() bool { return c.getDedupePolicy() == dedupePolicySlow }},
				{"SET SOLAR 15", "Solar summaries: every 15 minutes\n", (*Server).handleSolarCommand,
					func(v *filter.SettingsConfiguration) { v.SolarSummaryMinutes = 15 }, func() bool { return c.solarNextSummaryAt.Equal(time.Date(2026, 10, 9, 12, 15, 0, 0, time.UTC)) }},
				{"SET SOLAR 30", "Solar summaries: every 30 minutes\n", (*Server).handleSolarCommand,
					func(v *filter.SettingsConfiguration) { v.SolarSummaryMinutes = 30 }, func() bool { return c.solarNextSummaryAt.Equal(time.Date(2026, 10, 9, 12, 30, 0, 0, time.UTC)) }},
				{"SET SOLAR 60", "Solar summaries: every 60 minutes\n", (*Server).handleSolarCommand,
					func(v *filter.SettingsConfiguration) { v.SolarSummaryMinutes = 60 }, func() bool { return c.solarNextSummaryAt.Equal(time.Date(2026, 10, 9, 13, 0, 0, 0, time.UTC)) }},
				{"SET SOLAR OFF", "Solar summaries: OFF\n", (*Server).handleSolarCommand,
					func(v *filter.SettingsConfiguration) { v.SolarSummaryMinutes = 0 }, func() bool { return c.solarNextSummaryAt.IsZero() && c.getSolarSummaryMinutes() == 0 }},
			} {
				if err := c.saveFilter(); err != nil {
					t.Fatal(err)
				}
				want := c.configuredSettings
				tc.apply(&want)
				response, handled := tc.handle(s, c, " \t"+strings.ToLower(tc.command)+"  ")
				stored, err := filter.LoadUserRecord(c.callsign)
				if err != nil {
					t.Fatal(err)
				}
				storedSettings := filter.SettingsConfiguration{Dialect: stored.Dialect, Grid: stored.Grid, NoiseClass: stored.NoiseClass,
					DedupePolicy: stored.DedupePolicy, PathMinObservationCount: stored.PathMinObservationCount, SolarSummaryMinutes: stored.SolarSummaryMinutes}
				if !handled || response != tc.response || c.configuredSettings != want || !tc.live() ||
					!filter.ConfigurationFromFilter(&stored.Filter, storedSettings).Equal(filter.ConfigurationFromFilter(c.filter, want)) {
					t.Fatalf("valid setting lost live/durable behavior: %s -> %q, err=%v", tc.command, response, err)
				}
			}
			for _, alias := range []string{"go", "gocluster", "classic", "default", "cc"} {
				c.dialect, c.configuredSettings.Dialect = DialectCC, "cc"
				want := DialectGo
				if alias == "cc" {
					c.dialect, c.configuredSettings.Dialect, want = DialectGo, "go", DialectCC
				}
				response, handled := s.handleDialectCommand(c, "dialect "+alias)
				stored, err := filter.LoadUserRecord(c.callsign)
				if !handled || response != "Dialect set to "+strings.ToUpper(string(want))+"\n" ||
					c.dialect != want || err != nil || stored.Dialect != string(want) {
					t.Fatalf("valid dialect alias changed: %s -> %q, err=%v", alias, response, err)
				}
			}
			for _, tc := range []struct{ command, response string }{
				{"DIALECT", "Current dialect: CC\n"},
				{"DIALECT LIST", "Available dialects: CC, GO\n"},
				{"SHOW DEDUPE", "Dedupe: SLOW (cqzone) (fast=on med=on slow=on)\n"},
			} {
				attempts := 0
				s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { attempts++; return nil }
				handler := s.handleDialectCommand
				if strings.HasPrefix(tc.command, "SHOW") {
					handler = s.handleDedupeCommand
				}
				response, handled := handler(c, tc.command)
				s.commitConfigurationFn = nil
				if !handled || response != tc.response || attempts != 0 {
					t.Fatalf("valid query changed: %s -> %q, saves=%d", tc.command, response, attempts)
				}
			}
			// An unchanged human cadence intentionally reschedules from the new clock.
			s.handleSolarCommand(c, "SET SOLAR 30")
			now = time.Date(2026, 10, 9, 12, 31, 0, 0, time.UTC)
			s.handleSolarCommand(c, "SET SOLAR 30")
			if c.solarNextSummaryAt != time.Date(2026, 10, 9, 13, 0, 0, 0, time.UTC) {
				t.Fatal("unchanged valid human SOLAR stopped deliberately rescheduling")
			}
		})
	}
}

func TestFixedSettingsDiagnosticControls(t *testing.T) {
	s := presetTestServer(t)
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		c := configurationTestClient(s, "W1ABC-1")
		c.dialect = dialect
		for _, tc := range []struct {
			token, canonical string
			mode             diagMode
		}{{"OFF", "OFF", diagModeOff}, {"DEDUPE", "DEDUPE", diagModeDedupe}, {"SOURCE", "SOURCE", diagModeSource},
			{"CONF", "CONF", diagModeConfidence}, {"CONFIDENCE", "CONF", diagModeConfidence}, {"PATH", "PATH", diagModePath}, {"MODE", "MODE", diagModeMode}} {
			c.setDiagMode(diagModeDedupe)
			if tc.mode == diagModeDedupe {
				c.setDiagMode(diagModeOff)
			}
			prior := c.getDiagMode()
			response, handled := s.handleDiagCommand(c, "set\tdiag "+strings.ToLower(tc.token)+" EXTRA")
			if !handled || response != diagUsage || c.getDiagMode() != prior {
				t.Fatalf("malformed diagnostic changed session mode: %s -> %q", tc.token, response)
			}
			response, handled = s.handleDiagCommand(c, "set\tdiag "+strings.ToLower(tc.token))
			if !handled || response != "Diagnostic comments: "+tc.canonical+"\n" || c.getDiagMode() != tc.mode ||
				s.captureReadbackStatus(c, c).Session.DiagnosticComments != tc.canonical {
				t.Fatalf("valid diagnostic/alias changed: %s -> %q", tc.token, response)
			}
		}
	}
}

func FuzzFixedSettingsArgumentRefusal(f *testing.F) {
	for _, tail := range []string{"EXTRA", "ON", "OFF", "ALL", "\tMORE WORDS", "", "é"} {
		f.Add(tail)
	}
	s := &Server{filterEngine: newFilterCommandEngine()}
	f.Fuzz(func(t *testing.T, tail string) {
		if len(tail) > 256 || len(strings.Fields(tail)) == 0 {
			return
		}
		for _, tc := range []struct {
			command string
			stage   func(*Server, *Client, string) humanCommandOutcome
		}{{"DIALECT CC", (*Server).stageDialectCommand}, {"DIALECT LIST", (*Server).stageDialectCommand},
			{"SET GRID FN31", (*Server).stagePathSettingsCommand}, {"SET NOISE URBAN", (*Server).stagePathSettingsCommand},
			{"SET PATHSAMPLES 30", (*Server).stagePathSettingsCommand}, {"SET SOLAR 15", (*Server).stageSolarCommand},
			{"SET DEDUPE FAST", (*Server).stageDedupeCommand}, {"SHOW DEDUPE", (*Server).stageDedupeCommand}} {
			c := newTestClient()
			before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
			outcome := tc.stage(s, c, tc.command+" "+tail)
			if !outcome.handled || outcome.publish || outcome.forceSolar || !strings.HasPrefix(outcome.response, "Usage:") ||
				!before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) {
				t.Fatalf("arbitrary tail acquired setting effects: %s %q -> %+v", tc.command, tail, outcome)
			}
		}
		c := &Client{}
		c.setDiagMode(diagModeSource)
		if response, handled := s.handleDiagCommand(c, "SET DIAG OFF "+tail); !handled || response != diagUsage || c.getDiagMode() != diagModeSource {
			t.Fatal("arbitrary diagnostic tail changed session mode")
		}
	})
}
