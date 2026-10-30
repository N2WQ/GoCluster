package telnet

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"dxcluster/filter"
)

func TestV7PresetLoadFallbackAndReconnectTCP(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		for _, dialect := range []string{"go", "cc"} {
			t.Run(transport+"/"+dialect, func(t *testing.T) {
				var options ServerOptions
				s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
					opts.Transport, opts.EchoMode, opts.LoginGreeting = transport, "off", "READY\n"
					opts.CommandRatePerSecond, opts.CommandBurst = 1000, 1000
					opts.DedupeFastEnabled, opts.DedupeMedEnabled, opts.DedupeSlowEnabled = true, false, false
					options = *opts
				})
				const call = "N0CALL-89"
				conn, reader, done, _ := connectUpgradeSession(t, s, call)
				w := nativeTCPWitness{t: t, server: s, conn: conn, reader: reader}
				other := "go"
				if dialect == "go" {
					other = "cc"
				}
				before, _ := w.configuration("DIALECT GO\r\nPASS RULE 4 MODE=RTTY\r\nDIALECT CC\r\nSET/NOSELF\r\nDIALECT " + other + "\r\nSAVE PRESET KEEP\r\n")
				prior, err := filter.LoadV7UserRecord(call)
				if err != nil {
					t.Fatal(err)
				}
				// A snapshot from when SLOW was available remains legal storage.
				// LOAD must reconcile it with this node's current FAST-only policy.
				set, err := filter.V7PresetFromConfiguration(before, dialect)
				if err != nil {
					t.Fatal(err)
				}
				set.Settings.DedupePolicy = "SLOW"
				if dialect == "go" {
					set.GO.Rules = []filter.RuleDefinition{{Slot: 5, Action: filter.RulePass, Expr: "MODE=CW"}}
				} else {
					set.CC.Modes = []string{"CW"}
				}
				if err := filter.SaveV7Preset(call, "CONTEST", set); err != nil {
					t.Fatal(err)
				}
				want := before.Clone()
				want.Settings = set.Settings
				want.Settings.DedupePolicy = "FAST"
				if dialect == "go" {
					want.Filters.GO = *set.GO
				} else {
					want.Filters.CC = *set.CC
				}
				actual, feedback := w.configuration("LOAD PRESET CONTEST\r\n")
				if !strings.Contains(feedback, "Note: dedupe SLOW unavailable; using FAST.\r\n") || !strings.Contains(feedback, "Dialect switched to "+strings.ToUpper(dialect)) {
					t.Fatalf("LOAD did not announce adjustment/switch: %q", feedback)
				}
				verify := func(actual filter.V7Configuration) {
					t.Helper()
					if !actual.Equal(want) {
						t.Fatalf("LOAD/reconnect lost applied settings or bank ownership: got=%+v want=%+v", actual.Settings, want.Settings)
					}
					record, err := filter.LoadV7UserRecord(call)
					if err != nil || !record.Configuration.Equal(want) {
						t.Fatalf("command did not immediately persist adjusted complete profile: %v", err)
					}
					applied, inactive, oldInactive := record.Presets.GO, record.Presets.CC, prior.Presets.CC
					if dialect == "cc" {
						applied, inactive, oldInactive = record.Presets.CC, record.Presets.GO, prior.Presets.GO
					}
					if applied == nil || applied.Name != "CONTEST" || applied.Baseline.Settings.DedupePolicy != "FAST" || !applied.Baseline.MatchesConfiguration(actual) || !reflect.DeepEqual(inactive, oldInactive) {
						t.Fatal("applied baseline does not describe FAST, or inactive association changed")
					}
					library, err := filter.LoadV7Preset(call, "CONTEST")
					if err != nil || library.Settings.DedupePolicy != "SLOW" {
						t.Fatalf("LOAD rewrote the shared named preset: %v", err)
					}
					s.clientsMutex.RLock()
					client := s.clients[call]
					s.clientsMutex.RUnlock()
					if client == nil || client.getDedupePolicy() != dedupePolicyFast {
						t.Fatal("actual session did not retain FAST")
					}
				}
				verify(actual) // Verify before disconnect autosave can repair anything.
				closeHandshakeTranscriptSession(t, conn, done)
				conn, reader, done, _ = connectUpgradeSession(t, s, call)
				w = nativeTCPWitness{t: t, server: s, conn: conn, reader: reader}
				actual, _ = w.configuration("")
				verify(actual)
				closeHandshakeTranscriptSession(t, conn, done)
				// A subsequent node configuration re-enables SLOW. The applied FAST
				// choice must not silently revert to the library's original choice.
				options.DedupeSlowEnabled = true
				s = NewServer(options, nil)
				conn, reader, _, _ = connectUpgradeSession(t, s, call)
				w = nativeTCPWitness{t: t, server: s, conn: conn, reader: reader}
				actual, _ = w.configuration("")
				verify(actual)
			})
		}
	}
}

func TestV7PresetLoadAppliedDefaultsAndAdjustments(t *testing.T) {
	for _, tc := range []struct {
		name, stored, want, warning string
		path, wantPath              int
		slow, sameSelection         bool
	}{
		{name: "inherited", stored: "", want: ""},
		{name: "available", stored: "SLOW", want: "SLOW", path: 25, wantPath: 25, slow: true},
		{name: "path-below-floor", stored: "FAST", want: "FAST", path: 15, warning: "Note: PATHSAMPLES 15 unavailable; using the cluster default.\n"},
		{name: "same-unavailable-selection", stored: "SLOW", want: "FAST", sameSelection: true, warning: "Note: dedupe SLOW unavailable; using FAST.\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c := v7IndependentClient(t)
			s.dedupeSlowEnabled = tc.slow
			if tc.sameSelection {
				c.configuredSettings.DedupePolicy = tc.stored
			}
			c.grid, c.gridDerived = "IO91", true
			s.gridLookup = func(string) (string, bool, bool) {
				t.Fatal("unchanged GRID performed a new lookup")
				return "", false, false
			}
			before, err := c.captureV7Configuration()
			if err != nil {
				t.Fatal(err)
			}
			set, err := filter.V7PresetFromConfiguration(before, "cc")
			if err != nil {
				t.Fatal(err)
			}
			set.Settings.DedupePolicy, set.Settings.PathMinObservationCount = tc.stored, tc.path
			if err := filter.SaveV7Preset(c.callsign, "APPLIED", set); err != nil {
				t.Fatal(err)
			}
			response, handled := s.handlePresetCommand(c, "LOAD PRESET APPLIED")
			if !handled || !strings.HasPrefix(response, "Loaded preset APPLIED (CC);") || !strings.HasSuffix(response, tc.warning) || tc.warning == "" && strings.Contains(response, "Note:") {
				t.Fatalf("wrong LOAD acknowledgement: %q", response)
			}
			record, err := filter.LoadV7UserRecord(c.callsign)
			if err != nil {
				t.Fatal(err)
			}
			for _, settings := range []filter.SettingsConfiguration{c.configuredSettings, record.Configuration.Settings, record.Presets.CC.Baseline.Settings} {
				if settings.DedupePolicy != tc.want || settings.PathMinObservationCount != tc.wantPath || settings.Grid != "" {
					t.Fatalf("applied configuration/baseline lost adjustment or sentinel: %+v", settings)
				}
			}
			if c.grid != "IO91" || !c.gridDerived {
				t.Fatal("LOAD changed the unchanged derived runtime GRID")
			}
		})
	}
}

func TestV7PresetLoadAdjustmentCommitFailure(t *testing.T) {
	s, c := v7IndependentClient(t)
	s.dedupeSlowEnabled = false
	if response, _ := s.handlePresetCommand(c, "SAVE PRESET KEEP"); !strings.HasPrefix(response, "Saved preset KEEP") {
		t.Fatal(response)
	}
	before, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	set, err := filter.V7PresetFromConfiguration(before, "go")
	if err != nil {
		t.Fatal(err)
	}
	set.Settings.DedupePolicy, set.Settings.PathMinObservationCount = "SLOW", 15
	if err := filter.SaveV7Preset(c.callsign, "FAIL", set); err != nil {
		t.Fatal(err)
	}
	profilePath := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	profile := presetDiskBytes(t, profilePath)
	refs, pointer := c.v7PresetReferences, c.filter
	revision, err := c.v7ConfigurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { return os.ErrPermission }
	response, _ := s.handlePresetCommand(c, "LOAD PRESET FAIL")
	after, captureErr := c.captureV7Configuration()
	afterRevision, revisionErr := c.v7ConfigurationRevisionToken()
	if response != "LOAD PRESET failed: permission denied\n" || captureErr != nil || revisionErr != nil || !before.Equal(after) || c.v7PresetReferences != refs || c.filter != pointer || afterRevision != revision || !bytes.Equal(profile, presetDiskBytes(t, profilePath)) {
		t.Fatalf("failed adjusted LOAD published state/reference/revision or saved bytes: %q", response)
	}
}
