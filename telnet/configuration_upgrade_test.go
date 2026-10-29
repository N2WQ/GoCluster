package telnet

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

// Literal flat keys follow the previous binary's yaml tags, not the new DTO.
func oldLoginRecord(version int) []byte {
	if version == 5 {
		return []byte(`configuration_version: 5
configuration:
  filters:
    rules: [{slot: 4, action: reject, expr: "MODE=CW"}]
    nearby_enabled: true
    include_beacons: false
    allow_wwv: false
    allow_wcy: true
    allow_announce: false
    allow_self: false
    allow_toxic: false
  settings:
    dialect: cc
    grid: FN31
    noise_class: URBAN
    dedupe_policy: MED
    path_min_observation_count: 42
    solar_summary_minutes: 15
recent_ips: [192.0.2.7, 192.0.2.8]
last_login_utc: 2026-10-01T12:00:00Z
`)
	}

	marker := ""
	if version != 0 {
		marker = fmt.Sprintf("configuration_version: %d\n", version)
	}
	return []byte(marker + `bands: {40M: true}
modes: {FT8: true}
sources: {HUMAN: true}
allbands: false
allmodes: false
allsources: false
nearby_enabled: true
include_beacons: false
allow_wwv: false
allow_wcy: true
allow_announce: false
allow_self: false
allow_toxic: false
dialect: cc
grid: FN31
noise_class: URBAN
dedupe_policy: MED
path_min_observation_count: 42
solar_summary_minutes: 15
recent_ips: [192.0.2.7, 192.0.2.8]
last_login_utc: 2026-10-01T12:00:00Z
`)
}

func connectUpgradeSession(t *testing.T, s *Server, call string) (net.Conn, *bufio.Reader, <-chan struct{}, string) {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		s.handleClient(conn, nil)
	}()
	conn, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(t.Context(), "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeHandshakeTranscriptSession(t, conn, done) })
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	prompt := make([]byte, len("login: "))
	if _, err := io.ReadFull(reader, prompt); err != nil || string(prompt) != "login: " {
		t.Fatalf("login prompt=%q err=%v", prompt, err)
	}
	if _, err := io.WriteString(conn, call+"\r\n"); err != nil {
		t.Fatal(err)
	}
	var greeting strings.Builder
	for !strings.HasSuffix(greeting.String(), "READY\r\n") {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("login did not finish: %v, transcript=%q", err, greeting.String())
		}
		greeting.WriteString(line)
		if greeting.Len() > maxYAMLBytes {
			t.Fatal("unbounded upgrade greeting")
		}
	}
	return conn, reader, done, greeting.String()
}

func upgradeWireGet(t *testing.T, conn net.Conn, reader *bufio.Reader, prefix string) (filter.V6Configuration, string) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, prefix+"GET YAML CONFIG SCHEMA 6 ID Upgrade-1\r\n"); err != nil {
		t.Fatal(err)
	}
	var response strings.Builder
	for response.Len() < maxYAMLBytes {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("upgrade command response: %v, partial=%q", err, response.String())
		}
		response.WriteString(line)
		if line == "---\r\n" {
			frame := readMachineTestFrame(t, bufio.NewReader(io.MultiReader(strings.NewReader("---\r\n"), reader)))
			var envelope struct {
				Configuration filter.V6Configuration `yaml:"configuration"`
			}
			if err := yaml.Unmarshal([]byte(frame), &envelope); err != nil {
				t.Fatal(err)
			}
			return envelope.Configuration, response.String()
		}
	}
	t.Fatal("missing bounded correlated upgrade readback")
	return filter.V6Configuration{}, ""
}

func TestOldProfileUpgradeActualTCPLoginAndReconnect(t *testing.T) {
	requireH3Mappings(t)
	if err := filter.ConfigureDefaultRules([]string{"CW", "RTTY"}, []string{"HUMAN"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := filter.ConfigureDefaultRules(nil, nil); err != nil {
			t.Error(err)
		}
	})
	for _, transport := range []string{"native", "ziutek"} {
		for _, version := range []int{0, 1, 2, 3, 4, 5} {
			t.Run(fmt.Sprintf("%s/version-%d", transport, version), func(t *testing.T) {
				s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
					opts.Transport, opts.EchoMode, opts.LoginGreeting = transport, "off", "READY\n"
					opts.DedupeFastEnabled, opts.DedupeMedEnabled, opts.DedupeSlowEnabled = true, true, true
				})
				call := "LZ3NY-11"
				path := filepath.Join(filter.UserDataDir, call+".yaml")
				original := oldLoginRecord(version)
				if err := os.WriteFile(path, original, 0o600); err != nil {
					t.Fatal(err)
				}
				library := presetLibraryPathFor(call)
				if err := os.MkdirAll(filepath.Dir(library), 0o700); err != nil {
					t.Fatal(err)
				}
				oldPreset := strings.Split(string(oldLoginRecord(4)), "recent_ips:")[0]
				libraryData := "presets:\n  OLD:\n    " + strings.ReplaceAll(strings.TrimSuffix(oldPreset, "\n"), "\n", "\n    ") + "\n"
				if err := os.WriteFile(library, []byte(libraryData), 0o600); err != nil {
					t.Fatal(err)
				}
				conn, reader, done, greeting := connectUpgradeSession(t, s, call)
				cfg, loginTail := upgradeWireGet(t, conn, reader, "")
				if !strings.Contains(greeting+loginTail, "Your old filters were reset. Your settings were kept.") {
					t.Fatal("successful old-profile reset was not explained to the user")
				}
				if strings.Contains(greeting, "temporary defaults") {
					t.Fatalf("old-format login is still protected: %s", greeting)
				}
				wantSettings := filter.SettingsConfiguration{
					Dialect: "cc", Grid: "FN31", NoiseClass: "URBAN", DedupePolicy: "MED",
					PathMinObservationCount: 42, SolarSummaryMinutes: 15,
				}
				if cfg.Settings != wantSettings ||
					!reflect.DeepEqual(cfg.Filters.Rules, []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "MODE=CW,RTTY AND SOURCE=HUMAN"}}) {
					t.Fatalf("old preferences/default-mode reset lost: %+v", cfg)
				}
				if cfg.Filters.AllowSelf != filter.DefaultBoolFalse || cfg.Filters.AllowWCY != filter.DefaultBoolTrue ||
					cfg.Filters.IncludeBeacons != filter.DefaultBoolFalse || cfg.Filters.AllowWWV != filter.DefaultBoolFalse ||
					cfg.Filters.AllowAnnounce != filter.DefaultBoolFalse || cfg.Filters.AllowToxic != filter.DefaultBoolFalse {
					t.Fatal("separate controls were reset")
				}
				_, value, err := filter.PrepareV6Configuration(cfg)
				if err != nil {
					t.Fatal(err)
				}
				skimmer := spot.NewSpot("LZ1ABC", "LZ2ABC", 14020, "CW")
				skimmer.IsHuman = false
				if value.Matches(skimmer) {
					t.Fatal("upgrade did not use the configured HUMAN source default")
				}
				skimmer.IsHuman = true
				if !value.Matches(skimmer) {
					t.Fatal("upgrade did not allow matching human reports")
				}
				loaded, err := filter.LoadUserRecord(call)
				if err != nil || loaded.ConfigurationVersion != 6 || loaded.Preset != nil ||
					!slices.Contains(loaded.RecentIPs, "192.0.2.7") {
					t.Fatalf("upgraded durable metadata: %v %+v", err, loaded)
				}
				if names, err := filter.ListPresets(call); err != nil || len(names) != 0 {
					t.Fatalf("old preset library still blocks the upgraded user: %v %v", names, err)
				}
				_, saveResponse := upgradeWireGet(t, conn, reader, "SAVE PRESET AFTERUPGRADE\r\n")
				if !strings.Contains(saveResponse, "Saved preset AFTERUPGRADE for") {
					t.Fatalf("upgraded user cannot save a new preset: %s", saveResponse)
				}
				changed, response := upgradeWireGet(t, conn, reader, "PASS RULE 0 MODE=FT8\r\n")
				if strings.Contains(response, "failed to persist") || changed.Filters.Rules[0].Expr != "MODE=FT8" {
					t.Fatalf("post-upgrade human save failed: %s", response)
				}
				closeHandshakeTranscriptSession(t, conn, done)
				conn2, reader2, _, greeting2 := connectUpgradeSession(t, s, call)
				reloaded, _ := upgradeWireGet(t, conn2, reader2, "")
				if !reloaded.Equal(changed) || strings.Contains(greeting2, "temporary defaults") {
					t.Fatal("second login reset the user's new Boolean rules")
				}
				if names, err := filter.ListPresets(call); err != nil || !slices.Equal(names, []string{"AFTERUPGRADE"}) {
					t.Fatalf("second login wiped the new Boolean preset: %v %v", names, err)
				}
				after, err := os.ReadFile(path)
				if err != nil || bytes.Equal(after, original) {
					t.Fatal("old profile was not durably replaced")
				}
			})
		}
	}
}

func TestOldProfileUpgradeFailedCommitPreservesOriginal(t *testing.T) {
	for _, failure := range []error{os.ErrPermission, fmt.Errorf("replacement failed: %w", os.ErrNotExist)} {
		t.Run(failure.Error(), func(t *testing.T) {
			s := presetTestServer(t)
			call := "LZ3NY-11"
			path := filepath.Join(filter.UserDataDir, call+".yaml")
			original := oldLoginRecord(4)
			if err := os.WriteFile(path, original, 0o600); err != nil {
				t.Fatal(err)
			}
			attempts := 0
			s.commitLoginRecordFn = func(payload *filter.PreparedUserRecord) error {
				attempts++
				if attempts == 1 {
					return failure
				}
				// A second ordinary metadata commit must never run after a failed
				// upgrade, even when it would successfully overwrite the file.
				return payload.Commit()
			}
			c := configurationTestClient(s, call)
			result := s.restoreClientRecordOwned(c, time.Now().UTC())
			if attempts != 1 || result.created || !c.recordProtected || result.loadError == nil ||
				!errors.Is(result.loadError, failure) || !strings.Contains(result.warning, "temporary defaults") {
				t.Fatalf("failed upgrade claimed a writable login: attempts=%d protected=%v result=%+v", attempts, c.recordProtected, result)
			}
			if err := c.saveFilter(); !errors.Is(err, errProtectedRecord) {
				t.Fatalf("failed-upgrade session could overwrite the original: %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, original) {
				t.Fatal("failed upgrade changed the old profile bytes")
			}
		})
	}
}

func TestNewSSIDWithOldSharedPresetsCanSaveImmediately(t *testing.T) {
	if err := filter.ConfigureDefaultRules([]string{"CW"}, []string{"HUMAN"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := filter.ConfigureDefaultRules(nil, nil); err != nil {
			t.Error(err)
		}
	})
	s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
		opts.EchoMode, opts.LoginGreeting = "off", "READY\n"
		opts.DedupeFastEnabled = true
	})
	call := "LZ3NY-12"
	library := presetLibraryPathFor(call)
	if err := os.MkdirAll(filepath.Dir(library), 0o700); err != nil {
		t.Fatal(err)
	}
	original := []byte("presets:\n  OLD: {configuration_version: 4, modes: {FT8: true}}\n")
	if err := os.WriteFile(library, original, 0o600); err != nil {
		t.Fatal(err)
	}
	conn, reader, _, greeting := connectUpgradeSession(t, s, call)
	cfg, tail := upgradeWireGet(t, conn, reader, "")
	if !strings.Contains(greeting+tail, "Old shared presets were reset") ||
		strings.Contains(greeting+tail, "reset for Boolean filtering") {
		t.Fatal("new SSID received a misleading filter-reset notice")
	}
	if !slices.Equal(cfg.Filters.Rules, []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "MODE=CW AND SOURCE=HUMAN"}}) {
		t.Fatal("new SSID did not retain new-user source defaults")
	}
	if names, err := filter.ListPresets(call); err != nil || len(names) != 0 {
		t.Fatalf("old shared library blocks a new SSID: %v %v", names, err)
	}
	if backup, err := os.ReadFile(library + ".pre-v6.bak"); err != nil || !bytes.Equal(backup, original) {
		t.Fatal("new-SSID cleanup failed to preserve the original library backup")
	}
	_, reply := upgradeWireGet(t, conn, reader, "SAVE PRESET NEWSSID\r\n")
	if !strings.Contains(reply, "Saved preset NEWSSID for") {
		t.Fatalf("new SSID still cannot save: %s", reply)
	}
}

func TestNewSSIDPresetCleanupFailureAndInitialCommitBoundary(t *testing.T) {
	for _, failCommit := range []bool{false, true} {
		t.Run(fmt.Sprint(failCommit), func(t *testing.T) {
			s := presetTestServer(t)
			call := "LZ3NY-12"
			library := presetLibraryPathFor(call)
			if err := os.MkdirAll(filepath.Dir(library), 0o700); err != nil {
				t.Fatal(err)
			}
			original := []byte("presets:\n  OLD: {configuration_version: 4, modes: {FT8: true}}\n")
			if !failCommit {
				original = []byte("presets: [malformed\n")
			}
			if err := os.WriteFile(library, original, 0o600); err != nil {
				t.Fatal(err)
			}
			attempts := 0
			s.commitLoginRecordFn = func(payload *filter.PreparedUserRecord) error {
				attempts++
				if failCommit {
					return os.ErrPermission
				}
				return payload.Commit()
			}
			c := configurationTestClient(s, call)
			result := s.restoreClientRecordOwned(c, time.Now().UTC())
			if !result.created || c.recordProtected || attempts != 1 ||
				c.filter.PreparedRules() != filter.NewFilter().PreparedRules() {
				t.Fatalf("new-SSID failure changed normal creation: %+v", result)
			}
			if failCommit {
				if strings.Contains(result.warning, "shared preset cleanup") ||
					!strings.Contains(result.warning, "starting settings are active but could not be saved") {
					t.Fatal("failed initial profile commit reached library cleanup")
				}
			} else {
				if !strings.Contains(result.warning, "shared preset cleanup could not finish") {
					t.Fatal("bad library cleanup was not reported")
				}
				if err := c.saveFilter(); err != nil {
					t.Fatalf("bad shared library made new preferences unsaveable: %v", err)
				}
			}
			after, err := os.ReadFile(library)
			if err != nil || !bytes.Equal(after, original) {
				t.Fatal("failed new-SSID cleanup changed library bytes")
			}
			if _, err := os.Stat(library + ".pre-v6.bak"); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("unadmitted cleanup created an original backup")
			}
		})
	}
}

func TestCurrentProfileBadLibraryDoesNotResetOrProtectLogin(t *testing.T) {
	s := presetTestServer(t)
	call := "LZ3NY-11"
	f := testRuleFilter(t, filter.RuleDefinition{Slot: 3, Action: filter.RuleReject, Expr: "MODE=CW"})
	if err := filter.SaveUserRecord(call, &filter.UserRecord{
		Filter: *f, Dialect: "cc", Grid: "FN31", NoiseClass: "URBAN", DedupePolicy: "MED",
	}); err != nil {
		t.Fatal(err)
	}
	library := presetLibraryPathFor(call)
	if err := os.MkdirAll(filepath.Dir(library), 0o700); err != nil {
		t.Fatal(err)
	}
	original := []byte("presets: [broken\n")
	if err := os.WriteFile(library, original, 0o600); err != nil {
		t.Fatal(err)
	}
	c := configurationTestClient(s, call)
	result := s.restoreClientRecordOwned(c, time.Now().UTC())
	if c.recordProtected || result.loadError != nil || !strings.Contains(result.warning, "preset cleanup failed") ||
		c.configuredSettings.Dialect != "cc" || c.configuredSettings.Grid != "FN31" ||
		!slices.Equal(c.filter.PreparedRules().Definitions(), f.PreparedRules().Definitions()) {
		t.Fatalf("library-only failure damaged a current profile: %+v", result)
	}
	c.configuredSettings.NoiseClass = "QUIET"
	if err := c.saveFilter(); err != nil {
		t.Fatalf("unrelated preferences became unsaveable: %v", err)
	}
	after, err := os.ReadFile(library)
	if err != nil || !bytes.Equal(after, original) {
		t.Fatal("unrecognized library was overwritten")
	}
}
