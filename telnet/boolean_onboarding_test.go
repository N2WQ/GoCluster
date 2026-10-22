package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
	"gopkg.in/yaml.v3"
)

func shippedBooleanGreeting(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "data", "config", "runtime.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Telnet struct {
			Greeting string `yaml:"login_greeting"`
		} `yaml:"telnet"`
	}
	if err := yaml.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	if strings.Count(document.Telnet.Greeting, "<SETTINGS_FILTERS>") != 1 {
		t.Fatal("shipped greeting must place the settings/filter block exactly once")
	}
	return document.Telnet.Greeting
}

func TestBooleanOnboardingLoginJourneys(t *testing.T) {
	if err := filter.ConfigureDefaultRules([]string{"CW", "FT8"}, []string{"HUMAN"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := filter.ConfigureDefaultRules(nil, nil); err != nil {
			t.Error(err)
		}
	})
	greeting := shippedBooleanGreeting(t)
	complexRules := []filter.RuleDefinition{
		{Slot: 0, Action: filter.RulePass, Expr: "MODE=CW AND BAND=20M"},
		{Slot: 1, Action: filter.RulePass, Expr: `MODE=FT8 AND BAND=40M AND SOURCE=SKIMMER`},
		{Slot: 9, Action: filter.RuleReject, Expr: `DXCONT=EU OR COMMENT="CQ ""DX"" \ "`},
	}
	for _, transport := range []string{"native", "ziutek"} {
		for _, journey := range []string{"new", "legacy", "returning", "library-only", "empty"} {
			t.Run(transport+"/"+journey, func(t *testing.T) {
				s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
					opts.Transport, opts.EchoMode = transport, "off"
					opts.LoginGreeting = greeting + "\nREADY\n"
					opts.DefaultDedupePolicy = "SLOW"
					opts.DedupeFastEnabled, opts.DedupeMedEnabled, opts.DedupeSlowEnabled = true, true, true
					opts.GridLookup = func(string) (string, bool, bool) { return "KN23", true, true }
				})
				call := "LZ3NY-11"
				switch journey {
				case "new":
					call = "LZ3NY-12"
				case "legacy":
					old := strings.ReplaceAll(string(oldLoginRecord(4)), "FN31", "KN23")
					old = strings.ReplaceAll(old, "sources: {HUMAN: true}", "sources: {SKIMMER: true}")
					if err := os.WriteFile(filepath.Join(filter.UserDataDir, call+".yaml"), []byte(old), 0o600); err != nil {
						t.Fatal(err)
					}
				default:
					definitions := complexRules
					if journey == "empty" {
						definitions = nil
					}
					record := &filter.UserRecord{
						Filter: *testRuleFilter(t, definitions...), Dialect: "cc", Grid: "KN23",
						NoiseClass: "URBAN", DedupePolicy: "MED",
						LastLoginUTC: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC),
					}
					if err := filter.SaveUserRecord(call, record); err != nil {
						t.Fatal(err)
					}
					if journey == "library-only" {
						path := presetLibraryPathFor(call)
						if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(path, []byte("presets:\n  OLD: {configuration_version: 4, modes: {FT8: true}}\n"), 0o600); err != nil {
							t.Fatal(err)
						}
					}
				}
				conn, reader, _, transcript := connectUpgradeSession(t, s, call)
				cfg, _ := upgradeWireGet(t, conn, reader, "")
				assertHumanWire(t, transcript)
				title := "Current settings and filters:"
				if journey == "new" {
					title = "Default settings and filters:"
				}
				if strings.Count(transcript, title) != 1 || strings.Contains(transcript, "<SETTINGS_FILTERS>") {
					t.Fatalf("wrong or repeated login block:\n%s", transcript)
				}
				reset := strings.Contains(transcript, "Your old filters were reset. Your settings were kept.")
				if reset != (journey == "legacy") {
					t.Fatalf("profile reset confused with new-user/library-only login:\n%s", transcript)
				}
				first := journey == "new" || journey == "legacy"
				if strings.Contains(transcript, "To receive CW and FT8 from humans and skimmers:") != first {
					t.Fatalf("first Boolean guidance has the wrong audience:\n%s", transcript)
				}
				if first {
					want := []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "MODE=CW,FT8 AND SOURCE=HUMAN"}}
					if !slices.Equal(cfg.Filters.Rules, want) || !strings.Contains(transcript, "RBN and PSKReporter: OFF") {
						t.Fatalf("new and upgraded starting rules differ: %+v\n%s", cfg.Filters.Rules, transcript)
					}
				} else if journey == "empty" {
					if len(cfg.Filters.Rules) != 0 || strings.Contains(transcript, "PASS RULE 0") {
						t.Fatal("returning empty rules were defaulted")
					}
				} else {
					for _, rule := range complexRules {
						label := fmt.Sprintf("%s RULE %d", strings.ToUpper(string(rule.Action)), rule.Slot)
						if booleanDisplayedExpression(t, transcript, label) != rule.Expr {
							t.Fatal("login lost a saved Boolean relationship or COMMENT byte")
						}
					}
				}
				if strings.Contains(transcript, "Modes:") || strings.Contains(transcript, "Bands:") {
					t.Fatal("login flattened Boolean expressions")
				}
				if journey == "new" {
					for _, want := range []string{"GO", "KN23 (callsign lookup)", "QUIET", "SLOW"} {
						if !strings.Contains(transcript, want) {
							t.Fatalf("actual default %q missing:\n%s", want, transcript)
						}
					}
				} else {
					for _, want := range []string{"CC", "KN23", "URBAN", "MED"} {
						if !strings.Contains(transcript, want) {
							t.Fatalf("retained setting %q missing:\n%s", want, transcript)
						}
					}
				}
			})
		}
	}
}

func TestBooleanOnboardingShortRulesAreCommands(t *testing.T) {
	expression := `COMMENT="CQ ""DX"" \ " AND MODE=CW`
	cfg := passRuleTestFilter(t, expression).PreparedRules().Definitions()[0]
	body, err := renderBooleanHumanBody(func(h *humanResponse) error { return writeBooleanSavedRule(h, cfg) })
	if err != nil {
		t.Fatal(err)
	}
	assertHumanWire(t, body)
	command := strings.TrimSuffix(body, "\r\n")
	if command != "PASS RULE 0 "+expression {
		t.Fatalf("short rule is not directly reusable: %q", body)
	}
	parsed, handled, usage := parseBooleanRuleCommand(command)
	if !handled || usage != "" || parsed.expression != expression {
		t.Fatal("display changed expression quoting or backslashes")
	}
}

func TestBooleanOnboardingSourceConsumers(t *testing.T) {
	for _, test := range []struct {
		expression  string
		human, skim bool
	}{
		{"MODE=CW", true, false},
		{"MODE=CW AND SOURCE=SKIMMER", false, true},
		{"MODE=CW AND SOURCE=HUMAN,SKIMMER", true, true},
		{"MODE=FT8 OR SOURCE=SKIMMER", false, true},
		{"MODE=CW AND NOT SOURCE=HUMAN", false, true},
	} {
		t.Run(test.expression, func(t *testing.T) {
			s := presetTestServer(t)
			s.filterEngine = newFilterCommandEngine()
			c := configurationTestClient(s, "LZ3NY-11")
			c.spotChan = make(chan *spotEnvelope, 1)
			if result := s.restoreClientRecordOwned(c, time.Now().UTC()); result.loadError != nil {
				t.Fatal(result.loadError)
			}
			response, handled := s.filterEngine.Handle(c, "PASS RULE 0 "+test.expression)
			if !handled || !strings.Contains(response, "Saved and active.") {
				t.Fatalf("edit did not save: %s", response)
			}
			record, err := filter.LoadUserRecord(c.callsign)
			if err != nil {
				t.Fatal(err)
			}
			cfg := c.captureV5Configuration()
			body, err := yaml.Marshal(struct {
				Schema        int                          `yaml:"schema_version"`
				RequestID     string                       `yaml:"request_id"`
				IfRevision    string                       `yaml:"if_revision"`
				Configuration filter.V5FilterConfiguration `yaml:"configuration"`
			}{5, "onboarding-1", "revision-1", cfg.Filters})
			if err != nil {
				t.Fatal(err)
			}
			request, err := decodeV5MachineRequest(body, machineCommand{Verb: "PUT", Resource: "FILTER"})
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := request.apply(cfg)
			if err != nil {
				t.Fatal(err)
			}
			_, machine, err := filter.PrepareV5Configuration(candidate)
			if err != nil {
				t.Fatal(err)
			}
			for _, human := range []bool{true, false} {
				want := test.skim
				if human {
					want = test.human
				}
				sp := spot.NewSpot("LZ1ABC", "LZ2ABC", 14025, "CW")
				sp.IsHuman = human
				if record.Matches(sp) != want || machine.Matches(sp) != want ||
					c.captureHistoryFilter().matches(s, sp) != want {
					t.Fatalf("saved/YAML/history mismatch human=%v want=%v", human, want)
				}
				s.deliverJob(broadcastJob{spot: sp, clients: []*Client{c}, allowFast: true, allowMed: true, allowSlow: true})
				select {
				case <-c.spotChan:
					if !want {
						t.Fatal("rejected source leaked into live delivery")
					}
				default:
					if want {
						t.Fatal("positive source witness missing from live delivery")
					}
				}
			}
		})
	}
}

func TestBooleanOnboardingSaveFailureDoesNotClaimSaved(t *testing.T) {
	s := presetTestServer(t)
	s.filterEngine = newFilterCommandEngine()
	c := configurationTestClient(s, "LZ3NY-12")
	if result := s.restoreClientRecordOwned(c, time.Now().UTC()); result.loadError != nil {
		t.Fatal(result.loadError)
	}
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	before := presetDiskBytes(t, path)
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { return errors.New("disk denied") }
	response, handled := s.filterEngine.Handle(c, "PASS RULE 0 MODE=CW AND SOURCE=SKIMMER")
	if !handled || !strings.Contains(response, "changes are active but were not saved") ||
		strings.Contains(response, "Saved and active.") || !bytes.Equal(before, presetDiskBytes(t, path)) {
		t.Fatalf("false save result: %s", response)
	}
	sp := spot.NewSpot("LZ1ABC", "LZ2ABC", 14025, "CW")
	sp.IsHuman = false
	if !c.filter.Matches(sp) {
		t.Fatal("save failure incorrectly rolled back active edit")
	}
	previous := c.captureV5Configuration()
	response, _ = s.filterEngine.Handle(c, "PASS RULE 0 MODE=CW AND")
	if !strings.Contains(response, "No changes applied.") || !previous.Equal(c.captureV5Configuration()) {
		t.Fatal("invalid edit changed the active rule")
	}
}

func TestBooleanOnboardingFirstEditAndReconnectTCP(t *testing.T) {
	if err := filter.ConfigureDefaultRules([]string{"CW", "FT8"}, []string{"HUMAN"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := filter.ConfigureDefaultRules(nil, nil); err != nil {
			t.Error(err)
		}
	})
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
				opts.Transport, opts.EchoMode, opts.LoginGreeting = transport, "off", "<SETTINGS_FILTERS>\nREADY\n"
			})
			call := "LZ3NY-12"
			conn, reader, done, greeting := connectUpgradeSession(t, s, call)
			if !strings.Contains(greeting, "Default settings and filters:") {
				t.Fatal("new login did not explain starting state")
			}
			first, response := upgradeWireGet(t, conn, reader, "PASS RULE 0 BAND=20M AND MODE=CW\r\nSHOW FILTER\r\nRESUME\r\n")
			if !strings.Contains(response, "Saved and active.") || !strings.Contains(response, "Current settings and filters:") ||
				booleanDisplayedExpression(t, response, "PASS RULE 0") != "BAND=20M AND MODE=CW" {
				t.Fatalf("first edit/readback failed:\n%s", response)
			}
			s.clientsMutex.RLock()
			client := s.clients[call]
			s.clientsMutex.RUnlock()
			if client == nil || client.readPausePending.Load() || client.readPauseUntilUnixNano.Load() != 0 {
				t.Fatal("RESUME did not clear the readback pause")
			}
			unchanged, refusal := upgradeWireGet(t, conn, reader, "PASS RULE 0 BAND=20M AND\r\n")
			if !strings.Contains(refusal, "No changes applied.") || !unchanged.Equal(first) {
				t.Fatal("invalid first edit changed the rules")
			}
			final, response := upgradeWireGet(t, conn, reader, "PASS RULE 0 BAND=20M AND MODE=CW,FT8 AND SOURCE=HUMAN,SKIMMER\r\n")
			if !strings.Contains(response, "Saved and active.") {
				t.Fatal("explicit skimmer edit was not saved")
			}
			record, err := filter.LoadUserRecord(call)
			if err != nil {
				t.Fatal(err)
			}
			for _, human := range []bool{true, false} {
				sp := spot.NewSpot("LZ1ABC", "LZ2ABC", 14074, "FT8")
				sp.IsHuman = human
				if !record.Matches(sp) {
					t.Fatalf("saved explicit-source rule excludes human=%v", human)
				}
			}
			closeHandshakeTranscriptSession(t, conn, done)
			conn2, reader2, _, returning := connectUpgradeSession(t, s, call)
			reloaded, _ := upgradeWireGet(t, conn2, reader2, "")
			if !reloaded.Equal(final) || !strings.Contains(returning, "Current settings and filters:") ||
				strings.Contains(returning, "Default settings and filters:") ||
				strings.Contains(returning, "To receive CW and FT8 from humans and skimmers:") {
				t.Fatalf("returning login reset or misdescribed the selection:\n%s", returning)
			}
		})
	}
}

func TestBooleanOnboardingLoginDisplayBudget(t *testing.T) {
	s, c := readbackTestClient()
	c.grid = strings.Repeat("X", maxYAMLBytes)
	response, err := s.renderBooleanLoginBlock(c, false, false)
	if response != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("oversized login display returned partial success")
	}
}

func TestBooleanOnboardingPartialUpgradeNotice(t *testing.T) {
	s := presetTestServer(t)
	call := "LZ3NY-11"
	path := filepath.Join(filter.UserDataDir, call+".yaml")
	original := oldLoginRecord(4)
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	library := presetLibraryPathFor(call)
	if err := os.MkdirAll(filepath.Dir(library), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(library, []byte("presets:\n  OLD: {configuration_version: 4, modes: {FT8: true}}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.commitLoginRecordFn = func(*filter.PreparedUserRecord) error { return os.ErrPermission }
	c := configurationTestClient(s, call)
	result := s.restoreClientRecordOwned(c, time.Now().UTC())
	if result.profileReset || !c.recordProtected || result.loadError == nil ||
		!strings.Contains(result.warning, "profile could not be upgraded") ||
		strings.Contains(result.warning, "Boolean rules were kept") ||
		strings.Contains(result.warning, "Your old filters were reset.") ||
		!bytes.Equal(original, presetDiskBytes(t, path)) {
		t.Fatalf("partial library/profile write claimed a completed upgrade: %+v", result)
	}
}
