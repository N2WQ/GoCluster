package telnet

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/commands"
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
	if strings.Count(document.Telnet.Greeting, "<LOGIN_SUMMARY>") != 1 ||
		strings.Count(document.Telnet.Greeting, "<UPGRADE_NOTICE>") != 1 {
		t.Fatal("shipped greeting must place the login summary and upgrade notice exactly once")
	}
	return document.Telnet.Greeting
}

func TestBooleanOnboardingLoginJourneys(t *testing.T) {
	if err := filter.ConfigureDefaultRules([]string{"CW", "FT8"}, []string{"HUMAN", "SKIMMER"}); err != nil {
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
					configuration := currentV7GOTestConfiguration(t, definitions, filter.SettingsConfiguration{
						Dialect: "go", Grid: "KN23", NoiseClass: "URBAN", DedupePolicy: "MED",
					})
					saveCurrentV7TestRecord(t, call, &filter.V7UserRecord{Configuration: configuration,
						LastLoginUTC: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)})
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
				cfg, tail := upgradeWireGet(t, s, call, conn, reader, "")
				assertHumanWire(t, transcript)
				for _, guidance := range []string{
					"Use DIALECT LIST or DIALECT ",
					"SET GRID <grid> | SET NOISE QUIET|RURAL|SUBURBAN|URBAN|INDUSTRIAL",
					"SET DEDUPE FAST|MED|SLOW",
				} {
					if !strings.Contains(transcript, guidance) {
						t.Fatalf("missing login guidance %q: %q", guidance, transcript)
					}
				}
				title := "Saved settings loaded (GO)."
				switch journey {
				case "new":
					title = "Starting filters are active (GO)."
				case "legacy":
					title = "SHOW/FILTER: your filters"
				}
				if strings.Count(transcript, title) != 1 || strings.Contains(transcript, "<LOGIN_SUMMARY>") ||
					(strings.Contains(transcript, "PASS RULE BAND=20M AND SOURCE=HUMAN") != (journey == "new")) ||
					(strings.Contains(transcript, "Coming from AR-Cluster? See HELP FILTERS.") != (journey == "new")) {
					t.Fatalf("wrong or repeated login block:\n%s", transcript)
				}
				reset := strings.Contains(transcript, "Upgrade: Your GO and CC filters")
				if reset != (journey == "legacy") {
					t.Fatalf("profile reset confused with new-user/library-only login:\n%s", transcript)
				}
				if reset && strings.Index(transcript, "Upgrade:") > strings.Index(transcript, " UTC>") {
					t.Fatal("reset notice followed the command prompt")
				}
				if strings.Contains(transcript, "The upgrade cleared presets saved in the old format.") != (journey == "library-only") ||
					strings.Contains(tail, "The upgrade cleared presets") {
					t.Fatal("wrong preset notice or notice followed the prompt")
				}
				first := journey == "new"
				if first || journey == "legacy" {
					want := []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "MODE=CW,FT8 AND SOURCE=HUMAN,SKIMMER"}}
					if !slices.Equal(cfg.Filters.GO.Rules, want) || !slices.Equal(cfg.Filters.CC.Modes, []string{"CW", "FT8"}) ||
						!slices.Equal(cfg.Filters.CC.Sources, []string{"HUMAN", "SKIMMER"}) || strings.Contains(transcript, "Report sources: HUMAN only") {
						t.Fatalf("new and upgraded starting banks differ: %+v\n%s", cfg.Filters, transcript)
					}
					if journey == "legacy" && strings.Contains(transcript, "PASS RULE") {
						t.Fatal("CC onboarding exposed Boolean rule editing")
					}
				} else if journey == "empty" {
					if len(cfg.Filters.GO.Rules) != 0 || strings.Contains(transcript, "PASS RULE 0") {
						t.Fatal("returning empty rules were defaulted")
					}
				} else if !slices.Equal(cfg.Filters.GO.Rules, complexRules) {
					t.Fatal("login lost a saved Boolean relationship or COMMENT byte")
				}
				if journey != "legacy" && (strings.Contains(transcript, "Modes:") || strings.Contains(transcript, "Bands:")) {
					t.Fatal("login flattened Boolean expressions")
				}
				if strings.Contains(transcript, "Grid not set.") {
					t.Fatal("available grid reported missing")
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
		{"MODE=CW", true, true},
		{"MODE=CW AND SOURCE=HUMAN", true, false},
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
			s.registerClient(c)
			if result := s.restoreClientRecordOwned(c, time.Now().UTC()); result.loadError != nil {
				t.Fatal(result.loadError)
			}
			response, handled := s.filterEngine.Handle(c, "PASS RULE 0 "+test.expression)
			if !handled || !strings.Contains(response, "Saved and active.") {
				t.Fatalf("edit did not save: %s", response)
			}
			record, err := filter.LoadV7UserRecord(c.callsign)
			if err != nil {
				t.Fatal(err)
			}
			_, savedGO, _, err := filter.PrepareV7Configuration(record.Configuration)
			if err != nil {
				t.Fatal(err)
			}
			cfg := c.captureV6Configuration()
			s.filterEngine.Handle(c, "RESET FILTER")
			body, err := yaml.Marshal(cfg.Filters)
			if err != nil {
				t.Fatal(err)
			}
			cmd, request := bankedV6Proposal(t, c, "PUT", "FILTER", string(body))
			v6CoordinatorSuccess(t, s.applyBankedV6MachineRequest(c, cmd, request), true)
			machine := c.captureMatchingSnapshot()
			if response, handled := s.handlePresetCommand(c, "SAVE PRESET SOURCES"); !handled ||
				!strings.Contains(response, "Saved preset SOURCES") {
				t.Fatalf("preset save failed: %s", response)
			}
			s.filterEngine.Handle(c, "RESET FILTER")
			if response, handled := s.handlePresetCommand(c, "LOAD PRESET SOURCES"); !handled ||
				!strings.Contains(response, "Loaded preset SOURCES") || !c.captureV6Configuration().Equal(cfg) {
				t.Fatalf("preset load changed saved expression: %s", response)
			}
			for _, human := range []bool{true, false} {
				want := test.skim
				if human {
					want = test.human
				}
				sp := spot.NewSpot("LZ1ABC", "LZ2ABC", 14025, "CW")
				sp.IsHuman = human
				machineMatch, _, _ := s.matchSnapshot(machine, sp)
				if savedGO.Matches(sp) != want || machineMatch != want || c.filter.Matches(sp) != want ||
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
	previous := c.captureV6Configuration()
	response, _ = s.filterEngine.Handle(c, "PASS RULE 0 MODE=CW AND")
	if !strings.Contains(response, "No changes applied.") || !previous.Equal(c.captureV6Configuration()) {
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
			if !strings.Contains(greeting, "Default settings and filters:") ||
				!strings.Contains(greeting, "Report sources: HUMAN only") {
				t.Fatal("new login did not explain starting state")
			}
			first, response := upgradeWireGet(t, s, call, conn, reader, "PASS RULE BAND=20M AND SOURCE=HUMAN\r\n")
			response += upgradeHumanCommand(t, s, call, conn, reader, "SHOW FILTER")
			response += upgradeHumanCommand(t, s, call, conn, reader, "RESUME")
			if !strings.Contains(response, "Saved and active.") || !strings.Contains(response, "Current settings and filters:") ||
				booleanDisplayedExpression(t, response, "PASS RULE 0") != "BAND=20M AND SOURCE=HUMAN" {
				t.Fatalf("first edit/readback failed:\n%s", response)
			}
			s.clientsMutex.RLock()
			client := s.clients[call]
			s.clientsMutex.RUnlock()
			if client == nil || client.readPausePending.Load() || client.readPauseUntilUnixNano.Load() != 0 {
				t.Fatal("RESUME did not clear the readback pause")
			}
			unchanged, refusal := upgradeWireGet(t, s, call, conn, reader, "PASS RULE BAND=20M AND\r\n")
			if !strings.Contains(refusal, "No changes applied.") || !unchanged.Equal(first) {
				t.Fatal("invalid first edit changed the rules")
			}
			final, response := upgradeWireGet(t, s, call, conn, reader, "PASS RULE 0 BAND=20M AND MODE=CW,FT8 AND SOURCE=HUMAN,SKIMMER\r\n")
			if !strings.Contains(response, "Saved and active.") {
				t.Fatal("explicit skimmer edit was not saved")
			}
			record, err := filter.LoadV7UserRecord(call)
			if err != nil {
				t.Fatal(err)
			}
			_, savedGO, _, err := filter.PrepareV7Configuration(record.Configuration)
			if err != nil {
				t.Fatal(err)
			}
			for _, human := range []bool{true, false} {
				sp := spot.NewSpot("LZ1ABC", "LZ2ABC", 14074, "FT8")
				sp.IsHuman = human
				if !savedGO.Matches(sp) {
					t.Fatalf("saved explicit-source rule excludes human=%v", human)
				}
			}
			closeHandshakeTranscriptSession(t, conn, done)
			conn2, reader2, _, returning := connectUpgradeSession(t, s, call)
			reloaded, _ := upgradeWireGet(t, s, call, conn2, reader2, "")
			if !reloaded.Equal(final) || !strings.Contains(returning, "Current settings and filters:") ||
				strings.Contains(returning, "Default settings and filters:") ||
				strings.Contains(returning, "PASS RULE BAND=20M AND SOURCE=HUMAN") {
				t.Fatalf("returning login reset or misdescribed the selection:\n%s", returning)
			}
		})
	}
}

func TestBooleanOnboardingLoginDisplayBudget(t *testing.T) {
	s, c := readbackTestClient()
	c.grid = strings.Repeat("X", maxYAMLBytes)
	response, err := s.renderBooleanLoginBlock(c, false, false, true)
	if response != "" || !errors.Is(err, errReadbackTooLarge) {
		t.Fatal("oversized login display returned partial success")
	}
}

func TestBooleanOnboardingRecipeAcrossGreetingBlocks(t *testing.T) {
	const summary = "SUMMARY\n<LOGIN_SUMMARY>\nEND SUMMARY\n"
	const details = "DETAILS\n<SETTINGS_FILTERS>\nEND DETAILS\n"
	const command = "  PASS RULE BAND=20M AND SOURCE=HUMAN"
	const replacement = "This replaces all of rule 0. Include every condition you want to keep."
	for _, transport := range []string{"native", "ziutek"} {
		for _, test := range []struct{ name, template string }{
			{"summary", summary}, {"details", details},
			{"summary-first", summary + details}, {"details-first", details + summary},
		} {
			t.Run(transport+"/"+test.name, func(t *testing.T) {
				s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
					opts.Transport, opts.EchoMode = transport, "off"
					opts.LoginGreeting = test.template + "READY\n"
				})
				const call = "LZ3NY-12"
				conn, _, done, first := connectUpgradeSession(t, s, call)
				assertHumanWire(t, first)
				if strings.Count(first, command) != 1 || !strings.Contains(first, command+"\r\n"+replacement) {
					t.Fatalf("first login omitted or duplicated the complete recipe:\n%s", first)
				}
				hasSummary := strings.Contains(test.template, "<LOGIN_SUMMARY>")
				hasDetails := strings.Contains(test.template, "<SETTINGS_FILTERS>")
				if strings.Contains(first, "Starting filters are active (GO).") != hasSummary ||
					strings.Contains(first, "Default settings and filters:") != hasDetails {
					t.Fatalf("template lost a requested block:\n%s", first)
				}
				if hasSummary && strings.Index(first, command) > strings.Index(first, "END SUMMARY") {
					t.Fatalf("the recipe did not appear in the short summary:\n%s", first)
				}
				closeHandshakeTranscriptSession(t, conn, done)
				_, _, _, returning := connectUpgradeSession(t, s, call)
				if strings.Contains(returning, command) || strings.Contains(returning, replacement) {
					t.Fatalf("returning login received the first-use recipe:\n%s", returning)
				}
			})
		}
	}
}

func TestBooleanPASSShortcutTCP(t *testing.T) {
	for _, transport := range []string{"native", "ziutek"} {
		t.Run(transport, func(t *testing.T) {
			s := newHandshakeTranscriptServerWithOptions(t, func(opts *ServerOptions) {
				opts.Transport, opts.EchoMode = transport, "off"
				opts.LoginGreeting = "READY\n"
				opts.InputInvalidCharMessage = "Invalid command.\n"
				opts.AutoReadPauseMinRows, opts.AutoReadPauseDuration = 10, 30*time.Second
			})
			s.processor = commands.NewProcessor(nil, nil, nil, nil, nil, nil)
			const call = "LZ3NY-12"
			otherRule := filter.RuleDefinition{Slot: 1, Action: filter.RulePass, Expr: "MODE=FT8"}
			configuration := currentV7GOTestConfiguration(t, []filter.RuleDefinition{otherRule}, filter.SettingsConfiguration{
				Dialect: "go", NoiseClass: "URBAN", DedupePolicy: "MED",
			})
			saveCurrentV7TestRecord(t, call, &filter.V7UserRecord{Configuration: configuration})
			conn, reader, done, _ := connectUpgradeSession(t, s, call)
			get := func(command string) (filter.V7Configuration, string) {
				return upgradeWireGet(t, s, call, conn, reader, command+"\r\n")
			}
			const expression = `(BAND=20M AND MODE=CW) AND COMMENT="MiXeD  ""quote"""`
			short, response := get("pass rule " + expression)
			wantRules := []filter.RuleDefinition{
				{Slot: 0, Action: filter.RulePass, Expr: `BAND=20M AND MODE=CW AND COMMENT="MiXeD  ""quote"""`},
				otherRule,
			}
			if !strings.Contains(response, "Rule 0 set.") || !strings.Contains(response, "Saved and active.") ||
				!slices.Equal(short.Filters.GO.Rules, wantRules) {
				t.Fatalf("shortcut lost a rule or changed expression bytes:\n%s", response)
			}
			numbered, response := get("PASS RULE 0 " + expression)
			if !numbered.Equal(short) || !strings.Contains(response, "Rule 0 replaced.") {
				t.Fatalf("explicit rule differs from shortcut:\n%s", response)
			}
			blocked, response := get("REJECT RULE 0 BAND=20M")
			if !strings.Contains(response, "Saved and active.") ||
				!slices.Equal(blocked.Filters.GO.Rules, []filter.RuleDefinition{{Slot: 0, Action: filter.RuleReject, Expr: "BAND=20M"}, otherRule}) {
				t.Fatalf("failed to set a rule-0 veto:\n%s", response)
			}
			path := filepath.Join(filter.UserDataDir, call+".yaml")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			refused, refusal := get("PASS RULE BAND=20M")
			if !refused.Equal(blocked) || !strings.Contains(refusal, "Rule 0 is a REJECT. Include 0 after PASS RULE to replace it.") ||
				!strings.Contains(refusal, "No changes applied.") {
				t.Fatalf("shortcut replaced a veto or gave incomplete feedback:\n%s", refusal)
			}
			_, yamlTail, _ := strings.Cut(response, "\r\nrevision: ")
			revision, _, _ := strings.Cut(yamlTail, "\r\n")
			if revision == "" {
				t.Fatal("missing revision after setting rule 0")
			}
			for _, test := range []struct{ command, hint string }{
				{"PASS RULE 01 BAND=20M", "Usage: PASS RULE <expression>"},
				{"PASS RULE 10 BAND=20M", "Usage: PASS RULE <expression>"},
				{"REJECT BAND 20M", "Use REJECT RULE <0..9> <expression>."},
				{"REJECT INVENTED", "Use REJECT RULE <0..9> <expression>."},
				{"reject band=20m", "Use REJECT RULE <0..9> <expression>."},
				{"REJECT RULE (BAND=20M)", "Use REJECT RULE <0..9> <expression>."},
				{"REJECT RULE 9 MODE=NOT_A_MODE", "Use REJECT RULE <0..9> <expression>."},
			} {
				got, reply := get(test.command)
				if !got.Equal(blocked) || !strings.Contains(reply, "\r\nrevision: "+revision+"\r\n") ||
					!strings.Contains(reply, "No changes applied.") || !strings.Contains(reply, test.hint) {
					t.Fatalf("malformed %q changed rules or gave incomplete feedback:\n%s", test.command, reply)
				}
				if strings.HasPrefix(test.hint, "Use REJECT") &&
					(!strings.Contains(reply, "Pick an empty rule number to keep your current rules.") ||
						strings.Contains(reply, "PASS RULE <expression>")) {
					t.Fatalf("malformed %q steered users toward PASS:\n%s", test.command, reply)
				}
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, before) {
				t.Fatalf("refusals changed persisted bytes: %v", err)
			}
			s.clientsMutex.RLock()
			client := s.clients[call]
			s.clientsMutex.RUnlock()
			if client == nil {
				t.Fatal("missing live client")
			}
			for _, test := range []struct {
				freqKHz float64
				want    bool
			}{{14074, false}, {7074, true}} {
				sp := spot.NewSpot("LZ1ABC", "LZ2ABC", test.freqKHz, "FT8")
				live, _, _ := s.matchSnapshot(client.captureMatchingSnapshot(), sp)
				history := client.captureHistoryFilter().matches(s, sp)
				if live != test.want || history != test.want {
					t.Fatalf("FT8 %.0f after refusal: live=%v history=%v want=%v", test.freqKHz, live, history, test.want)
				}
			}
			_, help := get("HELP FILTERS")
			if !strings.Contains(help, "Coming from AR-Cluster?") || !strings.Contains(help, "This replaces all of rule 0.") ||
				strings.Contains(help, "Type RESUME to restart now") ||
				client.readPausePending.Load() || client.readPauseUntilUnixNano.Load() != 0 {
				t.Fatalf("short HELP omitted the shortcut or paused live spots:\n%s", help)
			}
			show := upgradeHumanCommand(t, s, call, conn, reader, "SHOW FILTER")
			if booleanDisplayedExpression(t, show, "REJECT RULE 0") != "BAND=20M" {
				t.Fatalf("guarded veto missing from numbered readback:\n%s", show)
			}
			closeHandshakeTranscriptSession(t, conn, done)
			conn, reader, _, _ = connectUpgradeSession(t, s, call)
			reloaded, _ := upgradeWireGet(t, s, call, conn, reader, "")
			if !reloaded.Equal(blocked) {
				t.Fatal("reconnect lost the guarded veto or another rule")
			}
			changed, response := get("PASS RULE 0 BAND=20M")
			if !strings.Contains(response, "Saved and active.") ||
				!slices.Equal(changed.Filters.GO.Rules, []filter.RuleDefinition{{Slot: 0, Action: filter.RulePass, Expr: "BAND=20M"}, otherRule}) {
				t.Fatalf("explicit 0 failed to replace the veto:\n%s", response)
			}
			cc, _ := get("DIALECT cc")
			ccAfter, response := get("PASS RULE (BAND=20M)")
			if cc.Settings.Dialect != "cc" || !ccAfter.Equal(cc) || !strings.Contains(response, "Use HELP SET/FILTER for native CC filters.") {
				t.Fatalf("shortcut bypasses CC guidance or changes a bank:\n%s", response)
			}
		})
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
		!strings.Contains(result.warning, "Could not load or upgrade your saved settings.") ||
		strings.Contains(result.warning, "Boolean rules were kept") ||
		strings.Contains(result.warning, "Your old filters were reset.") ||
		!bytes.Equal(original, presetDiskBytes(t, path)) {
		t.Fatalf("partial library/profile write claimed a completed upgrade: %+v", result)
	}
}
