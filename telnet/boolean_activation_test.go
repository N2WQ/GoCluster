package telnet

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
)

func TestBooleanActivationLiveAndHistoryUsePreparedRules(t *testing.T) {
	f := testRuleFilter(t,
		filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "BAND=ALL AND MODE=FT8"},
		filter.RuleDefinition{Slot: 1, Action: filter.RuleReject, Expr: `BAND=ALL AND COMMENT="blocked"`},
	)
	c := &Client{filter: f, callsign: "W1ABC", spotChan: make(chan *spotEnvelope, 1)}
	s := &Server{}
	for _, test := range []struct {
		name, dx, mode, comment string
		unknownBand             bool
		self                    bool
		live, history           bool
	}{
		{"ordinary pass", "K1ABC", "FT8", "CQ", false, true, true, true},
		{"unknown band pass", "K1ABC", "FT8", "CQ", true, true, true, true},
		{"unknown band reject", "K1ABC", "FT8", "BLOCKED", true, true, false, false},
		{"wrong mode", "K1ABC", "CW", "CQ", false, true, false, false},
		{"self bypass", "W1ABC", "CW", "BLOCKED", false, true, true, true},
		{"history self exception", "W1ABC", "CW", "BLOCKED", false, false, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f.SetSelfEnabled(test.self)
			sp := spot.NewSpot(test.dx, "N2AAA", 14074, test.mode)
			if test.unknownBand {
				sp.Band, sp.BandNorm = "", ""
			}
			sp.Comment = test.comment
			s.deliverJob(broadcastJob{spot: sp, clients: []*Client{c}, allowFast: true, allowMed: true, allowSlow: true})
			delivered := false
			select {
			case <-c.spotChan:
				delivered = true
			default:
			}
			if delivered != test.live {
				t.Fatalf("live delivery=%v, want %v", delivered, test.live)
			}
			if got := c.captureHistoryFilter().matches(s, sp); got != test.history {
				t.Fatalf("history match=%v, want %v", got, test.history)
			}
		})
	}
}

func TestBooleanActivationHumanSavePresetAndReload(t *testing.T) {
	s, c := v7MachineTestClient(t)
	bankedV6SelectGO(t, c)
	s.filterEngine = newFilterCommandEngine()
	initial, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	saveCurrentV7TestRecord(t, c.callsign, &filter.V7UserRecord{Configuration: initial})
	const expression = `BAND=ALL AND COMMENT="CQ test"`
	response, handled := s.filterEngine.Handle(c, "PASS RULE 3 "+expression)
	if !handled || !strings.Contains(response, "Rule 3 set.") || !strings.Contains(response, expression) || strings.Contains(response, "warning:") {
		t.Fatalf("active human rule dispatch failed: %s", response)
	}
	disk := v6CoordinatorDisk(t, c)
	before := snapshotBooleanCommand(c)
	revision, err := c.v7ConfigurationRevisionToken()
	if err != nil {
		t.Fatal(err)
	}
	response, handled = s.filterEngine.Handle(c, "PASS RULE 3 SNR>ALL")
	afterRevision, err := c.v7ConfigurationRevisionToken()
	if err != nil || !handled || !strings.Contains(response, "col 5: SNR>ALL needs a number") ||
		!strings.Contains(response, "Use SNR=ALL") || afterRevision != revision || !bytes.Equal(v6CoordinatorDisk(t, c), disk) {
		t.Fatalf("invalid edit lost its diagnostic or changed durable state: %s (%v)", response, err)
	}
	before.assertUnchanged(t, c)
	record, err := filter.LoadV7UserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	current, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	if record.ConfigurationVersion != 7 || !record.Configuration.Equal(current) {
		t.Fatal("native record route lost the active rule/settings state")
	}
	if response, handled := s.handlePresetCommand(c, "SAVE PRESET EXPERIMENT"); !handled ||
		!strings.Contains(response, "Saved preset EXPERIMENT (GO) for") {
		t.Fatalf("native preset SAVE failed: %s", response)
	}
	saved := c.captureV6Configuration()
	settings := saved.Settings
	if response, _ := s.filterEngine.Handle(c, "RESET FILTER"); !strings.Contains(response, "Personal rules cleared") {
		t.Fatalf("active reset failed: %s", response)
	}
	if len(c.captureV6Configuration().Filters.Rules) != 0 || c.captureV6Configuration().Settings != settings {
		t.Fatal("reset reinstalled defaults or changed unrelated settings")
	}
	if response, handled := s.handlePresetCommand(c, "LOAD PRESET EXPERIMENT"); !handled ||
		!strings.Contains(response, "Loaded preset EXPERIMENT (GO);") || !c.captureV6Configuration().Equal(saved) {
		t.Fatalf("native preset LOAD did not restore full Boolean state: %s", response)
	}
	replacement := configurationTestClient(s, c.callsign)
	result := s.restoreClientRecordOwned(replacement, time.Now().UTC())
	if result.loadError != nil || !replacement.captureV6Configuration().Equal(saved) {
		t.Fatalf("login restore lost persisted rules/settings: %v", result.loadError)
	}
}

func TestBooleanOperandsYAMLWireAndRefusal(t *testing.T) {
	for _, schema := range []struct {
		version                 int
		patch, rules, errorPath string
	}{
		{6, "PATCH YAML FILTER", "  rules: %s\n", "configuration.rules[0].expr"},
		{7, "PATCH YAML FILTER SCHEMA 7", "  dialect: go\n  filters:\n    rules: %s\n", "configuration.filters.rules[0].expr"},
	} {
		t.Run(fmt.Sprint(schema.version), func(t *testing.T) {
			s, c := v7MachineTestClient(t)
			bankedV6SelectGO(t, c)
			revision := func() string {
				t.Helper()
				value, err := c.v7ConfigurationRevisionToken()
				if err != nil {
					t.Fatal(err)
				}
				return value
			}
			command := func(line string) string {
				t.Helper()
				dispatch := s.handleV7MachineCommand(c, line)
				if !dispatch.handled || dispatch.terminal {
					t.Fatalf("%s dispatch failed: %+v", line, dispatch)
				}
				select {
				case reply := <-c.controlChan:
					if reply.configurationReservation != nil {
						reply.configurationReservation.Release()
					}
					return string(reply.raw)
				default:
					t.Fatalf("%s did not return a reply", line)
					return ""
				}
			}
			patch := func(expression string) string {
				t.Helper()
				rules := fmt.Sprintf("[{slot: 0, action: pass, expr: '%s'}]", expression)
				body := fmt.Sprintf("---\nschema_version: %d\nrequest_id: wildcard\nif_revision: %s\nconfiguration:\n", schema.version, revision())
				transport, _ := newYAMLTestClient(body + fmt.Sprintf(schema.rules, rules) + "...\n")
				c.conn, c.reader = transport.conn, transport.reader
				return command(schema.patch)
			}

			if response := patch("band=[all] AND MODE=CW AND DECONT=mycont AND DXDXCC=mydxcc"); !strings.Contains(response, "persisted: true") {
				t.Fatalf("valid operand edit failed: %s", response)
			}
			const canonical = "BAND=ALL AND MODE=CW AND DECONT=MYCONT AND DXDXCC=MYDXCC"
			saved, err := filter.LoadV7UserRecord(c.callsign)
			if err != nil || len(saved.Configuration.Filters.GO.Rules) != 1 || saved.Configuration.Filters.GO.Rules[0].Expr != canonical {
				t.Fatalf("operands were not saved canonically: %+v (%v)", saved, err)
			}
			readback := command(fmt.Sprintf("GET YAML FILTER SCHEMA %d", schema.version))
			matchingSpot := &spot.Spot{Mode: "CW", DEMetadata: spot.CallMetadata{Continent: "NA"}, DXMetadata: spot.CallMetadata{ADIF: 1}}
			wrongMode := &spot.Spot{Mode: "FT8", DEMetadata: matchingSpot.DEMetadata, DXMetadata: matchingSpot.DXMetadata}
			context := filter.EvaluationContext{OperatingGeography: filter.OperatingGeography{Continent: "NA", DXCC: 1}}
			if !strings.Contains(readback, canonical) || !c.filter.MatchesWithContext(matchingSpot, context) ||
				c.filter.MatchesWithContext(wrongMode, context) || c.filter.Matches(matchingSpot) {
				t.Fatalf("operands differ between readback and matching: %s", readback)
			}

			before, disk, savedRevision := snapshotBooleanCommand(c), v6CoordinatorDisk(t, c), revision()
			response := patch("SNR>ALL")
			if !strings.Contains(response, "code: invalid_document") || !strings.Contains(response, schema.errorPath) ||
				!strings.Contains(response, "col 5: SNR>ALL") || !strings.Contains(response, "Use SNR=ALL") ||
				revision() != savedRevision || !bytes.Equal(v6CoordinatorDisk(t, c), disk) {
				t.Fatalf("refused wildcard lost its wire diagnostic or changed durable state: %s", response)
			}
			before.assertUnchanged(t, c)
			response = patch("DECONT=MYDXCC")
			if !strings.Contains(response, "code: invalid_document") || !strings.Contains(response, schema.errorPath) ||
				revision() != savedRevision || !bytes.Equal(v6CoordinatorDisk(t, c), disk) {
				t.Fatalf("wrong field type was not refused atomically: %s", response)
			}
			before.assertUnchanged(t, c)
		})
	}
}

func TestBooleanActivationNewUserVersusRestoredEmpty(t *testing.T) {
	s := presetTestServer(t)
	s.filterEngine = newFilterCommandEngine()
	c := configurationTestClient(s, "W1NEW-1")
	first := s.restoreClientRecordOwned(c, time.Now().UTC())
	definitions := c.filter.PreparedRules().Definitions()
	if first.loadError != nil || !first.created || len(definitions) != 1 || definitions[0].Slot != 0 ||
		definitions[0].Action != filter.RulePass || !strings.Contains(definitions[0].Expr, "MODE=") {
		t.Fatalf("new user did not receive the visible default slot: %+v %v", definitions, first.loadError)
	}
	if response, _ := s.filterEngine.Handle(c, "RESET FILTER"); !strings.Contains(response, "Personal rules cleared") {
		t.Fatalf("reset failed: %s", response)
	}
	settings := c.captureV6Configuration().Settings
	replacement := configurationTestClient(s, c.callsign)
	restored := s.restoreClientRecordOwned(replacement, time.Now().UTC())
	if restored.loadError != nil || restored.created || len(replacement.filter.PreparedRules().Definitions()) != 0 ||
		replacement.captureV6Configuration().Settings != settings {
		t.Fatalf("restored empty user received new-user defaults: %+v %v",
			replacement.filter.PreparedRules().Definitions(), restored.loadError)
	}
	if slices.Equal(definitions, replacement.filter.PreparedRules().Definitions()) {
		t.Fatal("new-user and explicit-empty records were not distinct")
	}
}
