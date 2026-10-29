package telnet

import (
	"slices"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
)

func TestBooleanActivationLiveAndHistoryUsePreparedRules(t *testing.T) {
	f := testRuleFilter(t,
		filter.RuleDefinition{Slot: 0, Action: filter.RulePass, Expr: "MODE=FT8"},
		filter.RuleDefinition{Slot: 1, Action: filter.RuleReject, Expr: `COMMENT="blocked"`},
	)
	c := &Client{filter: f, callsign: "W1ABC", spotChan: make(chan *spotEnvelope, 1)}
	s := &Server{}
	for _, test := range []struct {
		name, dx, mode, comment string
		self                    bool
		live, history           bool
	}{
		{"ordinary pass", "K1ABC", "FT8", "CQ", true, true, true},
		{"ordinary reject", "K1ABC", "FT8", "BLOCKED", true, false, false},
		{"wrong mode", "K1ABC", "CW", "CQ", true, false, false},
		{"self bypass", "W1ABC", "CW", "BLOCKED", true, true, true},
		{"history self exception", "W1ABC", "CW", "BLOCKED", false, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f.SetSelfEnabled(test.self)
			sp := spot.NewSpot(test.dx, "N2AAA", 14074, test.mode)
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
	s, c, cfg := v6CoordinatorFixture(t)
	s.filterEngine = newFilterCommandEngine()
	v6CoordinatorSeed(t, c, cfg)
	response, handled := s.filterEngine.Handle(c, `PASS RULE 3 COMMENT="CQ test"`)
	if !handled || !strings.Contains(response, "Rule 3 set.") || strings.Contains(response, "warning:") {
		t.Fatalf("active human rule dispatch failed: %s", response)
	}
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	native := filter.ConfigurationFromFilter(&record.Filter, c.configuredSettings)
	if record.ConfigurationVersion != 6 || !native.V6().Equal(c.captureV6Configuration()) {
		t.Fatal("native record route lost the active rule/settings state")
	}
	if response, handled := s.handlePresetCommand(c, "SAVE PRESET EXPERIMENT"); !handled ||
		!strings.Contains(response, "Saved preset EXPERIMENT for") {
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
		!strings.Contains(response, "Loaded preset EXPERIMENT;") || !c.captureV6Configuration().Equal(saved) {
		t.Fatalf("native preset LOAD did not restore full Boolean state: %s", response)
	}
	replacement := configurationTestClient(s, c.callsign)
	result := s.restoreClientRecordOwned(replacement, time.Now().UTC())
	if result.loadError != nil || !replacement.captureV6Configuration().Equal(saved) {
		t.Fatalf("login restore lost persisted rules/settings: %v", result.loadError)
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
