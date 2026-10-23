package telnet

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/filter/expr"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func booleanCommandFixture(t *testing.T, dialect DialectName, nearby bool) (*Client, *Client, filter.V5Configuration) {
	t.Helper()
	cfg := filter.V5Configuration{
		Filters: filter.V5FilterConfiguration{
			Rules: []filter.RuleDefinition{
				{Slot: 0, Action: filter.RulePass, Expr: "MODE=CW,FT8 AND SOURCE=HUMAN"},
				{Slot: 4, Action: filter.RuleReject, Expr: "DXCONT=EU AND COMMENT=\"No  DX!\""},
			},
			NearbyEnabled: nearby, IncludeBeacons: filter.DefaultBoolFalse,
			AllowWWV: filter.DefaultBoolDefault, AllowWCY: filter.DefaultBoolTrue,
			AllowAnnounce: filter.DefaultBoolFalse, AllowSelf: filter.DefaultBoolDefault,
			AllowToxic: filter.DefaultBoolTrue,
		},
		Settings: filter.SettingsConfiguration{
			Dialect: string(dialect), Grid: "", NoiseClass: "URBAN", DedupePolicy: "FAST",
			PathMinObservationCount: 0, SolarSummaryMinutes: 0,
		},
	}
	canonical, value, err := filter.PrepareV5Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	value.NearbyUserFine, value.NearbyUserCoarse = 101, 202
	source := &Client{filter: &value, configuredSettings: canonical.Settings, dialect: dialect, configurationInitialized: true}
	candidate := newHumanPreferenceCandidate(source)
	if candidate.filter == source.filter {
		t.Fatal("fixture did not detach candidate")
	}
	return candidate, source, canonical
}

type booleanCommandSnapshot struct {
	cfg          filter.V5Configuration
	fingerprint  [32]byte
	pointer      *filter.Filter
	programs     *filter.PreparedRuleSet
	readback     []filter.RuleReadback
	counts       filter.RuleSetCounts
	fine, coarse pathreliability.CellID
}

func snapshotBooleanCommand(c *Client) booleanCommandSnapshot {
	cfg := filter.V5ConfigurationFromFilter(c.filter, c.configuredSettings)
	set := c.filter.PreparedRules()
	return booleanCommandSnapshot{
		cfg: cfg.Clone(), fingerprint: cfg.Fingerprint(), pointer: c.filter, programs: set,
		readback: set.Readback(true), counts: set.Counts(),
		fine: c.filter.NearbyUserFine, coarse: c.filter.NearbyUserCoarse,
	}
}

func (s booleanCommandSnapshot) assertUnchanged(t *testing.T, c *Client) {
	t.Helper()
	current := filter.V5ConfigurationFromFilter(c.filter, c.configuredSettings)
	if !s.cfg.Equal(current) || s.fingerprint != current.Fingerprint() || c.filter != s.pointer ||
		c.filter.PreparedRules() != s.programs || !reflect.DeepEqual(s.readback, s.programs.Readback(true)) ||
		s.programs.Counts() != s.counts || s.fine != c.filter.NearbyUserFine ||
		s.coarse != c.filter.NearbyUserCoarse {
		t.Fatal("command changed candidate, original snapshot, controls, settings or effective cells")
	}
}

func assertBooleanAccepted(t *testing.T, outcome humanCommandOutcome) {
	t.Helper()
	if !outcome.handled || !outcome.publish || outcome.warning != humanWarningFilter ||
		strings.HasSuffix(outcome.response, "\r\nNo changes applied.\r\n") {
		t.Fatalf("incorrect accepted intent: %+v", outcome)
	}
	assertBooleanHumanWidth(t, outcome.response)
}

func TestBooleanRuleCommandLiteralRemainder(t *testing.T) {
	for _, expression := range []string{
		`comment=" MiXeD  CQ;=[20](x)! ""DX"" \ " AND mode=cw`,
		`COMMENT=Cq-Dx?@#/*`,
		`COMMENT="  padded   spaces  "   `,
		`COMMENT="No changes applied."`,
		`mode = [ cw , ft8 ] or not (snr <> -10)`,
	} {
		line := "  pAsS   rUlE  9   " + expression
		parsed, handled, usage := parseBooleanRuleCommand(line)
		if !handled || usage != "" || parsed.expression != expression ||
			parsed.slot != 9 || parsed.action != filter.RulePass {
			t.Fatalf("literal remainder changed: %+v, %v, %q", parsed, handled, usage)
		}
	}
}

func TestBooleanRuleCommandsDialectAndCanonicalAcceptance(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			candidate, source, original := booleanCommandFixture(t, dialect, true)
			live := snapshotBooleanCommand(source)
			oldSet := candidate.filter.PreparedRules()
			oldConfig := filter.V5ConfigurationFromFilter(candidate.filter, candidate.configuredSettings).Clone()
			line := `pAsS   rUlE  9   comment=" MiXeD  CQ;=[20](x)! ""DX"" \ " AND mode = cw   `
			outcome := newFilterCommandEngine().stageBooleanFilterCommand(candidate, line)
			assertBooleanAccepted(t, outcome)
			want := `COMMENT=" MiXeD  CQ;=[20](x)! ""DX"" \ " AND MODE=CW`
			rules := candidate.filter.PreparedRules().Definitions()
			if len(rules) != 3 || rules[2] != (filter.RuleDefinition{Slot: 9, Action: filter.RulePass, Expr: want}) {
				t.Fatalf("full canonical literal lost: %+v", rules)
			}
			if booleanDisplayedExpression(t, outcome.response, "PASS RULE 9") != want {
				t.Fatalf("accepted output did not echo canonical form: %q", outcome.response)
			}
			if candidate.configuredSettings != original.Settings || candidate.filter.NearbyUserFine != 101 ||
				candidate.filter.NearbyUserCoarse != 202 || !candidate.filter.NearbyEnabled {
				t.Fatal("accepted edit lost exact settings or nearby state")
			}
			live.assertUnchanged(t, source)
			if !oldConfig.Equal(filter.V5ConfigurationFromFilter(source.filter, source.configuredSettings)) ||
				!reflect.DeepEqual(oldSet.Definitions(), oldConfig.Filters.Rules) {
				t.Fatal("edit changed immutable original/canonical references")
			}
			// Exercise the existing warning owner, without performing persistence.
			warning := outcome.persistenceFailure(candidate, errors.New("save failed"))
			if strings.Contains(warning, "No changes applied.") ||
				!strings.Contains(warning, "changes are active but were not saved") {
				t.Fatalf("later save failure falsely refused the edit: %q", warning)
			}
		})
	}
}

func TestBooleanRuleCommandsReplaceRemoveAndReset(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, test := range []struct {
			line    string
			count   int
			nearby  bool
			message string
		}{
			{"REJECT RULE 0 BAND=20", 2, true, "replaced"},
			{"RESET FILTER RULE 4", 1, true, "removed"},
			{"RESET FILTER RULE ALL", 0, true, "cleared"},
			{"RESET FILTER", 0, false, "NEARBY OFF"},
		} {
			t.Run(string(dialect)+"/"+test.line, func(t *testing.T) {
				candidate, source, cfg := booleanCommandFixture(t, dialect, true)
				live := snapshotBooleanCommand(source)
				identity := candidate.filter
				outcome := newFilterCommandEngine().stageBooleanFilterCommand(candidate, test.line)
				assertBooleanAccepted(t, outcome)
				if !strings.Contains(outcome.response, test.message) || candidate.filter != identity {
					t.Fatalf("wrong acknowledgment or replaced filter identity: %+v", outcome)
				}
				actual := filter.V5ConfigurationFromFilter(candidate.filter, candidate.configuredSettings)
				want := cfg.Clone()
				want.Filters.Rules = candidate.filter.PreparedRules().Definitions()
				want.Filters.NearbyEnabled = test.nearby
				if !want.Equal(actual) || len(actual.Filters.Rules) != test.count {
					t.Fatal("reset/replacement changed unrelated preferences or retained removed rules")
				}
				if test.nearby {
					if candidate.filter.NearbyUserFine != 101 || candidate.filter.NearbyUserCoarse != 202 {
						t.Fatal("rule-only edit lost effective cells")
					}
				} else if candidate.filter.NearbyUserFine != pathreliability.InvalidCell ||
					candidate.filter.NearbyUserCoarse != pathreliability.InvalidCell {
					t.Fatal("full reset retained effective cells")
				}
				if strings.HasPrefix(test.line, "REJECT") {
					rule := actual.Filters.Rules[0]
					if rule.Slot != 0 || rule.Action != filter.RuleReject || rule.Expr != "BAND=20M" {
						t.Fatalf("replacement did not change action and expression: %+v", rule)
					}
				}
				live.assertUnchanged(t, source)
			})
		}
	}
}

func TestBooleanRuleCommandsRepeatedSetAndAbsentRemoval(t *testing.T) {
	candidate, source, _ := booleanCommandFixture(t, DialectGo, false)
	live := snapshotBooleanCommand(source)
	engine := newFilterCommandEngine()
	assertBooleanAccepted(t, engine.stageBooleanFilterCommand(candidate, "PASS RULE 0 MODE=CW,FT8 AND SOURCE=HUMAN"))
	before := snapshotBooleanCommand(candidate)
	outcome := engine.stageBooleanFilterCommand(candidate, "RESET FILTER RULE 7")
	if !outcome.handled || outcome.publish || !strings.Contains(outcome.response, "already empty") {
		t.Fatalf("absent removal is not an accepted no-op: %+v", outcome)
	}
	before.assertUnchanged(t, candidate)
	live.assertUnchanged(t, source)
	for range 2 {
		assertBooleanAccepted(t, engine.stageBooleanFilterCommand(candidate, "RESET FILTER RULE ALL"))
		if candidate.filter.PreparedRules() != nil {
			t.Fatal("reset installed defaults instead of the empty set")
		}
	}
}

func TestBooleanRuleCommandsNearbyProjectionAndUnavailableCells(t *testing.T) {
	for _, usable := range []bool{true, false} {
		candidate, source, cfg := booleanCommandFixture(t, DialectGo, true)
		if !usable {
			candidate.filter.NearbyUserFine, candidate.filter.NearbyUserCoarse = pathreliability.InvalidCell, pathreliability.InvalidCell
		}
		live := snapshotBooleanCommand(source)
		outcome := newFilterCommandEngine().stageBooleanFilterCommand(candidate,
			`PASS RULE 0 DXCONT=EU AND (MODE=CW OR COMMENT="Mixed  DX!")`)
		assertBooleanAccepted(t, outcome)
		rows := candidate.filter.PreparedRules().Readback(true)
		if rows[0].Expr != `DXCONT=EU AND (MODE=CW OR COMMENT="Mixed  DX!")` ||
			rows[0].EffectiveExpr != `MODE=CW OR COMMENT="Mixed  DX!"` || rows[0].Suspended {
			t.Fatalf("edit erased full geography or failed to project: %+v", rows[0])
		}
		if candidate.configuredSettings != cfg.Settings || !candidate.filter.NearbyEnabled ||
			!usable && (candidate.filter.NearbyUserFine != pathreliability.InvalidCell ||
				candidate.filter.NearbyUserCoarse != pathreliability.InvalidCell) {
			t.Fatal("editing without grid reinterpreted settings or cells")
		}
		probe := &spot.Spot{Mode: "CW", IsHuman: true}
		if !candidate.filter.PreparedRules().Matches(probe, "", true) ||
			candidate.filter.PreparedRules().Matches(probe, "", false) {
			t.Fatal("full/effective rule meaning was not retained")
		}
		live.assertUnchanged(t, source)
	}
}

func TestBooleanRuleCommandsRefusalsAreAtomic(t *testing.T) {
	lines := []string{
		"PASS", "REJECT", "PASS RULE", "REJECT RULE 1", "PASS RULE 1 ",
		"PASS RULE -1 MODE=CW", "PASS RULE 10 MODE=CW", "REJECT RULE 12 MODE=CW",
		"PASS RULE 01 MODE=CW", "PASS RULE +1 MODE=CW", "PASS RULE 1x MODE=CW",
		"RESET RULE 0", "RESET FILTER RULE", "RESET FILTER RULE -1",
		"RESET FILTER RULE 10", "RESET FILTER RULE 12", "RESET FILTER RULE 0 EXTRA",
		"RESET FILTER RULE ALL EXTRA", "RESET FILTER EXTRA", "RESET FILTER RULE ALL 0",
		"PASS RULE 0 MODE=NOT_A_MODE", "PASS RULE 0 MINSNR=10",
		"PASS RULE 0 CONFIDENCE=C", "PASS RULE 0 MODE=CW AND",
		`REJECT RULE 4 COMMENT="unterminated`, `PASS RULE 0 COMMENT=""`,
		"PASS RULE 0 MODE=CW\t", "PASS RULE 0 COMMENT=\"é\"",
		"PASS RULE 0 MODE=CW" + strings.Repeat(" ", expr.MaxBytes),
	}
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, line := range lines {
			t.Run(string(dialect)+"/"+line[:min(len(line), 90)], func(t *testing.T) {
				candidate, source, _ := booleanCommandFixture(t, dialect, true)
				before, live := snapshotBooleanCommand(candidate), snapshotBooleanCommand(source)
				outcome := newFilterCommandEngine().stageBooleanFilterCommand(candidate, line)
				if !outcome.handled || outcome.publish ||
					!strings.HasSuffix(outcome.response, "No changes applied.\r\n") {
					t.Fatalf("refusal lacks explicit no-publication intent: %+v", outcome)
				}
				if strings.Contains(line, "RULE 10") || strings.Contains(line, "RULE 12") ||
					strings.Contains(line, "RULE -1") || strings.HasPrefix(line, "RESET") {
					usage := "RESET FILTER RULE <0..9|ALL>"
					if !strings.HasPrefix(line, "RESET") {
						usage = "PASS RULE <0..9> <expression>"
					}
					if !strings.Contains(outcome.response, usage) {
						t.Fatalf("missing exact usage: %q", outcome.response)
					}
				}
				assertBooleanHumanWidth(t, outcome.response)
				before.assertUnchanged(t, candidate)
				live.assertUnchanged(t, source)
			})
		}
	}
}

func TestBooleanRuleCommandsAggregateBudgetRefusal(t *testing.T) {
	candidate, source, _ := booleanCommandFixture(t, DialectGo, true)
	// Two existing rules use exactly all 512 values before the candidate edit.
	list := strings.Repeat("1,", 31) + "1"
	expression := strings.Repeat("SNR="+list+" OR ", 7) + "SNR=" + list
	set, err := filter.PrepareRuleSet([]filter.RuleDefinition{
		{Slot: 0, Action: filter.RulePass, Expr: expression},
		{Slot: 4, Action: filter.RuleReject, Expr: expression},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := filter.V5ConfigurationFromFilter(candidate.filter, candidate.configuredSettings)
	cfg.Filters = cfg.Filters.WithRuleSet(set)
	_, value, err := filter.PrepareV5Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	*candidate.filter = value
	before, live := snapshotBooleanCommand(candidate), snapshotBooleanCommand(source)
	outcome := newFilterCommandEngine().stageBooleanFilterCommand(candidate, "PASS RULE 8 SNR=1")
	if !outcome.handled || outcome.publish || !strings.Contains(outcome.response, "total value limit") ||
		!strings.HasSuffix(outcome.response, "No changes applied.\r\n") {
		t.Fatalf("aggregate failure not refused: %+v", outcome)
	}
	before.assertUnchanged(t, candidate)
	live.assertUnchanged(t, source)
}

func TestBooleanRuleCommandsLegacyAndUnownedHeads(t *testing.T) {
	refused := []string{
		"PASS BAND 20", "REJECT MODE CW", "PASS MINSNR CW 5", "PASS SOURCE HUMAN",
		"PASS EVENT POTA", "PASS COMMENT Keep  Mixed!", "REMOVE PASS COMMENT phrase",
		"REMOVE REJECT COMMENT phrase", "REJECT DXCALL W1*", "PASS DECALL *ABC",
		"PASS DXCONT EU", "REJECT DEZONE 5", "PASS DXDXCC 291", "PASS DESTATE CA",
		"PASS DXGRID2 FN", "REJECT CONFIDENCE ?", "PASS PATH HIGH", "PASS DXBM 20",
		"PASS NOFILTER", "SET/FILTER BAND 20", "UNSET/FILTER MODE CW", "SET/FT8",
		"SET/NOFT8", "SET/FT8 EXTRA", "SET/SKIMMER", "SET/NOSKIMMER EXTRA",
		"SET/NOFILTER", "SET/FILTER", "UNSET/FILTER",
		"PASS MODE=CW", "REJECT NOT (SNR>=10)", "PASS (BAND=20M AND MODE=CW)",
		"PASS NOT MODE CW", "REJECT (MODE CW)",
	}
	unhandled := []string{
		"SHOW FILTER", "SHOW FILTER RULE", "SHOW FILTER FULL", "HELP RULE", "HELP",
		"SHOW/FILTER", "SH/FILTER", "SHOW DX", "SHOW SETTINGS", "SET GRID FN31",
		"PASS NEARBY ON", "PASS SELF ON", "REJECT BEACON", "PASS TOXIC ON",
		"REJECT WWV", "PASS WCY", "PASS ANNOUNCE", "REJECT PC93",
		"SET/ANN", "SET/NOANN", "SET/SELF", "SET/NOSELF", "SET/BEACON",
		"SET/NOBEACON", "SET/WWV", "SET/NOWWV", "SET/WCY", "SET/NOWCY",
		"SET/SELF EXTRA", "SET/FILTER SELF/OFF EXTRA", "SET/FILTER NEARBY ON",
		"UNSET/FILTER TOXIC", "PASS UNRELATED THING", "SET/UNRELATED",
		"REMOVE PASS COMMENTARY phrase", "RESET SOMETHING", "DIALECT CC", "",
		"SET DX FILTER MODE=CW",
	}
	engine := newFilterCommandEngine()
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, refuse := range []bool{true, false} {
			lines := unhandled
			if refuse {
				lines = refused
			}
			for _, line := range lines {
				t.Run(fmt.Sprintf("%s/%t/%s", dialect, refuse, line), func(t *testing.T) {
					candidate, source, _ := booleanCommandFixture(t, dialect, true)
					before, live := snapshotBooleanCommand(candidate), snapshotBooleanCommand(source)
					outcome := engine.stageBooleanFilterCommand(candidate, line)
					if outcome.handled != refuse || outcome.publish {
						t.Fatalf("wrong ownership: %+v", outcome)
					}
					if refuse && (!strings.Contains(outcome.response, "PASS RULE") ||
						!strings.HasSuffix(outcome.response, "No changes applied.\r\n")) {
						t.Fatalf("missing numbered-rule guidance: %q", outcome.response)
					}
					before.assertUnchanged(t, candidate)
					live.assertUnchanged(t, source)
				})
			}
		}
	}
}
