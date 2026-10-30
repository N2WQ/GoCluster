package telnet

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"dxcluster/filter"
)

func assertBooleanHumanWidth(t *testing.T, body string) {
	t.Helper()
	if len(body) > maxYAMLBytes || !strings.HasSuffix(body, "\r\n") {
		t.Fatalf("invalid bounded response size/end: %d", len(body))
	}
	for _, line := range strings.Split(strings.TrimSuffix(body, "\r\n"), "\r\n") {
		if len(line) > humanReadbackWidth || strings.ContainsAny(line, "\r\n") {
			t.Fatalf("invalid response line width: %q", line)
		}
		for _, value := range []byte(line) {
			if value < 32 || value > 126 {
				t.Fatalf("nonprintable/non-ASCII readback: %q", line)
			}
		}
	}
}

// Reassemble the existing humanResponse exact-string convention instead of
// normalizing whitespace in stored COMMENT literals.
func booleanDisplayedExpression(t *testing.T, body, label string) string {
	t.Helper()
	prefix, continuation := humanPrefix(label), strings.Repeat(" ", humanLabelWidth)+"+ "
	started := false
	var decoded strings.Builder
	for _, line := range strings.Split(body, "\r\n") {
		var piece string
		switch {
		case !started && strings.HasPrefix(line, label+" ") &&
			!strings.HasPrefix(strings.TrimLeft(line[len(label):], " "), `"`):
			return strings.TrimLeft(line[len(label):], " ")
		case !started && strings.HasPrefix(line, prefix):
			piece, started = strings.TrimPrefix(line, prefix), true
		case started && strings.HasPrefix(line, continuation):
			piece = strings.TrimPrefix(line, continuation)
		case started:
			return decoded.String()
		default:
			continue
		}
		value, err := strconv.Unquote(piece)
		if err != nil {
			t.Fatalf("incomplete display piece %q: %v", piece, err)
		}
		decoded.WriteString(value)
	}
	if !started {
		t.Fatalf("missing %q in %q", label, body)
	}
	return decoded.String()
}

func TestBooleanFilterSnapshotCanonicalRulesAndCounts(t *testing.T) {
	candidate, _, _ := booleanCommandFixture(t, DialectGo)
	cfg := filter.V6ConfigurationFromFilter(candidate.filter, candidate.configuredSettings)
	cfg.Filters.Rules = []filter.RuleDefinition{
		{Slot: 0, Action: filter.RulePass, Expr: `NEARBY AND DXCONT=EU AND (MODE=CW OR COMMENT=" MiXeD  ! ""Q"" \ ")`},
		{Slot: 5, Action: filter.RuleReject, Expr: "DEZONE=5"},
		{Slot: 9, Action: filter.RuleReject, Expr: "SNR<-10"},
	}
	canonical, value, err := filter.PrepareV6Configuration(cfg)
	if err != nil {
		t.Fatal(err)
	}
	*candidate.filter = value
	before := snapshotBooleanCommand(candidate)
	body, err := formatBooleanFilterSnapshot(candidate.filter)
	if err != nil {
		t.Fatal(err)
	}
	assertBooleanHumanWidth(t, body)
	for _, rule := range canonical.Filters.Rules {
		label := fmt.Sprintf("%s RULE %d", strings.ToUpper(string(rule.Action)), rule.Slot)
		if got := booleanDisplayedExpression(t, body, label); got != rule.Expr {
			t.Fatalf("canonical expression changed: %q", got)
		}
	}
	flattened := strings.Join(strings.Fields(body), " ")
	for _, want := range []string{
		"PASS rules are ORed", "matching REJECT wins", "Slot IDs are not priorities",
		"ordinary editable rule", "adding another PASS broadens selection", "Edit/remove slot 0",
		"SELF ON can bypass ordinary personal rules", "TOXIC still applies", "ANNOUNCE/PC93",
		"DEFAULT (ON)", "separate control", "3/10 occupied slots", "atoms 6/128", "values 5/512",
		"Program nodes 9; atoms 6; values 5.",
	} {
		if !strings.Contains(flattened, want) {
			t.Fatalf("missing %q in %q", want, flattened)
		}
	}
	for _, forbidden := range []string{"Allow:", "Block:", "CATEGORY", "RESUME", "paused during", "User ",
		"Effective", "Suspended", "geometry usable", "NEARBY ON", "full atoms", "Retained", "Nearby nodes"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("formatter retained an obsolete or unrelated owner: %q", forbidden)
		}
	}
	before.assertUnchanged(t, candidate)
}

func TestBooleanFilterSnapshotEmptyDoesNotCreateDefaults(t *testing.T) {
	for _, f := range []*filter.Filter{nil, {}} {
		body, err := formatBooleanFilterSnapshot(f)
		if err != nil {
			t.Fatal(err)
		}
		assertBooleanHumanWidth(t, body)
		for _, want := range []string{"None; all sources, modes and bands allowed.", "0/10 occupied slots"} {
			if !strings.Contains(body, want) {
				t.Fatalf("missing empty-set readback %q: %q", want, body)
			}
		}
		for _, forbidden := range []string{"PASS RULE 0", "LEGACY", "legacy comment", "Effective"} {
			if strings.Contains(body, forbidden) {
				t.Fatalf("renderer created defaults or projected category state: %q", body)
			}
		}
	}
	if filter.V6ConfigurationVersion != 6 {
		t.Fatal("Boolean activation requires v6 serializers")
	}
}

func TestBooleanFilterSnapshotWorstCaseBoundedAndImmutable(t *testing.T) {
	// Backslashes double the existing exact display encoding. Ten near-maximum
	// canonical expressions exercise the full body budget.
	atom := `COMMENT="` + strings.Repeat(`\`, 64) + `"`
	plain := strings.Repeat(atom+" OR ", 11) + atom
	definitions := make([]filter.RuleDefinition, filter.MaxRuleSlots)
	for slot := range definitions {
		expression := plain
		if slot < 8 {
			expression = "DXCONT=EU AND (" + expression + ")"
		}
		definitions[slot] = filter.RuleDefinition{Slot: slot, Action: filter.RulePass, Expr: expression}
	}
	canonical, f, err := filter.PrepareV6Configuration(filter.V6Configuration{
		Filters: filter.V6FilterConfiguration{Rules: definitions},
	})
	if err != nil {
		t.Fatal(err)
	}
	candidate := &Client{filter: &f}
	before := snapshotBooleanCommand(candidate)
	body, err := formatBooleanFilterSnapshot(&f)
	if err != nil {
		t.Fatal(err)
	}
	assertBooleanHumanWidth(t, body)
	if len(body) < 10000 || !strings.Contains(body, "10/10 occupied slots") ||
		!strings.Contains(body, "atoms 128/128") {
		t.Fatalf("worst-budget fixture was not substantial/full: %d", len(body))
	}
	for _, definition := range canonical.Filters.Rules {
		if got := booleanDisplayedExpression(t, body, fmt.Sprintf("PASS RULE %d", definition.Slot)); got != definition.Expr {
			t.Fatalf("slot %d was truncated or changed", definition.Slot)
		}
	}
	// Repetition cannot mutate canonical definitions or cache identity.
	again, err := formatBooleanFilterSnapshot(&f)
	if err != nil || again != body || !reflect.DeepEqual(canonical.Filters.Rules, f.PreparedRules().Definitions()) {
		t.Fatalf("readback was unstable: %v", err)
	}
	before.assertUnchanged(t, candidate)
}

func TestBooleanHumanBodyBudgetReturnsNoPartialText(t *testing.T) {
	for _, length := range []int{maxYAMLBytes, maxYAMLBytes + 1} {
		body, err := renderBooleanHumanBody(func(h *humanResponse) error {
			if err := h.line("Header accepted before oversized exact value"); err != nil {
				return err
			}
			return h.quoted(humanPrefix("Saved"), strings.Repeat(`\`, length), "")
		})
		if body != "" || !errors.Is(err, errReadbackTooLarge) {
			t.Fatalf("oversized body leaked a prefix: %d bytes, %v", len(body), err)
		}
	}
}

func TestBooleanRuleStagingDoesNotClaimReadbacks(t *testing.T) {
	// Dispatch owns SHOW/HELP; the detached rule-edit leaf must not claim those
	// commands or acquire mutation intent.
	candidate, _, _ := booleanCommandFixture(t, DialectGo)
	engine := newFilterCommandEngine()
	for _, line := range []string{"HELP RULE", "SHOW FILTER", "SHOW FILTER RULE", "SHOW FILTER FULL"} {
		outcome := engine.stageBooleanFilterCommand(candidate, line)
		if outcome.handled || outcome.publish || outcome.response != "" {
			t.Fatalf("readback acquired rule-edit intent: %+v", outcome)
		}
	}
}
