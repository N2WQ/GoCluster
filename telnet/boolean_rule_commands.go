// File role: Stages numbered-rule edits on detached human candidates.
// Publication, persistence and input policy remain caller-owned.
package telnet

import (
	"fmt"
	"strings"

	"dxcluster/cty"
	"dxcluster/filter"
)

const booleanRuleSetUsage = "Usage: PASS RULE <expression>\n       PASS RULE <0..9> <expression>\n       REJECT RULE <0..9> <expression>"
const booleanRejectRuleUsage = "Use REJECT RULE <0..9> <expression>.\nPick an empty rule number to keep your current rules."
const booleanRuleResetUsage = "Usage: RESET FILTER RULE <0..9|ALL>\n       RESET FILTER"
const booleanRuleDiscovery = "Use PASS RULE <expression> to set rule 0.\n" + booleanRuleSetUsage + "\nType HELP RULE for fields and examples."

type booleanRuleEdit uint8

const (
	booleanRuleSet booleanRuleEdit = iota
	booleanRuleRemove
	booleanRuleClear
	booleanFilterClear
)

type booleanRuleCommand struct {
	edit        booleanRuleEdit
	slot        int
	omittedSlot bool
	action      filter.RuleAction
	expression  string
}

// Header words use ASCII spaces only. The expression is a borrowed, complete
// remainder, including trailing spaces and every literal byte inside quotes.
func parseBooleanRuleCommand(line string) (booleanRuleCommand, bool, string) {
	var command booleanRuleCommand
	verb, tail := takeCommentWord(line)
	word, rest := takeCommentWord(tail)
	switch verb {
	case "PASS", "REJECT":
		command.action = filter.RulePass
		if verb == "REJECT" {
			command.action = filter.RuleReject
		}
		usage := booleanRuleUsage(command.action)
		if word != "RULE" {
			if word == "" {
				return command, true, usage
			}
			return command, false, ""
		}
		start, omittedSlot, ok := booleanRuleExpressionStart([]byte(line))
		if !ok || start == len(line) {
			return command, true, usage
		}
		if !omittedSlot {
			slot, _ := takeCommentWord(rest)
			if !booleanRuleSlot(slot) {
				return command, true, usage
			}
			command.slot = int(slot[0] - '0')
		}
		command.omittedSlot, command.expression = omittedSlot, line[start:]
		return command, true, ""
	case "RESET":
		if word == "RULE" {
			return command, true, booleanRuleResetUsage
		}
		if word != "FILTER" {
			return command, false, ""
		}
		kind, rest := takeCommentWord(rest)
		if kind == "" {
			command.edit = booleanFilterClear
			return command, true, ""
		}
		if kind != "RULE" {
			return command, true, booleanRuleResetUsage
		}
		slot, extra := takeCommentWord(rest)
		if strings.Trim(extra, " ") != "" {
			return command, true, booleanRuleResetUsage
		}
		if slot == "ALL" {
			command.edit = booleanRuleClear
		} else if booleanRuleSlot(slot) {
			command.edit, command.slot = booleanRuleRemove, int(slot[0]-'0')
		} else {
			return command, true, booleanRuleResetUsage
		}
		return command, true, ""
	default:
		return command, false, ""
	}
}

func booleanRuleSlot(word string) bool {
	return len(word) == 1 && word[0] >= '0' && word[0] <= '9'
}

func booleanRuleUsage(action filter.RuleAction) string {
	if action == filter.RuleReject {
		return booleanRejectRuleUsage
	}
	return booleanRuleSetUsage
}

func booleanLegacyRuleDiscovery(line string, dialect DialectName) string {
	if dialect == DialectCC {
		return booleanRuleDiscovery
	}
	verb, tail := takeCommentWord(line)
	_, reject := booleanCCFilterExclusion(verb, tail)
	return booleanRuleDiscoveryFor(verb == "REJECT" || reject, dialect)
}

// Only the recovery text uses this classification. A header-only shortcut
// check carries exclusion intent even when dispatch refuses additional words.
func booleanCCFilterExclusion(verb, tail string) (domain string, reject bool) {
	if verb == "SET/FILTER" {
		if words := strings.Fields(tail); len(words) > 0 {
			if action, ok := legacyCCSpacedDXBMAction(words); ok {
				return "DXBM", action == actionBlock
			}
			domain, action, _ := nativeCCFilterAction(words)
			return domain, action == "REJECT"
		}
		return "", false
	}
	if !strings.HasPrefix(verb, "SET/") {
		return "", false
	}
	command, matched, refusal := parseNativeCCShortcut(verb, 1)
	return command.category, matched && refusal == "" && !command.enabled &&
		(command.edit == nativeCCMode || command.edit == nativeCCSource)
}

func booleanRuleDiscoveryFor(reject bool, dialect DialectName) string {
	if reject && dialect != DialectCC {
		return booleanRejectRuleUsage + "\nType HELP RULE for fields and examples."
	}
	return booleanRuleDiscovery
}

// This entry point must receive a detached candidate, never a live client. It
// validates and renders before assignment, so every refusal leaves the candidate
// and all previously published immutable programs untouched.
func (e *filterCommandEngine) stageBooleanFilterCommand(c *Client, line string) humanCommandOutcome {
	if e == nil || c == nil {
		return humanCommandOutcome{}
	}
	command, handled, usage := parseBooleanRuleCommand(line)
	if handled && c.dialect == DialectCC {
		return nativeCCRefusal(nativeCCCommandGuidance)
	}
	if !handled {
		if e.booleanLegacyMutation(line, c.dialect) {
			return booleanRuleRefusal(booleanLegacyRuleDiscovery(line, c.dialect))
		}
		return humanCommandOutcome{}
	}
	if usage != "" {
		return booleanRuleRefusal(usage + "\nType HELP RULE for fields and examples.")
	}
	c.filterMu.Lock()
	defer c.filterMu.Unlock()
	if c.filter == nil {
		return booleanRuleRefusal("Personal filter unavailable.")
	}
	previous := c.filter.PreparedRules()
	if command.omittedSlot {
		rules := previous.Definitions()
		if len(rules) != 0 && rules[0].Slot == 0 && rules[0].Action == filter.RuleReject {
			return booleanRuleRefusal("Rule 0 is a REJECT. Include 0 after PASS RULE to replace it.")
		}
	}
	var lookup func() *cty.CTYDatabase
	if c.server != nil {
		lookup = c.server.ctyLookup
	}
	next, err := command.prepare(previous, newRuleDXCCResolver(lookup))
	if err != nil {
		return booleanRuleRefusal("Invalid rule: " + err.Error() + "\n" + booleanRuleUsage(command.action))
	}
	cfg := filter.V6ConfigurationFromFilter(c.filter, c.configuredSettings)
	cfg.Filters = cfg.Filters.WithRuleSet(next)
	_, prepared, err := filter.PrepareV6Configuration(cfg)
	if err != nil {
		return booleanRuleRefusal("Cannot prepare personal filter: " + err.Error())
	}
	response, err := command.acceptedResponse(previous, next)
	if err != nil {
		return booleanRuleRefusal("Cannot display the complete accepted rule.")
	}
	// Repeated ordinary sets retain save intent. Removing an absent slot is an
	// accepted no-op; in particular, it must not discard old candidate state.
	publish := command.edit != booleanRuleRemove || previous != next
	if publish {
		*c.filter = prepared
	}
	return humanCommandOutcome{response: response, handled: true, publish: publish, warning: humanWarningFilter}
}

// One lazy index belongs to the whole request, including all machine rule slots.
// Numeric operands never call this closure; no CTY state survives preparation.
func newRuleDXCCResolver(lookup func() *cty.CTYDatabase) func(string) (int, error) {
	var index *cty.DXCCIndex
	var initialized bool
	return func(label string) (int, error) {
		if !initialized {
			initialized = true
			if lookup != nil {
				if db := lookup(); db != nil {
					index = cty.NewDXCCIndex(db)
				}
			}
		}
		if index == nil {
			return 0, fmt.Errorf("CTY database is not available")
		}
		return index.ResolveCanonical(label)
	}
}

func (command booleanRuleCommand) prepare(previous *filter.PreparedRuleSet, resolve func(string) (int, error)) (*filter.PreparedRuleSet, error) {
	switch command.edit {
	case booleanRuleSet:
		return previous.WithRuleDXCCResolver(filter.RuleDefinition{
			Slot: command.slot, Action: command.action, Expr: command.expression,
		}, resolve)
	case booleanRuleRemove:
		return previous.WithoutRule(command.slot)
	default:
		// Reset is deliberately empty, not the new-user default factory.
		return nil, nil //nolint:nilnil // Nil is the valid empty rule set.
	}
}

func (command booleanRuleCommand) acceptedResponse(previous, next *filter.PreparedRuleSet) (string, error) {
	return renderBooleanHumanBody(func(h *humanResponse) error {
		switch command.edit {
		case booleanRuleSet:
			verb := "set"
			for _, definition := range previous.Definitions() {
				if definition.Slot == command.slot {
					verb = "replaced"
					break
				}
			}
			if err := h.line(fmt.Sprintf("Rule %d %s.", command.slot, verb)); err != nil {
				return err
			}
			for _, definition := range next.Definitions() {
				if definition.Slot == command.slot {
					return writeBooleanSavedRule(h, definition)
				}
			}
			return fmt.Errorf("accepted rule missing")
		case booleanRuleRemove:
			if previous == next {
				return h.line(fmt.Sprintf("Rule %d is already empty.", command.slot))
			}
			return h.line(fmt.Sprintf("Rule %d removed.", command.slot))
		case booleanRuleClear:
			return h.line("All personal rule slots cleared. Settings and separate controls kept.")
		default:
			return h.line("Personal rules cleared. Settings and separate controls kept.")
		}
	})
}

func booleanRuleRefusal(message string) humanCommandOutcome {
	response, err := renderBooleanHumanBody(func(h *humanResponse) error {
		for _, line := range strings.Split(message, "\n") {
			if err := h.prose("", "  ", line); err != nil {
				return err
			}
		}
		return h.line("No changes applied.")
	})
	if err != nil {
		response = "Invalid rule command. Type HELP RULE for usage.\r\nNo changes applied.\r\n"
	}
	return humanHandled(response)
}

// Classification only: legacy category mutations cannot execute or translate.
// Separate controls and unrelated commands are left to their existing owners,
// including malformed control shortcuts whose old parser reports only usage.
func (e *filterCommandEngine) booleanLegacyMutation(line string, dialect DialectName) bool {
	verb, tail := takeCommentWord(line)
	domain, _ := takeCommentWord(tail)
	if (verb == "PASS" || verb == "REJECT") && booleanSeparateDomain(domain) {
		return false
	}
	if (verb == "PASS" || verb == "REJECT") &&
		(strings.ContainsAny(tail, "=<>") || domain == "NOT" || strings.HasPrefix(domain, "(")) {
		return true
	}
	tokens := strings.Fields(line)
	if len(tokens) == 0 {
		return false
	}
	upper := make([]string, len(tokens))
	for i := range tokens {
		upper[i] = strings.ToUpper(tokens[i])
	}
	for _, candidate := range []DialectName{dialect, DialectGo, DialectCC} {
		parsed, matched, _ := e.parse(tokens, upper, candidate)
		if matched && booleanSeparateDomain(parsed.domain) {
			return false
		}
		if matched && booleanLegacyDomain(parsed.domain) {
			return true
		}
	}
	// Shortcut usage errors intentionally contain no parsed domain. Classify a
	// header-only probe without accepting or executing the original command.
	if strings.HasPrefix(verb, "SET/") {
		probe := 1
		if verb == "SET/FILTER" && len(tokens) > 1 {
			probe = 2
		}
		parsed, _, _ := e.parse(tokens[:probe], upper[:probe], DialectCC)
		if booleanSeparateDomain(parsed.domain) {
			return false
		}
		if booleanLegacyDomain(parsed.domain) {
			return true
		}
	}
	if verb == "REMOVE" && (domain == "PASS" || domain == "REJECT") {
		_, rest := takeCommentWord(tail)
		category, _ := takeCommentWord(rest)
		return category == "COMMENT"
	}
	return verb == "SET/FILTER" || verb == "UNSET/FILTER"
}

func booleanSeparateDomain(domain string) bool {
	switch domain {
	case "SELF", "BEACON", "TOXIC", "WWV", "WCY", "ANNOUNCE", "PC93":
		return true
	default:
		return false
	}
}

func booleanLegacyDomain(domain string) bool {
	switch domain {
	case "BAND", "MODE", "MINSNR", "SOURCE", "EVENT", "COMMENT", "DXCALL", "DECALL",
		"DXCONT", "DECONT", "DXZONE", "DEZONE", "DXDXCC", "DEDXCC", "DXSTATE", "DESTATE",
		"DXGRID2", "DEGRID2", "CONFIDENCE", "CONF", "PATH", "DXBM", "NOFILTER", "NEARBY":
		return true
	default:
		return false
	}
}
