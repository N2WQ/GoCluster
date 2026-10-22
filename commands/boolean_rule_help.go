package commands

import (
	"fmt"
	"strings"

	"dxcluster/filter"
	"dxcluster/filter/expr"
	"dxcluster/spot"
)

// BooleanRuleHelpLines is the canonical reference shared by HELP RULE renderers.
// Keep literal expression examples byte-exact when presenting these lines.
func BooleanRuleHelpLines() []string {
	lines := make([]string, 0, 38)
	lines = append(lines,
		"Numbered Boolean rules (same grammar in GO and CC)",
		"PASS RULE <0..9> <expression>",
		"REJECT RULE <0..9> <expression>",
		"RESET FILTER RULE <0..9|ALL>",
		"RESET FILTER",
		"SHOW FILTER | SHOW FILTER RULE | SHOW FILTER FULL",
		"HELP RULE",
		"One slot holds one action/expression; setting it replaces both. RESET FILTER RULE clears only the selected slots. RESET FILTER clears all personal rules and turns NEARBY OFF. Neither resets separate controls or profile settings; neither reinstalls the default rule.",
		"New accounts start with configured default modes and sources in editable PASS rule 0. RESET FILTER does not reinstall that rule.",
		"Users moving from old filters receive the same starting rule and keep their settings. Existing Boolean rules are not reset.",
		"A PASS rule without SOURCE selects HUMAN only. If SOURCE appears anywhere in the rule, normal Boolean logic applies. REJECT has no HUMAN default. SOURCE=HUMAN,SKIMMER explicitly allows both; this selects reports, not feed connections.",
		"PASS rules are ORed; REJECT wins over PASS. Slot IDs are not priorities. Adding PASS broadens selection. To narrow the initial selection, edit or remove ordinary default slot 0 instead of adding another PASS.",
		"With no active PASS rule, selection is unrestricted except for matching REJECT rules. No rules means no personal restriction; separate controls still apply.",
		"Expressions: comparisons, parentheses, NOT, AND, OR. Precedence is NOT then AND then OR. Use parentheses to override it.",
		"Use = or <> with one value, comma lists or [comma,lists]. = matches any listed value; <> excludes all listed values. No ALL/ON/OFF wildcard values.",
		"Ordered < <= > >= comparisons take one value and apply only to FREQ, SNR, DXZONE and DEZONE. FREQ uses kHz, nonnegative finite numbers and inclusive low-high ranges with = or <>. SNR is a strictly integral report; zones are integers 1..40.",
		`Double-quote values containing spaces or punctuation. A literal " inside a quoted value is escaped as "". Backslash is a literal, not an escape.`,
		`COMMENT="CQ ""DX""  !;=()[]"`,
		"COMMENT preserves literal case, punctuation and spaces. Matching is ASCII case-insensitive literal substring, not a regex, glob or word match; each phrase is 1..64 printable ASCII bytes and must contain a non-space byte.",
		"Missing/unknown values make every comparison false, including <>; NOT flips that false to true. MODE UNKNOWN selects blank modes. EVENT NONE selects eventless spots. PATH INSUFFICIENT is an explicit class. CONF is unknown for exempt modes; there is no exemption bypass inside an expression.",
		"PATH classes are exact: UNLIKELY does not include CLOSED. SNR comparisons apply to any present report, including human spots. To retain the former MINSNR human exemption, include SOURCE=SKIMMER in the rule.",
		"NEARBY ON omits only geography leaves from the effective expression, retaining full saved rules. Emptied rules are ignored; non-geography conditions survive. BAND, MODE, calls, FREQ, SNR, SOURCE, EVENT, COMMENT, CONF and PATH are not geography. Rule edits need no usable grid; geometry may remain unavailable.",
		"NEARBY and SELF/BEACON/TOXIC/WWV/WCY/ANNOUNCE/PC93 remain separate controls. SELF ON can bypass personal rules and NEARBY; TOXIC still applies. Use existing control commands, not rule fields.",
		"Fields: "+strings.Join(filter.SupportedRuleFields(), " "),
		"DXCALL/DECALL accept exact calls or one * at either end (including * alone). DXDXCC/DEDXCC take positive ADIF integers. DXGRID2/DEGRID2 take two letters AA..RR. Values outside COMMENT are case-insensitive and normalized; CONFIDENCE is not a field alias.",
		"DX means the station being spotted; DE means the spotter. DXSTATE/DESTATE use US/Canadian registered mailing codes; missing values are unknown.",
		"Examples: PASS RULE 0 MODE=CW,FT8 AND SOURCE=HUMAN",
		"          REJECT RULE 9 SNR<-15 OR COMMENT=\"test beacon\"",
		"          PASS RULE 2 NOT (DXCONT=EU OR BAND=20M)",
		fmt.Sprintf("Limits: %d slots, %d bytes per expression (raw and canonical), %d atoms, %d nodes and depth %d per rule; %d values per atom; %d atoms and %d values total.",
			filter.MaxRuleSlots, expr.MaxBytes, expr.MaxAtoms, expr.MaxNodes, expr.MaxDepth,
			expr.MaxValuesPerAtom, filter.MaxTotalRuleAtoms, filter.MaxTotalRuleValues),
		"Human commands still obey the configured command-line byte limit. Use YAML schema 5 for expressions that cannot fit on a human command line.",
		"Old category mutation syntax and CC filter/mode shortcuts are not translated. Use numbered rules. No RESET RULE or SET DX FILTER alias.",
	)
	for _, vocabulary := range []struct {
		name   string
		values []string
	}{
		{"BAND", spot.SupportedBandNames()}, {"MODE", filter.SupportedModes()},
		{"SOURCE", filter.SupportedSources}, {"EVENT", append(filter.SupportedEvents(), "NONE")},
		{"DX/DECONT", filter.SupportedContinents}, {"DX/DESTATE", spot.StateCodes()},
		{"CONF", filter.SupportedConfidenceSymbols}, {"PATH", filter.SupportedPathClasses},
	} {
		lines = append(lines, vocabulary.name+": "+strings.Join(vocabulary.values, ", "))
	}
	return lines
}
