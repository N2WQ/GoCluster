// File role: Prepare bounded, owned domain rules without publishing active rules.
package filter

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"dxcluster/filter/expr"
	"dxcluster/spot"
)

// Syntax stays available during preparation. Nodes preserve its indexes and
// syntax is released before publishing the single immutable program.
// All strings detach from the upload buffer and all data freezes after compile.
type compiledRule struct {
	syntax                *expr.Expression
	nodes                 []compiledRuleNode
	root                  int
	canonical             string
	atomCount, valueCount int
	rawValueCount         int // Incoming operand count before meta expansion; admission only.
	needsPath             bool
	needsNearby           bool
}

type compiledRuleNode struct {
	kind        expr.Kind
	left, right int
	predicate   rulePredicate
}

func compileRule(source string) (*compiledRule, error) {
	return compileRuleWithDXCCResolver(source, nil, true)
}

func compileRuleWithDXCCResolver(source string, resolve func(string) (int, error), allowAll bool) (*compiledRule, error) {
	syntax, err := expr.Parse(source)
	if err != nil {
		return nil, err
	}
	program := &compiledRule{
		syntax: syntax, nodes: make([]compiledRuleNode, len(syntax.Nodes)),
		root: syntax.Root, atomCount: syntax.Atoms,
	}
	for i := range syntax.Nodes {
		node := &syntax.Nodes[i]
		compiled := &program.nodes[i]
		compiled.kind, compiled.left, compiled.right = node.Kind, node.Left, node.Right
		if node.Kind != expr.AtomKind && node.Kind != expr.PredicateKind {
			continue
		}
		// Even already-uppercase parser tokens and one-byte operators can be
		// substrings of a much larger caller-owned upload allocation.
		node.Atom.Field = strings.Clone(node.Atom.Field)
		node.Atom.Operator = expr.Operator(strings.Clone(string(node.Atom.Operator)))
		if node.Atom.Field == "COMMENT" {
			// Syntax folds bare tokens. COMMENT's literal spelling instead
			// comes from its exact ASCII token span, then detaches below.
			for j := range node.Atom.Values {
				value := &node.Atom.Values[j]
				if !value.Quoted {
					start := value.Column - 1
					value.Text = source[start : start+len(value.Text)]
				}
			}
		}
		program.rawValueCount += len(node.Atom.Values)
		predicate, err := compileRulePredicate(&node.Atom, resolve, allowAll)
		if err != nil {
			return nil, err
		}
		compiled.predicate = predicate
		program.valueCount += len(node.Atom.Values)
		program.needsPath = program.needsPath || predicate.field == ruleFieldPath
		program.needsNearby = program.needsNearby || predicate.field == ruleFieldNearby
	}
	program.canonical, err = syntax.Canonical()
	if err != nil {
		return nil, err
	}
	return program, nil
}

func ruleCompileError(column int, format string, args ...any) error {
	return &expr.SyntaxError{Column: column, Message: fmt.Sprintf(format, args...)}
}

func compileRulePredicate(atom *expr.Atom, resolve func(string) (int, error), allowAll bool) (rulePredicate, error) {
	field, ok := ruleFieldForName(atom.Field)
	if !ok {
		return rulePredicate{}, ruleCompileError(atom.Column, "unsupported field %q", atom.Field)
	}
	predicate := rulePredicate{field: field, operator: atom.Operator}
	if field == ruleFieldNearby {
		if atom.Operator != "" || len(atom.Values) != 0 {
			return rulePredicate{}, ruleCompileError(atom.Column, "NEARBY takes no operator or values")
		}
		return predicate, nil
	}
	ordered := atom.Operator != expr.Equal && atom.Operator != expr.NotEqual
	if ordered {
		switch field {
		case ruleFieldFreq, ruleFieldSNR, ruleFieldDXZone, ruleFieldDEZone:
		default:
			return rulePredicate{}, ruleCompileError(atom.Column, "%s does not support operator %s", atom.Field, atom.Operator)
		}
		if len(atom.Values) != 1 {
			return rulePredicate{}, ruleCompileError(atom.Column, "%s ordered comparisons require one value", atom.Field)
		}
	}
	if field == ruleFieldBand {
		if err := expandRuleMetaBands(atom, allowAll); err != nil {
			return rulePredicate{}, err
		}
	}
	hasAll := false
	acceptsAll := allowAll && field.supportsAllValues()
	for i := range atom.Values {
		value := &atom.Values[i]
		if acceptsAll && isAllOperand(*value) {
			if ordered {
				return rulePredicate{}, ruleCompileError(value.Column, "%s%sALL needs a number after %s. Use %s=ALL to match any value", atom.Field, atom.Operator, atom.Operator, atom.Field)
			}
			hasAll = true
			continue
		}
		text, err := predicate.compileValue(value, ordered, resolve)
		if err != nil {
			return rulePredicate{}, err
		}
		value.Text = text
		// Canonical taxonomy names can be ALL even when the input was an alias.
		// Keep that literal quoted so reparsing cannot widen the saved rule.
		value.Quoted = field == ruleFieldComment || (acceptsAll && text == "ALL")
	}
	if hasAll {
		// Validate every list member first. The constant then owns no lookup
		// operands and does not schedule path prediction.
		return rulePredicate{field: ruleFieldAllValues, operator: atom.Operator}, nil
	}
	return predicate, nil
}

func isAllOperand(value expr.Value) bool {
	return !value.Quoted && value.Text == "ALL"
}

// Only BAND atoms containing a fixed meta name are expanded and deduplicated.
// Other lists keep their multiplicity. Expanded lists retain a bare ALL
// alongside their physical members so the wildcard survives canonicalization.
func expandRuleMetaBands(atom *expr.Atom, allowAll bool) error {
	containsMeta := false
	for _, value := range atom.Values {
		if _, ok := spot.MetaBandMembers(value.Text); ok {
			containsMeta = true
			break
		}
	}
	if !containsMeta {
		return nil
	}
	values := make([]expr.Value, 0, len(atom.Values))
	for _, value := range atom.Values {
		members, ok := spot.MetaBandMembers(value.Text)
		if !ok {
			member := value.Text
			if !allowAll || !isAllOperand(value) {
				member = spot.NormalizeBand(value.Text)
				if !spot.IsValidBand(member) {
					return ruleCompileError(value.Column, "unsupported BAND value %q", value.Text)
				}
			}
			members = []string{member}
		}
		for _, member := range members {
			if slices.ContainsFunc(values, func(existing expr.Value) bool { return existing.Text == member }) {
				continue
			}
			if len(values) >= expr.MaxValuesPerAtom {
				return ruleCompileError(value.Column, "expanded BAND selection exceeds value limit %d", expr.MaxValuesPerAtom)
			}
			values = append(values, expr.Value{Text: member, Column: value.Column})
		}
	}
	atom.Values = values
	return nil
}

func (p *rulePredicate) compileValue(value *expr.Value, ordered bool, resolve func(string) (int, error)) (string, error) {
	switch p.field {
	case ruleFieldFreq:
		frequency, err := compileRuleFrequency(value.Text, !ordered)
		if err != nil {
			return "", ruleCompileError(value.Column, "invalid FREQ value %q: %s", value.Text, err)
		}
		p.frequencies = append(p.frequencies, frequency)
		low := strconv.FormatFloat(frequency.low, 'f', -1, 64)
		if frequency.low == frequency.high {
			return low, nil
		}
		return low + "-" + strconv.FormatFloat(frequency.high, 'f', -1, 64), nil
	case ruleFieldDXDXCC, ruleFieldDEDXCC:
		number, err := compileRuleDXCCValue(value, resolve)
		if err != nil {
			return "", err
		}
		p.integers = append(p.integers, number)
		return strconv.Itoa(number), nil
	case ruleFieldSNR, ruleFieldDXZone, ruleFieldDEZone:
		number, err := strconv.Atoi(value.Text)
		if err != nil {
			return "", ruleCompileError(value.Column, "expected a strictly integral value for %q: %s", value.Text, err)
		}
		if (p.field == ruleFieldDXZone || p.field == ruleFieldDEZone) && !IsSupportedZone(number) {
			return "", ruleCompileError(value.Column, "CQ zone must be 1..40")
		}
		p.integers = append(p.integers, number)
		return strconv.Itoa(number), nil
	default:
		return p.compileText(value)
	}
}

func compileRuleDXCCValue(value *expr.Value, resolve func(string) (int, error)) (int, error) {
	number, err := strconv.Atoi(value.Text)
	if err != nil {
		// Classify the whole token: Atoi can report overflow before reaching a
		// nonnumeric suffix in a canonical label. Only integers bypass resolution.
		if resolve == nil || ruleDXCCIntegerToken(value.Text) {
			return 0, ruleCompileError(value.Column, "expected a strictly integral value for %q: %s", value.Text, err)
		}
		number, err = resolve(value.Text)
		if err != nil {
			return 0, ruleCompileError(value.Column, "invalid DXCC value %q: %s", value.Text, err)
		}
	}
	if number <= 0 {
		return 0, ruleCompileError(value.Column, "DXCC must be a positive ADIF integer")
	}
	return number, nil
}

func ruleDXCCIntegerToken(value string) bool {
	if value != "" && (value[0] == '+' || value[0] == '-') {
		value = value[1:]
	}
	if value == "" {
		return false
	}
	for i := range len(value) {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func (p *rulePredicate) compileText(value *expr.Value) (string, error) {
	text := value.Text
	if p.field != ruleFieldComment {
		text = strings.ToUpper(text)
	}
	if p.field != ruleFieldComment && p.field != ruleFieldBand && p.field != ruleFieldMode && strings.TrimSpace(text) != text {
		return "", ruleCompileError(value.Column, "unsupported %s value %q", ruleFieldName(p.field), value.Text)
	}
	valid := false
	switch p.field {
	case ruleFieldBand:
		text = spot.NormalizeBand(text)
		valid = spot.IsValidBand(text)
	case ruleFieldMode:
		// Blank spot modes are UNKNOWN, but an empty rule operand is invalid.
		if strings.TrimSpace(text) != "" {
			text = canonicalModeToken(text)
			valid = IsSupportedMode(text)
		}
	case ruleFieldSource:
		valid = IsSupportedSource(text)
	case ruleFieldEvent:
		valid = text == "NONE" || IsSupportedEvent(text)
		if valid {
			if text == "NONE" {
				p.eventNone = true
			} else {
				p.events |= spot.EventMaskForName(text)
			}
		}
	case ruleFieldDXCall, ruleFieldDECall:
		valid = ValidCallsignPattern(text)
	case ruleFieldDXCont, ruleFieldDECont:
		valid = IsSupportedContinent(text)
	case ruleFieldDXState, ruleFieldDEState:
		valid = spot.IsState(text)
	case ruleFieldDXGrid2, ruleFieldDEGrid2:
		valid = ValidGrid2Prefix(text)
	case ruleFieldConf:
		valid = IsSupportedConfidenceSymbol(text)
	case ruleFieldPath:
		valid = IsSupportedPathClass(text)
	case ruleFieldComment:
		text = value.Text
		valid = ValidCommentPhrase(text)
	}
	if !valid {
		return "", ruleCompileError(value.Column, "unsupported %s value %q", ruleFieldName(p.field), value.Text)
	}
	text = strings.Clone(text)
	switch p.field {
	case ruleFieldDXCall, ruleFieldDECall:
		p.calls = append(p.calls, compileRuleCallPattern(text))
	case ruleFieldEvent:
	default:
		p.text = append(p.text, text)
	}
	return text, nil
}

func ruleFieldName(field ruleField) string {
	if field < ruleFieldCount {
		return ruleFieldNames[field]
	}
	return "unknown"
}

func ruleFrequencyValue(text string) (float64, bool) {
	number, err := strconv.ParseFloat(text, 64)
	// Both scalar and range canonical forms use the same nonnegative zero.
	if number == 0 {
		number = 0
	}
	return number, err == nil && number >= 0 && !math.IsNaN(number) && !math.IsInf(number, 0)
}

func compileRuleFrequency(text string, allowRange bool) (ruleFrequencyRange, error) {
	if number, valid := ruleFrequencyValue(text); valid {
		return ruleFrequencyRange{low: number, high: number}, nil
	}
	if allowRange {
		// A minus in an exponent is not the range separator. Both complete
		// sides must independently be finite, nonnegative numbers.
		for i := 1; i < len(text)-1; i++ {
			if text[i] != '-' {
				continue
			}
			low, lowOK := ruleFrequencyValue(text[:i])
			high, highOK := ruleFrequencyValue(text[i+1:])
			if lowOK && highOK && low <= high {
				return ruleFrequencyRange{low: low, high: high}, nil
			}
		}
	}
	return ruleFrequencyRange{}, fmt.Errorf("expected a finite nonnegative number%s", ruleFrequencyRangeHint(allowRange))
}

func ruleFrequencyRangeHint(allowRange bool) string {
	if allowRange {
		return " or inclusive low-high range"
	}
	return ""
}
