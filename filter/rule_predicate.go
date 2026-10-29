// File role: Internal typed rule operands, independent of active Filter matching.
package filter

import (
	"strings"

	"dxcluster/filter/expr"
	"dxcluster/spot"
)

type ruleField uint8

const (
	ruleFieldBand ruleField = iota
	ruleFieldMode
	ruleFieldSource
	ruleFieldEvent
	ruleFieldDXCall
	ruleFieldDECall
	ruleFieldDXCont
	ruleFieldDECont
	ruleFieldDXZone
	ruleFieldDEZone
	ruleFieldDXDXCC
	ruleFieldDEDXCC
	ruleFieldDXState
	ruleFieldDEState
	ruleFieldDXGrid2
	ruleFieldDEGrid2
	ruleFieldFreq
	ruleFieldSNR
	ruleFieldConf
	ruleFieldPath
	ruleFieldComment
	ruleFieldNearby
	ruleFieldCount
)

var ruleFieldNames = [ruleFieldCount]string{
	"BAND", "MODE", "SOURCE", "EVENT", "DXCALL", "DECALL",
	"DXCONT", "DECONT", "DXZONE", "DEZONE", "DXDXCC", "DEDXCC",
	"DXSTATE", "DESTATE", "DXGRID2", "DEGRID2", "FREQ", "SNR",
	"CONF", "PATH", "COMMENT", "NEARBY",
}

func ruleFieldForName(name string) (ruleField, bool) {
	for field, candidate := range ruleFieldNames {
		if candidate == name {
			return ruleField(field), true
		}
	}
	return 0, false
}

// Geographic classification is descriptive capability metadata only. NEARBY
// never removes these comparisons from a rule.
func isGeographicField(field ruleField) bool {
	switch field {
	case ruleFieldDXCont, ruleFieldDECont, ruleFieldDXZone, ruleFieldDEZone,
		ruleFieldDXDXCC, ruleFieldDEDXCC, ruleFieldDXState, ruleFieldDEState,
		ruleFieldDXGrid2, ruleFieldDEGrid2:
		return true
	default:
		return false
	}
}

type ruleFrequencyRange struct {
	low, high float64
}

type ruleCallKind uint8

const (
	ruleCallExact ruleCallKind = iota
	ruleCallPrefix
	ruleCallSuffix
)

type ruleCallPattern struct {
	kind ruleCallKind
	text string
}

func compileRuleCallPattern(text string) ruleCallPattern {
	if strings.HasSuffix(text, "*") {
		return ruleCallPattern{kind: ruleCallPrefix, text: text[:len(text)-1]}
	}
	if strings.HasPrefix(text, "*") {
		return ruleCallPattern{kind: ruleCallSuffix, text: text[1:]}
	}
	return ruleCallPattern{kind: ruleCallExact, text: text}
}

func (p ruleCallPattern) matches(call string) bool {
	switch p.kind {
	case ruleCallPrefix:
		return strings.HasPrefix(call, p.text)
	case ruleCallSuffix:
		return strings.HasSuffix(call, p.text)
	default:
		return call == p.text
	}
}

// Operands are owned by one bounded compiled program and immutable after compile.
// Only the slice corresponding to the field is populated; evaluation never
// parses operands or constructs per-leaf interfaces/callbacks.
type rulePredicate struct {
	field       ruleField
	operator    expr.Operator
	text        []string
	integers    []int
	frequencies []ruleFrequencyRange
	calls       []ruleCallPattern
	events      spot.EventMask
	eventNone   bool
}
