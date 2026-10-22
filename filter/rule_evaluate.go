package filter

import "dxcluster/filter/expr"

// evaluate consumes only prepared nodes/operands. Boolean operators retain Go's
// left-to-right short circuit, including around unknown comparisons and COMMENT.
// root is explicit so the later planner can retain syntax-indexed projections.
func (p *compiledRule) evaluate(view *ruleView, root int) bool {
	node := &p.nodes[root]
	switch node.kind {
	case expr.AtomKind:
		return node.predicate.matches(view)
	case expr.NotKind:
		return !p.evaluate(view, node.left)
	case expr.AndKind:
		return p.evaluate(view, node.left) && p.evaluate(view, node.right)
	case expr.OrKind:
		return p.evaluate(view, node.left) || p.evaluate(view, node.right)
	default:
		panic("filter: invalid compiled rule node")
	}
}

func (p *rulePredicate) matches(view *ruleView) bool {
	switch p.field {
	case ruleFieldSNR, ruleFieldDXZone, ruleFieldDEZone, ruleFieldDXDXCC, ruleFieldDEDXCC:
		number, known := view.integerValue(p.field)
		if !known {
			return false
		}
		for _, operand := range p.integers {
			if ruleCompareInt(number, operand, p.operator) {
				return p.operator != expr.NotEqual
			}
		}
	case ruleFieldFreq:
		frequency, known := view.frequencyValue()
		if !known {
			return false
		}
		for _, operand := range p.frequencies {
			if ruleCompareFrequency(frequency, operand, p.operator) {
				return p.operator != expr.NotEqual
			}
		}
	case ruleFieldEvent:
		if view.spot == nil {
			return false
		}
		matched := view.spot.Events&p.events != 0 || p.eventNone && view.spot.Events == 0
		if p.operator == expr.NotEqual {
			return !matched
		}
		return matched
	default:
		value := view.textValue(p.field)
		if value == "" {
			// Unknown fails every comparison, including inequality. NOT is
			// ordinary Boolean negation in the surrounding expression.
			return false
		}
		switch p.field {
		case ruleFieldDXCall, ruleFieldDECall:
			for _, operand := range p.calls {
				if operand.matches(value) {
					return p.operator != expr.NotEqual
				}
			}
		case ruleFieldComment:
			for _, operand := range p.text {
				if MatchCommentPhrase(value, operand) {
					return p.operator != expr.NotEqual
				}
			}
		default:
			for _, operand := range p.text {
				if value == operand {
					return p.operator != expr.NotEqual
				}
			}
		}
	}
	return p.operator == expr.NotEqual
}

// For <> a match means an excluded equal operand was found. Inverting happens
// once after the list scan, never by OR-ing independent not-equal comparisons.
func ruleCompareInt(value, operand int, operator expr.Operator) bool {
	switch operator {
	case expr.Equal, expr.NotEqual:
		return value == operand
	case expr.Less:
		return value < operand
	case expr.LessEqual:
		return value <= operand
	case expr.Greater:
		return value > operand
	case expr.GreaterEqual:
		return value >= operand
	default:
		return false
	}
}

func ruleCompareFrequency(value float64, operand ruleFrequencyRange, operator expr.Operator) bool {
	switch operator {
	case expr.Equal, expr.NotEqual:
		return value >= operand.low && value <= operand.high
	case expr.Less:
		return value < operand.low
	case expr.LessEqual:
		return value <= operand.low
	case expr.Greater:
		return value > operand.low
	case expr.GreaterEqual:
		return value >= operand.low
	default:
		return false
	}
}
