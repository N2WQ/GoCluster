package filter

import (
	"fmt"

	"dxcluster/filter/expr"
)

func prepareRule(definition RuleDefinition, resolve func(string) (int, error)) (preparedRule, error) {
	normal, err := compileRuleWithDXCCResolver(definition.Expr, resolve)
	if err != nil {
		return preparedRule{}, fmt.Errorf("rule slot %d: %w", definition.Slot, err)
	}
	nearby, err := projectNearbyRule(normal)
	if err != nil {
		return preparedRule{}, fmt.Errorf("rule slot %d NEARBY projection: %w", definition.Slot, err)
	}
	definition.Expr = normal.canonical
	// Detach actions too: a valid short action may be a larger upload substring.
	if definition.Action == RulePass {
		definition.Action = RulePass
	} else {
		definition.Action = RuleReject
	}
	// Only preparation needs syntax. Retaining it would duplicate the bounded
	// node vectors and operands for the entire lifetime of every saved rule.
	normal.syntax = nil
	return preparedRule{definition: definition, normal: normal, nearby: nearby}, nil
}

func projectNearbyRule(normal *compiledRule) (*compiledRule, error) {
	geographic := 0
	for i := range normal.nodes {
		node := &normal.nodes[i]
		if node.kind == expr.AtomKind && isGeographicField(node.predicate.field) {
			geographic++
		}
	}
	if geographic == 0 {
		return normal, nil
	}
	if geographic == normal.atomCount {
		return nil, nil //nolint:nilnil // A geography-only rule is suspended under NEARBY.
	}
	projection := ruleProjection{source: normal}
	projection.syntax.Nodes = make([]expr.Node, 0, len(normal.nodes))
	projection.syntax.Root = projection.project(normal.root)
	canonical, err := projection.syntax.Canonical()
	if err != nil {
		return nil, err
	}
	program := &compiledRule{
		nodes: make([]compiledRuleNode, len(projection.syntax.Nodes)), root: projection.syntax.Root,
		canonical: canonical, atomCount: projection.syntax.Atoms,
	}
	for i := range program.nodes {
		node := &projection.syntax.Nodes[i]
		// Typed predicate slices are immutable and shared with the full program;
		// only the reachable node vector is copied, with new child indexes.
		compiled := normal.nodes[projection.originals[i]]
		compiled.left, compiled.right = node.Left, node.Right
		program.nodes[i] = compiled
		if node.Kind == expr.AtomKind {
			program.valueCount += len(node.Atom.Values)
			program.needsPath = program.needsPath || compiled.predicate.field == ruleFieldPath
		}
	}
	return program, nil
}

type ruleProjection struct {
	source    *compiledRule
	syntax    expr.Expression
	originals [expr.MaxNodes]int
}

// project removes geography structurally, not by substituting a truth value.
// Appending only retained children and surviving operators produces a tree with
// no unreachable nodes, including when an entire NOT/AND/OR branch disappears.
func (p *ruleProjection) project(index int) int {
	node := p.source.syntax.Nodes[index]
	if node.Kind == expr.AtomKind {
		if isGeographicField(p.source.nodes[index].predicate.field) {
			return -1
		}
		p.syntax.Atoms++
	} else {
		node.Left = p.project(node.Left)
		if node.Kind == expr.NotKind {
			if node.Left < 0 {
				return -1
			}
		} else {
			node.Right = p.project(node.Right)
			if node.Left < 0 {
				return node.Right
			}
			if node.Right < 0 {
				return node.Left
			}
		}
	}
	result := len(p.syntax.Nodes)
	p.originals[result] = index
	p.syntax.Nodes = append(p.syntax.Nodes, node)
	return result
}
