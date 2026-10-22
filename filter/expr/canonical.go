package expr

import "strings"

// Canonical prints minimal Boolean parentheses and bare comma lists, preserving
// quoted contents. It validates caller-modified atoms and tree structure without
// mutating them. Shared children, cycles, unused nodes, and invalid budgets are
// rejected; callers must not mutate the expression concurrently.
func (e *Expression) Canonical() (string, error) {
	if e == nil || len(e.Nodes) == 0 {
		return "", syntaxError(1, "empty expression tree")
	}
	if len(e.Nodes) > MaxNodes {
		return "", syntaxError(1, "expression exceeds node limit")
	}
	if e.Atoms < 1 || e.Atoms > MaxAtoms {
		return "", syntaxError(1, "invalid expression atom count")
	}
	p := printer{expression: e}
	if err := p.node(e.Root, 0, 0); err != nil {
		return "", err
	}
	if p.visited != len(e.Nodes) {
		return "", syntaxError(1, "expression has unreachable nodes")
	}
	if p.atoms != e.Atoms {
		return "", syntaxError(1, "expression atom count does not match tree")
	}
	return p.output.String(), nil
}

type printer struct {
	expression *Expression
	output     strings.Builder
	seen       [MaxNodes]bool
	visited    int
	atoms      int
}

func precedence(kind Kind) int {
	switch kind {
	case AtomKind:
		return 4
	case NotKind:
		return 3
	case AndKind:
		return 2
	case OrKind:
		return 1
	default:
		return 0
	}
}

func (p *printer) write(text string, column int) error {
	if len(text) > MaxBytes-p.output.Len() {
		return syntaxError(column, "canonical expression exceeds byte limit")
	}
	p.output.WriteString(text)
	return nil
}

func (p *printer) node(index, parentPrecedence, depth int) error {
	if index < 0 || index >= len(p.expression.Nodes) {
		return syntaxError(1, "child or root index out of bounds")
	}
	n := &p.expression.Nodes[index]
	if p.seen[index] {
		return syntaxError(n.Atom.Column, "node is reused or forms a cycle")
	}
	p.seen[index] = true
	p.visited++
	prec := precedence(n.Kind)
	if prec == 0 {
		return syntaxError(n.Atom.Column, "invalid node kind")
	}
	wrap := prec < parentPrecedence
	if wrap {
		depth++
	}
	if depth > MaxDepth {
		return syntaxError(n.Atom.Column, "expression exceeds nesting limit")
	}
	if wrap {
		if err := p.write("(", n.Atom.Column); err != nil {
			return err
		}
	}
	if err := p.body(n, prec, depth); err != nil {
		return err
	}
	if wrap {
		return p.write(")", n.Atom.Column)
	}
	return nil
}

func (p *printer) body(n *Node, prec, depth int) error {
	switch n.Kind {
	case AtomKind:
		if n.Left != -1 || n.Right != -1 {
			return syntaxError(n.Atom.Column, "atom must not have children")
		}
		p.atoms++
		if p.atoms > MaxAtoms {
			return syntaxError(n.Atom.Column, "expression exceeds atom limit")
		}
		return p.atom(&n.Atom)
	case NotKind:
		if n.Right != -1 {
			return syntaxError(1, "NOT must have exactly one left child")
		}
		if depth >= MaxDepth {
			return syntaxError(1, "expression exceeds nesting limit")
		}
		if err := p.write("NOT ", 1); err != nil {
			return err
		}
		return p.node(n.Left, prec, depth+1)
	default:
		if err := p.node(n.Left, prec, depth); err != nil {
			return err
		}
		op := " AND "
		if n.Kind == OrKind {
			op = " OR "
		}
		if err := p.write(op, 1); err != nil {
			return err
		}
		return p.node(n.Right, prec, depth)
	}
}

func checkText(text string, quoted bool, column int) error {
	if len(text) > MaxBytes {
		return syntaxError(column, "field or value exceeds byte limit")
	}
	if !quoted && text == "" {
		return syntaxError(column, "empty field or unquoted value")
	}
	for i := 0; i < len(text); i++ {
		if text[i] < ' ' || text[i] > '~' || !quoted && !bareByte(text[i]) {
			return syntaxError(column, "invalid field or value byte")
		}
	}
	return nil
}

func (p *printer) atom(atom *Atom) error {
	if err := checkText(atom.Field, false, atom.Column); err != nil {
		return err
	}
	field := strings.ToUpper(atom.Field)
	if field == "NOT" || field == "AND" || field == "OR" {
		return syntaxError(atom.Column, "Boolean keyword cannot be a field")
	}
	if !validOperator(atom.Operator) {
		return syntaxError(atom.Column, "invalid comparison operator")
	}
	if len(atom.Values) == 0 || len(atom.Values) > MaxValuesPerAtom {
		return syntaxError(atom.Column, "invalid atom value count")
	}
	if err := p.write(field, atom.Column); err != nil {
		return err
	}
	if err := p.write(string(atom.Operator), atom.Column); err != nil {
		return err
	}
	for i := range atom.Values {
		value := &atom.Values[i]
		if err := checkText(value.Text, value.Quoted, value.Column); err != nil {
			return err
		}
		if i > 0 {
			if err := p.write(",", value.Column); err != nil {
				return err
			}
		}
		if err := p.value(value); err != nil {
			return err
		}
	}
	return nil
}

func (p *printer) value(value *Value) error {
	if !value.Quoted {
		return p.write(strings.ToUpper(value.Text), value.Column)
	}
	if err := p.write("\"", value.Column); err != nil {
		return err
	}
	for i := 0; i < len(value.Text); i++ {
		text := value.Text[i : i+1]
		if value.Text[i] == '"' {
			text = "\"\""
		}
		if err := p.write(text, value.Column); err != nil {
			return err
		}
	}
	return p.write("\"", value.Column)
}
