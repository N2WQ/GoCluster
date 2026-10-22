package expr

import "strings"

// Parse accepts printable ASCII with ASCII-space whitespace. Columns are
// 1-based source byte offsets, including for quoted values and end-of-input.
// Depth counts parentheses and NOT together, not flat AND/OR tree height.
func Parse(source string) (*Expression, error) {
	if len(source) > MaxBytes {
		return nil, syntaxError(MaxBytes+1, "expression exceeds byte limit")
	}
	for i := 0; i < len(source); i++ {
		if source[i] < ' ' || source[i] > '~' {
			return nil, syntaxError(i+1, "expected printable ASCII")
		}
	}
	p := parser{source: source}
	root, err := p.parseOr(0)
	if err != nil {
		return nil, err
	}
	p.spaces()
	if p.pos != len(source) {
		return nil, syntaxError(p.pos+1, "unexpected token after expression")
	}
	p.expression.Root = root
	// Canonical spacing can be longer than the source. Admission must enforce
	// both budgets so every accepted tree has a printable bounded form.
	if _, err := p.expression.Canonical(); err != nil {
		return nil, err
	}
	return &p.expression, nil
}

type parser struct {
	source     string
	pos        int
	expression Expression
}

func (p *parser) spaces() {
	for p.pos < len(p.source) && p.source[p.pos] == ' ' {
		p.pos++
	}
}

func (p *parser) take(b byte) bool {
	p.spaces()
	if p.pos < len(p.source) && p.source[p.pos] == b {
		p.pos++
		return true
	}
	return false
}

func (p *parser) word() string {
	p.spaces()
	end := p.pos
	for end < len(p.source) && bareByte(p.source[end]) {
		end++
	}
	return p.source[p.pos:end]
}

func (p *parser) keyword(word string) bool {
	return strings.EqualFold(p.word(), word)
}

func (p *parser) add(node Node, column int) (int, error) {
	if len(p.expression.Nodes) >= MaxNodes {
		return -1, syntaxError(column, "expression exceeds node limit")
	}
	index := len(p.expression.Nodes)
	p.expression.Nodes = append(p.expression.Nodes, node)
	return index, nil
}

func (p *parser) parseOr(depth int) (int, error) {
	left, err := p.parseAnd(depth)
	if err != nil {
		return -1, err
	}
	for p.keyword("OR") {
		column := p.pos + 1
		p.pos += 2
		right, err := p.parseAnd(depth)
		if err != nil {
			return -1, err
		}
		left, err = p.add(Node{Kind: OrKind, Left: left, Right: right}, column)
		if err != nil {
			return -1, err
		}
	}
	return left, nil
}

func (p *parser) parseAnd(depth int) (int, error) {
	left, err := p.parseTerm(depth)
	if err != nil {
		return -1, err
	}
	for p.keyword("AND") {
		column := p.pos + 1
		p.pos += 3
		right, err := p.parseTerm(depth)
		if err != nil {
			return -1, err
		}
		left, err = p.add(Node{Kind: AndKind, Left: left, Right: right}, column)
		if err != nil {
			return -1, err
		}
	}
	return left, nil
}

func (p *parser) parseTerm(depth int) (int, error) {
	p.spaces()
	column := p.pos + 1
	if p.keyword("NOT") {
		if depth >= MaxDepth {
			return -1, syntaxError(column, "expression exceeds nesting limit")
		}
		p.pos += 3
		child, err := p.parseTerm(depth + 1)
		if err != nil {
			return -1, err
		}
		return p.add(Node{Kind: NotKind, Left: child, Right: -1}, column)
	}
	if p.take('(') {
		if depth >= MaxDepth {
			return -1, syntaxError(column, "expression exceeds nesting limit")
		}
		child, err := p.parseOr(depth + 1)
		if err != nil {
			return -1, err
		}
		if !p.take(')') {
			return -1, syntaxError(p.pos+1, "expected closing parenthesis")
		}
		return child, nil
	}
	return p.parseAtom()
}

func (p *parser) parseAtom() (int, error) {
	field := p.word()
	column := p.pos + 1
	if field == "" || strings.EqualFold(field, "AND") || strings.EqualFold(field, "OR") {
		return -1, syntaxError(column, "expected field or Boolean term")
	}
	if p.expression.Atoms >= MaxAtoms {
		return -1, syntaxError(column, "expression exceeds atom limit")
	}
	p.pos += len(field)
	op, err := p.operator()
	if err != nil {
		return -1, err
	}
	values, err := p.values()
	if err != nil {
		return -1, err
	}
	index, err := p.add(Node{
		Kind: AtomKind, Left: -1, Right: -1,
		Atom: Atom{Field: strings.ToUpper(field), Operator: op, Values: values, Column: column},
	}, column)
	if err != nil {
		return -1, err
	}
	p.expression.Atoms++
	return index, nil
}

func (p *parser) operator() (Operator, error) {
	p.spaces()
	start := p.pos
	if start < len(p.source) {
		switch p.source[p.pos] {
		case '=':
			p.pos++
		case '<', '>':
			p.pos++
			if p.pos < len(p.source) &&
				(p.source[p.pos] == '=' || p.source[start] == '<' && p.source[p.pos] == '>') {
				p.pos++
			}
		}
	}
	if start == p.pos {
		return "", syntaxError(start+1, "expected comparison operator")
	}
	return Operator(p.source[start:p.pos]), nil
}

func (p *parser) values() ([]Value, error) {
	bracketed := p.take('[')
	var values []Value
	for {
		p.spaces()
		if len(values) >= MaxValuesPerAtom {
			return nil, syntaxError(p.pos+1, "atom exceeds value limit")
		}
		value, err := p.value()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		if !p.take(',') {
			break
		}
	}
	if bracketed && !p.take(']') {
		return nil, syntaxError(p.pos+1, "expected comma or closing bracket")
	}
	return values, nil
}

func (p *parser) value() (Value, error) {
	p.spaces()
	column := p.pos + 1
	if p.take('"') {
		var text strings.Builder
		for p.pos < len(p.source) {
			b := p.source[p.pos]
			p.pos++
			if b != '"' {
				text.WriteByte(b)
			} else if p.pos < len(p.source) && p.source[p.pos] == '"' {
				text.WriteByte('"')
				p.pos++
			} else {
				return Value{Text: text.String(), Quoted: true, Column: column}, nil
			}
		}
		return Value{}, syntaxError(column, "unterminated quoted value")
	}
	text := p.word()
	if text == "" {
		return Value{}, syntaxError(column, "expected value")
	}
	p.pos += len(text)
	return Value{Text: strings.ToUpper(text), Column: column}, nil
}
