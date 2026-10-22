// Package expr parses and prints bounded Boolean syntax. It does not validate
// domain fields, interpret values, or evaluate expressions.
package expr

import "fmt"

const (
	MaxBytes         = 1024
	MaxAtoms         = 32
	MaxNodes         = 128
	MaxDepth         = 8
	MaxValuesPerAtom = 32
)

type Kind uint8

const (
	AtomKind Kind = iota
	NotKind
	AndKind
	OrKind
)

type Operator string

const (
	Equal        Operator = "="
	NotEqual     Operator = "<>"
	Less         Operator = "<"
	LessEqual    Operator = "<="
	Greater      Operator = ">"
	GreaterEqual Operator = ">="
)

type Value struct {
	Text   string
	Quoted bool
	Column int
}

type Atom struct {
	Field    string
	Operator Operator
	Values   []Value
	Column   int
}

// Node children index Expression.Nodes. Unused children are -1; NOT uses Left.
type Node struct {
	Kind        Kind
	Left, Right int
	Atom        Atom
}

// Expression owns its nodes and value slices. Parse retains no caller-owned
// mutable storage. Callers may normalize atoms before freezing the expression;
// neither Parse nor Canonical uses shared state.
type Expression struct {
	Nodes []Node
	Root  int
	Atoms int
}

type SyntaxError struct {
	Column  int
	Message string
}

func (e *SyntaxError) Error() string {
	return fmt.Sprintf("col %d: %s", e.Column, e.Message)
}

func syntaxError(column int, message string) error {
	if column < 1 {
		column = 1
	}
	return &SyntaxError{Column: column, Message: message}
}

func bareByte(b byte) bool {
	return b >= 'A' && b <= 'Z' || b >= 'a' && b <= 'z' ||
		b >= '0' && b <= '9' || b == '_' || b == '-' || b == '.' ||
		b == '/' || b == '*' || b == '?' || b == '#' || b == '@'
}

func validOperator(op Operator) bool {
	switch op {
	case Equal, NotEqual, Less, LessEqual, Greater, GreaterEqual:
		return true
	default:
		return false
	}
}
