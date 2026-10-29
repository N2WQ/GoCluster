package expr

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func mustParse(t *testing.T, source string) *Expression {
	t.Helper()
	e, err := Parse(source)
	if err != nil {
		t.Fatalf("Parse(%q): %v", source, err)
	}
	return e
}

func TestParseTree(t *testing.T) {
	e := mustParse(t, "a=1 OR b=2 AND NOT c=3")
	if e.Root != 5 || e.Atoms != 3 || len(e.Nodes) != 6 {
		t.Fatalf("unexpected tree: %+v", e)
	}
	wantKinds := []Kind{AtomKind, AtomKind, AtomKind, NotKind, AndKind, OrKind}
	wantChildren := [][2]int{{-1, -1}, {-1, -1}, {-1, -1}, {2, -1}, {1, 3}, {0, 4}}
	for i, node := range e.Nodes {
		if node.Kind != wantKinds[i] || node.Left != wantChildren[i][0] || node.Right != wantChildren[i][1] {
			t.Fatalf("node %d: %+v", i, node)
		}
	}
	for i, want := range []struct {
		field        string
		fieldColumn  int
		valueColumn  int
		valueContent string
	}{{"A", 1, 3, "1"}, {"B", 8, 10, "2"}, {"C", 20, 22, "3"}} {
		atom := e.Nodes[i].Atom
		if atom.Field != want.field || atom.Column != want.fieldColumn || atom.Operator != Equal ||
			len(atom.Values) != 1 || atom.Values[0].Column != want.valueColumn ||
			atom.Values[0].Text != want.valueContent || atom.Values[0].Quoted {
			t.Fatalf("atom %d: %+v", i, atom)
		}
	}

	chain := mustParse(t, "A=1 AND B=2 AND C=3")
	root := chain.Nodes[chain.Root]
	if root.Kind != AndKind || root.Left != 2 || root.Right != 3 ||
		chain.Nodes[root.Left].Kind != AndKind || chain.Nodes[root.Left].Left != 0 ||
		chain.Nodes[root.Left].Right != 1 {
		t.Fatalf("flat chain is not left associative: %+v", chain)
	}
}

// These truth masks are fixed from Boolean truth tables, not from the printer.
// Bit i assigns A=i&1, B=i&2, C=i&4.
func TestBooleanTruth(t *testing.T) {
	for _, tt := range []struct {
		source string
		mask   uint8
	}{
		{"A=1 OR B=1 AND C=1", 0xea},
		{"(A=1 OR B=1) AND C=1", 0xe0},
		{"NOT A=1 AND B=1 OR C=1", 0xf4},
		{"NOT (A=1 OR B=1) AND C=1", 0x10},
		{"NOT A=1 OR B=1 AND NOT C=1", 0x5d},
		{"NOT (A=1 AND (B=1 OR C=1))", 0x57},
		{"NOT NOT A=1 OR B=1 AND C=1", 0xea},
	} {
		t.Run(tt.source, func(t *testing.T) {
			e := mustParse(t, tt.source)
			canonical, err := e.Canonical()
			if err != nil {
				t.Fatal(err)
			}
			reparsed := mustParse(t, canonical)
			for i := 0; i < 8; i++ {
				values := map[string]bool{"A": i&1 != 0, "B": i&2 != 0, "C": i&4 != 0}
				want := tt.mask&(1<<i) != 0
				if got := interpret(t, e, e.Root, values); got != want {
					t.Fatalf("original assignment %d: got %v, want %v", i, got, want)
				}
				if got := interpret(t, reparsed, reparsed.Root, values); got != want {
					t.Fatalf("canonical %q assignment %d: got %v, want %v", canonical, i, got, want)
				}
			}
		})
	}
}

func interpret(t *testing.T, e *Expression, index int, values map[string]bool) bool {
	t.Helper()
	node := e.Nodes[index]
	switch node.Kind {
	case AtomKind, PredicateKind:
		value, ok := values[node.Atom.Field]
		if !ok {
			t.Fatalf("unassigned test field %q", node.Atom.Field)
		}
		return value
	case NotKind:
		return !interpret(t, e, node.Left, values)
	case AndKind:
		return interpret(t, e, node.Left, values) && interpret(t, e, node.Right, values)
	case OrKind:
		return interpret(t, e, node.Left, values) || interpret(t, e, node.Right, values)
	default:
		t.Fatalf("unknown test node kind %d", node.Kind)
		return false
	}
}

func TestParseValues(t *testing.T) {
	e := mustParse(t, ` x = [ and,OR,not,-12,7000-7040,k1abc/p,ab_1.*?#@,"MiXeD ""quote"", x[]!","" ] `)
	atom := e.Nodes[e.Root].Atom
	want := []string{"AND", "OR", "NOT", "-12", "7000-7040", "K1ABC/P", "AB_1.*?#@", `MiXeD "quote", x[]!`, ""}
	if atom.Field != "X" || atom.Column != 2 || len(atom.Values) != len(want) {
		t.Fatalf("unexpected atom: %+v", atom)
	}
	for i, text := range want {
		if atom.Values[i].Text != text || atom.Values[i].Quoted != (i >= 7) {
			t.Fatalf("value %d: %+v, want %q", i, atom.Values[i], text)
		}
	}
	quoted := mustParse(t, `  comment = "  CQ ""DX""  "`)
	value := quoted.Nodes[quoted.Root].Atom.Values[0]
	if value.Column != 13 || value.Text != `  CQ "DX"  ` || !value.Quoted {
		t.Fatalf("quoted value: %+v", value)
	}
	for _, op := range []Operator{Equal, NotEqual, Less, LessEqual, Greater, GreaterEqual} {
		t.Run(string(op), func(t *testing.T) {
			e := mustParse(t, "snr"+string(op)+"-12")
			if e.Nodes[e.Root].Atom.Operator != op {
				t.Fatalf("comparison operator: %+v", e.Nodes[e.Root].Atom)
			}
		})
	}
	// Names remain syntax, not domain aliases or a built-in field vocabulary.
	if got := mustParse(t, "ar=1").Nodes[0].Atom.Field; got != "AR" {
		t.Fatalf("field was rewritten: %q", got)
	}
}

func TestParseErrorColumns(t *testing.T) {
	for _, tt := range []struct {
		source string
		column int
	}{
		{"", 1}, {"   ", 4}, {"SKIMMER", 8}, {"HUMAN", 6},
		{"A", 2}, {"A 1", 3}, {"A==1", 3}, {"A!=1", 2},
		{"A=>1", 3}, {"A><1", 3}, {"A=!1", 3}, {"A=", 3},
		{"A=[]", 4}, {"A=[1,]", 6}, {"A=1,", 5}, {"A=,1", 3},
		{"A=[,1]", 4}, {"A=[1", 5}, {"A=[1 2]", 6}, {"A=[1],2", 6},
		{"A=[1,2]]", 8}, {"A=1 B=2", 5}, {"A=1 AND", 8},
		{"AND A=1", 1}, {"OR A=1", 1}, {"NOT", 4}, {"()", 2},
		{"(A=1", 5}, {"A=1)", 4}, {"A=1 OR )", 8}, {`A="abc`, 3},
		{"A=\t1", 3}, {"A=é", 3}, {"A=1\n", 4}, {"A={x}", 3},
		{"A='x'", 3}, {"A=1 XOR B=2", 5}, {"A=1 NOT B=2", 5},
		{`A="x""`, 3}, {"A=\x7f", 3}, {"A=\x00", 3},
	} {
		t.Run(fmt.Sprintf("%q", tt.source), func(t *testing.T) {
			e, err := Parse(tt.source)
			var syntax *SyntaxError
			if e != nil || !errors.As(err, &syntax) || syntax.Column != tt.column {
				t.Fatalf("got tree=%v error=%v, want syntax column %d", e, err, tt.column)
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("col %d:", tt.column)) {
				t.Fatalf("error does not report its column: %v", err)
			}
		})
	}
}

func TestParseLimits(t *testing.T) {
	t.Run("bytes", func(t *testing.T) {
		mustParse(t, strings.Repeat(" ", MaxBytes-3)+"A=1")
		maxCanonical := `A="` + strings.Repeat("x", MaxBytes-4) + `"`
		e := mustParse(t, maxCanonical)
		if got, err := e.Canonical(); err != nil || got != maxCanonical {
			t.Fatalf("exact canonical bound: length=%d, error=%v", len(got), err)
		}
		rawOverflow := maxCanonical + " "
		if _, err := Parse(rawOverflow); err == nil {
			t.Fatal("accepted overlong raw source")
		} else {
			var syntax *SyntaxError
			if !errors.As(err, &syntax) || syntax.Column != MaxBytes+1 {
				t.Fatalf("raw overflow column: %v", err)
			}
		}
		// A quote delimits OR without a source space; canonical adds that space.
		canonicalOverflow := `A="` + strings.Repeat("x", MaxBytes-10) + `"OR B=1`
		if len(canonicalOverflow) != MaxBytes {
			t.Fatalf("bad fixture length %d", len(canonicalOverflow))
		}
		if _, err := Parse(canonicalOverflow); err == nil || !strings.Contains(err.Error(), "canonical") {
			t.Fatalf("accepted overlong canonical source: %v", err)
		}
	})
	t.Run("atoms", func(t *testing.T) {
		source := strings.TrimSuffix(strings.Repeat("SNR=1 AND ", MaxAtoms), " AND ")
		e := mustParse(t, source)
		if e.Atoms != MaxAtoms || len(e.Nodes) != 2*MaxAtoms-1 {
			t.Fatalf("atom bound: %+v", e)
		}
		if _, err := Parse(source + " AND SNR=1"); err == nil || !strings.Contains(err.Error(), "atom limit") {
			t.Fatalf("33 atoms: %v", err)
		}
	})
	t.Run("nodes", func(t *testing.T) {
		terms := make([]string, MaxAtoms)
		for i := range terms {
			terms[i] = "NOT NOT SNR=1"
		}
		e := mustParse(t, strings.Join(terms, " AND "))
		if len(e.Nodes) != MaxNodes-1 {
			t.Fatalf("127 nodes: %d", len(e.Nodes))
		}
		terms[0] = "NOT " + terms[0]
		e = mustParse(t, strings.Join(terms, " AND "))
		if len(e.Nodes) != MaxNodes {
			t.Fatalf("128 nodes: %d", len(e.Nodes))
		}
		terms[0] = "NOT " + terms[0]
		if _, err := Parse(strings.Join(terms, " AND ")); err == nil || !strings.Contains(err.Error(), "node limit") {
			t.Fatalf("129 nodes: %v", err)
		}
	})
	t.Run("depth", func(t *testing.T) {
		for _, shape := range []struct {
			parens int
			nots   int
		}{{8, 0}, {0, 8}, {4, 4}} {
			source := strings.Repeat("(", shape.parens) + strings.Repeat("NOT ", shape.nots) +
				"SNR=1" + strings.Repeat(")", shape.parens)
			mustParse(t, source)
			if _, err := Parse("NOT " + source); err == nil || !strings.Contains(err.Error(), "nesting limit") {
				t.Fatalf("nine combined levels (%+v): %v", shape, err)
			}
		}
	})
	t.Run("values", func(t *testing.T) {
		values := strings.TrimSuffix(strings.Repeat("1,", MaxValuesPerAtom), ",")
		for _, bracketed := range []bool{false, true} {
			list := values
			over := values + ",1"
			if bracketed {
				list, over = "["+list+"]", "["+over+"]"
			}
			e := mustParse(t, "A="+list)
			if len(e.Nodes[0].Atom.Values) != MaxValuesPerAtom {
				t.Fatal("wrong list count")
			}
			if _, err := Parse("A=" + over); err == nil || !strings.Contains(err.Error(), "value limit") {
				t.Fatalf("33 values (bracketed=%v): %v", bracketed, err)
			}
		}
	})
}
