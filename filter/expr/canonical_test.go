package expr

import (
	"strings"
	"testing"
)

func TestCanonical(t *testing.T) {
	for _, tt := range []struct {
		source string
		want   string
	}{
		{" ( snr >= [ -12, 1 ] ) ", "SNR>=-12,1"},
		{"a=1 or b=2 and not c=3", "A=1 OR B=2 AND NOT C=3"},
		{"(a=1 or b=2) and c=3", "(A=1 OR B=2) AND C=3"},
		{"not(a=1 or b=2)", "NOT (A=1 OR B=2)"},
		{"not(a=1 and b=2)", "NOT (A=1 AND B=2)"},
		{"not(not(a=1))", "NOT NOT A=1"},
		{"a=1 and (b=2 and c=3)", "A=1 AND B=2 AND C=3"},
		{"a=1 or (b=2 or c=3)", "A=1 OR B=2 OR C=3"},
		{"a=1 or (b=2 and c=3)", "A=1 OR B=2 AND C=3"},
		{"((a=1))", "A=1"},
		{`comment="MiXeD ""quote"" ; != [] , ()"`, `COMMENT="MiXeD ""quote"" ; != [] , ()"`},
		{`comment=["","""","  two spaces  "]`, `COMMENT="","""","  two spaces  "`},
		{"x=and,or,not AND y=not OR z=AND", "X=AND,OR,NOT AND Y=NOT OR Z=AND"},
		{`x="AND"OR(y=OR)`, `X="AND" OR Y=OR`},
	} {
		t.Run(tt.source, func(t *testing.T) {
			e := mustParse(t, tt.source)
			got, err := e.Canonical()
			if err != nil || got != tt.want {
				t.Fatalf("got %q, %v; want %q", got, err, tt.want)
			}
			reparsed := mustParse(t, got)
			stable, err := reparsed.Canonical()
			if err != nil || stable != got {
				t.Fatalf("unstable canonical %q, %v", stable, err)
			}
		})
	}
}

func TestCanonicalNormalizedAtoms(t *testing.T) {
	e := mustParse(t, `mode=cw AND comment="original"`)
	e.Nodes[0].Atom.Field = "taxonomy"
	e.Nodes[0].Atom.Values[0].Text = "and"
	e.Nodes[1].Atom.Values[0].Text = `New "Case"`
	got, err := e.Canonical()
	if err != nil || got != `TAXONOMY=AND AND COMMENT="New ""Case"""` {
		t.Fatalf("normalized canonical: %q, %v", got, err)
	}
	if e.Nodes[0].Atom.Field != "taxonomy" || e.Nodes[0].Atom.Values[0].Text != "and" {
		t.Fatal("Canonical mutated caller data")
	}
	mustParse(t, got)
}

func TestCanonicalMalformedTree(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*Expression)
	}{
		{"empty", func(e *Expression) { e.Nodes = nil }},
		{"root_negative", func(e *Expression) { e.Root = -1 }},
		{"root_out_of_bounds", func(e *Expression) { e.Root = len(e.Nodes) }},
		{"left_negative", func(e *Expression) { e.Nodes[e.Root].Left = -1 }},
		{"right_out_of_bounds", func(e *Expression) { e.Nodes[e.Root].Right = len(e.Nodes) }},
		{"cycle", func(e *Expression) { e.Nodes[e.Root].Left = e.Root }},
		{"shared_child", func(e *Expression) { e.Nodes[e.Root].Right = e.Nodes[e.Root].Left }},
		{"unknown_kind", func(e *Expression) { e.Nodes[e.Root].Kind = Kind(255) }},
		{"atom_child", func(e *Expression) { e.Nodes[0].Left = 0 }},
		{"not_extra_child", func(e *Expression) { e.Nodes[e.Root].Kind = NotKind }},
		{"unreachable", func(e *Expression) { e.Root = 0 }},
		{"atom_count_zero", func(e *Expression) { e.Atoms = 0 }},
		{"atom_count_negative", func(e *Expression) { e.Atoms = -1 }},
		{"atom_count_mismatch", func(e *Expression) { e.Atoms = 1 }},
		{"atom_count_overflow", func(e *Expression) { e.Atoms = MaxAtoms + 1 }},
		{"node_count_overflow", func(e *Expression) { e.Nodes = make([]Node, MaxNodes+1) }},
		{"empty_field", func(e *Expression) { e.Nodes[0].Atom.Field = "" }},
		{"keyword_field", func(e *Expression) { e.Nodes[0].Atom.Field = "not" }},
		{"invalid_field", func(e *Expression) { e.Nodes[0].Atom.Field = "A B" }},
		{"non_ascii_field", func(e *Expression) { e.Nodes[0].Atom.Field = "é" }},
		{"invalid_operator", func(e *Expression) { e.Nodes[0].Atom.Operator = "!=" }},
		{"empty_values", func(e *Expression) { e.Nodes[0].Atom.Values = nil }},
		{"many_values", func(e *Expression) { e.Nodes[0].Atom.Values = make([]Value, MaxValuesPerAtom+1) }},
		{"empty_bare", func(e *Expression) { e.Nodes[0].Atom.Values[0].Text = "" }},
		{"invalid_bare", func(e *Expression) { e.Nodes[0].Atom.Values[0].Text = "x,y" }},
		{"quoted_control", func(e *Expression) { e.Nodes[0].Atom.Values[0] = Value{Text: "\t", Quoted: true} }},
		{"quoted_non_ascii", func(e *Expression) { e.Nodes[0].Atom.Values[0] = Value{Text: "é", Quoted: true} }},
		{"long_field", func(e *Expression) { e.Nodes[0].Atom.Field = strings.Repeat("A", MaxBytes+1) }},
		{"long_value", func(e *Expression) { e.Nodes[0].Atom.Values[0].Text = strings.Repeat("1", MaxBytes+1) }},
		{"canonical_overflow", func(e *Expression) { e.Nodes[0].Atom.Values[0].Text = strings.Repeat("1", MaxBytes) }},
		{"escaped_overflow", func(e *Expression) {
			e.Nodes[0].Atom.Values[0] = Value{Text: strings.Repeat("\"", MaxBytes/2), Quoted: true}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			e := mustParse(t, "A=1 AND B=2")
			tt.mutate(e)
			if got, err := e.Canonical(); err == nil || got != "" {
				t.Fatalf("malformed tree printed %q, error=%v", got, err)
			}
		})
	}
	var nilExpression *Expression
	if _, err := nilExpression.Canonical(); err == nil {
		t.Fatal("nil tree accepted")
	}
}

func TestCanonicalDepthBudget(t *testing.T) {
	e := mustParse(t, strings.Repeat("NOT ", MaxDepth)+"A=1")
	e.Nodes = append(e.Nodes, Node{Kind: NotKind, Left: e.Root, Right: -1})
	e.Root = len(e.Nodes) - 1
	if _, err := e.Canonical(); err == nil || !strings.Contains(err.Error(), "nesting limit") {
		t.Fatalf("nine NOT levels: %v", err)
	}

	// The child OR requires parentheses under NOT: seven NOTs + one pair fits,
	// but adding an eighth NOT must count both kinds of nesting together.
	e = mustParse(t, strings.Repeat("NOT ", MaxDepth-1)+"(A=1 OR B=2)")
	e.Nodes = append(e.Nodes, Node{Kind: NotKind, Left: e.Root, Right: -1})
	e.Root = len(e.Nodes) - 1
	if _, err := e.Canonical(); err == nil || !strings.Contains(err.Error(), "nesting limit") {
		t.Fatalf("NOT and required parentheses: %v", err)
	}
}
