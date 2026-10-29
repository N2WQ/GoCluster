package expr

import (
	"strings"
	"testing"
)

func TestNearbyPredicateSyntax(t *testing.T) {
	for _, tc := range []struct{ source, want string }{
		{"nearby", "NEARBY"}, {"not(nearby OR mode=cw)", "NOT (NEARBY OR MODE=CW)"},
		{`COMMENT="NEARBY" AND NeArBy`, `COMMENT="NEARBY" AND NEARBY`},
	} {
		tree := mustParse(t, tc.source)
		got, err := tree.Canonical()
		if err != nil || got != tc.want {
			t.Fatalf("canonical=%q err=%v", got, err)
		}
		for _, node := range tree.Nodes {
			if node.Kind == PredicateKind && (node.Atom.Field != "NEARBY" || node.Atom.Operator != "" || len(node.Atom.Values) != 0) {
				t.Fatal("predicate has comparison operands")
			}
		}
	}
	for _, source := range []string{"NEARBYX", "NEARBY ON", "NEARBY=TRUE", "NEARBY<>FALSE", "NEARBY[1]", "NEARBY NEARBY", "MODE", "NEARBY OR", "NEARBY=", "NEARBY(NEARBY)"} {
		if tree, err := Parse(source); err == nil || tree != nil {
			t.Fatalf("malformed predicate accepted: %q", source)
		}
	}
	for _, mutate := range []func(*Node){
		func(n *Node) { n.Atom.Field = "UNKNOWN" }, func(n *Node) { n.Atom.Operator = Equal },
		func(n *Node) { n.Atom.Values = []Value{{Text: "TRUE"}} }, func(n *Node) { n.Left = 0 },
		func(n *Node) { n.Kind = AtomKind },
	} {
		tree := mustParse(t, "NEARBY")
		mutate(&tree.Nodes[0])
		if _, err := tree.Canonical(); err == nil {
			t.Fatal("malformed typed predicate printed")
		}
	}
}

func TestNearbyPredicateLimits(t *testing.T) {
	atoms := strings.Repeat("NEARBY OR ", MaxAtoms-1) + "NEARBY"
	nodes := strings.Repeat("NOT NOT NEARBY OR ", 31) + "NOT NOT NOT NEARBY"
	for _, source := range []string{atoms, nodes, strings.Repeat("NOT ", MaxDepth) + "NEARBY", "NEARBY" + strings.Repeat(" ", MaxBytes-len("NEARBY"))} {
		tree := mustParse(t, source)
		if tree.Atoms > MaxAtoms {
			t.Fatal("atom overflow")
		}
	}
	for _, source := range []string{atoms + " OR NEARBY", "NOT " + nodes, strings.Repeat("NOT ", MaxDepth+1) + "NEARBY", "NEARBY" + strings.Repeat(" ", MaxBytes+1-len("NEARBY"))} {
		if _, err := Parse(source); err == nil {
			t.Fatal("predicate exceeded budget")
		}
	}
}
