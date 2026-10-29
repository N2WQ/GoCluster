package expr

import "testing"

func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		"NEARBY", "NOT NEARBY", "MODE=CW OR NEARBY", "", "SNR=1", "A=1 OR B=2 AND NOT C=3", "NOT (A=1 OR B=2)",
		`COMMENT="CQ ""DX"""`, `X=[AND,OR,NOT,"",k1abc/p]`,
		"A=1,", "A=[1,]", "NOT NOT NOT NOT NOT NOT NOT NOT A=1",
		"(((((((((A=1)))))))))", "A!=1", "A=é", "A=\x00",
	} {
		f.Add(seed)
	}
	for _, source := range []string{"NEARBY", "NOT NEARBY", "MODE=CW OR NEARBY"} {
		if _, err := Parse(source); err != nil {
			f.Fatalf("positive predicate seed refused: %v", err)
		}
	}
	f.Fuzz(func(t *testing.T, source string) {
		e, err := Parse(source)
		if err != nil {
			if e != nil {
				t.Fatal("failed parse returned a tree")
			}
			return
		}
		if len(source) > MaxBytes || len(e.Nodes) < 1 || len(e.Nodes) > MaxNodes ||
			e.Atoms < 1 || e.Atoms > MaxAtoms || e.Root < 0 || e.Root >= len(e.Nodes) {
			t.Fatalf("unbounded successful parse: %+v", e)
		}
		atoms := 0
		for _, node := range e.Nodes {
			if node.Kind == AtomKind || node.Kind == PredicateKind {
				atoms++
				if node.Kind == PredicateKind {
					if node.Atom.Field != "NEARBY" || node.Atom.Operator != "" || len(node.Atom.Values) != 0 {
						t.Fatal("invalid predicate")
					}
				} else if len(node.Atom.Values) < 1 || len(node.Atom.Values) > MaxValuesPerAtom {
					t.Fatal("unbounded value list")
				}
			}
		}
		if atoms != e.Atoms {
			t.Fatalf("atom count %d != %d", atoms, e.Atoms)
		}
		canonical, err := e.Canonical()
		if err != nil || len(canonical) > MaxBytes {
			t.Fatalf("successful tree not printable: length=%d error=%v", len(canonical), err)
		}
		reparsed, err := Parse(canonical)
		if err != nil {
			t.Fatalf("canonical %q not parseable: %v", canonical, err)
		}
		stable, err := reparsed.Canonical()
		if err != nil || stable != canonical {
			t.Fatalf("unstable canonical %q -> %q: %v", canonical, stable, err)
		}
	})
}
