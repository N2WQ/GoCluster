package filter

import "testing"

var ruleEvaluationSink bool

func TestRuleEvaluateAllocs(t *testing.T) {
	program, err := compileRule(ruleAllFieldsSource)
	if err != nil {
		t.Fatal(err)
	}
	s := normalizedRuleTestSpot()
	view := newRuleView(s, PathClassHigh)
	if !program.evaluate(&view, program.syntax.Root) {
		t.Fatal("allocation fixture must match every predicate")
	}
	allocations := testing.AllocsPerRun(1000, func() {
		// Recreate the local cache every time: warming must not conceal
		// per-spot normalization allocations.
		view := newRuleView(s, PathClassHigh)
		ruleEvaluationSink = program.evaluate(&view, program.syntax.Root)
	})
	if !ruleEvaluationSink {
		t.Fatal("allocation fixture changed its truth value")
	}
	if allocations != 0 {
		t.Fatalf("normalized evaluation allocated %g objects, want 0", allocations)
	}
}

func BenchmarkRuleEvaluate(b *testing.B) {
	for _, test := range []struct {
		name, source string
		want         bool
	}{
		{"all-fields", ruleAllFieldsSource, true},
		{"mixed-reject", `MODE=CW OR MODE=FT8 AND BAND=6m`, false},
		{"comment-short-circuit", `MODE=FT8 OR COMMENT="POTA * ?"`, true},
		{"comment-literal", `COMMENT="POTA * ?"`, true},
		{"comment-miss", `COMMENT="SOTA activation"`, false},
	} {
		b.Run(test.name, func(b *testing.B) {
			program, err := compileRule(test.source)
			if err != nil {
				b.Fatal(err)
			}
			s := normalizedRuleTestSpot()
			view := newRuleView(s, PathClassHigh)
			if got := program.evaluate(&view, program.syntax.Root); got != test.want {
				b.Fatalf("fixture got %v, want %v", got, test.want)
			}
			b.ReportAllocs()
			b.ResetTimer()
			var got bool
			for b.Loop() {
				view := newRuleView(s, PathClassHigh)
				got = program.evaluate(&view, program.syntax.Root)
			}
			ruleEvaluationSink = got
			if got != test.want {
				b.Fatalf("timed fixture got %v, want %v", got, test.want)
			}
		})
	}
}
