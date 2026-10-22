package filter

import (
	"reflect"
	"strings"
	"sync"
	"testing"

	"dxcluster/spot"
)

func mustPrepareRuleSet(t testing.TB, definitions ...RuleDefinition) *PreparedRuleSet {
	t.Helper()
	set, err := PrepareRuleSet(definitions)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func TestRuleSetTruth(t *testing.T) {
	for _, test := range []struct {
		name        string
		definitions []RuleDefinition
		cw, ft8     bool
	}{
		{"empty", nil, true, true},
		{"one-pass", []RuleDefinition{{0, RulePass, "MODE=CW"}}, true, false},
		{"or-passes", []RuleDefinition{{9, RulePass, "MODE=FT8"}, {0, RulePass, "MODE=CW"}}, true, true},
		{"reject-only", []RuleDefinition{{9, RuleReject, "MODE=CW"}}, false, true},
		{"lower-reject", []RuleDefinition{{9, RulePass, "MODE=CW,FT8"}, {0, RuleReject, "MODE=CW"}}, false, true},
		{"higher-reject", []RuleDefinition{{0, RulePass, "MODE=CW,FT8"}, {9, RuleReject, "MODE=CW"}}, false, true},
		{"reject-among-passes", []RuleDefinition{
			{0, RulePass, "MODE=CW"}, {4, RuleReject, "MODE=CW"}, {9, RulePass, "MODE=FT8"},
		}, false, true},
		{"no-matching-pass", []RuleDefinition{
			{0, RuleReject, "MODE=RTTY"}, {9, RulePass, "MODE=USB"},
		}, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			set := mustPrepareRuleSet(t, test.definitions...)
			for _, nearby := range []bool{false, true} {
				for _, probe := range []struct {
					mode string
					want bool
				}{{"CW", test.cw}, {"FT8", test.ft8}} {
					sp := &spot.Spot{Mode: probe.mode, IsHuman: true}
					if got := set.Matches(sp, "", nearby); got != probe.want {
						t.Fatalf("mode=%s nearby=%v got %v, want %v", probe.mode, nearby, got, probe.want)
					}
				}
				if set.Matches(nil, PathClassHigh, nearby) {
					t.Fatal("nil spot accepted")
				}
			}
		})
	}
}

func TestRuleSetReadbackOwnershipAndEquality(t *testing.T) {
	input := []RuleDefinition{{9, RuleReject, "mode=psk31"}, {0, RulePass, `(mode=cw)`}}
	original := append([]RuleDefinition(nil), input...)
	set := mustPrepareRuleSet(t, input...)
	if !reflect.DeepEqual(input, original) {
		t.Fatal("preparation mutated caller definitions")
	}
	want := []RuleDefinition{{0, RulePass, "MODE=CW"}, {9, RuleReject, "MODE=PSK"}}
	if got := set.Definitions(); !reflect.DeepEqual(got, want) {
		t.Fatalf("definitions got %#v, want %#v", got, want)
	}
	input[0] = RuleDefinition{2, RulePass, "MODE=FT8"}
	detached := set.Definitions()
	detached[0].Expr, detached[0].Action = "MODE=USB", RuleReject
	readback := set.Readback(false)
	readback[0].Slot, readback[0].Expr, readback[0].EffectiveExpr = 5, "", ""
	if got := set.Definitions(); !reflect.DeepEqual(got, want) {
		t.Fatal("caller slice mutation changed prepared state")
	}
	equal := mustPrepareRuleSet(t, want[1], want[0])
	if !set.Equal(equal) || !equal.Equal(set) || !set.Equal(set) {
		t.Fatal("canonical equal definitions depended on input order or identity")
	}
	for _, changed := range []RuleDefinition{
		{1, RulePass, "MODE=CW"}, {0, RuleReject, "MODE=CW"}, {0, RulePass, "MODE=FT8"},
	} {
		other := mustPrepareRuleSet(t, changed, want[1])
		if set.Equal(other) || other.Equal(set) {
			t.Fatalf("different definition compared equal: %#v", changed)
		}
	}
	var empty *PreparedRuleSet
	if !empty.Equal(nil) || !empty.Equal(&PreparedRuleSet{}) || set.Equal(nil) || empty.Equal(set) {
		t.Fatal("empty equality is inconsistent")
	}
	if empty.Definitions() != nil || empty.Readback(true) != nil || empty.NeedsPath(false) || empty.Counts() != (RuleSetCounts{}) {
		t.Fatal("empty metadata is not empty")
	}
}

func TestRuleSetEditsPreserveSnapshots(t *testing.T) {
	original := mustPrepareRuleSet(t, RuleDefinition{9, RulePass, "MODE=CW"}, RuleDefinition{0, RuleReject, "MODE=RTTY"})
	before := original.Definitions()
	changed, err := original.WithRule(RuleDefinition{9, RuleReject, "MODE=FT8"})
	if err != nil {
		t.Fatal(err)
	}
	if changed.rules[0].normal != original.rules[0].normal || changed.rules[0].nearby != original.rules[0].nearby {
		t.Fatal("unchanged slot was recompiled")
	}
	if changed.rules[1].normal == original.rules[1].normal {
		t.Fatal("changed expression was not replaced")
	}
	if changed.Matches(&spot.Spot{Mode: "FT8"}, "", false) || !changed.Matches(&spot.Spot{Mode: "USB"}, "", false) {
		t.Fatal("replacement failed to change action and expression")
	}
	actionOnly, err := original.WithRule(RuleDefinition{9, RuleReject, "MODE=CW"})
	if err != nil || actionOnly.Matches(&spot.Spot{Mode: "CW"}, "", false) ||
		!actionOnly.Matches(&spot.Spot{Mode: "USB"}, "", false) {
		t.Fatalf("action-only replacement failed: %v", err)
	}
	inserted, err := changed.WithRule(RuleDefinition{4, RulePass, "MODE=USB"})
	if err != nil {
		t.Fatal(err)
	}
	if got := inserted.Definitions(); len(got) != 3 || got[1].Slot != 4 {
		t.Fatal("new slot is not sorted")
	}
	removed, err := inserted.WithoutRule(4)
	if err != nil || !removed.Equal(changed) {
		t.Fatalf("removal failed: %v", err)
	}
	same, err := removed.WithoutRule(5)
	if err != nil || same != removed {
		t.Fatalf("missing valid slot changed set: %v", err)
	}
	for _, slot := range []int{0, 9} {
		removed, err = removed.WithoutRule(slot)
		if err != nil {
			t.Fatal(err)
		}
	}
	if removed != nil || !removed.Matches(&spot.Spot{Mode: "FT8"}, "", true) {
		t.Fatal("last removal did not reset to empty")
	}
	fromEmpty, err := removed.WithRule(RuleDefinition{0, RulePass, "MODE=CW"})
	if err != nil || len(fromEmpty.Definitions()) != 1 {
		t.Fatalf("insertion into nil set failed: %v", err)
	}
	if !reflect.DeepEqual(original.Definitions(), before) || !original.Matches(&spot.Spot{Mode: "CW", IsHuman: true}, "", false) {
		t.Fatal("edit changed prior snapshot")
	}
}

func TestRuleSetInvalidInputs(t *testing.T) {
	original := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, "MODE=CW"})
	before := original.Definitions()
	for _, definition := range []RuleDefinition{
		{-1, RulePass, "MODE=CW"}, {10, RulePass, "MODE=CW"},
		{0, "", "MODE=CW"}, {0, "PASS", "MODE=CW"}, {0, "reject ", "MODE=CW"},
		{0, RulePass, ""}, {0, RuleReject, "MODE=bad"},
		{9, RulePass, strings.Repeat("x", 1025)},
	} {
		if got, err := PrepareRuleSet([]RuleDefinition{definition}); err == nil || got != nil {
			t.Fatalf("invalid replacement accepted: %#v", definition)
		}

		if got, err := original.WithRule(definition); err == nil || got != nil {
			t.Fatalf("invalid edit accepted: %#v", definition)
		}
		if !reflect.DeepEqual(original.Definitions(), before) {
			t.Fatal("failed edit mutated prior set")
		}
	}
	for _, definitions := range [][]RuleDefinition{
		{{0, RulePass, "MODE=CW"}, {0, RuleReject, "MODE=FT8"}},
		make([]RuleDefinition, MaxRuleSlots+1),
	} {
		if got, err := PrepareRuleSet(definitions); err == nil || got != nil {
			t.Fatal("duplicate or oversized slot list admitted")
		}
	}
	for _, slot := range []int{-1, 10} {
		for _, set := range []*PreparedRuleSet{nil, original} {
			if got, err := set.WithoutRule(slot); err == nil || got != nil {
				t.Fatalf("invalid removal slot %d admitted", slot)
			}
		}
	}
	if got, err := (*PreparedRuleSet)(nil).WithoutRule(9); err != nil || got != nil {
		t.Fatalf("valid removal from empty failed: %v", err)
	}
	// Structural admission must finish before compiling any expression.
	if _, err := PrepareRuleSet([]RuleDefinition{{0, RulePass, "bad"}, {0, RulePass, "bad"}}); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("definitions were compiled before duplicate validation: %v", err)
	}
}

func TestRuleSetConcurrentReaders(t *testing.T) {
	original := mustPrepareRuleSet(t, RuleDefinition{0, RulePass, `DXCONT=EU OR COMMENT="POTA * ?"`})
	sp := normalizedRuleTestSpot()
	sp.IsHuman = true
	start := make(chan struct{})
	failures := make(chan string, 4)
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for range 100 {
				if !original.Matches(sp, PathClassHigh, false) || !original.Matches(sp, PathClassHigh, true) ||
					original.Definitions()[0].Expr != `DXCONT=EU OR COMMENT="POTA * ?"` {
					failures <- "editing another snapshot changed an immutable reader"
					return
				}
			}
		}()
	}
	close(start)
	current := original
	for range 100 {
		changed, err := current.WithRule(RuleDefinition{9, RuleReject, "SNR<6"})
		if err != nil {
			t.Error(err)
			break
		}
		current, err = changed.WithoutRule(9)
		if err != nil {
			t.Error(err)
			break
		}
	}
	readers.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	if !current.Equal(original) {
		t.Fatal("repeated edits did not preserve the surviving rule")
	}
}
