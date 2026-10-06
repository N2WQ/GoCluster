package cty

import (
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func canonicalFixture() *CTYDatabase {
	return &CTYDatabase{Data: map[string]PrefixInfo{
		"K": {Prefix: "K", ADIF: 291}, "W6": {Prefix: " K ", ADIF: 291},
		"I": {Prefix: "I", ADIF: 248}, "IT9": {Prefix: "IT9", ADIF: 248}, "IG9": {Prefix: "IG9", ADIF: 248},
		"3D2": {Prefix: "3D2", ADIF: 176}, "R": {Prefix: "UA", ADIF: 54},
		"ROTUMA-CALL": {Prefix: "3D2/R", ADIF: 460},
		"PETER-CALL":  {Prefix: "3Y/P", ADIF: 199},
		"NO-ADIF":     {Prefix: "BAD", ADIF: 0}, "NO-LABEL": {ADIF: 123},
	}}
}

func TestCanonicalDXCCLabels(t *testing.T) {
	index := NewDXCCIndex(canonicalFixture())
	for input, want := range map[string]int{"k": 291, " IT9 ": 248, "3D2/R": 460, "3y/p": 199, "UA": 54} {
		got, err := index.ResolveCanonical(input)
		if err != nil || got != want {
			t.Fatalf("%q: got %d, %v; want %d", input, got, err, want)
		}
	}
	for _, input := range []string{"", "W6", "K1ABC", "BAD", "ZZ", "291"} {
		if _, err := index.ResolveCanonical(input); !errors.Is(err, ErrUnknownCanonicalPrefix) {
			t.Fatalf("%q must be unknown: %v", input, err)
		}
	}
	if got := index.Prefixes(248); !reflect.DeepEqual(got, []string{"I", "IG9", "IT9"}) {
		t.Fatalf("shared entity labels: %v", got)
	}
	if got := index.Prefixes(291); !reflect.DeepEqual(got, []string{"K"}) {
		t.Fatalf("duplicate records not collapsed: %v", got)
	}
	if len(index.canonical) > len(canonicalFixture().Data) {
		t.Fatal("index cardinality exceeds source records")
	}
}

func TestCanonicalDXCCConflictAndSnapshotOwnership(t *testing.T) {
	db := canonicalFixture()
	db.Data["OTHER"] = PrefixInfo{Prefix: "k", ADIF: 999}
	for range 20 {
		if _, err := NewDXCCIndex(db).ResolveCanonical("K"); !errors.Is(err, ErrConflictingCanonicalPrefix) {
			t.Fatalf("map order selected a conflicting entity: %v", err)
		}
	}
	first := NewDXCCIndex(canonicalFixture())
	second := NewDXCCIndex(&CTYDatabase{Data: map[string]PrefixInfo{"NEW": {Prefix: "K", ADIF: 999}}})
	if adif, _ := first.ResolveCanonical("K"); adif != 291 {
		t.Fatal("old snapshot changed")
	}
	if adif, _ := second.ResolveCanonical("K"); adif != 999 {
		t.Fatal("replacement snapshot not used")
	}
	for _, index := range []*DXCCIndex{nil, NewDXCCIndex(nil)} {
		if _, err := index.ResolveCanonical("K"); !errors.Is(err, ErrUnknownCanonicalPrefix) || index.Prefixes(291) != nil {
			t.Fatal("nil database/index must be unresolved")
		}
	}
}

func TestCanonicalDXCCConflictDisplayLabels(t *testing.T) {
	for _, alternatives := range []bool{false, true} {
		db := &CTYDatabase{Data: map[string]PrefixInfo{
			"FIRST": {Prefix: "K", ADIF: 291}, "DUPLICATE": {Prefix: " k ", ADIF: 291},
			"SECOND": {Prefix: "k", ADIF: 999},
		}}
		want := map[int][]string{291: {}, 999: {}}
		if alternatives {
			db.Data["ALT-FIRST"] = PrefixInfo{Prefix: "W", ADIF: 291}
			db.Data["ALT-FIRST-DUPLICATE"] = PrefixInfo{Prefix: " w ", ADIF: 291}
			db.Data["ALT-FIRST-SORT"] = PrefixInfo{Prefix: "AA", ADIF: 291}
			db.Data["ALT-SECOND"] = PrefixInfo{Prefix: "Z", ADIF: 999}
			want[291], want[999] = []string{"AA", "W"}, []string{"Z"}
		}
		for range 20 {
			index := NewDXCCIndex(db)
			if _, err := index.ResolveCanonical(" K "); !errors.Is(err, ErrConflictingCanonicalPrefix) {
				t.Fatalf("alternatives=%t: conflicting input resolved: %v", alternatives, err)
			}
			for _, code := range []int{291, 999} {
				if got := index.Prefixes(code); !slices.Equal(got, want[code]) {
					t.Fatalf("alternatives=%t: ADIF %d labels=%v, want %v", alternatives, code, got, want[code])
				}
			}
			if alternatives {
				for label, code := range map[string]int{"AA": 291, "W": 291, "Z": 999} {
					if got, err := index.ResolveCanonical(label); err != nil || got != code {
						t.Fatalf("unambiguous alternative %q: %d, %v", label, got, err)
					}
				}
			}
		}
	}
}

func FuzzCanonicalDXCCResolution(f *testing.F) {
	for _, seed := range []string{"K", " IT9 ", "3D2/R", "3Y/P", "W6", "291", "", "k1abc", "\xff"} {
		f.Add(seed)
	}
	index := NewDXCCIndex(canonicalFixture())
	expected := map[string]int{"K": 291, "I": 248, "IG9": 248, "IT9": 248, "3D2": 176, "UA": 54, "3D2/R": 460, "3Y/P": 199}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 128 {
			return
		}
		got, err := index.ResolveCanonical(input)
		want, exists := expected[strings.ToUpper(strings.TrimSpace(input))]
		if exists && (err != nil || got != want) {
			t.Fatalf("literal canonical oracle disagrees: %q -> %d, %v", input, got, err)
		}
		if !exists && !errors.Is(err, ErrUnknownCanonicalPrefix) {
			t.Fatalf("noncanonical input resolved: %q -> %d, %v", input, got, err)
		}
	})
}
