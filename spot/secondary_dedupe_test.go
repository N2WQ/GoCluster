package spot

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestSecondaryModeIdentityCanonicalAndBounded(t *testing.T) {
	modes := []ModeDefinition{
		{Name: "XMAIN"},
		{Name: "XFAMILY", CanonicalFamily: "XMAIN", Variants: []string{"XALT"}},
		{Name: "HIDDEN"},
		{Name: UnknownModeToken},
	}
	a, err := buildTaxonomy(taxonomyFile{Modes: modes})
	if err != nil {
		t.Fatal(err)
	}
	for i, j := 0, len(modes)-1; i < j; i, j = i+1, j-1 {
		modes[i], modes[j] = modes[j], modes[i]
	}
	b, err := buildTaxonomy(taxonomyFile{Modes: modes})
	if err != nil {
		t.Fatal(err)
	}
	main, canonical := a.SecondaryModeIdentity("XMAIN")
	if main == 0 || canonical != "XMAIN" {
		t.Fatalf("main=%d/%s", main, canonical)
	}
	for _, name := range []string{"XMAIN", "XFAMILY", "XALT", " xalt "} {
		id, canonical := a.SecondaryModeIdentity(name)
		if id != main || canonical != "XMAIN" {
			t.Fatalf("%s=%d/%s, want %d/XMAIN", name, id, canonical, main)
		}
	}
	for _, name := range []string{"XMAIN", "HIDDEN", "USB", "LSB"} {
		id, canonical := a.SecondaryModeIdentity(name)
		other, otherCanonical := b.SecondaryModeIdentity(name)
		if id == 0 || id != other || canonical != otherCanonical {
			t.Fatalf("unstable identity %s: %d/%s vs %d/%s", name, id, canonical, other, otherCanonical)
		}
		if name != "XMAIN" && id == main {
			t.Fatalf("distinct mode %s shares XMAIN identity", name)
		}
	}
	for _, name := range []string{"", UnknownModeToken, "UNREGISTERED"} {
		id, canonical := a.SecondaryModeIdentity(name)
		if id != 0 || canonical != UnknownModeToken {
			t.Fatalf("unknown %q=%d/%s", name, id, canonical)
		}
	}
	if allocations := testing.AllocsPerRun(100, func() { a.SecondaryModeIdentity("XALT") }); allocations != 0 {
		t.Fatalf("normalized lookup allocated: %g", allocations)
	}
}

func TestSecondaryModeIdentityMaximumTaxonomy(t *testing.T) {
	modes := make([]ModeDefinition, maxTaxonomyModes)
	for i := range modes {
		modes[i].Name = fmt.Sprintf("M%03d", i)
	}
	taxonomy, err := buildTaxonomy(taxonomyFile{Modes: modes})
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[byte]string)
	for _, mode := range append(modes, ModeDefinition{Name: "USB"}, ModeDefinition{Name: "LSB"}) {
		id, _ := taxonomy.SecondaryModeIdentity(mode.Name)
		if previous, exists := seen[id]; id == 0 || exists {
			t.Fatalf("mode %s id=%d previously=%s", mode.Name, id, previous)
		}
		seen[id] = mode.Name
	}
}

func TestSecondaryModeIdentityOverlappingAliasesAreDeterministic(t *testing.T) {
	file := taxonomyFile{Modes: []ModeDefinition{
		{Name: "A", Variants: []string{"B"}},
		{Name: "B", Variants: []string{"C"}},
		{Name: "C", Variants: []string{"D"}},
	}}
	// The loader allows overlapping names/variants. Each alias uses its
	// owner's immutable, one-step canonical definition, not map iteration.
	for range 100 {
		taxonomy, err := buildTaxonomy(file)
		if err != nil {
			t.Fatal(err)
		}
		for name, want := range map[string]string{"A": "A", "B": "A", "C": "A", "D": "B"} {
			id, canonical := taxonomy.SecondaryModeIdentity(name)
			if id == 0 || canonical != want {
				t.Fatalf("%s=%d/%s, want %s", name, id, canonical, want)
			}
		}
	}
}

func TestSecondaryDedupeEvidenceCloneAndJSONBoundary(t *testing.T) {
	s := NewSpot("K1ABC", "W1XYZ", 14025, "CW")
	s.SecondaryDedupe[1] = SecondaryDedupeEvidence{Enabled: true, HasAnchor: true, AnchorHz: 14024000, ModeID: 7}
	clone := s.CloneWithComment("replacement")
	s.SecondaryDedupe[1].AnchorHz = 14026000
	if clone.SecondaryDedupe[1].AnchorHz != 14024000 {
		t.Fatal("clone retained mutable dedupe evidence")
	}
	encoded, err := json.Marshal(clone)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "SecondaryDedupe") || strings.Contains(string(encoded), "14024000") {
		t.Fatal("ephemeral diagnostic evidence entered JSON output")
	}
}
