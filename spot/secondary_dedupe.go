package spot

import "sort"

// SecondaryDedupeEvidence is owned value evidence for a live FAST/MED/SLOW
// decision. HasAnchor distinguishes a new/bypassed entry from a comparison
// against an existing anchor. Enabled is false in off and shadow output.
type SecondaryDedupeEvidence struct {
	AnchorHz  int64
	ModeID    byte
	HasAnchor bool
	Enabled   bool
}

type secondaryModeIdentity struct {
	id        byte
	canonical string
}

// buildSecondaryModeIdentities compiles only the bounded startup taxonomy.
// No incoming string is ever interned. Human spots do not use these IDs.
func (t *Taxonomy) buildSecondaryModeIdentities() {
	names := make(map[string]struct{}, len(t.modesByName)+2)
	for name := range t.modesByName {
		canonical := t.CanonicalMode(name)
		if canonical != UnknownModeToken {
			names[canonical] = struct{}{}
		}
	}
	// Generic voice tokens can normalize to these fixed modes even when the
	// operator's taxonomy hides them or omits their filter definitions.
	names["USB"], names["LSB"] = struct{}{}, struct{}{}
	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)
	ids := make(map[string]byte, len(ordered))
	for i, name := range ordered {
		ids[name] = byte(i + 1) // At most 128 configured names plus USB/LSB.
	}
	t.secondaryModes = make(map[string]secondaryModeIdentity, len(t.modesByName)+len(t.modeVariants)+2)
	declared := make(map[string]secondaryModeIdentity, len(t.modesByName))
	for name := range t.modesByName {
		canonical := t.CanonicalMode(name)
		identity := secondaryModeIdentity{ids[canonical], canonical}
		declared[name] = identity
		t.secondaryModes[name] = identity
	}
	for alias, owner := range t.modeVariants {
		// A declared name may itself be another definition's variant. Read
		// the immutable declared view, never an earlier alias assignment.
		t.secondaryModes[alias] = declared[owner]
	}
	for _, name := range []string{"USB", "LSB"} {
		if _, ok := t.secondaryModes[name]; !ok {
			t.secondaryModes[name] = secondaryModeIdentity{ids[name], name}
		}
	}
}

// SecondaryModeIdentity returns a deterministic byte and canonical label for
// configured modes, including hidden/custom modes and their aliases. Blank,
// UNKNOWN and unregistered labels share the reserved zero bucket. The current
// production skimmer parsers accept taxonomy modes only; zero is defensive
// legacy grouping, not a process-lifetime registry of arbitrary source labels.
func SecondaryModeIdentity(mode string) (byte, string) {
	return CurrentTaxonomy().SecondaryModeIdentity(mode)
}

// SecondaryModeIdentity uses this immutable taxonomy rather than global state.
func (t *Taxonomy) SecondaryModeIdentity(mode string) (byte, string) {
	upper := normalizeUpperASCIITrim(mode)
	identity, ok := t.secondaryModes[upper]
	if !ok || identity.id == 0 {
		return 0, UnknownModeToken
	}
	return identity.id, identity.canonical
}
