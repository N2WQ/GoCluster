package skew

import "testing"

// Purpose: Ensure filtering keeps entries whose absolute skew meets threshold.
// Key aspects: Inclusive threshold behavior and negative skew handling.
// Upstream: go test.
// Downstream: FilterEntries.
func TestFilterEntriesByMinAbsSkew(t *testing.T) {
	entries := []Entry{
		{Callsign: "LOW", SkewHz: 0.9, CorrectionFactor: 1.0},
		{Callsign: "EDGE", SkewHz: 1.0, CorrectionFactor: 1.0},
		{Callsign: "NEG", SkewHz: -1.2, CorrectionFactor: 0.999},
		{Callsign: "ZERO", SkewHz: 0.0, CorrectionFactor: 1.2},
	}

	filtered := FilterEntries(entries, 1.0)
	if len(filtered) != 2 {
		t.Fatalf("expected 2 entries at threshold 1.0, got %d", len(filtered))
	}
	if filtered[0].Callsign != "EDGE" || filtered[1].Callsign != "NEG" {
		t.Fatalf("unexpected retained entries: %#v", filtered)
	}
}

// Purpose: Ensure negative thresholds are normalized to zero.
// Key aspects: A negative threshold should keep all entries.
// Upstream: go test.
// Downstream: FilterEntries.
func TestFilterEntriesNegativeThreshold(t *testing.T) {
	entries := []Entry{
		{Callsign: "A", SkewHz: 0.0},
		{Callsign: "B", SkewHz: 0.3},
	}
	filtered := FilterEntries(entries, -2.0)
	if len(filtered) != len(entries) {
		t.Fatalf("expected all entries retained, got %d want %d", len(filtered), len(entries))
	}
}

// Purpose: Ensure CSV parsing no longer drops correction_factor==1 rows.
// Key aspects: Selection is based on skew threshold later, not factor value.
// Upstream: Fetch.
// Downstream: parseCSV.
func TestParseCSVKeepsFactorOne(t *testing.T) {
	raw := []byte("callsign,skew,spots,correction_factor\nSKM1,1.5,100,1.0\nSKM2,2.0,200,0.999\n")
	entries, err := parseCSV(raw)
	if err != nil {
		t.Fatalf("parseCSV error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
}
