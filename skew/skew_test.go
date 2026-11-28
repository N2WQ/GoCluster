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

// Purpose: Ensure ApplyCorrection rounds corrected frequency to nearest 10 Hz using half-up rules.
// Key aspects: Verifies behavior at the .005 kHz boundary.
// Upstream: RBN/PSKReporter ingest.
// Downstream: ApplyCorrection.
func TestApplyCorrectionRoundsTo10HzHalfUp(t *testing.T) {
	table, err := NewTable([]Entry{{Callsign: "N0CALL", CorrectionFactor: 1.0}})
	if err != nil {
		t.Fatalf("NewTable: %v", err)
	}
	store := NewStore()
	store.Set(table)

	tests := []struct {
		name string
		in   float64
		want float64
	}{
		{name: "below half rounds down", in: 7014.994, want: 7014.99},
		{name: "at half rounds up", in: 7014.995, want: 7015.00},
		{name: "above half rounds up", in: 7014.996, want: 7015.00},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ApplyCorrection(store, "N0CALL", tt.in)
			if got != tt.want {
				t.Fatalf("ApplyCorrection(..., %.3f)=%.2f want %.2f", tt.in, got, tt.want)
			}
		})
	}
}

func TestReceiverSkewLookupPreservesExactAndNumericIDs(t *testing.T) {
	table, err := NewTable([]Entry{
		{Callsign: "LB9KJ", CorrectionFactor: 1.001},
		{Callsign: "LB9KJ-1", CorrectionFactor: 1.000014694},
		{Callsign: "LB9KJ-2", CorrectionFactor: .999},
		{Callsign: "LB9KJ-2-#", CorrectionFactor: 1.002},
		{Callsign: "LB9KJ-3", CorrectionFactor: .998},
		{Callsign: "LB9KJ-3-#", CorrectionFactor: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		receiver string
		factor   float64
		found    bool
	}{
		{"LB9KJ-1", 1.000014694, true}, {" lb9kj-1-# ", 1.000014694, true},
		{"LB9KJ-2-#", 1.002, true}, {"LB9KJ-3-#", 0, true},
		{"LB9KJ-4-#", 0, false}, {"LB9KJ-2-#-#", 0, false},
		{"LB9KJ-1#", 0, false}, {"LB9KJ-1-@", 0, false}, {"-#", 0, false}, {"", 0, false},
	} {
		if got, found := table.Lookup(tc.receiver); got != tc.factor || found != tc.found {
			t.Errorf("Lookup(%q) = (%g, %t), want (%g, %t)", tc.receiver, got, found, tc.factor, tc.found)
		}
	}
	store := NewStore()
	if got := ApplyCorrection(store, "LB9KJ-1-#", 14020); got != 14020 {
		t.Errorf("empty store: got %.2f, want 14020.00", got)
	}
	store.Set(table)
	if got := ApplyCorrection(store, "LB9KJ-1-#", 14020); got != 14020.21 {
		t.Errorf("fallback correction: got %.2f, want 14020.21", got)
	}
	if got := ApplyCorrection(store, "LB9KJ-3-#", 14020); got != 14020 {
		t.Errorf("nonpositive exact factor: got %.2f, want 14020.00", got)
	}
}

func BenchmarkReceiverSkewLookup(b *testing.B) {
	table, err := NewTable([]Entry{
		{Callsign: "LB9KJ-1", CorrectionFactor: 1.000014694},
		{Callsign: "LB9KJ-2-#", CorrectionFactor: 1.002},
	})
	if err != nil {
		b.Fatal(err)
	}
	for _, tc := range []struct{ name, receiver string }{
		{"bare_hit", "LB9KJ-1"}, {"tagged_hit", "LB9KJ-2-#"}, {"miss", "LB9KJ-3"},
		{"tagged_fallback", "LB9KJ-1-#"}, {"tagged_miss", "LB9KJ-3-#"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				table.Lookup(tc.receiver)
			}
		})
	}
}
