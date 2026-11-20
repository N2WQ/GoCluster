package correctionflow

import (
	"testing"
	"time"

	"dxcluster/spot"
)

func TestOrdinaryCWSeparation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*OrdinaryCWSeparationInput, []spot.ResolverEvidenceRow) []spot.ResolverEvidenceRow
		want   bool
	}{
		{name: "separated", want: true},
		{name: "bridged", change: func(_ *OrdinaryCWSeparationInput, rows []spot.ResolverEvidenceRow) []spot.ResolverEvidenceRow {
			return append(rows, f0Row("N1ABC", "R4", 7021.1, 0), f0Row("N1ABC", "R5", 7021.2, 0))
		}},
		{name: "cochannel", change: func(_ *OrdinaryCWSeparationInput, rows []spot.ResolverEvidenceRow) []spot.ResolverEvidenceRow {
			for i := range rows {
				rows[i].Observation.FrequencyKHz = 7021
			}
			return rows
		}},
		{name: "stale original", change: func(_ *OrdinaryCWSeparationInput, rows []spot.ResolverEvidenceRow) []spot.ResolverEvidenceRow {
			rows[1].Observation.ObservedAt = f0At.Add(-61 * time.Second)
			return rows
		}},
		{name: "missing winner frequency", change: func(_ *OrdinaryCWSeparationInput, rows []spot.ResolverEvidenceRow) []spot.ResolverEvidenceRow {
			rows[2].Observation.FrequencyKHz = 0
			return rows
		}},
		{name: "frequency conflict", change: func(_ *OrdinaryCWSeparationInput, rows []spot.ResolverEvidenceRow) []spot.ResolverEvidenceRow {
			rows[2].Observation.FrequencyConflict = true
			rows[2].Observation.FrequencyKHz = 0
			return rows
		}},
		{name: "late witness", change: func(_ *OrdinaryCWSeparationInput, rows []spot.ResolverEvidenceRow) []spot.ResolverEvidenceRow {
			rows[2].Observation.ObservedAt = f0At.Add(time.Second)
			return rows
		}},
		{name: "focal family does not corroborate itself", change: func(in *OrdinaryCWSeparationInput, rows []spot.ResolverEvidenceRow) []spot.ResolverEvidenceRow {
			in.FocalFamily = "R1"
			return rows
		}},
		{name: "aliased families count once", change: func(in *OrdinaryCWSeparationInput, rows []spot.ResolverEvidenceRow) []spot.ResolverEvidenceRow {
			in.ReceiverFamily = func(string) string { return "same-family" }
			return rows
		}},
		{name: "witness reports are too far apart in time", change: func(_ *OrdinaryCWSeparationInput, rows []spot.ResolverEvidenceRow) []spot.ResolverEvidenceRow {
			rows[2].Observation.ObservedAt = f0At.Add(-61 * time.Second)
			return rows
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := OrdinaryCWSeparationInput{
				Original: "K1ABC", Winner: "DL9ZZZ", FocalKHz: 7021, At: f0At,
				FocalFamily: "R6", ReceiverFamily: func(receiver string) string { return receiver },
			}
			rows := f0BaseRows()
			if tc.change != nil {
				rows = tc.change(&input, rows)
			}
			input.Key, _, input.Sources, input.Config = f0Fixture(t, rows)
			if got := OrdinaryCWSeparation(input); got != tc.want {
				t.Fatalf("separation = %t, want %t", got, tc.want)
			}
		})
	}
}
