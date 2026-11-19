package spot

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCCV2F0EvidenceOwnership(t *testing.T) {
	at := time.Unix(1000, 0)
	rows := []ResolverEvidenceRow{
		{Call: "K1ABC", Observation: ResolverReceiverObservation{Receiver: "R1", ObservedAt: at, FrequencyKHz: 7021, WeightMilli: 700}},
		{Call: "K1ABC", Observation: ResolverReceiverObservation{Receiver: "R2", ObservedAt: at, FrequencyKHz: 7021, WeightMilli: 800}},
		{Call: "DL9ZZZ", Observation: ResolverReceiverObservation{Receiver: "R2", ObservedAt: at, FrequencyKHz: 7021.3, WeightMilli: 900}},
		{Call: "DL9ZZZ", Observation: ResolverReceiverObservation{Receiver: "R3", ObservedAt: at, WeightMilli: 1000}},
	}
	v, err := NewStudyResolverEvidence(rows, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	rows[0].Call, rows[0].Observation.Receiver = "MUTATED", "CHANGED"
	if count, weight, conflict := v.ReceiverTotals(); count != 3 || weight != 2600 || conflict {
		t.Fatalf("union=%d/%d/%v", count, weight, conflict)
	}
	filtered := v.Filter(func(call string, r ResolverReceiverObservation) bool { return call == "K1ABC" || r.FrequencyKHz == 0 })
	if filtered.Len() != 3 || v.Len() != 4 {
		t.Fatal("filter mutated input or dropped unknown dissent")
	}
	ranks := filtered.CanonicalSupport(2)
	if len(ranks) != 2 || ranks[0].Call != "DL9ZZZ" || ranks[0].Support != 1 || ranks[1].Support != 2 {
		t.Fatalf("ranks=%+v", ranks)
	}
	ranks[0].Call = "CHANGED"
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Go(func() {
			for j := 0; j < 50; j++ {
				f := v.Filter(func(_ string, _ ResolverReceiverObservation) bool { return true })
				f.Range(func(_ string, row ResolverReceiverObservation) bool {
					row.Receiver = "LOCAL"
					return row.Receiver == "LOCAL"
				})
				if c, w, _ := f.ReceiverTotals(); c != 3 || w != 2600 {
					t.Errorf("concurrent union=%d/%d", c, w)
				}
			}
		})
	}
	wg.Wait()
	if v.CanonicalSupport(2)[0].Call != "DL9ZZZ" {
		t.Fatal("output header mutated publication")
	}
}

func TestCCV2F0StudyEvidenceBounds(t *testing.T) {
	at := time.Unix(1000, 0)
	row := ResolverEvidenceRow{Call: "K1ABC", Observation: ResolverReceiverObservation{Receiver: "R1", ObservedAt: at, WeightMilli: 1000}}
	for _, tc := range []struct {
		name   string
		rows   []ResolverEvidenceRow
		window time.Duration
	}{
		{"duplicate", []ResolverEvidenceRow{row, row}, time.Minute},
		{"zero_window", []ResolverEvidenceRow{row}, 0},
		{"row_limit", make([]ResolverEvidenceRow, 1025), time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewStudyResolverEvidence(tc.rows, tc.window); err == nil {
				t.Fatal("invalid import accepted")
			}
		})
	}
	var rows []ResolverEvidenceRow
	for c := 0; c < 16; c++ {
		for r := 0; r < 64; r++ {
			rows = append(rows, ResolverEvidenceRow{Call: fmt.Sprintf("K%dABC", c), Observation: ResolverReceiverObservation{Receiver: fmt.Sprintf("R%d", r), ObservedAt: at}})
		}
	}
	if v, err := NewStudyResolverEvidence(rows, time.Minute); err != nil || v.Len() != 1024 {
		t.Fatalf("exact cap: %v", err)
	}
	rows[1023].Call = "W1ZZZ"
	if _, err := NewStudyResolverEvidence(rows, time.Minute); err == nil {
		t.Fatal("17 spellings accepted")
	}
}
