package spot

import (
	"fmt"
	"testing"
	"time"
)

func lossTestDriver(t testing.TB, cfg SignalResolverConfig) (*SignalResolver, *SignalResolverDriver, ResolverSignalKey, time.Time) {
	t.Helper()
	if cfg.QueueSize == 0 {
		cfg.QueueSize = 64
	}
	if cfg.MaxActiveKeys == 0 {
		cfg.MaxActiveKeys = 4
	}
	if cfg.MaxCandidatesPerKey == 0 {
		cfg.MaxCandidatesPerKey = 4
	}
	if cfg.MaxReportersPerCand == 0 {
		cfg.MaxReportersPerCand = 4
	}
	if cfg.EvalMinInterval == 0 {
		cfg.EvalMinInterval = 500 * time.Millisecond
	}
	if cfg.SweepInterval == 0 {
		cfg.SweepInterval = time.Nanosecond
	}
	if cfg.InactiveTTL == 0 {
		cfg.InactiveTTL = 24 * time.Hour
	}
	if cfg.HysteresisWindows == 0 {
		cfg.HysteresisWindows = 1
	}
	r, d, key, at := evidenceTestDriver(t, cfg)
	if err := d.SetProcessElapsed(0); err != nil {
		t.Fatal(err)
	}
	return r, d, key, at
}

func lossTestAdmit(t testing.TB, r *SignalResolver, d *SignalResolverDriver, key ResolverSignalKey, call, receiver string, at time.Time, window time.Duration) {
	t.Helper()
	if !r.Enqueue(ResolverEvidence{Key: key, DXCall: call, Spotter: receiver, ObservedAt: at, FrequencyKHz: 7010.1, RecencyWindow: window}) {
		t.Fatal("enqueue cap-loss evidence")
	}
	if d.Drain() != 1 {
		t.Fatal("owner did not consume receipt")
	}
}

func lossTestSnapshot(t testing.TB, r *SignalResolver, key ResolverSignalKey) ResolverSnapshot {
	t.Helper()
	s, ok := r.Lookup(key)
	if !ok {
		t.Fatal("missing generation publication")
	}
	if s.Generation == 0 {
		t.Fatal("publication has no lifecycle UID")
	}
	return s
}

func lossTestRow(s ResolverSnapshot, call, receiver string) (ResolverReceiverObservation, bool) {
	var found ResolverReceiverObservation
	var ok bool
	s.ReceiverEvidence().Range(func(c string, row ResolverReceiverObservation) bool {
		if c == call && row.Receiver == receiver {
			found, ok = row, true
		}
		return true
	})
	return found, ok
}

func lossTestRank(t testing.TB, s ResolverSnapshot, call string, support, weight int) {
	t.Helper()
	for _, rank := range s.CandidateRanks {
		if rank.Call == call {
			if rank.Support != support || rank.WeightedSupportMilli != weight {
				t.Fatalf("%s rank %+v, want %d/%d", call, rank, support, weight)
			}
			return
		}
	}
	t.Fatalf("missing candidate %s in %+v", call, s.CandidateRanks)
}

func TestResolverCapLossPublishesCompleteRetainedPopulation(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		candidates, receivers      int
		seedA, wantA, wantTotal    int
		incoming                   string
		victimCall, victimReceiver string
	}{
		{"candidate", 2, 4, 3, 3, 4, "K1ABE", "K1ABD", "N0B1"},
		{"receiver", 4, 2, 2, 2, 3, "K1ABC", "K1ABC", "N0A1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, d, key, at := lossTestDriver(t, SignalResolverConfig{MaxCandidatesPerKey: tc.candidates, MaxReportersPerCand: tc.receivers})
			for i := 1; i <= tc.seedA; i++ {
				lossTestAdmit(t, r, d, key, "K1ABC", fmt.Sprintf("N0A%d", i), at, time.Minute)
			}
			lossTestAdmit(t, r, d, key, "K1ABD", "N0B1", at, time.Minute)
			d.Sweep(at.Add(time.Second))
			before := lossTestSnapshot(t, r, key)
			if before.EvidenceLoss || before.Winner != "K1ABC" || before.State != ResolverStateConfident {
				t.Fatalf("healthy source %+v", before)
			}
			st := d.states[key]
			lastEval, nextEval, pending := st.lastEvalAt, st.nextEvalAt, st.pendingWins
			lossTestAdmit(t, r, d, key, tc.incoming, "N0Z1", at.Add(time.Second+time.Millisecond), time.Minute)
			after := lossTestSnapshot(t, r, key)
			if !after.EvidenceLoss || after.Generation != before.Generation || after.TotalReporters != tc.wantTotal || after.TotalWeightedSupportMilli != tc.wantTotal*1000 || after.ReceiverEvidence().Len() != tc.wantTotal {
				t.Fatalf("loss control %+v", after)
			}
			lossTestRank(t, after, "K1ABC", tc.wantA, tc.wantA*1000)
			if _, exists := lossTestRow(after, tc.victimCall, tc.victimReceiver); exists {
				t.Fatal("discarded receiver still in retained population")
			}
			if row, exists := lossTestRow(before, tc.victimCall, tc.victimReceiver); !exists || !row.ObservedAt.Equal(at) || row.WeightMilli != 1000 {
				t.Fatalf("old publication changed: %+v %v", row, exists)
			}
			if _, exists := lossTestRow(after, tc.incoming, "N0Z1"); !exists {
				t.Fatal("incoming retained receiver omitted")
			}
			if !st.lastEvalAt.Equal(lastEval) || !st.nextEvalAt.Equal(nextEval) || st.pendingWins != pending {
				t.Fatal("control advanced ordinary evaluation/hysteresis")
			}
			compact := after.Compact()
			if compact.Generation != after.Generation || !compact.EvidenceLoss || compact.ReceiverEvidence() != nil || compact.lossMinAt != at || compact.lossMaxAt != at {
				t.Fatalf("compact lost metadata %+v", compact)
			}
			if tc.name == "receiver" {
				// Same timestamp bounds cannot suppress a second actual discard.
				lossTestAdmit(t, r, d, key, "K1ABC", "N0Z2", at.Add(time.Second+2*time.Millisecond), time.Minute)
				repeated := lossTestSnapshot(t, r, key)
				if repeated.lossMinAt != at || repeated.lossMaxAt != at || repeated.ReceiverEvidence() == after.ReceiverEvidence() {
					t.Fatal("unchanged loss bounds skipped publication")
				}
				if _, exists := lossTestRow(repeated, "K1ABC", "N0A2"); exists {
					t.Fatal("second victim survived")
				}
				if _, exists := lossTestRow(repeated, "K1ABC", "N0Z2"); !exists {
					t.Fatal("second retained receipt absent")
				}
			}
		})
	}
}

func TestResolverLossEligibilityUnionBoundariesAndRowlessProjection(t *testing.T) {
	r, d, key, at := lossTestDriver(t, SignalResolverConfig{MaxReportersPerCand: 1, EvalMinInterval: time.Hour})
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A1", at, time.Minute)
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A2", at.Add(10*time.Second), time.Minute)
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A3", at.Add(20*time.Second), time.Minute)
	raw := lossTestSnapshot(t, r, key)
	if !raw.EvidenceLoss || raw.lossMinAt != at || raw.lossMaxAt != at.Add(10*time.Second) {
		t.Fatalf("loss envelope %+v", raw)
	}
	for _, tc := range []struct {
		name  string
		query time.Time
		loss  bool
		rows  int
	}{
		{"before lost population", at.Add(-time.Nanosecond), false, 0},
		{"inclusive first lost", at, true, 0},
		{"conservative interval", at.Add(5 * time.Second), true, 0},
		{"inclusive expiry", at.Add(70 * time.Second), true, 1},
		{"strictly expired", at.Add(70*time.Second + time.Nanosecond), false, 1},
		{"all expired", at.Add(81 * time.Second), false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, ok := r.LookupEligible(key, tc.query)
			if !ok || s.EvidenceLoss != tc.loss || s.Generation != raw.Generation || s.ReceiverEvidence().Len() != tc.rows || s.TotalReporters != tc.rows {
				t.Fatalf("projection %+v / ok=%v rows=%d", s, ok, s.ReceiverEvidence().Len())
			}
			if tc.rows == 0 && (s.Winner != "" || s.State != ResolverStateUncertain || s.TotalWeightedSupportMilli != 0) {
				t.Fatalf("rowless projection granted support %+v", s)
			}
		})
	}
	// Raw replay lookup stays at the owner publication clock, independently of
	// query projections. Reads neither mutate its flag nor its receiver rows.
	if again := lossTestSnapshot(t, r, key); !again.EvidenceLoss || again.ReceiverEvidence().Len() != 1 {
		t.Fatalf("query changed raw publication %+v", again)
	}
}

func TestResolverAgePruningDoesNotManufactureCapLoss(t *testing.T) {
	r, d, key, at := lossTestDriver(t, SignalResolverConfig{MaxReportersPerCand: 1})
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A1", at, time.Second)
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A2", at.Add(2*time.Second), time.Second)
	s := lossTestSnapshot(t, r, key)
	if s.EvidenceLoss || !s.lossMinAt.IsZero() || s.TotalReporters != 1 || r.MetricsSnapshot().EvictedReporters != 0 {
		t.Fatalf("ordinary expiry reported cap loss %+v", s)
	}
	if _, exists := lossTestRow(s, "K1ABC", "N0A2"); !exists {
		t.Fatal("fresh replacement missing")
	}
}
