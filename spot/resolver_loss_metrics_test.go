package spot

import (
	"fmt"
	"testing"
	"time"
)

func lossMetricsWant(t testing.TB, r *SignalResolver, discards, recoveries, idle uint64, published, active int) {
	t.Helper()
	metrics := r.MetricsSnapshot()
	if metrics.CapLossDiscards != discards || metrics.LossPublicationRecoveries != recoveries || metrics.LossIdleRetirements != idle || metrics.PublishedLossKeys != published || metrics.ActiveKeys != active {
		t.Fatalf("loss metrics: discards=%d recoveries=%d idle=%d published=%d active=%d, want %d/%d/%d/%d/%d", metrics.CapLossDiscards, metrics.LossPublicationRecoveries, metrics.LossIdleRetirements, metrics.PublishedLossKeys, metrics.ActiveKeys, discards, recoveries, idle, published, active)
	}
}

func TestResolverLossMetricsCountDiscardedObservationIncidences(t *testing.T) {
	r, d, key, at := lossTestDriver(t, SignalResolverConfig{MaxCandidatesPerKey: 2, MaxReportersPerCand: 4})
	for i := 1; i <= 4; i++ {
		lossTestAdmit(t, r, d, key, "K1ABC", fmt.Sprintf("N0A%d", i), at, 2*time.Minute)
	}
	for i := 1; i <= 3; i++ {
		lossTestAdmit(t, r, d, key, "K1ABD", fmt.Sprintf("N0A%d", i), at, 2*time.Minute)
	}
	d.Sweep(at.Add(time.Second))
	lossMetricsWant(t, r, 0, 0, 0, 0, 1)
	if before := lossTestSnapshot(t, r, key); before.TotalReporters != 4 || before.ReceiverEvidence().Len() != 7 {
		t.Fatal("shared receiver fixture did not distinguish reporters from incidences")
	}
	lossTestAdmit(t, r, d, key, "K1ABE", "N0C1", at.Add(time.Second+time.Millisecond), 2*time.Minute)
	// Evicting B loses its three observation rows, even though all three
	// receivers still support A. Evicting A's oldest receiver then loses one row.
	lossMetricsWant(t, r, 3, 0, 0, 1, 1)
	if row, exists := lossTestRow(lossTestSnapshot(t, r, key), "K1ABD", "N0A1"); exists {
		t.Fatalf("candidate eviction did not remove B: %+v", row)
	}
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A5", at.Add(time.Second+2*time.Millisecond), 2*time.Minute)
	lossMetricsWant(t, r, 4, 0, 0, 1, 1)
	for i, call := range []string{"K1ABF", "K1ABG"} {
		lossTestAdmit(t, r, d, key, call, fmt.Sprintf("N0D%d", i), at.Add(time.Second+time.Duration(i+3)*time.Millisecond), 2*time.Minute)
	}
	lossMetricsWant(t, r, 6, 0, 0, 1, 1)
	metrics := r.MetricsSnapshot()
	if metrics.EvictedCandidates != 3 || metrics.EvictedReporters != 1 || metrics.PublishedReceiverRecords != 5 {
		t.Fatalf("incidence stimulus did not exercise both owner caps %+v", metrics)
	}
	// Repeated reads at unrelated query clocks cannot turn cumulative counters
	// or the latest owner-publication gauge into query-age state.
	for i := 0; i < 20; i++ {
		if s, ok := r.LookupEligible(key, at.Add(time.Hour)); !ok || s.EvidenceLoss || s.TotalReporters != 0 {
			t.Fatalf("query expiry fixture %+v / %v", s, ok)
		}
		if s, ok := r.Lookup(key); !ok || !s.EvidenceLoss {
			t.Fatal("query expiry changed the raw publication")
		}
		lossMetricsWant(t, r, 6, 0, 0, 1, 1)
	}
}

func TestResolverLossMetricsRefusedReceiptIsOneIncidence(t *testing.T) {
	for _, candidate := range []bool{true, false} {
		t.Run(fmt.Sprintf("candidate-cap=%v", candidate), func(t *testing.T) {
			r, d, key, at := lossTestDriver(t, SignalResolverConfig{MaxCandidatesPerKey: 1})
			// The public constructor guarantees positive caps. Force a private
			// zero-cap fixture only for the retained defensive refusal branches;
			// this is not a reachable refusal under valid production configuration.
			if candidate {
				r.cfg.MaxCandidatesPerKey = 0
			} else {
				r.cfg.MaxReportersPerCand = 0
			}
			lossTestAdmit(t, r, d, key, "K1ABD", "N0NEW", at, time.Minute)
			st := d.states[key]
			lossMetricsWant(t, r, 1, 0, 0, 1, 1)
			metrics := r.MetricsSnapshot()
			if candidate && (metrics.DropMaxCandidates != 1 || metrics.DropMaxReporters != 0) || !candidate && (metrics.DropMaxCandidates != 0 || metrics.DropMaxReporters != 1) {
				t.Fatal("defensive cap fixture exercised the wrong refusal")
			}
			if r.ownerAdmitted.Load() || len(st.reporterRefs) != 0 || st.cell.activity.Load() != 0 {
				t.Fatal("defensive cap refusal became admitted evidence")
			}
			if s := lossTestSnapshot(t, r, key); !s.EvidenceLoss || s.TotalReporters != 0 || s.ReceiverEvidence().Len() != 0 {
				t.Fatalf("refused incoming evidence was tallied %+v", s)
			}
		})
	}
}

func lossMetricsOldDiscard(t testing.TB, cfg SignalResolverConfig) (*SignalResolver, *SignalResolverDriver, ResolverSignalKey, time.Time) {
	t.Helper()
	cfg.MaxReportersPerCand = 1
	r, d, key, at := lossTestDriver(t, cfg)
	lossTestAdmit(t, r, d, key, "K1ABC", "N0OLD", at.Add(-100*time.Second), 2*time.Minute)
	lossTestAdmit(t, r, d, key, "K1ABC", "N0NEW", at, 2*time.Minute)
	lossMetricsWant(t, r, 1, 0, 0, 1, 1)
	return r, d, key, at
}

func TestResolverLossMetricsRecoverOnlyOnOwnerPublication(t *testing.T) {
	r, d, key, at := lossMetricsOldDiscard(t, SignalResolverConfig{})
	for i := 0; i < 20; i++ {
		if s, ok := r.LookupEligible(key, at.Add(21*time.Second)); !ok || s.EvidenceLoss || s.TotalReporters != 1 {
			t.Fatalf("eligible expiry %+v / %v", s, ok)
		}
		lossMetricsWant(t, r, 1, 0, 0, 1, 1)
	}
	lossTestAdmit(t, r, d, key, "K1ABC", "N0NEW", at.Add(time.Millisecond), 20*time.Second)
	lossMetricsWant(t, r, 1, 1, 0, 0, 1)
	d.Sweep(at.Add(500 * time.Millisecond))
	lossMetricsWant(t, r, 1, 1, 0, 0, 1)
	// Window expansion reactivates protection, with no new discarded observation
	// or recovery. A later real true-to-false publication is a second recovery.
	lossTestAdmit(t, r, d, key, "K1ABC", "N0NEW", at.Add(600*time.Millisecond), 2*time.Minute)
	for i := 0; i < 20; i++ {
		lossMetricsWant(t, r, 1, 1, 0, 1, 1)
		r.Lookup(key)
		r.LookupEligible(key, at)
	}
	lossTestAdmit(t, r, d, key, "K1ABC", "N0NEW", at.Add(601*time.Millisecond), 20*time.Second)
	lossMetricsWant(t, r, 1, 2, 0, 0, 1)
	d.Sweep(at.Add(time.Second))
	lossMetricsWant(t, r, 1, 2, 0, 0, 1)
}

func TestResolverLossMetricsOrdinaryExpiryPublicationRecoversOnce(t *testing.T) {
	r, d, key, at := lossMetricsOldDiscard(t, SignalResolverConfig{})
	d.Sweep(at.Add(21 * time.Second))
	if s := lossTestSnapshot(t, r, key); s.EvidenceLoss || s.TotalReporters != 1 {
		t.Fatalf("owner expiry failed to publish recovery %+v", s)
	}
	lossMetricsWant(t, r, 1, 1, 0, 0, 1)
	d.Sweep(at.Add(22 * time.Second))
	lossMetricsWant(t, r, 1, 1, 0, 0, 1)
}

func TestResolverLossMetricsLogicalIdleGaugeAndPhysicalRetirement(t *testing.T) {
	for _, arrival := range []bool{false, true} {
		t.Run(fmt.Sprintf("retire-on-arrival=%v", arrival), func(t *testing.T) {
			r, d, key, at, old := lossTestQuarantine(t, SignalResolverConfig{})
			lossMetricsWant(t, r, 1, 0, 0, 1, 1)
			if err := d.SetProcessElapsed(10 * time.Minute); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 20; i++ {
				if _, ok := r.Lookup(key); ok {
					t.Fatal("logically idle raw generation readable")
				}
				if _, ok := r.LookupEligible(key, at); ok {
					t.Fatal("logically idle eligible generation readable")
				}
				lossMetricsWant(t, r, 1, 0, 0, 0, 1)
			}
			if arrival {
				lossTestAdmit(t, r, d, key, "K1ABD", "N0NEW", at, time.Hour)
				if current := lossTestSnapshot(t, r, key); current.Generation == old.Generation || current.EvidenceLoss {
					t.Fatal("arrival failed to create independent generation")
				}
				lossMetricsWant(t, r, 1, 0, 1, 0, 1)
			} else {
				d.Sweep(at)
				lossMetricsWant(t, r, 1, 0, 1, 0, 0)
				lossTestAdmit(t, r, d, key, "K1ABD", "N0NEW", at, time.Hour)
				if current := lossTestSnapshot(t, r, key); current.Generation == old.Generation || current.EvidenceLoss {
					t.Fatal("swept retirement did not create independent generation")
				}
				lossMetricsWant(t, r, 1, 0, 1, 0, 1)
			}
			d.Sweep(at)
			d.Sweep(at)
			lossMetricsWant(t, r, 1, 0, 1, 0, 1)
		})
	}
}

func TestResolverLossMetricsCleanupIsNeitherRecoveryNorIdleRetirement(t *testing.T) {
	for _, ttl := range []bool{false, true} {
		t.Run(fmt.Sprintf("observation-ttl=%v", ttl), func(t *testing.T) {
			cfg := SignalResolverConfig{}
			if ttl {
				cfg.InactiveTTL = 5 * time.Second
			}
			r, d, key, at := lossMetricsOldDiscard(t, cfg)
			query := at.Add(121 * time.Second)
			if ttl {
				query = at.Add(21 * time.Second)
			}
			d.Sweep(query)
			if _, ok := r.Lookup(key); ok || len(d.states) != 0 {
				t.Fatal("cleanup fixture still retained generation")
			}
			lossMetricsWant(t, r, 1, 0, 0, 0, 0)
			d.Sweep(query.Add(time.Second))
			lossMetricsWant(t, r, 1, 0, 0, 0, 0)
		})
	}
	if metrics := (*SignalResolver)(nil).MetricsSnapshot(); metrics.PublishedLossKeys != 0 || metrics.CapLossDiscards != 0 || metrics.LossPublicationRecoveries != 0 || metrics.LossIdleRetirements != 0 {
		t.Fatalf("nil metrics %+v", metrics)
	}
}
