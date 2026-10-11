// File role: Checks SNR presence through real resolver admission, representative
// metadata updates, immutable publication, and fresh receiver projection.
package spot

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func b22SNRPresenceOwner(t *testing.T) (*SignalResolver, *SignalResolverDriver, ResolverSignalKey, time.Time) {
	t.Helper()
	r, d := b21ResolverDriver(t, SignalResolverConfig{
		ConfusionModel: b22SNRDomainModels(t)["direct"], ConfusionWeight: 1,
	})
	return r, d, NewResolverSignalKey(14020, "20m", "CW", 500), time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
}

func b22SNRPresenceEvidence(key ResolverSignalKey, call, receiver string, at time.Time, report int, present bool, window time.Duration) ResolverEvidence {
	return ResolverEvidence{ObservedAt: at, Key: key, DXCall: call, Spotter: receiver,
		Report: report, HasReport: present, FrequencyKHz: 14020, RecencyWindow: window}
}

func b22SNRPresenceAdmit(t *testing.T, r *SignalResolver, d *SignalResolverDriver, evidence ...ResolverEvidence) ResolverSnapshot {
	t.Helper()
	if len(evidence) == 0 {
		t.Fatal("presence fixture needs evidence")
	}
	key := evidence[0].Key
	sweepAt := evidence[0].ObservedAt.Add(time.Nanosecond)
	for _, ev := range evidence {
		if !r.Enqueue(ev) {
			t.Fatal("presence fixture evidence was rejected at ingress")
		}
		if ev.ObservedAt.After(sweepAt) {
			sweepAt = ev.ObservedAt.Add(time.Nanosecond)
		}
	}
	if got := d.Drain(); got != len(evidence) {
		t.Fatalf("processed %d receipts; want %d", got, len(evidence))
	}
	if st := d.states[key]; st != nil && st.nextEvalAt.After(sweepAt) {
		sweepAt = st.nextEvalAt
	}
	d.Sweep(sweepAt)
	snapshot, ok := r.Lookup(key)
	if !ok {
		t.Fatal("owner did not publish admitted evidence")
	}
	return snapshot
}

func b22SNRPresenceRequireTie(t *testing.T, snapshot ResolverSnapshot, winner string) {
	t.Helper()
	if snapshot.Winner != winner || snapshot.TotalReporters != 2 || snapshot.TotalWeightedSupportMilli != 2000 || snapshot.ReceiverEvidence().Len() != 2 {
		t.Fatalf("tie publication=%+v rows=%d; want winner=%s and two retained unit-weight receivers", snapshot, snapshot.ReceiverEvidence().Len(), winner)
	}
	freshProjectionCandidate(t, snapshot, "K1AAA", 1, 1000)
	freshProjectionCandidate(t, snapshot, "K1AAB", 1, 1000)
}

func TestB22ResolverSNRPresenceControlsOnlyObservedContribution(t *testing.T) {
	model := b22SNRDomainModels(t)["direct"]
	// A candidate's own missing SNR does not disable scoring. Only the other
	// observed identity's present, in-domain SNR supplies this contribution.
	for _, tc := range []struct {
		name    string
		report  int
		present bool
		want    float64
	}{
		{"missing_nonzero", 5, false, 0},
		{"reported_zero", 0, true, 1000 * math.Log(4.0/10)},
		{"exclusive_lower", -10, true, 0},
		{"above_upper", 11, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := rankedResolverCandidate{call: "K1AAA", callRunes: []rune("K1AAA"), weightedSupportMilli: 1000}
			observed := rankedResolverCandidate{call: "K1AAB", callRunes: []rune("K1AAB"),
				lastReport: tc.report, lastHasReport: tc.present, weightedSupportMilli: 1000}
			var workspace confusionAlignmentWorkspace
			got := resolverConfusionAggregateScore(candidate, []rankedResolverCandidate{candidate, observed}, "CW", model, &workspace)
			if math.IsNaN(got) || math.IsInf(got, 0) || math.Abs(got-tc.want) > 1e-12 {
				t.Errorf("observed contribution=%v, want literal %v", got, tc.want)
			}
		})
	}
}

func TestB22ResolverAdmissionPreservesSNRPresenceAndSupport(t *testing.T) {
	for _, tc := range []struct {
		name     string
		aReport  int
		aPresent bool
		bReport  int
		bPresent bool
		winner   string
	}{
		{"both_missing_nonzero", 5, false, 5, false, "K1AAA"},
		{"both_reported_zero", 0, true, 0, true, "K1AAB"},
		{"only_B_observed_present", 5, false, 0, true, "K1AAB"},
		{"only_A_observed_present", 0, true, 5, false, "K1AAA"},
		{"exclusive_lower_does_not_break_tie", -10, true, -10, true, "K1AAA"},
		{"above_upper_does_not_break_tie", 11, true, 11, true, "K1AAA"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, d, key, at := b22SNRPresenceOwner(t)
			a := b22SNRPresenceEvidence(key, "K1AAA", "N0AAA", at, tc.aReport, tc.aPresent, time.Minute)
			b := b22SNRPresenceEvidence(key, "K1AAB", "N0BBB", at, tc.bReport, tc.bPresent, time.Minute)
			for _, ev := range []ResolverEvidence{a, b} {
				normalized, ok := normalizeResolverEvidence(ev)
				if !ok || normalized.Report != ev.Report || normalized.HasReport != ev.HasReport {
					t.Fatalf("normalization changed SNR value/presence: %+v accepted=%t", normalized, ok)
				}
				if !r.Enqueue(ev) {
					t.Fatal("evidence was not queued")
				}
				// The producer's by-value evidence can be reused immediately.
				ev.Report, ev.HasReport = 99, !ev.HasReport
			}
			if d.Drain() != 2 {
				t.Fatal("expected both queued receivers to be processed")
			}
			d.Sweep(at.Add(time.Nanosecond))
			snapshot, ok := r.Lookup(key)
			if !ok {
				t.Fatal("missing owner publication")
			}
			b22SNRPresenceRequireTie(t, snapshot, tc.winner)
			for _, original := range []ResolverEvidence{a, b} {
				candidate := d.states[key].candidates[original.DXCall]
				if candidate.lastReport != original.Report || candidate.lastHasReport != original.HasReport {
					t.Errorf("%s queued metadata changed: report=%d present=%t", original.DXCall, candidate.lastReport, candidate.lastHasReport)
				}
			}
			fresh, ok := r.LookupEligible(key, at)
			if !ok || !reflect.DeepEqual(fresh, snapshot) {
				t.Fatal("all-fresh projection changed SNR-derived ranking or receiver support")
			}
			compact := snapshot.Compact()
			b22SNRPresenceRequireTie(t, snapshot, tc.winner)
			if compact.ReceiverEvidence() != nil || compact.Winner != tc.winner || compact.TotalReporters != 2 || compact.TotalWeightedSupportMilli != 2000 || !reflect.DeepEqual(compact.CandidateRanks, snapshot.CandidateRanks) {
				t.Fatalf("Compact lost scoring/support tallies: %+v", compact)
			}
		})
	}
}

func TestB22ResolverSNREqualTimeUsesLaterReceiptPresence(t *testing.T) {
	for _, tc := range []struct {
		name        string
		firstReport int
		first       bool
		lastReport  int
		last        bool
		winner      string
	}{
		{"present_then_missing", 0, true, 5, false, "K1AAA"},
		{"missing_then_present_zero", 5, false, 0, true, "K1AAB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, d, key, at := b22SNRPresenceOwner(t)
			snapshot := b22SNRPresenceAdmit(t, r, d,
				b22SNRPresenceEvidence(key, "K1AAA", "N0AAA", at, 5, false, time.Minute),
				b22SNRPresenceEvidence(key, "K1AAB", "N0BBB", at, tc.firstReport, tc.first, time.Minute),
				b22SNRPresenceEvidence(key, "K1AAB", "N0BBB", at, tc.lastReport, tc.last, time.Minute))
			b22SNRPresenceRequireTie(t, snapshot, tc.winner)
			candidate := d.states[key].candidates["K1AAB"]
			if candidate.lastReport != tc.lastReport || candidate.lastHasReport != tc.last || !candidate.lastSeen.Equal(at) {
				t.Fatalf("equal-time representative did not follow later receipt: %+v", candidate)
			}
		})
	}
}

func TestB22ResolverNewerMissingSNRClearsPresenceAndOlderCannotRestore(t *testing.T) {
	r, d, key, at := b22SNRPresenceOwner(t)
	published := b22SNRPresenceAdmit(t, r, d,
		b22SNRPresenceEvidence(key, "K1AAA", "N0AAA", at, 5, false, time.Minute),
		b22SNRPresenceEvidence(key, "K1AAB", "N0BBB", at, 0, true, time.Minute))
	b22SNRPresenceRequireTie(t, published, "K1AAB")
	compact, copied := published.Compact(), published
	newest := at.Add(time.Second)
	current := b22SNRPresenceAdmit(t, r, d,
		b22SNRPresenceEvidence(key, "K1AAB", "N0BBB", newest, 5, false, time.Minute),
		b22SNRPresenceEvidence(key, "K1AAA", "N0AAA", newest, 5, false, time.Minute))
	// The unchanged tie hysteresis keeps the prior identity but withdraws
	// authority: omitted SNR cannot invent a positive switching margin.
	if current.State != ResolverStateUncertain || current.Winner != "K1AAB" || current.CandidateRanks[0].Call != "K1AAA" {
		t.Fatalf("missing newest SNR retained confusion authority: %+v", current)
	}
	b22SNRPresenceRequireTie(t, current, "K1AAB")
	candidate := d.states[key].candidates["K1AAB"]
	if candidate.lastHasReport || candidate.lastReport != 5 || !candidate.lastSeen.Equal(newest) {
		t.Fatalf("newest missing receipt reused older reported zero: %+v", candidate)
	}
	current = b22SNRPresenceAdmit(t, r, d,
		b22SNRPresenceEvidence(key, "K1AAB", "N0BBB", at.Add(500*time.Millisecond), 0, true, time.Minute))
	if candidate.lastHasReport || candidate.lastReport != 5 || !candidate.lastSeen.Equal(newest) || current.State != ResolverStateUncertain || current.CandidateRanks[0].Call != "K1AAA" {
		t.Fatalf("older receipt restored obsolete SNR authority: candidate=%+v snapshot=%+v", candidate, current)
	}
	b22SNRPresenceRequireTie(t, current, "K1AAB")
	if !reflect.DeepEqual(copied, published) || copied.ReceiverEvidence() == current.ReceiverEvidence() || compact.Winner != "K1AAB" || compact.TotalReporters != 2 || !reflect.DeepEqual(compact.CandidateRanks, copied.CandidateRanks) {
		t.Fatal("later representative mutation changed a retained immutable publication or compact tallies")
	}
	for _, retained := range []ResolverSnapshot{published, copied, compact} {
		if retained.State != ResolverStateProbable || len(retained.CandidateRanks) != 2 || retained.CandidateRanks[0].Call != "K1AAB" || retained.CandidateRanks[1].Call != "K1AAA" {
			t.Fatalf("later missing SNR rewrote the prior reported-zero ranking: %+v", retained)
		}
		freshProjectionCandidate(t, retained, "K1AAA", 1, 1000)
		freshProjectionCandidate(t, retained, "K1AAB", 1, 1000)
	}
	copied.ReceiverEvidence().Range(func(_ string, row ResolverReceiverObservation) bool {
		if !row.ObservedAt.Equal(at) {
			t.Errorf("retained receiver publication changed timestamp: %+v", row)
		}
		return true
	})
}

func TestB22ResolverSNRNewestRepresentativeAndPartialFreshProjection(t *testing.T) {
	r, d, key, at := b22SNRPresenceOwner(t)
	window := 10 * time.Second
	b22SNRPresenceAdmit(t, r, d,
		b22SNRPresenceEvidence(key, "K1AAA", "N0AAA", at, 9, true, window))
	newest := at.Add(time.Second)
	published := b22SNRPresenceAdmit(t, r, d,
		b22SNRPresenceEvidence(key, "K1AAA", "N0AAB", newest, 0, true, window),
		b22SNRPresenceEvidence(key, "K1AAB", "N0BBA", newest, 0, true, window),
		b22SNRPresenceEvidence(key, "K1AAB", "N0BBB", newest, 0, true, window))
	// At zero, B's aggregate is 2000*log(2/4), above A's
	// 2000*log(4/10). Reusing A's older second-band SNR would favor A.
	if published.Winner != "K1AAB" || published.TotalReporters != 4 || published.TotalWeightedSupportMilli != 4000 || published.ReceiverEvidence().Len() != 4 {
		t.Fatalf("representative SNR did not use newest timestamp: %+v", published)
	}
	freshProjectionCandidate(t, published, "K1AAA", 2, 2000)
	freshProjectionCandidate(t, published, "K1AAB", 2, 2000)
	candidate := d.states[key].candidates["K1AAA"]
	if !candidate.lastHasReport || candidate.lastReport != 0 || !candidate.lastSeen.Equal(newest) {
		t.Fatalf("newest reported zero was not represented: %+v", candidate)
	}
	fresh, ok := r.LookupEligible(key, at.Add(11*time.Second))
	if !ok || fresh.Winner != "K1AAB" || fresh.State != ResolverStateConfident || fresh.TotalReporters != 3 || fresh.TotalWeightedSupportMilli != 3000 || fresh.ReceiverEvidence().Len() != 3 {
		t.Fatalf("partial fresh projection=%+v available=%t; want B2/A1 over three receivers", fresh, ok)
	}
	freshProjectionCandidate(t, fresh, "K1AAA", 1, 1000)
	freshProjectionCandidate(t, fresh, "K1AAB", 2, 2000)
	if fresh.ReceiverEvidence() == published.ReceiverEvidence() || published.ReceiverEvidence().Len() != 4 || published.TotalReporters != 4 {
		t.Fatal("projection mutated the immutable owner publication")
	}
	compact := published.Compact()
	if compact.ReceiverEvidence() != nil || compact.Winner != "K1AAB" || compact.TotalReporters != 4 || compact.TotalWeightedSupportMilli != 4000 || !reflect.DeepEqual(compact.CandidateRanks, published.CandidateRanks) {
		t.Fatal("Compact lost SNR-derived ranking or distinct receiver support")
	}
}
