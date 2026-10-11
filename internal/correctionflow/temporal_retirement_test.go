package correctionflow

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func temporalRetirementDecoder(action string) *TemporalDecoder {
	return NewTemporalDecoder(config.CallCorrectionConfig{TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{
		Enabled: true, Scope: "all_correction_candidates", LagSeconds: 1, MaxWaitSeconds: 5,
		BeamSize: 4, MaxObsCandidates: 4, MinMarginScore: 80, OverflowAction: action,
		MaxPending: 64, MaxActiveKeys: 1, MaxEventsPerKey: 1,
	}})
}

func temporalRetirementObserve(t *testing.T, d *TemporalDecoder, id uint64, key spot.ResolverSignalKey, at time.Time, strong bool) {
	t.Helper()
	a, b := 2500, 2500
	if strong {
		a, b = 4500, 500
	}
	if ok, reason := d.Observe(TemporalObservation{ID: id, ObservedAt: at, Key: key, SubjectCall: "K1ABD",
		Selection: testTemporalSelection(key, spot.ResolverStateProbable, "K1ABC", a, "K1ABD", b)}); !ok {
		t.Fatalf("Observe(%d) refused: %s", id, reason)
	}
}

// One retained event evicts an older deferred request, without retiring its
// ownership. Only the final request's terminal decision may reclaim the key.
func TestTemporalRetirementReclaimsFinalEvictedRequest(t *testing.T) {
	for _, action := range []string{"fallback_resolver", "abstain", "bypass"} {
		for _, oldFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/old_first_%t", action, oldFirst), func(t *testing.T) {
				d := temporalRetirementDecoder(action)
				at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
				key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
				d.ConfirmCorrection(key, "K1ABC")
				temporalRetirementObserve(t, d, 1, key, at, false)
				if got := d.Evaluate(1, at.Add(time.Second), false); got.Action != TemporalDecisionActionDefer {
					t.Fatalf("older tie did not defer: %+v", got)
				}
				temporalRetirementObserve(t, d, 2, key, at.Add(time.Second), true)
				old := func() {
					retireAt := at.Add(5 * time.Second)
					if oldFirst {
						retireAt = at.Add(2 * time.Second)
					}
					got := d.Evaluate(1, retireAt, oldFirst)
					if got.Reason != "event_evicted" || got.Action != d.fallbackActionLocked() {
						t.Fatalf("evicted retirement: %+v", got)
					}
				}
				newer := func() {
					if got := d.Evaluate(2, at.Add(2*time.Second), false); got.Action != TemporalDecisionActionApply {
						t.Fatalf("newer retained event did not propose: %+v", got)
					}
				}
				if oldFirst {
					old()
				} else {
					newer()
				}
				if d.PendingCount() != 1 || len(d.keyStates) != 1 {
					t.Fatal("first retirement deleted the other request's key")
				}
				if oldFirst {
					newer()
				} else {
					old()
				}
				if d.PendingCount() != 0 || len(d.keyStates) != 0 {
					t.Fatalf("terminal ownership leaked: pending=%d keys=%d", d.PendingCount(), len(d.keyStates))
				}
				if d.confirmationSequence != 1 || d.lastCommitted[key].call != "K1ABC" || d.ConfirmedHistoryMetrics().Keys != 1 {
					t.Fatal("retirement or proposal mutated accepted identity history")
				}
				if got := d.Evaluate(1, at.Add(6*time.Second), true); got.Reason != "missing_request" {
					t.Fatal("terminal request evaluated twice")
				}
				key.Bucket++
				temporalRetirementObserve(t, d, 3, key, at.Add(6*time.Second), true)
			})
		}
	}
}

func TestTemporalRetirementOrdinaryForcedMissingAndChurn(t *testing.T) {
	for _, action := range []string{"fallback_resolver", "abstain", "bypass"} {
		t.Run(action, func(t *testing.T) {
			d := temporalRetirementDecoder(action)
			at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
			key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
			for i := 0; i < 120; i++ {
				key.Bucket++
				id := uint64(i + 1)
				temporalRetirementObserve(t, d, id, key, at, i%3 == 0)
				if i%3 == 2 {
					// Exercise the defensive missing-state terminal path.
					delete(d.keyStates, key)
				}
				got := d.Evaluate(id, at.Add(500*time.Millisecond), true)
				if got.Action == TemporalDecisionActionDefer || d.PendingCount() != 0 || len(d.keyStates) != 0 {
					t.Fatalf("forced terminal churn retained ownership: %+v", got)
				}
				if i%3 == 2 && got.Reason != "missing_key_state" {
					t.Fatal("missing-state control did not reach its terminal path")
				}
			}
		})
	}
}

func TestTemporalRetirementConcurrentSameKey(t *testing.T) {
	d := temporalRetirementDecoder("abstain")
	at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	d.ConfirmCorrection(key, "K1ABC")
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Go(func() {
			for j := 0; j < 20; j++ {
				id := uint64(i*20 + j + 1)
				if ok, reason := d.Observe(TemporalObservation{ID: id, ObservedAt: at, Key: key, SubjectCall: "K1ABD",
					Selection: testTemporalSelection(key, spot.ResolverStateProbable, "K1ABC", 2500, "K1ABD", 2500)}); !ok {
					t.Errorf("concurrent Observe(%d) refused: %s", id, reason)
					return
				}
				if got := d.Evaluate(id, at, true); got.Action == TemporalDecisionActionDefer {
					t.Error("forced concurrent request retained ownership")
				}
			}
		})
	}
	wg.Wait()
	if d.PendingCount() != 0 || len(d.keyStates) != 0 || d.confirmationSequence != 1 || len(d.lastCommitted) != 1 {
		t.Fatalf("concurrent terminal state: pending=%d keys=%d sequence=%d history=%d",
			d.PendingCount(), len(d.keyStates), d.confirmationSequence, len(d.lastCommitted))
	}
}

func TestTemporalRetirementEvictedKeyChurn(t *testing.T) {
	d := temporalRetirementDecoder("abstain")
	at := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	key := spot.NewResolverSignalKey(7010, "40m", "CW", 500)
	for i := 0; i < 128; i++ {
		key.Bucket++
		first := uint64(i*2 + 1)
		temporalRetirementObserve(t, d, first, key, at, false)
		temporalRetirementObserve(t, d, first+1, key, at.Add(time.Second), true)
		d.Evaluate(first+1, at.Add(2*time.Second), false)
		d.Evaluate(first, at.Add(5*time.Second), false)
		if d.PendingCount() != 0 || len(d.keyStates) != 0 {
			t.Fatalf("key churn %d retained pending/event state", i)
		}
	}
}
