//go:build qualification

package peer

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// retryEvidence stores bounded facts for one identity. Production nextDue and
// healthySince are deliberately absent from the timing oracle.
type retryEvidence struct {
	mu       sync.Mutex
	call     string
	events   [256]QualificationAdmissionEvent
	count    int
	overflow bool
}

func (e *retryEvidence) observe(event QualificationAdmissionEvent) {
	if event.Call != e.call {
		return
	}
	switch event.Kind {
	case "failure", "startup_grant", "recovery_flush", "established", "healthy_reset", "retired", "ingress_invalidated":
	default:
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.count == len(e.events) {
		e.overflow = true
		return
	}
	e.events[e.count] = event
	e.count++
}

func (e *retryEvidence) latest(kind string) (QualificationAdmissionEvent, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.overflow {
		return QualificationAdmissionEvent{}, fmt.Errorf("retry evidence overflow")
	}
	for i := e.count - 1; i >= 0; i-- {
		if e.events[i].Kind == kind {
			return e.events[i], nil
		}
	}
	return QualificationAdmissionEvent{}, nil
}

// check derives delay from literal fixture settings and reset timing from
// matching successful establishment and local C/A Flush facts. Initial failure
// can precede observer installation's first startup event. Global-gate episodes
// are tested separately; this oracle requires an uninterrupted admission retry.
func (e *retryEvidence) check(base, maximum time.Duration, requireReset bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	// Observation time follows the synchronized event snapshot. A callback
	// arriving between a caller's time.Now and this lock is not future evidence.
	observed := time.Now()
	if e.overflow || base <= 0 || maximum < base {
		return fmt.Errorf("invalid or overflowed retry evidence")
	}
	var sequence, generation, failedGeneration, invalidatedGeneration uint64
	var failedAt, lastGrant, established, flushed time.Time
	delay := base
	failures, grants, resets := 0, 0, 0
	for _, event := range e.events[:e.count] {
		if event.Sequence <= sequence || event.Generation == 0 || event.At.IsZero() || event.At.After(observed) {
			return fmt.Errorf("invalid retry sequence, generation or timestamp: %+v", event)
		}
		sequence = event.Sequence
		switch event.Kind {
		case "failure":
			if event.Generation == failedGeneration || (generation != 0 && event.Generation != generation) {
				return fmt.Errorf("duplicate or stale attempt failure")
			}
			if failures > 0 {
				delay = min(maximum, delay*2)
			}
			failures++
			failedGeneration, failedAt = event.Generation, event.At
			generation, established, flushed = event.Generation, time.Time{}, time.Time{}
		case "ingress_invalidated":
			if event.Generation != failedGeneration || event.At.Before(failedAt) || invalidatedGeneration == event.Generation {
				return fmt.Errorf("missing, duplicate or stale ingress invalidation")
			}
			invalidatedGeneration = event.Generation
		case "startup_grant":
			if failures == 0 || event.Generation <= generation || invalidatedGeneration != failedGeneration || event.At.Before(failedAt.Add(delay)) {
				return fmt.Errorf("startup lacks invalidation, cooldown or fresh attempt")
			}
			if !lastGrant.IsZero() && event.At.Sub(lastGrant) < time.Second {
				return fmt.Errorf("startup grants violate one-second pacing")
			}
			generation, lastGrant = event.Generation, event.At
			established, flushed = time.Time{}, time.Time{}
			grants++
		case "established", "recovery_flush":
			if grants == 0 || event.Generation != generation || event.At.Before(lastGrant) || failedGeneration == generation {
				return fmt.Errorf("successful receipt belongs to missing, stale or failed attempt")
			}
			if event.Kind == "established" {
				if !established.IsZero() {
					return fmt.Errorf("duplicate established receipt")
				}
				established = event.At
			} else {
				if !flushed.IsZero() {
					return fmt.Errorf("duplicate recovery Flush receipt")
				}
				flushed = event.At
			}
		case "healthy_reset":
			if event.Generation != generation || established.IsZero() || flushed.IsZero() || resets != 0 {
				return fmt.Errorf("reset lacks matching establishment and recovery Flush")
			}
			start := established
			if flushed.After(start) {
				start = flushed
			}
			if event.At.Before(start.Add(60*time.Second)) || event.At.After(start.Add(61*time.Second)) {
				return fmt.Errorf("reset outside qualified 60-to-61-second interval")
			}
			resets++
		case "retired":
			if event.Generation == generation {
				established, flushed = time.Time{}, time.Time{}
			}
		}
	}
	if failures == 0 || grants == 0 || requireReset && resets != 1 {
		return fmt.Errorf("incomplete retry evidence: failures=%d grants=%d resets=%d", failures, grants, resets)
	}
	return nil
}

type retryCappedGrant struct {
	Call                               string
	FailureOrdinal                     int
	FailureGeneration, GrantGeneration uint64
	FailedAt, GrantedAt                time.Time
}

// cappedGrant proves the cap was exercised from actual failed-attempt and grant
// facts. A long test duration alone is insufficient evidence. The caller first
// checks the complete episode; extraction additionally rejects an early cap.
func (e *retryEvidence) cappedGrant(base, maximum time.Duration) (retryCappedGrant, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.overflow || base <= 0 || maximum < base {
		return retryCappedGrant{}, false, fmt.Errorf("invalid capped retry evidence")
	}
	delay, ordinal := base, 0
	var failure QualificationAdmissionEvent
	for _, event := range e.events[:e.count] {
		switch event.Kind {
		case "failure":
			if ordinal > 0 {
				delay = min(maximum, delay*2)
			}
			ordinal++
			failure = event
		case "startup_grant":
			if ordinal == 0 || delay != maximum {
				continue
			}
			if event.Generation <= failure.Generation || event.At.Before(failure.At.Add(maximum)) {
				return retryCappedGrant{}, false, fmt.Errorf("capped retry grant preceded its independent %s delay", maximum)
			}
			return retryCappedGrant{Call: e.call, FailureOrdinal: ordinal, FailureGeneration: failure.Generation,
				GrantGeneration: event.Generation, FailedAt: failure.At, GrantedAt: event.At}, true, nil
		case "healthy_reset":
			delay, ordinal = base, 0
			failure = QualificationAdmissionEvent{}
		}
	}
	return retryCappedGrant{}, false, nil
}

func TestPC92V14RetryCappedGrantEvidence(t *testing.T) {
	for _, scenario := range []string{"valid", "absent", "early_cap"} {
		t.Run(scenario, func(t *testing.T) {
			e := &retryEvidence{call: "GB7SRC"}
			at := time.Now().Add(-2000 * time.Second)
			var sequence uint64
			admit := func(kind string, generation uint64, at time.Time) {
				sequence++
				e.observe(QualificationAdmissionEvent{Call: e.call, Sequence: sequence, Kind: kind, Generation: generation, At: at})
			}
			// Literal external profile, independent of the production retry
			// delay helper. The ninth failure first reaches the 300s cap.
			for index, seconds := range []int{2, 4, 8, 16, 32, 64, 128, 256, 300} {
				if scenario == "absent" && index == 8 {
					break
				}
				generation := uint64(index + 1)
				admit("failure", generation, at)
				admit("ingress_invalidated", generation, at.Add(time.Millisecond))
				grant := at.Add(time.Duration(seconds) * time.Second)
				if scenario == "early_cap" && index == 8 {
					grant = grant.Add(-time.Nanosecond)
				}
				admit("startup_grant", generation+1, grant)
				at = grant.Add(time.Second)
			}
			ordinaryErr := e.check(2*time.Second, 300*time.Second, false)
			fact, found, err := e.cappedGrant(2*time.Second, 300*time.Second)
			if scenario == "early_cap" {
				if ordinaryErr == nil || err == nil || found {
					t.Fatalf("early capped grant passed: ordinary=%v cap=%v found=%v", ordinaryErr, err, found)
				}
				return
			}
			if ordinaryErr != nil || err != nil || found != (scenario == "valid") {
				t.Fatalf("scenario=%s ordinary=%v cap=%v found=%v", scenario, ordinaryErr, err, found)
			}
			if found && (fact.FailureOrdinal != 9 || fact.FailureGeneration != 9 || fact.GrantGeneration != 10 || fact.GrantedAt.Sub(fact.FailedAt) != 300*time.Second) {
				t.Fatalf("incorrect cap evidence: %+v", fact)
			}
		})
	}
}

func TestPC92V14RetryOracleNegativeControls(t *testing.T) {
	for _, scenario := range []string{"valid", "early_grant", "missing_invalidation", "wrong_generation", "duplicate_failure", "missing_flush", "initial_flush", "early_reset", "late_reset", "missing_reset", "future_event", "stale_sequence", "canceled", "overflow"} {
		t.Run(scenario, func(t *testing.T) {
			start := time.Now().Add(-70 * time.Second)
			e := &retryEvidence{call: "GB7SRC"}
			events := []QualificationAdmissionEvent{
				{Kind: "failure", Generation: 1, At: start},
				{Kind: "ingress_invalidated", Generation: 1, At: start.Add(time.Millisecond)},
				{Kind: "startup_grant", Generation: 2, At: start.Add(2 * time.Second)},
				{Kind: "established", Generation: 2, At: start.Add(3 * time.Second)},
				{Kind: "recovery_flush", Generation: 2, At: start.Add(4 * time.Second)},
				{Kind: "healthy_reset", Generation: 2, At: start.Add(64 * time.Second)},
			}
			switch scenario {
			case "early_grant":
				events[2].At = start.Add(time.Second)
			case "missing_invalidation":
				events[1].Kind = "ignored"
			case "wrong_generation":
				events[4].Generation = 1
			case "duplicate_failure":
				events[1].Kind = "failure"
			case "missing_flush":
				events[4].Kind = "ignored"
			case "initial_flush":
				events[4].At = start.Add(time.Second)
			case "early_reset":
				events[5].At = start.Add(63 * time.Second)
			case "late_reset":
				events[5].At = start.Add(66 * time.Second)
			case "missing_reset":
				events[5].Kind = "ignored"
			case "future_event":
				events[5].At = time.Now().Add(time.Second)
			case "canceled":
				events[4].Kind = "retired"
			}
			for i, event := range events {
				event.Call, event.Sequence = e.call, uint64(i+1)
				if scenario == "stale_sequence" && i == 4 {
					event.Sequence = 1
				}
				e.observe(event)
			}
			e.overflow = scenario == "overflow"
			err := e.check(2*time.Second, 300*time.Second, true)
			if (scenario == "valid") != (err == nil) {
				t.Fatalf("scenario=%s err=%v", scenario, err)
			}
		})
	}
}

func TestPC92V14RetryObserverOverflow(t *testing.T) {
	e := &retryEvidence{call: "GB7SRC"}
	for index := 0; index <= len(e.events); index++ {
		e.observe(QualificationAdmissionEvent{Kind: "failure", Call: e.call, Generation: uint64(index + 1), Sequence: uint64(index + 1), At: time.Now()})
	}
	if !e.overflow || e.count != len(e.events) {
		t.Fatal("observer grew or failed to report bounded evidence overflow")
	}
	if _, err := e.latest("failure"); err == nil {
		t.Fatal("overflow treated as complete evidence")
	}
}

func TestPC92V12Q5ObservationDoesNotPrune(t *testing.T) {
	c := newBoundedDedupe(600*time.Second, 2, 64)
	at := time.Now().Add(-601 * time.Second)
	if c.admit("old", at) != dedupeAccepted {
		t.Fatal("setup failed")
	}
	if observed, present := q5StoredAdmission(c, "old"); !present || !observed.Equal(at) {
		t.Fatalf("lost original age: %s present=%v", observed, present)
	}
	if q5CacheHasLive(c, "old") {
		t.Fatal("old cache age treated as live")
	}
	if count, _, _ := c.occupancy(); count != 1 {
		t.Fatal("qualification observation pruned its own evidence")
	}
}

func TestPC92V12Q5RejectsDuplicateAgeRenewal(t *testing.T) {
	c := newBoundedDedupe(600*time.Second, 2, 64)
	at := time.Now()
	if c.admit("first", at) != dedupeAccepted {
		t.Fatal("setup failed")
	}
	if err := q5VerifyOriginalAdmission(c, "first", at); err != nil {
		t.Fatal(err)
	}
	// Fault injection exercises the actual checker, without cleanup helpers.
	c.mu.Lock()
	c.items.Set("first", int64(time.Second))
	c.mu.Unlock()
	if err := q5VerifyOriginalAdmission(c, "first", at); err == nil {
		t.Fatal("Q5 accepted duplicate renewal hidden by another key's expiry")
	}
}

func TestPC92V14Q5ExpiryOracle(t *testing.T) {
	for _, scenario := range []string{"valid", "early", "late", "renewed"} {
		t.Run(scenario, func(t *testing.T) {
			cache := newBoundedDedupe(600*time.Second, 2, 64)
			admitted := time.Now().Add(-600*time.Second - 2*time.Millisecond)
			if scenario == "early" {
				admitted = time.Now().Add(-599 * time.Second)
			}
			if scenario == "late" {
				admitted = time.Now().Add(-602 * time.Second)
			}
			if cache.admit("first", admitted) != dedupeAccepted {
				t.Fatal("setup failed")
			}
			switch scenario {
			case "valid":
				cache.prune(time.Now())
			case "early":
				cache.mu.Lock()
				cache.items.Delete("first")
				cache.mu.Unlock()
			case "renewed":
				cache.mu.Lock()
				cache.items.Set("first", int64(time.Second))
				cache.mu.Unlock()
			}
			err := q5AwaitOriginalExpiry(t.Context(), cache, "first", admitted)
			if (scenario == "valid") != (err == nil) {
				t.Fatalf("scenario=%s err=%v", scenario, err)
			}
			if scenario == "late" {
				if count, _, _ := cache.occupancy(); count != 1 {
					t.Fatal("expiry observer performed its own cleanup")
				}
			}
		})
	}
}
