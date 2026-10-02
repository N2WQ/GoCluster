//go:build qualification

package peer

import (
	"context"
	"errors"
	"testing"
	"time"
)

// qualificationAwaitUntil uses one absolute deadline through observation, predicate
// evaluation and the final successful return. Observations must honor ctx;
// creating detached observer goroutines would leave unbounded unfinished work.
func qualificationAwaitUntil[T any](parent context.Context, deadline time.Time, interval time.Duration, observe func(context.Context) (T, error), predicate func(T) bool) (T, error) {
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	var last T
	fence := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return context.DeadlineExceeded
		}
		return nil
	}
	for {
		if err := fence(); err != nil {
			return last, err
		}
		var err error
		last, err = observe(ctx)
		if err != nil {
			return last, err
		}
		if err := fence(); err != nil {
			return last, err
		}
		ready := predicate(last)
		if err := fence(); err != nil {
			return last, err
		}
		if ready {
			return last, fence()
		}
		if err := qualificationWait(ctx, min(interval, max(time.Duration(0), time.Until(deadline)))); err != nil {
			return last, err
		}
	}
}

func TestPC92V12DeadlineWaiters(t *testing.T) {
	for _, phase := range []string{"Expired", "LateObservation", "LatePredicate", "CanceledBefore", "CanceledObservation", "CanceledPredicate", "BlockedObservation", "ObservationError", "Success"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			deadline := time.Now().Add(30 * time.Millisecond)
			calls := 0
			if phase == "Expired" {
				deadline = time.Now().Add(-time.Second)
			}
			if phase == "CanceledBefore" {
				cancel()
			}
			observationError := errors.New("observation failed")
			value, err := qualificationAwaitUntil(ctx, deadline, time.Millisecond, func(ctx context.Context) (int, error) {
				calls++
				switch phase {
				case "LateObservation":
					time.Sleep(time.Until(deadline) + time.Millisecond)
				case "CanceledObservation":
					cancel()
				case "BlockedObservation":
					<-ctx.Done()
					return 0, ctx.Err()
				case "ObservationError":
					return 0, observationError
				}
				return 42, nil
			}, func(int) bool {
				switch phase {
				case "LatePredicate":
					time.Sleep(time.Until(deadline) + time.Millisecond)
				case "CanceledPredicate":
					cancel()
				}
				return true
			})
			if phase == "Success" {
				if err != nil || value != 42 {
					t.Fatalf("valid success: value=%d err=%v", value, err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid success passed absolute deadline/cancellation fence")
			}
			if (phase == "Expired" || phase == "CanceledBefore") && calls != 0 {
				t.Fatal("unusable deadline still invoked observation")
			}
			if phase == "ObservationError" && !errors.Is(err, observationError) {
				t.Fatalf("lost observation error: %v", err)
			}
		})
	}
}
