package floodcontrol

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func TestFloodAdmissionSinkAfterPolicyAndBeforePressure(t *testing.T) {
	for _, action := range []string{config.FloodActionObserve, config.FloodActionSuppress, config.FloodActionDrop} {
		t.Run(action, func(t *testing.T) {
			queue := make(chan *spot.Spot, 1)
			c := New(testFloodConfig(action, 8), queue, nil, nil)
			calls, successes := 0, 0
			c.SetAdmissionSink(func(s *spot.Spot) bool {
				calls++
				// The flood mutex must have been released before primary admission.
				if !c.mu.TryLock() {
					t.Fatal("flood lock held across primary admission")
				}
				c.mu.Unlock()
				select {
				case queue <- s:
					successes++
					return true
				default:
					return false
				}
			})
			at := time.Unix(1, 0)
			c.process(at, testSpot("W1AAA", spot.SourceRBN))
			c.process(at, testSpot("W1AAA", spot.SourceRBN))
			want := 1
			if action == config.FloodActionObserve {
				want = 2
			}
			if calls != want || successes != 1 {
				t.Fatalf("policy/primary-pressure calls=%d successes=%d", calls, successes)
			}
			// A new actor passes policy but the full primary input refuses it.
			c.process(at, testSpot("W1BBB", spot.SourceRBN))
			if calls != want+1 || successes != 1 {
				t.Fatal("input refusal bypassed the admission sink")
			}
		})
	}
}
