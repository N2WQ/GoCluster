//go:build !qualification

// File role: Checks that actual held/reheld/suppressed scheduler work cannot
// teach accepted history, using a positive per-occurrence eligibility control.
package cluster

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestAcceptedLearningPathsHoldResolution(t *testing.T) {
	source := acceptedLearningPathsConfigSource(t)
	for _, custom := range []bool{false, true} {
		for _, action := range []string{stabilizerTimeoutRelease, stabilizerTimeoutSuppress} {
			t.Run(fmt.Sprintf("custom_%t/%s", custom, action), func(t *testing.T) {
				temporalDeliveryDirectory(t)
				f := newFamilyDeliveryFixture(t, source, custom, false, 1, 8, action, true)
				p := f.pipeline
				p.correctionCfg.StabilizerMaxChecks = 3
				at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
				control := acceptedLearningPathsControl(t, f, at)
				incoming := familyDeliveryInput{"K1WAIT", "N0AAA", "held learning source", "?", at.Add(time.Second)}
				lanes := acceptedLearningPathsRails(t, f, incoming, false)
				p.processSpot(incoming.spot(), nil)
				for completed := 0; completed < 3; completed++ {
					if p.tracker.StabilizerHeld() != uint64(completed+1) ||
						p.tracker.StabilizerHeldByReason()[stabilizerDelayReasonUnknownOrNonRecent.String()] != uint64(completed+1) || f.ring.GetCount() != 1 {
						t.Fatal("fixture did not positively qualify a real pending/reheld source")
					}
					acceptedLearningPathsWho(t, f, incoming.call, 0)
					if acceptedLearningPathsSupport(f, incoming.call) != 0 {
						t.Fatal("pending/reheld source taught receiver history")
					}
					envelope := familyDeliveryReceiveRelease(t, p.telnetStabilizer)
					if envelope.checksCompleted != completed || !envelope.hasResolverKey || envelope.spot.DXCallNorm != incoming.call ||
						envelope.delayReason != stabilizerDelayReasonUnknownOrNonRecent.String() || envelope.peerOriginal == nil {
						t.Fatal("actual scheduler lost pending ownership, retry count or reason")
					}
					p.handleStabilizerRelease(envelope)
				}
				if p.telnetStabilizer.Pending() != 0 || p.tracker.StabilizerHeld() != 3 {
					t.Fatal("three-check budget did not resolve its only owned source")
				}
				wantWho, wantSupport := 0, 0
				if action == stabilizerTimeoutRelease {
					wantWho = 1
					if !custom {
						wantSupport = 1
					}
					acceptedLearningPathsAssertRails(t, lanes, false)
					if p.tracker.StabilizerSuppressedTimeout() != 0 {
						t.Fatal("release policy suppressed the eligible accepted source")
					}
				} else {
					if p.tracker.StabilizerSuppressedTimeout() != 1 ||
						p.tracker.StabilizerSuppressedByReason()[stabilizerDelayReasonUnknownOrNonRecent.String()] != 1 {
						t.Fatal("suppression fixture did not reach the actual exhausted core decision")
					}
					for _, d := range lanes {
						processed, duplicates, _ := d.GetStats()
						if processed != 1 || duplicates != 0 {
							t.Fatal("suppressed source reached a secondary gate")
						}
					}
				}
				acceptedLearningPathsWho(t, f, incoming.call, wantWho)
				if n := acceptedLearningPathsSupport(f, incoming.call); n != wantSupport || f.ring.GetCount() != 1 {
					t.Fatalf("resolved source support=%d want %d, ring=%d want control only", n, wantSupport, f.ring.GetCount())
				}
				// Joining the actual scheduler cannot add another learning event.
				p.telnetStabilizer.Stop()
				acceptedLearningPathsWho(t, f, incoming.call, wantWho)
				if f.custom != nil {
					if f.custom.StaticContains(incoming.call) || f.custom.StatsSnapshot().ObservationSpotters != 1 {
						t.Fatal("unknown or suppressed source bypassed CustomSCP V-only admission")
					}
					acceptedLearningPathsAssertCustomDisk(t, f, []familyDeliveryInput{control, incoming})
				}
				// Who is the occurrence oracle; raw receiver counts alone would
				// remain one even if the accepted release had recorded twice.
				if got := f.who.CountryCountsByContinent(control.call, "20m", time.Now().UTC()); len(got["NA"]) != 1 || got["NA"][0] != (spot.WhoSpotsMeCountryCount{ADIF: 291, Count: 1}) {
					t.Fatal("eligibility control no longer proves an intact counting path")
				}
			})
		}
	}
}
