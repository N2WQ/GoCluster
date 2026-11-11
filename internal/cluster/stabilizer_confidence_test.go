//go:build !qualification

package cluster

import (
	"fmt"
	"strconv"
	"testing"
	"time"
	"unsafe"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/dedup"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func retentionConfig() config.CallCorrectionConfig {
	cfg := correctionLossConfig()
	cfg.MinConsensusReports = 100 // No alternate is eligible to replace this identity.
	cfg.StabilizerEnabled, cfg.StabilizerMaxChecks = true, 12
	cfg.StabilizerPDelayConfidencePercent, cfg.StabilizerPDelayMaxChecks = 25, 3
	cfg.StabilizerTimeoutAction = stabilizerTimeoutRelease
	cfg.RecentBandRecordMinUniqueSpotters = 2
	return cfg
}

func seedRetentionPopulation(t *testing.T, r *spot.SignalResolver, d *spot.SignalResolverDriver, key spot.ResolverSignalKey, at time.Time) {
	t.Helper()
	correctionLossAdmit(t, r, d, key, "K1ABC", 14020, at, 2*time.Minute, "N0BBB", "N0CCC", "N0FFF", "N0GGG")
	correctionLossAdmit(t, r, d, key, "K1ABD", 14020, at, 2*time.Minute, "N0AAA")
	snap := correctionLossPopulation(t, r, key, 5, map[string]int{"K1ABC": 4, "K1ABD": 1})
	if got, ok := correctionflow.ResolverCallConfidencePercent(snap, "K1ABD"); !ok || got != 20 {
		t.Fatal("literal unit-weight population must give retained call 1/5 = 20%")
	}
}

// A new observation contracts the owner's window and publishes genuine rows.
// They expire before the next release without a sweep deleting the publication.
// No saved snapshot or Spot letter is manufactured.
func expireRetentionPopulation(t *testing.T, r *spot.SignalResolver, d *spot.SignalResolverDriver, key spot.ResolverSignalKey) {
	t.Helper()
	previous, ok := r.Lookup(key)
	if !ok {
		t.Fatal("fixture lost its published key before expiry")
	}
	at := retentionClockAfter(t, previous.EvaluatedAt.Add(time.Nanosecond))
	if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: "K1ABC", Spotter: "N0BBB",
		FrequencyKHz: 14020, RecencyWindow: time.Nanosecond}) || d.Drain() != 1 {
		t.Fatal("window-contraction observation was not owner-admitted")
	}
	now := retentionClockAfter(t, at.Add(time.Nanosecond))
	snap, ok := r.LookupEligible(key, now)
	if !ok || snap.TotalReporters != 0 || snap.State != spot.ResolverStateUncertain || snap.EvidenceLoss {
		t.Fatalf("actual owner publication did not lose only current eligible rows: %+v/%t", snap, ok)
	}
}

// Windows UTC may remain equal across several real scheduler checks. Wait for
// the independent observation clock boundary, rather than sleeping an assumed
// duration or polling the production projection for the desired answer.
func retentionClockAfter(t *testing.T, boundary time.Time) time.Time {
	t.Helper()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		if now := time.Now().UTC(); now.After(boundary) {
			return now
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("UTC did not cross fixture observation boundary")
		}
	}
}

func TestStabilizerRetentionFreshEvidenceAndReasonBudget(t *testing.T) {
	for _, floor := range []bool{false, true} {
		for _, action := range []string{stabilizerTimeoutRelease, stabilizerTimeoutSuppress, "retry_overflow", "thinned"} {
			t.Run(fmt.Sprintf("floor_%t/%s", floor, action), func(t *testing.T) {
				temporalDeliveryDirectory(t)
				r, d, key := correctionLossResolver(t, 4, 8)
				cfg := retentionConfig()
				cfg.RecentBandBonusEnabled = floor
				if action == stabilizerTimeoutSuppress {
					cfg.StabilizerTimeoutAction = action
				}
				at := time.Now().UTC().Truncate(time.Second)
				seedRetentionPopulation(t, r, d, key, at.Add(-3*time.Second))
				f := newAmbiguityDeliveryFixture(t, r, cfg)
				p := f.pipeline
				if floor {
					for _, receiver := range []string{"H0AAA", "H0BBB"} {
						p.recentBandStore.(*spot.RecentBandStore).Record("K1ABD", "20m", "CW", receiver, at.Add(-time.Minute))
					}
				}
				p.stabilizerEnabled = true
				p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 4)
				p.telnetStabilizer.Start()
				t.Cleanup(p.telnetStabilizer.Stop)
				s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
				s.SourceType = spot.SourceManual
				p.processSpot(s, nil)
				if s.Confidence != "P" || !s.ConfidencePercentOK || s.ConfidencePercent != 20 || d.Drain() != 1 {
					t.Fatal("fixture must initially admit a real 20% P and consume its one repeated receiver")
				}
				if action == "thinned" {
					p.secondaryMed = dedup.NewSecondaryDeduper(time.Hour, false)
					if !p.secondaryMed.ShouldForward(s) {
						t.Fatal("secondary control did not admit its first observation")
					}
				}
				turns := 12
				if action == "retry_overflow" {
					turns = 3
				}
				var last *telnetStabilizerEnvelope
				for check := 1; check <= turns; check++ {
					if check == 3 {
						expireRetentionPopulation(t, r, d, key)
					}
					envelope := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, check-1)
					if envelope.entryGlyph != "P" || envelope.bestConfidence != stabilizerConfidencePartial {
						t.Fatalf("check %d lost entry or running maximum: %+v", check, envelope)
					}
					if action == "retry_overflow" && check == 3 {
						// Transfer actual held work before filling a replacement's
						// reservation, so no short timer can release the filler.
						p.telnetStabilizer.Stop()
						p.telnetStabilizer = newTelnetSpotStabilizer(time.Hour, 1)
						t.Cleanup(p.telnetStabilizer.Stop)
						if !p.telnetStabilizer.Enqueue(newDeliveryTestSpot("K1FILL", "N0AAA", at)) {
							t.Fatal("failed to reserve retry pressure control")
						}
					}
					p.handleStabilizerRelease(envelope)
					last = envelope
					if check < 3 {
						if envelope.spot.Confidence != "P" || !envelope.spot.ConfidencePercentOK || envelope.spot.ConfidencePercent != 20 {
							t.Fatal("early retry lost current 20% support")
						}
					} else if check < turns {
						want := "?"
						if floor {
							want = "S"
						}
						if envelope.spot.Confidence != want || envelope.spot.ConfidencePercentOK || envelope.spot.ConfidencePercent != 0 {
							t.Fatalf("check %d must keep working evidence fresh, got %s/%d/%t", check,
								envelope.spot.Confidence, envelope.spot.ConfidencePercent, envelope.spot.ConfidencePercentOK)
						}
					}
					if check < turns && (f.ring.GetCount() != 0 || p.tracker.StabilizerRetainedAboveFresh() != 0 || p.telnetStabilizer.Pending() != 1) {
						t.Fatalf("check %d prematurely completed or lost its fresh twelve-check hold", check)
					}
				}
				pending := int64(0)
				if action == "retry_overflow" {
					pending = 1
				}
				if last.spot.Confidence != "P" || last.spot.ConfidencePercentOK || p.tracker.StabilizerRetainedAboveFresh() != 1 || p.telnetStabilizer.Pending() != pending {
					t.Fatal("terminal projection retained stale support, lost P, or counted retries as terminal outcomes")
				}
				assertStabilizerTransition(t, p.tracker, 2, 2, 1, float64(turns))
				switch action {
				case stabilizerTimeoutSuppress:
					if f.ring.GetCount() != 0 || p.tracker.StabilizerSuppressedTimeout() != 1 {
						t.Fatal("suppression was bypassed by retained P")
					}
				case "thinned":
					processed, duplicates, _ := p.secondaryMed.GetStats()
					if f.ring.GetCount() != 0 || processed != 2 || duplicates != 1 || p.tracker.StabilizerReleasedDelayed() != 0 {
						t.Fatal("retained completion bypassed actual secondary thinning")
					}
					f.writer.Stop()
					reopened, err := archive.NewWriter(f.archiveCfg)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(reopened.Stop)
					if rows, err := reopened.Recent(2); err != nil || len(rows) != 0 {
						t.Fatal("thinned retained completion reached persisted archive")
					}
				default:
					temporalDeliveryCheckWithoutPercent(t, f, correctionEvidenceDeliveryCase{subject: "K1ABD", wantCall: "K1ABD", wantGlyph: "P"}, at, true)
				}
				if action == "retry_overflow" && p.tracker.StabilizerOverflowRelease() != 1 {
					t.Fatal("retained overflow fixture did not exercise admission refusal")
				}
			})
		}
	}
}

func TestStabilizerRetentionKeepsIntermediatePromotion(t *testing.T) {
	temporalDeliveryDirectory(t)
	r, d, key := correctionLossResolver(t, 4, 8)
	cfg := retentionConfig()
	cfg.StabilizerMaxChecks, cfg.RecentBandBonusEnabled, cfg.StabilizerEditNeighborEnabled = 3, true, true
	f := newAmbiguityDeliveryFixture(t, r, cfg)
	p := f.pipeline
	at := time.Now().UTC().Truncate(time.Second)
	for call, receivers := range map[string][]string{
		"K1ABD": {"H0AAA", "H0BBB"}, "K1ABC": {"H0AAA", "H0BBB", "H0CCC"},
	} {
		for _, receiver := range receivers {
			p.recentBandStore.(*spot.RecentBandStore).Record(call, "20m", "CW", receiver, at.Add(-time.Minute))
		}
	}
	p.stabilizerEnabled = true
	p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 4)
	p.telnetStabilizer.Start()
	t.Cleanup(p.telnetStabilizer.Stop)
	s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
	s.SourceType = spot.SourceManual
	p.processSpot(s, nil)
	if s.Confidence != "S" || d.Drain() != 1 {
		t.Fatal("independent recognition and edit-neighbor history did not hold the real S")
	}
	seedRetentionPopulation(t, r, d, key, time.Now().UTC())
	first := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 0)
	p.handleStabilizerRelease(first)
	second := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 1)
	if second.entryGlyph != "S" || second.bestConfidence != stabilizerConfidencePartial || second.spot.Confidence != "P" {
		t.Fatal("S-to-P promotion was not carried separately from entry telemetry")
	}
	expireRetentionPopulation(t, r, d, key)
	p.handleStabilizerRelease(second)
	third := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 2)
	if third.entryGlyph != "S" || third.bestConfidence != stabilizerConfidencePartial || third.spot.Confidence != "S" {
		t.Fatal("later S evidence erased the strongest reached P or was overwritten by it")
	}
	p.handleStabilizerRelease(third)
	assertStabilizerTransition(t, p.tracker, 1, 2, 1, 3)
	temporalDeliveryCheckWithoutPercent(t, f, correctionEvidenceDeliveryCase{subject: "K1ABD", wantCall: "K1ABD", wantGlyph: "P"}, at, true)
}

func TestStabilizerTerminalConfidenceSpecialOutcomesAndSize(t *testing.T) {
	for _, tc := range []struct {
		fresh, want string
		best        stabilizerOrdinaryConfidence
		retained    bool
	}{
		{"B", "B", stabilizerConfidencePartial, false},
		{"C", "C", stabilizerConfidencePartial, false},
		{"V", "V", stabilizerConfidenceSupported, false},
		{"P", "P", stabilizerConfidencePartial, false},
		{"?", "S", stabilizerConfidenceSupported, true},
	} {
		if got, retained := terminalStabilizerConfidence(tc.fresh, tc.best); got != tc.want || retained != tc.retained {
			t.Errorf("fresh=%s best=%d: %s/%t", tc.fresh, tc.best, got, retained)
		}
	}
	if strconv.IntSize == 64 {
		// The byte occupies existing bool padding on the supported 64-bit ABI.
		if unsafe.Sizeof(telnetStabilizerEnvelope{}) > 120 || unsafe.Sizeof(telnetStabilizerItem{}) > 152 {
			t.Fatal("confidence scalar grew the existing envelope/item layout")
		}
	}
}

func BenchmarkStabilizerTerminalConfidence(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if glyph, retained := terminalStabilizerConfidence("S", stabilizerConfidencePartial); glyph != "P" || !retained {
			b.Fatal("projection failed")
		}
	}
}
