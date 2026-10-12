//go:build !qualification

// File role: Falsifies stabilizer wiring that overlooks selected neighboring
// cap loss, retains stale loss on retry, or replaces unrelated exact-bucket
// policy inputs. Fixtures use actual owner discards and scheduler transfers.
package cluster

import (
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

type stabilizerLossFixture struct {
	ambiguityDeliveryFixture
	resolver *spot.SignalResolver
	driver   *spot.SignalResolverDriver
	key      spot.ResolverSignalKey
	neighbor spot.ResolverSignalKey
	history  *spot.RecentBandStore
	at       time.Time
}

func newStabilizerLossFixture(t *testing.T, exact bool, cause string) stabilizerLossFixture {
	t.Helper()
	candidateCap, receiverCap := 2, 8
	if cause == "receiver" {
		candidateCap, receiverCap = 4, 2
	}
	r, d, key := correctionLossResolver(t, candidateCap, receiverCap)
	cfg := correctionLossConfig()
	cfg.ResolverNeighborhoodEnabled, cfg.ResolverNeighborhoodBucketRadius = true, 1
	cfg.StabilizerEnabled, cfg.StabilizerMaxChecks, cfg.StabilizerAmbiguousMaxChecks = true, 1, 3
	cfg.StabilizerTimeoutAction = stabilizerTimeoutRelease
	cfg.RecentBandBonusEnabled, cfg.RecentBandRecordMinUniqueSpotters = true, 2
	f := stabilizerLossFixture{ambiguityDeliveryFixture: newAmbiguityDeliveryFixture(t, r, cfg),
		resolver: r, driver: d, key: key, neighbor: key, at: time.Now().UTC().Truncate(time.Second)}
	f.neighbor.Bucket++
	observed := f.at.Add(-3 * time.Second)
	if exact {
		correctionLossAdmit(t, r, d, key, "K1ABC", 14020, observed, 2*time.Minute, "N0AAA", "N0BBB")
	}
	switch cause {
	case "candidate":
		for i, call := range []string{"DL9ZZZ", "W9WWW", "K9YYY"} {
			correctionLossAdmit(t, r, d, f.neighbor, call, 14020.5, observed, 2*time.Minute, []string{"N0CCC", "N0DDD", "N0EEE"}[i])
		}
		correctionLossEvictions(t, r, 1, 0)
	case "receiver":
		correctionLossAdmit(t, r, d, f.neighbor, "DL9ZZZ", 14020.5, observed, 2*time.Minute, "N0CCC", "N0DDD", "N0EEE")
		correctionLossEvictions(t, r, 0, 1)
	default:
		t.Fatalf("unsupported actual loss cause %q", cause)
	}
	f.history = f.pipeline.recentBandStore.(*spot.RecentBandStore)
	for _, receiver := range []string{"H0AAA", "H0BBB"} {
		f.history.Record("K1ABC", "20m", "CW", receiver, observed.Add(-time.Minute))
	}
	if n := f.history.RecentSupportCount("K1ABC", "20m", "CW", time.Now().UTC()); n != 2 {
		t.Fatalf("independent historical S control has %d receivers, want 2", n)
	}
	probe := newDeliveryTestSpot("K1ABC", "N0AAA", f.at)
	applySupportFloor(probe, f.history, nil, nil, cfg)
	if probe.Confidence != "S" {
		t.Fatalf("independent accepted history did not qualify S: %s", probe.Confidence)
	}
	now := time.Now().UTC()
	narrow, ok := r.LookupEligible(key, now)
	if ok != exact || ok && (narrow.EvidenceLoss || narrow.State != spot.ResolverStateConfident) {
		t.Fatalf("exact bucket must be absent or independently healthy: present=%t snapshot=%+v", ok, narrow)
	}
	selected := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABC", now)
	full := correctionflow.EvaluateStabilizerDelay(probe, f.history, cfg, now, selected.Snapshot, selected.SnapshotOK)
	old := correctionflow.EvaluateStabilizerDelay(probe, f.history, cfg, now, narrow, ok)
	if !selected.SnapshotOK || !selected.Snapshot.EvidenceLoss || !full.ShouldDelay ||
		full.Reason != correctionflow.StabilizerDelayReasonAmbiguous || full.MaxChecks != 3 || old.ShouldDelay {
		t.Fatalf("loss must distinguish selected and exact policy with known history: selected=%+v full=%+v exact=%+v", selected, full, old)
	}
	f.pipeline.stabilizerEnabled = true
	f.pipeline.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 4)
	t.Cleanup(f.pipeline.telnetStabilizer.Stop)
	return f
}

func (f stabilizerLossFixture) incoming() *spot.Spot {
	s := newDeliveryTestSpot("K1ABC", "N0AAA", f.at)
	s.SourceType = spot.SourceManual
	return s
}

func (f stabilizerLossFixture) assertHeld(t *testing.T, holds uint64) {
	t.Helper()
	p := f.pipeline
	if f.ring.GetCount() != 0 || p.tracker.StabilizerHeld() != holds ||
		p.tracker.StabilizerHeldByReason()[stabilizerDelayReasonAmbiguous.String()] != holds ||
		p.tracker.StabilizerReleasedImmediate() != 0 || p.tracker.StabilizerReleasedDelayed() != 0 ||
		p.tracker.CallCorrections() != 0 {
		t.Fatalf("selected loss escaped a qualified hold: ring=%d held=%d reasons=%v immediate=%d delayed=%d corrections=%d",
			f.ring.GetCount(), p.tracker.StabilizerHeld(), p.tracker.StabilizerHeldByReason(),
			p.tracker.StabilizerReleasedImmediate(), p.tracker.StabilizerReleasedDelayed(), p.tracker.CallCorrections())
	}
	if n := f.history.RecentSupportCount("K1ABC", "20m", "CW", time.Now().UTC()); n != 2 {
		t.Fatalf("pending observation learned before resolution: support=%d want historical 2", n)
	}
}

func (f stabilizerLossFixture) checkLossRelease(t *testing.T) {
	t.Helper()
	temporalDeliveryCheckWithoutPercent(t, f.ambiguityDeliveryFixture,
		correctionEvidenceDeliveryCase{subject: "K1ABC", wantCall: "K1ABC", wantGlyph: "S"}, f.at, true)
}

func TestCorrectionStabilizerSelectedLossInitial(t *testing.T) {
	for _, exact := range []bool{true, false} {
		for _, cause := range []string{"candidate", "receiver"} {
			name := "absent_exact/" + cause
			if exact {
				name = "healthy_exact/" + cause
			}
			t.Run(name, func(t *testing.T) {
				temporalDeliveryDirectory(t)
				f := newStabilizerLossFixture(t, exact, cause)
				p := f.pipeline
				p.correctionCfg.StabilizerAmbiguousMaxChecks = 1
				p.processSpot(f.incoming(), nil)
				f.assertHeld(t, 1)
				if p.telnetStabilizer.Pending() != 1 {
					t.Fatal("initial selected loss did not reserve the bounded scheduler slot")
				}
				p.telnetStabilizer.Start()
				release := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, f.key, 0)
				if release.delayReason != stabilizerDelayReasonAmbiguous.String() || release.peerOriginal == nil {
					t.Fatal("actual scheduler lost initial loss reason or original publication")
				}
				p.handleStabilizerRelease(release)
				if p.telnetStabilizer.Pending() != 0 || p.tracker.StabilizerReleasedDelayed() != 1 ||
					p.tracker.StabilizerReleasedDelayedByReason()[stabilizerDelayReasonAmbiguous.String()] != 1 {
					t.Fatal("one-check loss release changed reservation or final reason accounting")
				}
				f.checkLossRelease(t)
			})
		}
	}
}

func TestCorrectionStabilizerSelectedLossRetryBudget(t *testing.T) {
	for _, action := range []string{stabilizerTimeoutRelease, stabilizerTimeoutSuppress} {
		t.Run(action, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			f := newStabilizerLossFixture(t, true, "candidate")
			p := f.pipeline
			p.correctionCfg.StabilizerTimeoutAction = action
			p.processSpot(f.incoming(), nil)
			f.assertHeld(t, 1)
			p.telnetStabilizer.Start()
			var original *originalPeerPublication
			for completed := 0; completed < 3; completed++ {
				envelope := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, f.key, completed)
				if completed == 0 {
					original = envelope.peerOriginal
				}
				if envelope.peerOriginal != original || envelope.delayReason != stabilizerDelayReasonAmbiguous.String() {
					t.Fatal("retry replaced the original tuple or selected-loss reason")
				}
				p.handleStabilizerRelease(envelope)
				if completed < 2 {
					f.assertHeld(t, uint64(completed+2))
				}
			}
			if p.telnetStabilizer.Pending() != 0 || p.tracker.StabilizerHeld() != 3 {
				t.Fatal("ambiguity budget must exhaust exactly at check 3, despite baseline budget 1")
			}
			if action == stabilizerTimeoutRelease {
				if p.tracker.StabilizerReleasedDelayed() != 1 || p.tracker.StabilizerSuppressedTimeout() != 0 ||
					p.tracker.StabilizerReleasedDelayedByReason()[stabilizerDelayReasonAmbiguous.String()] != 1 {
					t.Fatal("loss exhaustion did not preserve release accounting")
				}
				f.checkLossRelease(t)
			} else {
				if f.ring.GetCount() != 0 || p.tracker.StabilizerReleasedDelayed() != 0 ||
					p.tracker.StabilizerSuppressedTimeout() != 1 || p.tracker.StabilizerSuppressedByReason()[stabilizerDelayReasonAmbiguous.String()] != 1 {
					t.Fatal("loss exhaustion did not suppress before fanout")
				}
				f.assertArchiveEmpty(t)
			}
		})
	}
}

func (f stabilizerLossFixture) assertArchiveEmpty(t *testing.T) {
	t.Helper()
	f.writer.Stop()
	reopened, err := archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err := reopened.Recent(2)
	if err != nil || len(rows) != 0 {
		t.Fatalf("held/suppressed loss reached persisted archive: rows=%v err=%v", rows, err)
	}
}

func TestCorrectionStabilizerSelectedLossOverflow(t *testing.T) {
	for _, path := range []string{"initial", "retry"} {
		t.Run(path, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			f := newStabilizerLossFixture(t, true, "receiver")
			p := f.pipeline
			p.correctionCfg.StabilizerTimeoutAction = stabilizerTimeoutSuppress
			p.telnetStabilizer.Stop()
			p.telnetStabilizer = newTelnetSpotStabilizer(time.Hour, 1)
			t.Cleanup(p.telnetStabilizer.Stop)
			if path == "retry" {
				// Transfer real initial work before reserving the replacement's
				// one-hour filler. No hand-built retry can conceal context loss.
				p.telnetStabilizer.delay = time.Nanosecond
				p.processSpot(f.incoming(), nil)
				f.assertHeld(t, 1)
				p.telnetStabilizer.Start()
				envelope := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, f.key, 0)
				p.telnetStabilizer.Stop()
				p.telnetStabilizer = newTelnetSpotStabilizer(time.Hour, 1)
				t.Cleanup(p.telnetStabilizer.Stop)
				if !p.telnetStabilizer.Enqueue(newDeliveryTestSpot("K1FILL", "N0ZZZ", f.at)) {
					t.Fatal("retry filler did not reserve the actual capacity")
				}
				p.handleStabilizerRelease(envelope)
			} else {
				if !p.telnetStabilizer.Enqueue(newDeliveryTestSpot("K1FILL", "N0ZZZ", f.at)) {
					t.Fatal("initial filler did not reserve the actual capacity")
				}
				p.processSpot(f.incoming(), nil)
			}
			if p.telnetStabilizer.Pending() != 1 || p.tracker.StabilizerOverflowRelease() != 1 || p.tracker.StabilizerSuppressedTimeout() != 0 {
				t.Fatal("qualified selected-loss overflow did not preserve bounded fail-open behavior")
			}
			f.checkLossRelease(t)
		})
	}
}

func TestCorrectionStabilizerSelectedLossFreshRecovery(t *testing.T) {
	temporalDeliveryDirectory(t)
	f := newStabilizerLossFixture(t, true, "candidate")
	p := f.pipeline
	p.processSpot(f.incoming(), nil)
	f.assertHeld(t, 1)
	p.telnetStabilizer.Start()
	envelope := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, f.key, 0)
	if got := f.driver.Drain(); got != 1 {
		t.Fatalf("recovery owner barrier consumed %d queued incoming observations, want 1", got)
	}
	// Actual new owner traffic contracts the neighbor window. All discarded
	// reports are three seconds old, so the genuine unchanged loss interval no
	// longer intersects this short window. Nothing in the envelope is altered.
	fresh := time.Now().UTC()
	correctionLossAdmit(t, f.resolver, f.driver, f.neighbor, "K9YYY", 14020.5, fresh, time.Nanosecond, "N0EEE")
	selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(f.resolver, f.key, p.correctionCfg, "K1ABC", time.Now().UTC())
	if !selection.SnapshotOK || selection.Snapshot.EvidenceLoss || selection.Snapshot.Winner != "K1ABC" {
		t.Fatalf("actual window recovery did not restore independent exact authority: %+v", selection)
	}
	correctionLossEvictions(t, f.resolver, 1, 0)
	p.handleStabilizerRelease(envelope)
	if p.telnetStabilizer.Pending() != 0 || p.tracker.StabilizerHeld() != 1 || p.tracker.StabilizerReleasedDelayed() != 1 {
		t.Fatal("fresh retry remained held by stale initial loss authority")
	}
	f.check(t, correctionEvidenceDeliveryCase{subject: "K1ABC", wantCall: "K1ABC", wantGlyph: "V", wantPercent: 100}, f.at)
}

func TestCorrectionStabilizerSelectedLossTemporalAbstain(t *testing.T) {
	for _, path := range []string{"terminal", "overflow"} {
		t.Run(path, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			f := newStabilizerLossFixture(t, true, "candidate")
			p := f.pipeline
			p.correctionCfg.TemporalDecoder = runtimeTemporalTestConfig(1_000_000).TemporalDecoder
			p.correctionCfg.TemporalDecoder.OverflowAction = "abstain"
			p.correctionCfg.TemporalDecoder.MaxPending = 1
			p.correctionCfg.StabilizerAmbiguousMaxChecks = 1
			p.temporal = newRuntimeTemporalController(p.correctionCfg)
			if path == "overflow" {
				selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(f.resolver, f.key, p.correctionCfg, "K1ABC", time.Now().UTC())
				temporalDeliveryObserve(t, p.temporal, time.Now().UTC(), f.key, f.incoming(), selection)
				if p.temporal.decoder.PendingCount() != 1 {
					t.Fatal("actual decoder filler did not occupy pending capacity")
				}
			}
			p.processSpot(f.incoming(), nil)
			if path == "terminal" {
				if p.tracker.TemporalPending() != 1 || p.telnetStabilizer.Pending() != 0 || f.ring.GetCount() != 0 {
					t.Fatal("incoming loss did not first enter temporal ownership")
				}
				releases := p.temporal.Drain(time.Now().UTC(), true)
				if len(releases) != 1 || releases[0].decision.Action != correctionflow.TemporalDecisionActionAbstain {
					t.Fatalf("actual terminal decoder did not abstain: %+v", releases)
				}
				p.tracker.DecrementTemporalPending()
				p.processSpot(releases[0].pending.spot, &releases[0])
			}
			if p.tracker.TemporalAbstainLowMargin() != 1 || p.tracker.TemporalCommitted() != 0 ||
				p.tracker.TemporalPathSwitches() != 0 || p.tracker.CallCorrections() != 0 {
				t.Fatal("abstain fixture did not prove exactly one terminal/overflow decision and no accepted correction")
			}
			f.assertHeld(t, 1)
			if p.telnetStabilizer.Pending() != 1 {
				t.Fatal("abstain path skipped selected loss before stabilizer ownership")
			}
			p.telnetStabilizer.Start()
			envelope := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, f.key, 0)
			p.handleStabilizerRelease(envelope)
			f.checkLossRelease(t)
			if path == "overflow" {
				if releases := p.temporal.Drain(time.Now().UTC(), true); len(releases) != 1 {
					t.Fatal("overflow request consumed or stranded the existing temporal hold")
				}
			}
		})
	}
}

func TestCorrectionStabilizerSelectedLossHealthyExactInputs(t *testing.T) {
	for _, threshold := range []int{0, 75} {
		name := "ordinary_P"
		if threshold > 0 {
			name = "exact_low_P_control"
		}
		t.Run(name, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			r, d, key := correctionLossResolver(t, 4, 8)
			cfg := correctionLossConfig()
			cfg.ResolverNeighborhoodEnabled, cfg.ResolverNeighborhoodBucketRadius = true, 1
			cfg.StabilizerEnabled, cfg.StabilizerMaxChecks, cfg.StabilizerAmbiguousMaxChecks = true, 1, 3
			cfg.StabilizerPDelayConfidencePercent, cfg.StabilizerPDelayMaxChecks = threshold, 2
			f := newAmbiguityDeliveryFixture(t, r, cfg)
			at := time.Now().UTC().Truncate(time.Second)
			correctionLossAdmit(t, r, d, key, "K1ABC", 14020, at.Add(-3*time.Second), 2*time.Minute, "N0AAA", "N0BBB")
			neighbor := key
			neighbor.Bucket++
			correctionLossAdmit(t, r, d, neighbor, "K1ABD", 14020.5, at.Add(-3*time.Second), 2*time.Minute, "N0CCC", "N0DDD")
			correctionLossEvictions(t, r, 0, 0)
			selection := correctionflow.SelectResolverPrimarySnapshotForCallAt(r, key, cfg, "K1ABC", time.Now().UTC())
			exact, ok := r.LookupEligible(key, time.Now().UTC())
			if !selection.SnapshotOK || !selection.NeighborhoodSplit || selection.Snapshot.EvidenceLoss || !ok || exact.State != spot.ResolverStateConfident {
				t.Fatalf("healthy control must distinguish broader split from exact authority: selected=%+v exact=%+v", selection, exact)
			}
			p := f.pipeline
			p.stabilizerEnabled = true
			p.telnetStabilizer = newTelnetSpotStabilizer(time.Minute, 1)
			t.Cleanup(p.telnetStabilizer.Stop)
			s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
			s.SourceType = spot.SourceManual
			p.processSpot(s, nil)
			if p.telnetStabilizer.Pending() != 0 || p.tracker.StabilizerHeld() != 0 || p.tracker.StabilizerReleasedImmediate() != 1 {
				t.Fatal("selected non-loss state/share replaced existing exact-only stabilizer inputs")
			}
			f.check(t, correctionEvidenceDeliveryCase{subject: "K1ABC", wantCall: "K1ABC", wantGlyph: "P", wantPercent: 50}, at)
		})
	}
}
