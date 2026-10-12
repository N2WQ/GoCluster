//go:build !qualification

// File role: Proves accepted-observation learning across actual shutdown
// ownership transfers, distinguishing discarded holds from accepted releases.
package cluster

import (
	"testing"
	"time"

	"dxcluster/spot"
	"dxcluster/stats"
)

func acceptedLearningShutdownSpot(call, reporter, mode string) *spot.Spot {
	s := runtimeRetirementSpot(call, mode, "accepted shutdown "+call)
	s.SourceType = spot.SourceManual
	s.DECall, s.DECallNorm = reporter, ""
	s.DEMetadata.Continent = "NA"
	s.InvalidateMetadataCache()
	s.EnsureNormalized()
	return s
}

func acceptedLearningShutdownWhoCount(t *testing.T, store *spot.WhoSpotsMeStore, call string) int {
	t.Helper()
	counts := store.CountryCountsByContinent(call, "20m", time.Now().UTC())
	total := 0
	for continent, countries := range counts {
		for _, country := range countries {
			if continent != "NA" || country.ADIF != 291 {
				t.Fatalf("learning fixture counted unexpected metadata: %s %+v", continent, country)
			}
			total += country.Count
		}
	}
	return total
}

// The real local SELF contract keeps the eligibility control immediate even
// with long holds configured. Both recorders still consume the accepted spot.
// Reading both actual queues proves the source traversed final delivery rather
// than only calling the counter directly. Controls use isolated identities.
func acceptedLearningShutdownControl(t *testing.T, r *clusterRuntime, call, mode string) {
	t.Helper()
	s := acceptedLearningShutdownSpot(call, call, mode)
	runtimeRetirementSend(t, r, s)
	var archived, broadcast *spot.Spot
	runtimeRetirementWaitFor(t, "accepted positive archive control", func() bool {
		var ok bool
		archived, ok = tryReadArchiveQueuedSpot(r.archiveWriter)
		return ok
	})
	runtimeRetirementWaitFor(t, "accepted positive telnet control", func() bool {
		var ok bool
		broadcast, ok = tryReadTelnetBroadcastSpot(r.telnetServer)
		return ok
	})
	runtimeRetirementAssertSpot(t, archived, s)
	runtimeRetirementAssertSpot(t, broadcast, s)
	if s.Confidence != "V" || s.DEMetadata.ADIF != 291 || s.DEContinentNorm != "NA" || acceptedLearningShutdownWhoCount(t, r.whoSpotsMeStore, call) != 1 {
		t.Fatalf("positive control must establish exactly one eligible accepted record: %+v", s)
	}
	store := r.recentBandStore
	if modeSupportsFTConfidenceGlyph(mode) {
		store = r.outputPipeline.ftRecentBandStore
	}
	if store == nil || store.RecentSupportCount(call, "20m", mode, time.Now().UTC()) != 1 {
		t.Fatal("positive control did not establish store-specific recording eligibility")
	}
}

func TestAcceptedLearningShutdownDiscardedPending(t *testing.T) {
	r := runtimeRetirementRuntime(t, nil, 8)
	runtimeRetirementConfigureStabilizer(r, 60, 1, stabilizerTimeoutRelease)
	r.whoSpotsMeStore = spot.NewWhoSpotsMeStore(time.Hour)
	runtimeRetirementStart(r)
	acceptedLearningShutdownControl(t, r, "K1CTRL", "CW")
	held := acceptedLearningShutdownSpot("K1HELD", "N0AAA", "CW")
	runtimeRetirementSend(t, r, held)
	runtimeRetirementWaitFor(t, "real long stabilizer hold", func() bool {
		return r.statsTracker.StabilizerHeld() == 1 && r.outputPipeline.telnetStabilizer.Pending() == 1
	})
	if acceptedLearningShutdownWhoCount(t, r.whoSpotsMeStore, held.DXCall) != 0 || r.recentBandStore.RecentSupportCount(held.DXCall, "20m", "CW", time.Now().UTC()) != 0 {
		t.Fatal("pending real scheduler observation learned before acceptance")
	}
	stopped := runtimeRetirementCall(t, nil, r.retireOutputPipeline)
	runtimeRetirementWait(t, "discarded-hold retirement", stopped)
	runtimeRetirementAssertDone(t, r)
	if acceptedLearningShutdownWhoCount(t, r.whoSpotsMeStore, held.DXCall) != 0 || r.recentBandStore.RecentSupportCount(held.DXCall, "20m", "CW", time.Now().UTC()) != 0 || r.statsTracker.StabilizerReleasedDelayed() != 0 {
		t.Fatal("shutdown discard manufactured accepted learning or release")
	}
	runtimeRetirementAssertHandoffs(t, r)
	if r.spotBuffer.GetCount() != 1 {
		t.Fatal("discarded pending input reached the ring after its isolated control")
	}
}

func TestAcceptedLearningShutdownTransferredStabilizer(t *testing.T) {
	for _, action := range []string{stabilizerTimeoutRelease, stabilizerTimeoutSuppress} {
		t.Run(action, func(t *testing.T) {
			r := runtimeRetirementRuntime(t, nil, 8)
			runtimeRetirementConfigureStabilizer(r, 1, 1, action)
			r.cfg.CallCorrection.RecentBandBonusEnabled = true
			r.cfg.CallCorrection.RecentBandRecordMinUniqueSpotters = 2
			r.whoSpotsMeStore = spot.NewWhoSpotsMeStore(time.Hour)
			callback := newRuntimeRetirementBarrier(t)
			r.gridUpdater = func(call, _ string) {
				if call == "K1FIRST" {
					callback.enter()
				}
			}
			runtimeRetirementStart(r)
			acceptedLearningShutdownControl(t, r, "K1CTRL", "CW")
			first := acceptedLearningShutdownSpot("K1FIRST", "N0AAA", "CW")
			second := acceptedLearningShutdownSpot("K1SECOND", "N0AAA", "CW")
			runtimeRetirementSend(t, r, first)
			runtimeRetirementSend(t, r, second)
			runtimeRetirementWaitFor(t, "two real scheduler holds", func() bool {
				return r.statsTracker.StabilizerHeld() == 2 && r.outputPipeline.telnetStabilizer.Pending() == 2
			})
			if action == stabilizerTimeoutSuppress {
				// Only the first observation recovers while held. Its real accepted
				// release blocks the consumer; the second stays risky and exhausts.
				store := r.recentBandStore.(*spot.RecentBandStore)
				now := time.Now().UTC()
				for _, reporter := range []string{"N0BBB", "N0CCC"} {
					store.RecordAt(first.DXCall, "20m", "CW", reporter, now, now)
				}
			}
			runtimeRetirementWait(t, "first actual release processing barrier", callback.entered)
			p := r.outputPipeline
			runtimeRetirementWaitFor(t, "second actual scheduler envelope transferred into buffer", func() bool {
				return len(p.telnetStabilizer.release) == 1 && p.telnetStabilizer.Pending() == 0
			})
			if acceptedLearningShutdownWhoCount(t, r.whoSpotsMeStore, second.DXCall) != 0 || r.recentBandStore.RecentSupportCount(second.DXCall, "20m", "CW", time.Now().UTC()) != 0 {
				t.Fatal("buffered but unprocessed envelope learned before acceptance")
			}
			stopped := runtimeRetirementCall(t, callback.release, r.retireOutputPipeline)
			runtimeRetirementWait(t, "runtime stop before second acceptance", p.stopCh)
			runtimeRetirementWait(t, "scheduler stopped with transferred consumer-owned envelope", p.telnetStabilizer.done)
			select {
			case <-p.stabilizerReleaseDone:
				t.Fatal("release consumer retired while first processing remained blocked")
			case <-p.done:
				t.Fatal("output owner returned without joining transferred work")
			default:
			}
			callback.release()
			runtimeRetirementWait(t, "transferred-envelope retirement", stopped)
			runtimeRetirementAssertDone(t, r)
			wantSecond := 1
			if action == stabilizerTimeoutSuppress {
				wantSecond = 0
			}
			if acceptedLearningShutdownWhoCount(t, r.whoSpotsMeStore, first.DXCall) != 1 || acceptedLearningShutdownWhoCount(t, r.whoSpotsMeStore, second.DXCall) != wantSecond || r.recentBandStore.RecentSupportCount(second.DXCall, "20m", "CW", time.Now().UTC()) != wantSecond {
				t.Fatalf("transferred work must learn according to acceptance after stop: second want=%d", wantSecond)
			}
			if wantSecond == 1 {
				runtimeRetirementAssertHandoffs(t, r, first, second)
				if r.spotBuffer.GetCount() != 3 || r.statsTracker.StabilizerSuppressedTimeout() != 0 {
					t.Fatal("accepted shutdown release changed normal fanout/disposition")
				}
			} else {
				runtimeRetirementAssertHandoffs(t, r, first)
				if r.spotBuffer.GetCount() != 2 || r.statsTracker.StabilizerSuppressedTimeout() != 1 {
					t.Fatal("suppressed transferred release produced delivery or lost suppression")
				}
			}
		})
	}
}

func TestAcceptedLearningShutdownForcedTemporalAndFT(t *testing.T) {
	r := runtimeRetirementRuntime(t, nil, 8)
	r.cfg.CallCorrection = runtimeTemporalTestConfig(0)
	r.cfg.CallCorrection.TemporalDecoder.LagSeconds = 60
	r.cfg.CallCorrection.TemporalDecoder.MaxWaitSeconds = 120
	r.cfg.CallCorrection.FT8QuietGapSeconds, r.cfg.CallCorrection.FT8HardCapSeconds = 60, 120
	r.cfg.CallCorrection.RecentBandBonusEnabled = true
	r.cfg.CallCorrection.RecentBandRecordMinUniqueSpotters = 2
	r.cfg.CallCorrection.RecentBandWindowSeconds = 3600
	r.recentBandStore = spot.NewRecentBandStore(time.Hour)
	r.whoSpotsMeStore = spot.NewWhoSpotsMeStore(time.Hour)
	r.statsTracker = stats.NewTracker()
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 16, MaxActiveKeys: 4, MaxCandidatesPerKey: 4, MaxReportersPerCand: 8,
		EvalMinInterval: time.Nanosecond, HysteresisWindows: 1, InactiveTTL: time.Hour,
	})
	r.signalResolver = resolver
	t.Cleanup(resolver.Stop)
	driver, err := spot.NewSignalResolverDriver(resolver)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(14074, "20m", "CW", 500)
	seedAt := time.Now().UTC().Add(-3 * time.Second)
	for _, reporter := range []string{"N0AAA", "N0BBB"} {
		if !resolver.Enqueue(spot.ResolverEvidence{ObservedAt: seedAt, Key: key, DXCall: "K1TEMP", Spotter: reporter, FrequencyKHz: 14074, RecencyWindow: time.Hour}) {
			t.Fatal("actual temporal population seed refused")
		}
	}
	if driver.Drain() != 2 {
		t.Fatal("actual temporal population was not consumed")
	}
	driver.Sweep(seedAt.Add(time.Millisecond))
	if snap, ok := resolver.Lookup(key); !ok || snap.Winner != "K1TEMP" || snap.TotalReporters != 2 {
		t.Fatal("future-due temporal fixture needs an actual available population")
	}
	callback := newRuntimeRetirementBarrier(t)
	r.gridUpdater = func(call, _ string) {
		if call == "K1BARRIER" {
			callback.enter()
		}
	}
	runtimeRetirementStart(r)
	acceptedLearningShutdownControl(t, r, "K1CTRL", "CW")
	acceptedLearningShutdownControl(t, r, "K1FCTRL", "FT8")
	temporal := acceptedLearningShutdownSpot("K1TEMP", "N0AAA", "CW")
	firstFT := acceptedLearningShutdownSpot("K1FT", "N0AAA", "FT8")
	secondFT := acceptedLearningShutdownSpot("K1FT", "N0BBB", "FT8")
	barrier := acceptedLearningShutdownSpot("K1BARRIER", "K1BARRIER", "CW")
	for _, s := range []*spot.Spot{temporal, firstFT, secondFT, barrier} {
		runtimeRetirementSend(t, r, s)
	}
	runtimeRetirementWait(t, "FIFO output-owner barrier after actual temporal/FT holds", callback.entered)
	p := r.outputPipeline
	// The sole temporal/FT owner is blocked, making these inspection reads
	// synchronized and stable. Source timestamps are current; only release due
	// times lie in the future, so forced retirement must resolve accepted work.
	if len(p.temporal.pending) != 1 || p.ftConfidence.pendingSpots != 2 {
		t.Fatalf("fixture did not establish actual holds: temporal=%d FT=%d", len(p.temporal.pending), p.ftConfidence.pendingSpots)
	}
	for _, next := range []func() (time.Time, bool){p.temporal.NextDue, p.ftConfidence.NextDue} {
		if due, ok := next(); !ok || !due.After(time.Now().UTC().Add(30*time.Second)) {
			t.Fatal("shutdown fixture accidentally used ordinarily due holds")
		}
	}
	if acceptedLearningShutdownWhoCount(t, r.whoSpotsMeStore, "K1TEMP") != 0 || acceptedLearningShutdownWhoCount(t, r.whoSpotsMeStore, "K1FT") != 0 || r.recentBandStore.RecentSupportCount("K1TEMP", "20m", "CW", time.Now().UTC()) != 0 || p.ftRecentBandStore.RecentSupportCount("K1FT", "20m", "FT8", time.Now().UTC()) != 0 {
		t.Fatal("unresolved actual temporal/FT holds learned before acceptance")
	}
	stopped := runtimeRetirementCall(t, callback.release, r.retireOutputPipeline)
	runtimeRetirementWait(t, "stop before forced future-due release", p.stopCh)
	callback.release()
	runtimeRetirementWait(t, "forced temporal/FT retirement", stopped)
	runtimeRetirementAssertDone(t, r)
	if acceptedLearningShutdownWhoCount(t, r.whoSpotsMeStore, "K1TEMP") != 1 || acceptedLearningShutdownWhoCount(t, r.whoSpotsMeStore, "K1FT") != 2 || r.recentBandStore.RecentSupportCount("K1TEMP", "20m", "CW", time.Now().UTC()) != 1 || p.ftRecentBandStore.RecentSupportCount("K1FT", "20m", "FT8", time.Now().UTC()) != 2 {
		t.Fatal("accepted forced releases must record once per observation through actual stores")
	}
	if r.recentBandStore.RecentSupportCount("K1FT", "20m", "FT8", time.Now().UTC()) != 0 || p.ftRecentBandStore.RecentSupportCount("K1TEMP", "20m", "CW", time.Now().UTC()) != 0 {
		t.Fatal("forced release bypassed mode-specific store safeguards")
	}
	runtimeRetirementAssertHandoffs(t, r, barrier, temporal, firstFT, secondFT)
	if r.spotBuffer.GetCount() != 6 {
		t.Fatal("forced retirement lost or duplicated accepted output after two isolated controls")
	}
}
