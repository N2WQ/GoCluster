//go:build !qualification

// File role: Falsifies age boundaries with real held ownership, callback clock
// crossings, accepted occurrence counts and positive output receipts.
package cluster

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"
)

func TestOutputAgeExactBoundaryAndSentinels(t *testing.T) {
	at := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		observed time.Time
		limit    int
		delta    time.Duration
		stale    bool
	}{
		{"below", at, 300, 300*time.Second - time.Nanosecond, false},
		{"equal", at, 300, 300 * time.Second, false},
		{"above", at, 300, 300*time.Second + time.Nanosecond, true},
		{"disabled", at, 0, time.Hour, false},
		{"negative", at, -1, time.Hour, false},
		{"zero_timestamp", time.Time{}, 300, time.Hour, false},
		{"future", at.Add(time.Hour), 300, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newDeliveryTestSpot("K1ABC", "N0AAA", tc.observed)
			if got := isStaleAt(s, config.SpotPolicy{MaxAgeSeconds: tc.limit}, at.Add(tc.delta)); got != tc.stale {
				t.Fatalf("stale=%t want %t", got, tc.stale)
			}
		})
	}
	if isStaleAt(nil, config.SpotPolicy{MaxAgeSeconds: 300}, at) {
		t.Fatal("nil sentinel changed")
	}
}

func TestOutputAgeHeldAcceptanceAndFinalOffers(t *testing.T) {
	for _, stage := range []string{"below", "equal", "above", "location", "writeback"} {
		t.Run(stage, func(t *testing.T) {
			temporalDeliveryDirectory(t)
			r, driver, key := correctionLossResolver(t, 4, 8)
			cfg := correctionLossConfig()
			cfg.StabilizerEnabled, cfg.StabilizerMaxChecks = true, 1
			f := newAmbiguityDeliveryFixture(t, r, cfg)
			p := f.pipeline
			p.stabilizerEnabled = true
			p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 2)
			p.telnetStabilizer.Start()
			t.Cleanup(p.telnetStabilizer.Stop)
			p.familySuppressor = newTestTelnetFamilySuppressor()
			p.whoSpotsMeStore = spot.NewWhoSpotsMeStore(time.Hour)
			p.spotPolicy.MaxAgeSeconds = 300
			at := time.Now().UTC().Truncate(time.Second)
			now := at.Add(299 * time.Second)
			p.outputAgeNow = func() time.Time { return now }
			s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
			s.SourceType, s.DEMetadata.Continent = spot.SourceManual, "NA"
			s.DXMetadata.Grid = "FN31"
			p.processSpot(s, nil)
			envelope := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 0)
			if envelope.peerOriginal == nil || p.tracker.StabilizerHeld() != 1 || acceptedLearningShutdownWhoCount(t, p.whoSpotsMeStore, "K1ABC") != 0 {
				t.Fatal("positive held ownership control failed")
			}
			seedCorrectedGridWinner(t, r, driver, key)
			lookupCalls, writeCalls := 0, 0
			p.gridLookup = func(string) (string, bool, bool) {
				lookupCalls++
				if stage == "location" {
					now = at.Add(300*time.Second + time.Nanosecond)
				}
				return "JN58", false, true
			}
			p.gridUpdate = func(string, string) {
				writeCalls++
				if stage == "writeback" {
					now = at.Add(300*time.Second + time.Nanosecond)
				}
			}
			switch stage {
			case "below":
				now = at.Add(300*time.Second - time.Nanosecond)
			case "equal":
				now = at.Add(300 * time.Second)
			case "above":
				now = at.Add(300*time.Second + time.Nanosecond)
			}
			p.handleStabilizerRelease(envelope)
			delivered := stage == "below" || stage == "equal"
			accepted := delivered || stage == "writeback"
			wantAccepted, wantDrop := 0, uint64(1)
			if accepted {
				wantAccepted = 1
			}
			if delivered {
				wantDrop = 0
			}
			if n := acceptedLearningShutdownWhoCount(t, p.whoSpotsMeStore, "K1ABD"); n != wantAccepted {
				t.Fatalf("accepted occurrence count=%d want %d", n, wantAccepted)
			}
			if got := p.tracker.StabilizerTransitionStats()[0][4].Samples; got != uint64(wantAccepted) {
				t.Fatalf("pre-core expiry/accepted terminal count=%d want %d", got, wantAccepted)
			}
			if p.tracker.StabilizerAgeDropped() != wantDrop || p.tracker.StabilizerRetainedAboveFresh() != 0 || p.telnetStabilizer.Pending() != 0 {
				t.Fatal("age drop or retained counter/ownership mismatch")
			}
			_, witness := acceptedLearningOutputWitness(t, p.familySuppressor, "K1ABD")
			if witness != delivered || (p.buf.GetCount() == 1) != delivered || (p.tracker.StabilizerReleasedDelayed() == 1) != delivered {
				t.Fatal("expired work reached final output or a family witness")
			}
			if stage == "above" && lookupCalls != 0 || stage == "location" && (lookupCalls != 1 || writeCalls != 0) || stage == "writeback" && writeCalls != 1 {
				t.Fatalf("clock crossing did not exercise intended callback: lookup=%d write=%d", lookupCalls, writeCalls)
			}
			if delivered {
				peerOriginalAssertTCP(t, f, "K1ABD", "C")
				correctionEvidenceAssertOriginalPeer(t, f.peer, "K1ABC", at)
			}
			// A positive FIFO receipt proves absent expired output in both TCP
			// channels and the peer stream without a socket-silence assumption.
			now = at.Add(299 * time.Second)
			p.gridLookup, p.gridUpdate = nil, nil
			barrier := newDeliveryTestSpot("K1BAR", "K1BAR", at)
			barrier.SourceType, barrier.IsHuman = spot.SourceManual, true
			p.processSpot(barrier, nil)
			peerOriginalAssertTCP(t, f, "K1BAR", "V")
			peerOriginalAssertFields(t, f.peer, []string{"14020", "K1BAR", at.Format("02-Jan-2006"), at.Format("1504Z"), " ", "K1BAR", "N0CALL-1", "192.0.2.1"})
			f.writer.Stop()
			reopened, err := archive.NewWriter(f.archiveCfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(reopened.Stop)
			rows, err := reopened.Recent(4)
			wantRows := 1
			if delivered {
				wantRows++
			}
			if err != nil || len(rows) != wantRows {
				t.Fatalf("persisted rows=%d want %d, err=%v", len(rows), wantRows, err)
			}
		})
	}
}

func TestOutputAgeUnheldAndInitialOverflowAccounting(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		t.Run(fmt.Sprint(overflow), func(t *testing.T) {
			p := newDeliveryTestPipeline(nil, nil, nil)
			p.tracker = stats.NewTracker()
			p.spotPolicy.MaxAgeSeconds = 300
			p.whoSpotsMeStore = spot.NewWhoSpotsMeStore(time.Hour)
			at := time.Now().UTC()
			now := at.Add(299 * time.Second)
			p.outputAgeNow = func() time.Time { return now }
			if overflow {
				p.stabilizerEnabled = true
				p.telnetStabilizer = newTelnetSpotStabilizer(time.Hour, 1)
				t.Cleanup(p.telnetStabilizer.Stop)
				if !p.telnetStabilizer.Enqueue(newDeliveryTestSpot("K1FILL", "N0AAA", at)) {
					t.Fatal("no pressure control")
				}
			}
			p.gridUpdate = func(string, string) { now = at.Add(301 * time.Second) }
			s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
			s.DEMetadata.Continent = "NA"
			p.deliverSpot(&outputSpotContext{spot: s})
			if p.tracker.StabilizerAgeDropped() != 0 || acceptedLearningShutdownWhoCount(t, p.whoSpotsMeStore, "K1ABC") != 1 || (p.tracker.StabilizerOverflowRelease() == 1) != overflow {
				t.Fatal("unheld/initial-overflow final age drop entered held accounting or lost accepted learning")
			}
		})
	}
}

type ageCrossingSupportStore struct {
	spot.RecentSupportStore
	onQuery func()
}

func (s *ageCrossingSupportStore) RecentSupportCount(call, band, mode string, now time.Time) int {
	if s.onQuery != nil {
		s.onQuery()
	}
	return s.RecentSupportStore.RecentSupportCount(call, band, mode, now)
}

func (s *ageCrossingSupportStore) HasRecentSupport(call, band, mode string, minimum int, now time.Time) bool {
	if s.onQuery != nil {
		s.onQuery()
	}
	return s.RecentSupportStore.HasRecentSupport(call, band, mode, minimum, now)
}

func TestOutputAgeKeepsPostCoreRetentionAccounting(t *testing.T) {
	r, driver, key := correctionLossResolver(t, 4, 8)
	at := time.Now().UTC()
	seedRetentionPopulation(t, r, driver, key, at.Add(-time.Second))
	p := newDeliveryTestPipeline(nil, nil, telnet.NewServer(telnet.ServerOptions{BroadcastQueue: 2}, nil))
	p.signalResolver, p.tracker, p.correctionCfg = r, stats.NewTracker(), retentionConfig()
	p.correctionCfg.StabilizerMaxChecks, p.correctionCfg.StabilizerPDelayMaxChecks = 1, 1
	p.stabilizerEnabled = true
	p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 2)
	p.telnetStabilizer.Start()
	t.Cleanup(p.telnetStabilizer.Stop)
	p.spotPolicy.MaxAgeSeconds = 300
	p.whoSpotsMeStore = spot.NewWhoSpotsMeStore(time.Hour)
	now := at.Add(299 * time.Second)
	p.outputAgeNow = func() time.Time { return now }
	p.gridUpdate = func(string, string) { now = at.Add(301 * time.Second) }
	s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
	s.SourceType = spot.SourceManual
	s.DEMetadata.Continent = "NA"
	p.processSpot(s, nil)
	envelope := temporalDeliveryReceiveRetry(t, p.telnetStabilizer, key, 0)
	if envelope.bestConfidence != stabilizerConfidencePartial || driver.Drain() != 1 {
		t.Fatal("real P did not enter hold")
	}
	expireRetentionPopulation(t, r, driver, key)
	p.handleStabilizerRelease(envelope)
	if p.tracker.StabilizerRetainedAboveFresh() != 1 || p.tracker.StabilizerAgeDropped() != 1 || p.tracker.StabilizerReleasedDelayed() != 0 || acceptedLearningShutdownWhoCount(t, p.whoSpotsMeStore, "K1ABD") != 1 {
		t.Fatal("post-core age drop lost terminal retention or duplicated accepted learning")
	}
	assertStabilizerTransition(t, p.tracker, 2, 2, 1, 1)
}

func TestOutputAgeDuringFreshDelayDecision(t *testing.T) {
	for _, retry := range []bool{false, true} {
		for _, cross := range []bool{false, true} {
			t.Run(fmt.Sprintf("retry_%t/expire_%t", retry, cross), func(t *testing.T) {
				p := newDeliveryTestPipeline(nil, nil, nil)
				p.tracker = stats.NewTracker()
				p.stabilizerEnabled = true
				p.correctionCfg.StabilizerTimeoutAction = stabilizerTimeoutSuppress
				if retry {
					p.correctionCfg.StabilizerMaxChecks = 2
				}
				store := &ageCrossingSupportStore{RecentSupportStore: p.recentBandStore}
				p.recentBandStore = store
				p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 2)
				p.telnetStabilizer.Start()
				t.Cleanup(p.telnetStabilizer.Stop)
				at := time.Now().UTC()
				now := at.Add(299 * time.Second)
				p.outputAgeNow = func() time.Time { return now }
				p.spotPolicy.MaxAgeSeconds = 300
				p.processSpot(newDeliveryTestSpot("K1ABC", "N0AAA", at), nil)
				envelope := familyDeliveryReceiveRelease(t, p.telnetStabilizer)
				queries := 0
				store.onQuery = func() {
					queries++
					if cross {
						now = at.Add(301 * time.Second)
					}
				}
				p.handleStabilizerRelease(envelope)
				if queries == 0 {
					t.Fatal("fresh delay support query was not exercised")
				}
				wantDrop, wantSuppression, wantPending := uint64(0), uint64(0), int64(0)
				if cross {
					wantDrop = 1
				} else if retry {
					wantPending = 1
				} else {
					wantSuppression = 1
				}
				if p.tracker.StabilizerAgeDropped() != wantDrop || p.tracker.StabilizerSuppressedTimeout() != wantSuppression || p.telnetStabilizer.Pending() != wantPending {
					t.Fatal("support-query expiry was reheld or classified as timeout suppression")
				}
				if n := p.tracker.StabilizerTransitionStats()[0][0].Samples; n != wantSuppression {
					t.Fatal("pre-core age expiry entered the terminal matrix")
				}
			})
		}
	}
}

func TestOutputAgeImmediateDecisionCrossing(t *testing.T) {
	p := newDeliveryTestPipeline(nil, nil, nil)
	p.tracker = stats.NewTracker()
	p.stabilizerEnabled = true
	p.spotPolicy.MaxAgeSeconds = 300
	p.whoSpotsMeStore = spot.NewWhoSpotsMeStore(time.Hour)
	p.telnetStabilizer = newTelnetSpotStabilizer(time.Hour, 2)
	t.Cleanup(p.telnetStabilizer.Stop)
	at := time.Now().UTC()
	now := at.Add(299 * time.Second)
	p.outputAgeNow = func() time.Time { return now }
	queries := 0
	p.recentBandStore = &ageCrossingSupportStore{RecentSupportStore: p.recentBandStore, onQuery: func() {
		queries++
		now = at.Add(301 * time.Second)
	}}
	s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
	s.DEMetadata.Continent = "NA"
	p.processSpot(s, nil)
	if queries == 0 || p.telnetStabilizer.Pending() != 0 || p.tracker.StabilizerHeld() != 0 || p.tracker.StabilizerAgeDropped() != 0 || acceptedLearningShutdownWhoCount(t, p.whoSpotsMeStore, "K1ABC") != 0 {
		t.Fatal("immediate decision crossing learned or admitted stale work, or entered held age accounting")
	}
}

func TestOutputAgeInitialOverflowAfterAdmissionContention(t *testing.T) {
	p := newDeliveryTestPipeline(nil, nil, nil)
	p.tracker = stats.NewTracker()
	p.stabilizerEnabled = true
	p.spotPolicy.MaxAgeSeconds = 300
	p.whoSpotsMeStore = spot.NewWhoSpotsMeStore(time.Hour)
	p.telnetStabilizer = newTelnetSpotStabilizer(time.Hour, 1)
	t.Cleanup(p.telnetStabilizer.Stop)
	at := time.Now().UTC()
	if !p.telnetStabilizer.Enqueue(newDeliveryTestSpot("K1FILL", "N0AAA", at)) {
		t.Fatal("no real overflow pressure")
	}
	var seconds atomic.Int64
	seconds.Store(299)
	sampled := make(chan struct{})
	samples := 0
	p.outputAgeNow = func() time.Time {
		now := at.Add(time.Duration(seconds.Load()) * time.Second)
		samples++
		if samples == 2 {
			close(sampled)
		} // Final pre-enqueue age was sampled fresh.
		return now
	}
	s := newDeliveryTestSpot("K1ABC", "N0AAA", at)
	s.DEMetadata.Continent = "NA"
	p.telnetStabilizer.lifecycleMu.Lock()
	done := make(chan struct{})
	go func() { p.deliverSpot(&outputSpotContext{spot: s}); close(done) }()
	select {
	case <-sampled:
	case <-time.After(time.Second):
		p.telnetStabilizer.lifecycleMu.Unlock()
		t.Fatal("pre-admission age boundary was not reached")
	}
	seconds.Store(301)
	p.telnetStabilizer.lifecycleMu.Unlock()
	waitGridRetirement(t, done)
	if p.tracker.StabilizerAgeDropped() != 0 || p.tracker.StabilizerHeld() != 0 || acceptedLearningShutdownWhoCount(t, p.whoSpotsMeStore, "K1ABC") != 0 {
		t.Fatal("expired failed admission learned or entered held accounting")
	}
}

func BenchmarkOutputAgeBoundary(b *testing.B) {
	s := newDeliveryTestSpot("K1ABC", "N0AAA", time.Now().UTC())
	p := &outputPipeline{spotPolicy: config.SpotPolicy{MaxAgeSeconds: 300}}
	ctx := outputSpotContext{spot: s}
	b.ReportAllocs()
	for b.Loop() {
		if p.rejectExpiredOutput(&ctx) {
			b.Fatal("fresh input expired")
		}
	}
}
