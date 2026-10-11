//go:build !qualification

package cluster

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func TestCallCorrectionTemporalDeliveredFreshAuthority(t *testing.T) {
	temporalDeliveryDirectory(t)
	for _, mode := range []string{"expired_unswept_publication", "fresh_settled_recovery", "fresh_new_winner_recovers_from_unsupported_proposal", "fresh_split_blocks_saved_apply"} {
		t.Run(mode, func(t *testing.T) {
			cfg := temporalDeliveryConfig()
			at := time.Now().UTC().Truncate(time.Second)
			old := at.Add(-10 * time.Second)
			r, driver, key := temporalDeliveryPopulation(t, old, time.Second, "K1ABC")
			saved := correctionflow.SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABD")
			if !saved.SnapshotOK || saved.Snapshot.Winner != "K1ABC" || saved.Snapshot.TotalReporters != 4 {
				t.Fatalf("saved A3/B1 authority fixture was not published: %+v", saved)
			}
			fixture := newAmbiguityDeliveryFixture(t, r, cfg)
			p := fixture.pipeline
			p.recentBandStore, p.customSCPStore = nil, nil // No support floor may conceal missing resolver evidence.
			s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
			s.SourceType = spot.SourceManual
			controller := newRuntimeTemporalController(cfg)
			p.temporal = controller
			temporalDeliveryObserve(t, controller, old, key, s, saved)
			want := correctionEvidenceDeliveryCase{wantCall: "K1ABD", wantGlyph: "?"}
			if mode != "expired_unswept_publication" {
				fresh := time.Now().UTC()
				freshWinner := "K1ABC"
				if mode == "fresh_new_winner_recovers_from_unsupported_proposal" {
					freshWinner = "K1ABE"
					// Retire the old rows using the existing narrow key window
					// before restoring the longer window for new settled evidence.
					temporalDeliveryRetirePopulation(t, r, driver, key, fresh, freshWinner)
				}
				runnerCount := 1
				if mode == "fresh_split_blocks_saved_apply" {
					runnerCount = 2
				}
				temporalDeliverySeed(t, r, driver, key, fresh, 2*time.Minute, freshWinner, "K1ABD", 3, runnerCount)
				current, ok := r.Lookup(key)
				if !ok || current.TotalReporters != 3+runnerCount {
					t.Fatalf("fresh retained population was not published: %+v", current)
				}
				if runnerCount == 2 {
					if current.State != spot.ResolverStateSplit {
						t.Fatalf("fresh A3/B2 population did not qualify close split: %+v", current)
					}
					want.wantGlyph, want.wantPercent = "P", 40
				} else {
					if current.Winner != freshWinner || mode == "fresh_new_winner_recovers_from_unsupported_proposal" &&
						correctionflow.ResolverSupportForCall(current, "K1ABC") != 0 {
						t.Fatalf("new settled winner control retained unsupported saved proposal: %+v", current)
					}
					want.wantCall, want.wantGlyph, want.wantPercent, want.wantCorrections = freshWinner, "C", 75, 1
				}
			} else {
				// No owner sweep: Lookup remains a real previously published A3/B1
				// generation. Release must independently exclude its expired rows.
				current, ok := r.Lookup(key)
				if !ok || current.TotalReporters != 4 || current.ReceiverEvidence().RecencyWindow() != time.Second {
					t.Fatal("expired-publication fixture was swept or lost its retained evidence")
				}
			}
			release := temporalDeliveryDrainApply(t, controller, time.Now().UTC(), "K1ABC")
			p.processSpot(release.pending.spot, &release)
			temporalDeliveryAssertAccounting(t, p, 1, 1, int(want.wantCorrections))
			if p.tracker.TemporalCommitted() != want.wantCorrections || p.tracker.TemporalPathSwitches() != 0 {
				t.Errorf("accepted temporal authority counts: committed=%d switched=%d want accepted=%d switched=0",
					p.tracker.TemporalCommitted(), p.tracker.TemporalPathSwitches(), want.wantCorrections)
			}
			if mode == "expired_unswept_publication" {
				temporalDeliveryCheckWithoutPercent(t, fixture, want, at, true)
			} else {
				fixture.check(t, want, at)
			}
		})
	}
}

func TestCallCorrectionTemporalDeliveredAbstainUsesFreshConfidence(t *testing.T) {
	temporalDeliveryDirectory(t)
	cfg := temporalDeliveryConfig()
	cfg.TemporalDecoder.MinMarginScore, cfg.TemporalDecoder.OverflowAction = 50000, "abstain"
	at := time.Now().UTC().Truncate(time.Second)
	old := at.Add(-10 * time.Second)
	r, driver, key := temporalDeliveryPopulation(t, old, time.Second, "K1ABC")
	saved := correctionflow.SelectResolverPrimarySnapshotForCall(r, key, cfg, "K1ABD")
	fixture := newAmbiguityDeliveryFixture(t, r, cfg)
	controller := newRuntimeTemporalController(cfg)
	fixture.pipeline.temporal = controller
	s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
	s.SourceType = spot.SourceManual
	temporalDeliveryObserve(t, controller, old, key, s, saved)
	fresh := time.Now().UTC()
	temporalDeliveryRetirePopulation(t, r, driver, key, fresh, "K1ABD")
	temporalDeliverySeed(t, r, driver, key, fresh, 2*time.Minute, "K1ABD", "K1ABC", 3, 1)
	current, ok := r.Lookup(key)
	if !ok || current.Winner != "K1ABD" || current.TotalReporters != 4 {
		t.Fatalf("abstain confidence control did not publish fresh B3/A1: %+v", current)
	}
	releases := controller.Drain(time.Now().UTC(), false)
	if len(releases) != 1 || releases[0].decision.Action != correctionflow.TemporalDecisionActionAbstain {
		t.Fatalf("fixture did not exercise configured abstain: %+v", releases)
	}
	fixture.pipeline.processSpot(releases[0].pending.spot, &releases[0])
	temporalDeliveryAssertAccounting(t, fixture.pipeline, 0, 1, 0)
	if fixture.pipeline.tracker.TemporalCommitted() != 0 || fixture.pipeline.tracker.TemporalPathSwitches() != 0 {
		t.Error("abstained proposal updated confirmed history counters")
	}
	fixture.check(t, correctionEvidenceDeliveryCase{wantCall: "K1ABD", wantGlyph: "V", wantPercent: 75}, at)
}

func temporalDeliveryDirectory(t *testing.T) {
	t.Helper()
	t.Chdir(t.TempDir())
	previous := spot.CurrentDXClusterLayout()
	if err := spot.SetDXClusterLineLength(78); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := spot.SetDXClusterLineLength(previous.LineLength); err != nil {
			t.Error(err)
		}
	})
}

func temporalDeliveryConfig() config.CallCorrectionConfig {
	cfg := ambiguityDeliveryConfig(false)
	cfg.TemporalDecoder = runtimeTemporalTestConfig(0).TemporalDecoder
	cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 1, 1
	cfg.RecentBandBonusEnabled, cfg.CustomSCP.Enabled = false, false
	return cfg
}

func temporalDeliveryPopulation(t *testing.T, at time.Time, window time.Duration, winner string) (*spot.SignalResolver, *spot.SignalResolverDriver, spot.ResolverSignalKey) {
	t.Helper()
	r := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 32, MaxActiveKeys: 4, MaxCandidatesPerKey: 4, MaxReportersPerCand: 8,
		InactiveTTL: 5 * time.Minute, EvalMinInterval: time.Nanosecond, SweepInterval: time.Nanosecond,
		HysteresisWindows: 1, FreqGuardMinSeparationKHz: 0.3, FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance: 6, DistanceModelCW: "morse", DistanceModelRTTY: "baudot",
	})
	driver, err := spot.NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	key := spot.NewResolverSignalKey(14020, "20m", "CW", 500)
	temporalDeliverySeed(t, r, driver, key, at, window, winner, "K1ABD", 3, 1)
	return r, driver, key
}

func temporalDeliverySeed(t *testing.T, r *spot.SignalResolver, driver *spot.SignalResolverDriver, key spot.ResolverSignalKey, at time.Time, window time.Duration, winner, subject string, winnerCount, subjectCount int) {
	t.Helper()
	for _, group := range []struct {
		call      string
		frequency float64
		receivers []string
	}{
		{winner, 14020, []string{"N0AAA", "N0BBB", "N0CCC", "N0FFF"}[:winnerCount]},
		{subject, 14020.2, []string{"N0DDD", "N0EEE"}[:subjectCount]},
	} {
		for _, receiver := range group.receivers {
			if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: group.call,
				Spotter: receiver, FrequencyKHz: group.frequency, RecencyWindow: window}) {
				t.Fatal("temporal delivery fixture report was not admitted")
			}
		}
	}
	if got := driver.Drain(); got != winnerCount+subjectCount {
		t.Fatalf("temporal fixture consumed %d reports, want %d", got, winnerCount+subjectCount)
	}
	driver.Sweep(at.Add(2 * time.Nanosecond))
}

func temporalDeliveryRetirePopulation(t *testing.T, r *spot.SignalResolver, driver *spot.SignalResolverDriver, key spot.ResolverSignalKey, at time.Time, winner string) {
	t.Helper()
	if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: winner, Spotter: "N0AAA",
		FrequencyKHz: 14020, RecencyWindow: time.Second}) || driver.Drain() != 1 {
		t.Fatal("history control failed to retire the old receiver population")
	}
}
func temporalDeliveryObserve(t *testing.T, c *runtimeTemporalController, observed time.Time, key spot.ResolverSignalKey, s *spot.Spot, selection correctionflow.ResolverPrimarySelection) {
	t.Helper()
	accepted, _, reason := c.Observe(observed, key, s.DXCallNorm, runtimeTemporalPending{
		spot: s, evidence: spot.ResolverEvidence{Key: key}, hasEvidence: true,
		stabilizerResolverKey: key, hasStabilizerResolverKey: true, stabilizerEvidenceEnqueued: true, selection: selection,
	})
	if !accepted {
		t.Fatalf("actual temporal observation rejected fixture: %s", reason)
	}
}

func temporalDeliveryDrainApply(t *testing.T, c *runtimeTemporalController, at time.Time, winner string) runtimeTemporalRelease {
	t.Helper()
	releases := c.Drain(at, false)
	if len(releases) != 1 || releases[0].decision.Action != correctionflow.TemporalDecisionActionApply || releases[0].decision.Winner != winner {
		t.Fatalf("fixture did not positively exercise one saved temporal Apply for %s: %+v", winner, releases)
	}
	t.Logf("saved temporal proposal: winner=%s support=%d total=%d state=%s", winner,
		releases[0].decision.Selection.Snapshot.WinnerSupport, releases[0].decision.Selection.Snapshot.TotalReporters,
		releases[0].decision.Selection.Snapshot.State)
	return releases[0]
}

// Missing fresh evidence requires 0,false. Rejected-candidate percentage
// attribution belongs to P5, so the same destination checks can omit that
// assertion without making the old candidate percentage a test expectation.
func temporalDeliveryCheckWithoutPercent(t *testing.T, f ambiguityDeliveryFixture, want correctionEvidenceDeliveryCase, at time.Time, unavailable bool) {
	t.Helper()
	if got := f.pipeline.tracker.CallCorrections(); got != want.wantCorrections {
		t.Errorf("accepted identity changes=%d want %d", got, want.wantCorrections)
	}
	for _, diag := range []bool{false, true} {
		conn, reader := f.normalConn, f.normalReader
		if diag {
			conn, reader = f.diagConn, f.diagReader
		}
		got := correctionEvidenceReadDX(t, conn, reader)
		comment := "CW"
		if diag && unavailable {
			comment = "CW --%"
		}
		expected := correctionEvidenceWire(want.wantCall, want.wantGlyph, comment, at)
		if !diag || unavailable {
			if got != expected {
				t.Errorf("TCP diag=%t delivered %q want %q", diag, got, expected)
			}
		} else if len(got) != len(expected) || got[:39] != expected[:39] || got[70:] != expected[70:] || !strings.HasPrefix(got[39:70], "CW") {
			t.Errorf("DIAG retained identity/glyph/time fields disagree: %q", got)
		}
	}
	fields := strings.Split(peerAdmissionReadSpot(t, f.peer), "^")
	if len(fields) < 9 || fields[0] != "PC61" || fields[2] != want.wantCall || fields[6] != "N0AAA" || fields[7] != "N0CALL-1" {
		t.Errorf("peer retained identity/DE/origin mismatch: %v", fields)
	}
	rows := f.ring.GetRecent(2)
	if len(rows) != 1 {
		t.Errorf("retained buffer row count=%d", len(rows))
	} else {
		correctionEvidenceAssertSpot(t, "sealed ring", rows[0], want, at, false)
		if rows[0].SnapshotForAsync() != rows[0] {
			t.Error("retained spot was not sealed")
		}
		if unavailable && (rows[0].ConfidencePercent != 0 || rows[0].ConfidencePercentOK) {
			t.Errorf("expired support percentage=%d available=%t want 0,false", rows[0].ConfidencePercent, rows[0].ConfidencePercentOK)
		}
	}
	f.writer.Stop()
	reopened, err := archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Stop)
	rows, err = reopened.Recent(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Errorf("reopened retained row count=%d", len(rows))
	} else {
		correctionEvidenceAssertSpot(t, fmt.Sprintf("reopened archive unavailable=%t", unavailable), rows[0], want, at, false)
	}
}
