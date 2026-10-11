//go:build !qualification

// File role: Carries literal qualifying ambiguity populations through actual
// resolver, temporal, retry, TCP/peer, sealed buffer and reopened archive paths.
package cluster

import (
	"bufio"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"
)

func TestCallCorrectionMethodDeliveredAmbiguity(t *testing.T) {
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
	for _, tc := range []struct {
		name, path                 string
		neighborhood, changed, tie bool
		wantCall, wantGlyph        string
		wantPercent                int
		wantCorrections            uint64
	}{
		{"immediate_neighborhood", "immediate", true, false, false, "K1ABD", "P", 40, 0},
		{"temporal_apply_exact", "temporal_apply", false, false, false, "K1ABD", "P", 40, 0},
		{"temporal_apply_neighborhood", "temporal_apply", true, false, false, "K1ABD", "P", 40, 0},
		{"temporal_configured_fallback", "temporal_fallback", true, false, false, "K1ABD", "P", 40, 0},
		{"stabilizer_unchanged_split", "stabilizer", true, false, false, "K1ABD", "P", 40, 0},
		{"stabilizer_changed_exact_population", "stabilizer", true, true, false, "K1ABC", "C", 66, 1},
		{"equal_time_frequency_conflict", "immediate", false, false, true, "K1ABD", "P", 25, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := ambiguityDeliveryConfig(tc.neighborhood)
			at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
			resolver, driver, key := ambiguityDeliveryResolver(t, at, tc.tie)
			exact, ok := resolver.Lookup(key)
			if !ok {
				t.Fatal("fixture did not publish exact evidence")
			}
			if !tc.tie && (exact.State != spot.ResolverStateSplit || exact.TotalReporters != 5 ||
				correctionflow.ResolverSupportForCall(exact, "K1ABC") != 3 || correctionflow.ResolverSupportForCall(exact, "K1ABD") != 2) {
				t.Fatalf("fixture did not establish qualifying exact split: %+v", exact)
			}
			if tc.tie {
				conflict := false
				exact.ReceiverEvidence().Range(func(call string, row spot.ResolverReceiverObservation) bool {
					conflict = conflict || call == "K1ABC" && row.Receiver == "N0AAA" && row.FrequencyConflict
					return true
				})
				if !conflict || exact.TotalReporters != 4 {
					t.Fatal("fixture did not publish one conflicting receiver and A3/B1")
				}
			} else {
				neighborKey := key
				neighborKey.Bucket++
				neighbor, ok := resolver.Lookup(neighborKey)
				if !ok || neighbor.Winner != "K1ABC" || neighbor.WinnerSupport != 4 || neighbor.TotalReporters != 4 {
					t.Fatalf("neighbor must be a genuine eligible A4 proposal: %+v", neighbor)
				}
			}
			if tc.changed {
				// The fourth receiver is already present in the adjacent bucket.
				// One fresh exact membership changes A3/B2 to A4/B2. Shared
				// receivers stay deduplicated, so A's emitted share is 4/6=66%.
				ambiguityDeliveryAdd(t, resolver, key, "K1ABC", "N0FFF", 14020, at.Add(time.Nanosecond))
				if driver.Drain() != 1 {
					t.Fatal("changed exact report was not consumed")
				}
				driver.Sweep(at.Add(4 * time.Nanosecond))
				changed, ok := resolver.Lookup(key)
				if !ok || changed.State == spot.ResolverStateSplit || changed.Winner != "K1ABC" || changed.TotalReporters != 6 {
					t.Fatalf("fresh population failed legitimate exact reevaluation: %+v", changed)
				}
			}
			fixture := newAmbiguityDeliveryFixture(t, resolver, cfg)
			s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
			s.SourceType = spot.SourceManual
			switch tc.path {
			case "immediate":
				fixture.pipeline.processSpotBody(s, nil)
			case "stabilizer":
				fixture.pipeline.handleStabilizerRelease(&telnetStabilizerEnvelope{
					spot: s, resolverKey: key, hasResolverKey: true, evidenceEnqueued: true,
				})
			case "temporal_apply", "temporal_fallback":
				cfg.TemporalDecoder = runtimeTemporalTestConfig(0).TemporalDecoder
				cfg.TemporalDecoder.LagSeconds, cfg.TemporalDecoder.MaxWaitSeconds = 1, 1
				if tc.path == "temporal_fallback" {
					cfg.TemporalDecoder.MinMarginScore = 50000
				}
				fixture.pipeline.correctionCfg = cfg
				controller := newRuntimeTemporalController(cfg)
				fixture.pipeline.temporal = controller
				selection := correctionflow.SelectResolverPrimarySnapshotForCall(resolver, key, cfg, "K1ABD")
				accepted, _, reason := controller.Observe(at, key, "K1ABD", runtimeTemporalPending{
					spot: s, evidence: spot.ResolverEvidence{Key: key}, hasEvidence: true,
					stabilizerResolverKey: key, hasStabilizerResolverKey: true,
					stabilizerEvidenceEnqueued: true, selection: selection,
				})
				if !accepted {
					t.Fatalf("actual temporal controller refused fixture: %s", reason)
				}
				releases := controller.Drain(at.Add(2*time.Second), false)
				if len(releases) != 1 {
					t.Fatalf("actual temporal release count=%d, want 1", len(releases))
				}
				decision := releases[0].decision
				if tc.path == "temporal_apply" && (decision.Action != correctionflow.TemporalDecisionActionApply || decision.Winner != "K1ABC") {
					t.Fatalf("fixture failed to exercise eligible A temporal Apply: %+v", decision)
				}
				if tc.path == "temporal_fallback" && decision.Action != correctionflow.TemporalDecisionActionFallbackResolver {
					t.Fatalf("fixture failed to exercise configured fallback_resolver: %+v", decision)
				}
				t.Logf("actual temporal release: action=%s winner=%s state=%s", decision.Action, decision.Winner, decision.Selection.Snapshot.State)
				fixture.pipeline.processSpotBody(releases[0].pending.spot, &releases[0])
			default:
				t.Fatal("unknown delivery fixture path")
			}
			want := correctionEvidenceDeliveryCase{
				wantCall: tc.wantCall, wantGlyph: tc.wantGlyph, wantPercent: tc.wantPercent,
				wantCorrections: tc.wantCorrections,
			}
			fixture.check(t, want, at)
		})
	}
}

func TestCallCorrectionMethodDeliveredAmbiguityRetry(t *testing.T) {
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
	for _, mode := range []string{"unchanged_retry", "changed_retry", "timeout_suppress", "full_queue_fail_open"} {
		t.Run(mode, func(t *testing.T) {
			cfg := ambiguityDeliveryConfig(true)
			cfg.StabilizerEnabled = true
			cfg.StabilizerMaxChecks, cfg.StabilizerAmbiguousMaxChecks = 2, 2
			cfg.RecentBandRecordMinUniqueSpotters = 2
			at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
			resolver, driver, key := ambiguityDeliveryResolver(t, at, false)
			exact, ok := resolver.Lookup(key)
			if !ok || exact.State != spot.ResolverStateSplit || exact.TotalReporters != 5 {
				t.Fatalf("retry fixture did not establish qualifying exact ambiguity: %+v", exact)
			}
			fixture := newAmbiguityDeliveryFixture(t, resolver, cfg)
			p := fixture.pipeline
			p.stabilizerEnabled = true
			store := p.recentBandStore.(*spot.RecentBandStore)
			for _, call := range []string{"K1ABC", "K1ABD"} {
				store.Record(call, "20m", "CW", "N0AAA", at.Add(-time.Minute))
				store.Record(call, "20m", "CW", "N0BBB", at.Add(-time.Minute))
			}
			p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 1)
			if mode == "full_queue_fail_open" {
				// A far-future filler retains the one hard reservation while the
				// running scheduler and retry path contend for that bounded cap.
				p.telnetStabilizer = newTelnetSpotStabilizer(time.Hour, 1)
			}
			t.Cleanup(p.telnetStabilizer.Stop)
			p.telnetStabilizer.Start()
			if mode == "full_queue_fail_open" {
				if !p.telnetStabilizer.Enqueue(newDeliveryTestSpot("K1FILL", "N0AAA", at)) || p.telnetStabilizer.Pending() != 1 {
					t.Fatal("fixture failed to reserve the full stabilizer cap")
				}
				// Preserve fail-open on overflow even when exhaustion otherwise
				// suppresses a still-ambiguous spot.
				p.correctionCfg.StabilizerTimeoutAction = stabilizerTimeoutSuppress
			}
			if mode == "timeout_suppress" {
				p.correctionCfg.StabilizerMaxChecks, p.correctionCfg.StabilizerAmbiguousMaxChecks = 1, 1
				p.correctionCfg.StabilizerTimeoutAction = stabilizerTimeoutSuppress
			}
			s := newDeliveryTestSpot("K1ABD", "N0AAA", at)
			s.SourceType = spot.SourceManual
			probe := s.Clone()
			probe.Confidence = "P"
			eligibility := evaluateTelnetStabilizerDelay(probe, store, p.correctionCfg, time.Now().UTC(), exact, true)
			if !eligibility.ShouldDelay || eligibility.Reason != stabilizerDelayReasonAmbiguous ||
				eligibility.MaxChecks != p.correctionCfg.StabilizerAmbiguousMaxChecks {
				t.Fatalf("retry/suppress fixture policy did not positively qualify ambiguity: %+v", eligibility)
			}
			p.handleStabilizerRelease(&telnetStabilizerEnvelope{
				spot: s, resolverKey: key, hasResolverKey: true, evidenceEnqueued: true,
				delayReason: stabilizerDelayReasonAmbiguous.String(),
			})
			want := correctionEvidenceDeliveryCase{wantCall: "K1ABD", wantGlyph: "P", wantPercent: 40}
			switch mode {
			case "unchanged_retry", "changed_retry":
				// This synchronous stage must return without fanout after
				// reserving a retry. On pre-P2 code the wrong identity becomes C
				// and prevents entering the retry path; that is a blocked fixture,
				// not an additional independent unsafe-correction counterexample.
				if fixture.ring.GetCount() != 0 || p.tracker.StabilizerHeld() != 1 {
					t.Fatalf("retry fixture blocked by identity/glyph before scheduler admission: call=%s glyph=%s ring=%d held=%d",
						s.DXCallNorm, s.Confidence, fixture.ring.GetCount(), p.tracker.StabilizerHeld())
				}
				timer := time.NewTimer(3 * time.Second)
				defer timer.Stop()
				var retry *telnetStabilizerEnvelope
				select {
				case retry = <-p.telnetStabilizer.ReleaseChan():
				case <-timer.C:
					t.Fatal("actual scheduler did not positively release the admitted retry")
				}
				if retry == nil || retry.spot == nil || retry.checksCompleted != 1 || !retry.hasResolverKey ||
					retry.resolverKey != key || !retry.evidenceEnqueued || retry.delayReason != stabilizerDelayReasonAmbiguous.String() {
					t.Fatalf("actual scheduler lost retry ownership/context: %+v", retry)
				}
				if fixture.ring.GetCount() != 0 || p.telnetStabilizer.Pending() != 0 {
					t.Fatal("retry publication emitted early or retained its scheduler reservation")
				}
				if mode == "changed_retry" {
					ambiguityDeliveryAdd(t, resolver, key, "K1ABC", "N0FFF", 14020, at.Add(time.Nanosecond))
					if driver.Drain() != 1 {
						t.Fatal("fourth exact receiver was not consumed before second check")
					}
					driver.Sweep(at.Add(4 * time.Nanosecond))
					want.wantCall, want.wantGlyph, want.wantPercent, want.wantCorrections = "K1ABC", "C", 66, 1
				}
				p.handleStabilizerRelease(retry)
				if p.tracker.StabilizerHeld() != 1 || p.tracker.StabilizerReleasedDelayed() != 1 {
					t.Fatal("second check repeated the hold or failed to release one accepted retry")
				}
				fixture.check(t, want, at)
			case "full_queue_fail_open":
				if p.tracker.StabilizerOverflowRelease() != 1 || p.tracker.StabilizerSuppressedTimeout() != 0 || p.telnetStabilizer.Pending() != 1 {
					t.Errorf("full-queue retry failed to preserve fail-open policy: overflow=%d suppressed=%d pending=%d; call=%s glyph=%s",
						p.tracker.StabilizerOverflowRelease(), p.tracker.StabilizerSuppressedTimeout(), p.telnetStabilizer.Pending(), s.DXCallNorm, s.Confidence)
				}
				fixture.check(t, want, at)
			case "timeout_suppress":
				if fixture.ring.GetCount() != 0 || p.tracker.StabilizerSuppressedTimeout() != 1 || p.tracker.CallCorrections() != 0 {
					t.Errorf("exhausted ambiguity did not suppress before fanout: ring=%d suppressed=%d corrections=%d",
						fixture.ring.GetCount(), p.tracker.StabilizerSuppressedTimeout(), p.tracker.CallCorrections())
				}
				// The synchronous method has returned; join the archive worker
				// and inspect reopened storage, without a negative TCP timeout.
				fixture.writer.Stop()
				reopened, err := archive.NewWriter(fixture.archiveCfg)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(reopened.Stop)
				rows, err := reopened.Recent(2)
				if err != nil || len(rows) != 0 {
					t.Errorf("suppressed ambiguity reached persisted history: rows=%v err=%v", rows, err)
				}
			}
		})
	}
}

func ambiguityDeliveryConfig(neighborhood bool) config.CallCorrectionConfig {
	return config.CallCorrectionConfig{
		Enabled: true, RecencySeconds: 120, MaxEditDistance: 6, MinConsensusReports: 1,
		FrequencyToleranceHz: 500, FreqGuardMinSeparationKHz: 0.3, FreqGuardRunnerUpRatio: 0.6,
		DistanceModelCW: "morse", DistanceModelRTTY: "baudot", InvalidAction: "broadcast",
		ResolverNeighborhoodEnabled: neighborhood, ResolverNeighborhoodBucketRadius: 1, ResolverNeighborhoodMaxDistance: 6,
		StabilizerMaxChecks: 1, StabilizerTimeoutAction: stabilizerTimeoutRelease,
	}
}

func ambiguityDeliveryResolver(t *testing.T, at time.Time, tie bool) (*spot.SignalResolver, *spot.SignalResolverDriver, spot.ResolverSignalKey) {
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
	for _, receiver := range []string{"N0AAA", "N0BBB", "N0CCC"} {
		ambiguityDeliveryAdd(t, r, key, "K1ABC", receiver, 14020, at)
	}
	ambiguityDeliveryAdd(t, r, key, "K1ABD", "N0DDD", 14020.2, at)
	want := 5
	if tie {
		ambiguityDeliveryAdd(t, r, key, "K1ABC", "N0AAA", 14020.1, at)
	} else {
		ambiguityDeliveryAdd(t, r, key, "K1ABD", "N0EEE", 14020.2, at)
		neighbor := key
		neighbor.Bucket++
		for _, receiver := range []string{"N0AAA", "N0BBB", "N0CCC", "N0FFF"} {
			ambiguityDeliveryAdd(t, r, neighbor, "K1ABC", receiver, 14020.5, at)
		}
		want = 9
	}
	if got := driver.Drain(); got != want {
		t.Fatalf("fixture consumed %d reports, want %d", got, want)
	}
	driver.Sweep(at.Add(2 * time.Nanosecond))
	return r, driver, key
}

func ambiguityDeliveryAdd(t *testing.T, r *spot.SignalResolver, key spot.ResolverSignalKey, call, receiver string, frequency float64, at time.Time) {
	t.Helper()
	if !r.Enqueue(spot.ResolverEvidence{ObservedAt: at, Key: key, DXCall: call, Spotter: receiver, FrequencyKHz: frequency, RecencyWindow: 2 * time.Minute}) {
		t.Fatal("bounded ambiguity fixture report was not admitted")
	}
}

type ambiguityDeliveryFixture struct {
	pipeline                 *outputPipeline
	ring                     *buffer.RingBuffer
	writer                   *archive.Writer
	archiveCfg               config.ArchiveConfig
	normalConn, diagConn     net.Conn
	normalReader, diagReader *bufio.Reader
	peer                     *peerAdmissionSocket
}

func newAmbiguityDeliveryFixture(t *testing.T, resolver *spot.SignalResolver, cfg config.CallCorrectionConfig) ambiguityDeliveryFixture {
	t.Helper()
	f := ambiguityDeliveryFixture{ring: buffer.NewRingBuffer(4), archiveCfg: config.ArchiveConfig{
		DBPath: filepath.Join(t.TempDir(), "archive"), QueueSize: 4, BatchSize: 1, BatchIntervalMS: 1,
	}}
	var err error
	f.writer, err = archive.NewWriter(f.archiveCfg)
	if err != nil {
		t.Fatal(err)
	}
	f.writer.Start()
	t.Cleanup(f.writer.Stop)
	port := runtimeQualificationPort(t)
	srv := telnet.NewServer(telnet.ServerOptions{
		Port: port, LoginPrompt: "login: ", LoginGreeting: "ACCOUNTING_READY\n",
		MaxConnections: 4, BroadcastWorkers: 1, BroadcastQueue: 4, WorkerQueue: 4, ClientBuffer: 8,
		BroadcastBatchIntervalSet: true,
	}, nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)
	// Login helpers close sockets first; wait for their final profile writes
	// before the parent restores its fixture-owned working directory.
	t.Cleanup(func() {
		deadline := time.Now().Add(3 * time.Second)
		for len(srv.ListClientCallsigns()) != 0 {
			if time.Now().After(deadline) {
				t.Error("ambiguity clients did not retire")
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
	f.normalConn, f.normalReader = correctionEvidenceLogin(t, port, "DL1ABC", "OFF")
	f.diagConn, f.diagReader = correctionEvidenceLogin(t, port, "DL2ABC", "CONF")
	manager, recipient := correctionEvidencePeer(t)
	f.peer = recipient
	f.pipeline = newDeliveryTestPipeline(f.ring, f.writer, srv)
	f.pipeline.signalResolver, f.pipeline.correctionCfg, f.pipeline.peerManager = resolver, cfg, manager
	f.pipeline.tracker = stats.NewTracker()
	return f
}

func (f ambiguityDeliveryFixture) check(t *testing.T, want correctionEvidenceDeliveryCase, at time.Time) {
	t.Helper()
	if got := f.pipeline.tracker.CallCorrections(); got != want.wantCorrections {
		t.Errorf("accepted identity changes=%d want %d", got, want.wantCorrections)
	}
	for _, dst := range []struct {
		name, comment string
		conn          net.Conn
		reader        *bufio.Reader
	}{
		{"normal TCP", "CW", f.normalConn, f.normalReader},
		{"DIAG TCP", fmt.Sprintf("CW %d%%", want.wantPercent), f.diagConn, f.diagReader},
	} {
		got := correctionEvidenceReadDX(t, dst.conn, dst.reader)
		if expected := correctionEvidenceWire(want.wantCall, want.wantGlyph, dst.comment, at); got != expected {
			t.Errorf("%s delivered:\n got %q\nwant %q", dst.name, got, expected)
		}
	}
	fields := strings.Split(peerAdmissionReadSpot(t, f.peer), "^")
	if len(fields) < 9 {
		t.Errorf("peer receipt lacked PC61 fields: %v", fields)
	} else {
		frequency, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || fields[0] != "PC61" || frequency != 14020 || fields[2] != want.wantCall ||
			fields[3] != at.Format("02-Jan-2006") || fields[4] != at.Format("1504Z") || fields[6] != "N0AAA" || fields[7] != "N0CALL-1" {
			t.Errorf("actual peer delivered identity/date/DE/origin: %v", fields)
		}
	}
	rows := f.ring.GetRecent(2)
	if len(rows) != 1 {
		t.Errorf("buffer count=%d want 1", len(rows))
	} else {
		correctionEvidenceAssertSpot(t, "sealed ring", rows[0], want, at, true)
		if rows[0].SnapshotForAsync() != rows[0] {
			t.Error("buffer delivery was not sealed")
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
		t.Errorf("reopened archive count=%d want 1", len(rows))
	} else {
		correctionEvidenceAssertSpot(t, "reopened archive", rows[0], want, at, false)
	}
}
