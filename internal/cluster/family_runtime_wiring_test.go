//go:build !qualification

// File role: Witnesses production construction, live resolver publication and
// actual upstream/output/scheduler ownership through independently checked fanout.
package cluster

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestFamilyDeliveryRunningPipelineImmediateAndHeld(t *testing.T) {
	source := acceptedLearningPathsConfigSource(t)
	temporalDeliveryDirectory(t)
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize: 8, MaxActiveKeys: 4, MaxCandidatesPerKey: 4, MaxReportersPerCand: 8,
		InactiveTTL: 5 * time.Minute, EvalMinInterval: time.Millisecond, SweepInterval: time.Millisecond,
		HysteresisWindows: 1, MaxEditDistance: 3, DistanceModelCW: "morse", DistanceModelRTTY: "baudot",
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)
	f := newFamilyDeliveryFixtureWithResolver(t, source, false, false, 2, 8, stabilizerTimeoutRelease, true, resolver)
	// The seeded tie and reporter refresh deliberately keep the same probable
	// 50% winner. This scheduling witness holds that tie, then requires the
	// separately corroborated 3/4 result below. The default low-P threshold
	// (25%) intentionally permits a 50% P through.
	f.pipeline.correctionCfg.StabilizerPDelayConfidencePercent = 51
	f.pipeline.correctionCfg.StabilizerPDelayMaxChecks = 1
	at := time.Now().UTC().Truncate(time.Second).Add(-3 * time.Second)
	immediate := familyDeliveryInput{"K1NOW", "K1NOW", "wired now", "V", at}
	held := familyDeliveryInput{"K1HOLD", "N0AAA", "wired hold", "V", at.Add(time.Second)}
	barrier := familyDeliveryInput{"K1BAR", "K1BAR", "wired bar", "V", at.Add(2 * time.Second)}
	key := spot.NewResolverSignalKey(14030, "20m", "CW", f.pipeline.correctionCfg.FrequencyToleranceHz)
	publish := func(call, receiver string) {
		t.Helper()
		if !resolver.Enqueue(spot.ResolverEvidence{ObservedAt: held.at, Key: key, DXCall: call,
			Spotter: receiver, FrequencyKHz: 14030, RecencyWindow: 2 * time.Minute}) {
			t.Fatal("live resolver refused bounded fixture evidence")
		}
	}
	publish(held.call, "N0AAA")
	publish("K1HOLE", "N0DDD")
	runtimeRetirementWaitFor(t, "live contested publication", func() bool {
		snapshot, ok := resolver.Lookup(key)
		return ok && resolver.MetricsSnapshot().Processed == 2 && snapshot.TotalReporters == 2 &&
			snapshot.State == spot.ResolverStateProbable && snapshot.Winner == held.call && snapshot.RunnerUp == "K1HOLE" &&
			snapshot.WinnerSupport == 1 && snapshot.RunnerSupport == 1
	})
	f.deduplicator.Start()
	go f.pipeline.run()
	t.Cleanup(func() {
		f.deduplicator.Stop()
		runtimeRetirementWait(t, "running pipeline retirement", runtimeRetirementCall(t, nil, f.pipeline.stop))
	})
	// One producer submits all inputs to the actual dedup input queue. The
	// eligible trailing V cannot arrive before the preceding held spot was seen.
	for _, input := range []familyDeliveryInput{immediate, held, barrier} {
		select {
		case f.deduplicator.GetInputChannel() <- input.spot():
		case <-time.After(2 * time.Second):
			t.Fatal("actual upstream queue refused the bounded witness")
		}
	}
	familyRunningAssertTCP(t, f, []familyDeliveryInput{immediate, barrier}, false)
	f.assertPeerReceipts(t, []familyDeliveryInput{immediate, barrier}, barrier.call)
	if f.pipeline.telnetStabilizer.Pending() != 1 || f.pipeline.tracker.StabilizerHeld() != 1 {
		t.Fatal("FIFO barrier passed without exactly one held ownership")
	}
	f.assertRows(t, "held-at-barrier sealed ring", f.ring.GetRecent(8), []familyDeliveryInput{immediate, barrier}, true)
	rows, err := f.writer.Recent(8)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.DXCallNorm == held.call {
			t.Fatal("held row reached archive before live evidence permitted release")
		}
	}
	// Publication uses only the running resolver owner. No synchronous driver
	// operates alongside it or manufactures release by calling the stage directly.
	wantProcessed := resolver.MetricsSnapshot().Accepted + 2
	publish(held.call, "N0BBB")
	publish(held.call, "N0CCC")
	runtimeRetirementWaitFor(t, "live corroborated publication", func() bool {
		snapshot, ok := resolver.Lookup(key)
		return ok && resolver.MetricsSnapshot().Processed >= wantProcessed && snapshot.Winner == held.call && snapshot.WinnerSupport == 3 && snapshot.TotalReporters == 4
	})
	familyRunningAssertTCP(t, f, []familyDeliveryInput{held}, true)
	f.assertPeerReceipts(t, []familyDeliveryInput{held}, held.call)
	runtimeRetirementWait(t, "final output join", runtimeRetirementCall(t, nil, f.pipeline.stop))
	if f.pipeline.telnetStabilizer.Pending() != 0 || f.pipeline.tracker.StabilizerReleasedDelayed() != 1 {
		t.Fatal("actual scheduler did not release exactly its one held row")
	}
	f.assertStored(t, []familyDeliveryInput{immediate, barrier, held})
}

// Independent fixed-column bytes include the original comment and report.
// The DIAG recipient has a separately expected share instead of reformatting
// the emitted Spot or reading its confidence fields to manufacture an oracle.
func familyRunningAssertTCP(t *testing.T, f familyDeliveryFixture, inputs []familyDeliveryInput, share bool) {
	t.Helper()
	for _, dst := range []struct {
		conn   net.Conn
		reader *bufio.Reader
		diag   bool
	}{{f.normalConn, f.normalReader, false}, {f.diagConn, f.diagReader, true}, {f.selfConn, f.selfReader, false}} {
		for _, input := range inputs {
			line := []byte(strings.Repeat(" ", 78))
			copy(line, fmt.Sprintf("DX de %-11s14030.00", input.receiver+":"))
			copy(line[27:], input.call)
			comment := input.comment
			if dst.diag {
				comment = "--%"
				if share {
					comment = "75%"
				}
			}
			copy(line[39:], "CW 10 dB "+comment)
			line[71] = 'V'
			copy(line[73:], input.at.Format("1504Z"))
			if got, want := correctionEvidenceReadDX(t, dst.conn, dst.reader), string(line)+"\r\n"; got != want {
				t.Fatalf("running pipeline TCP receipt:\n got %q\nwant %q", got, want)
			}
		}
	}
}
