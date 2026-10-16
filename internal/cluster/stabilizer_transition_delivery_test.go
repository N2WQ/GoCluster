package cluster

import (
	"testing"
	"time"

	"dxcluster/buffer"
	"dxcluster/dedup"
	"dxcluster/stats"
)

func assertStabilizerTransition(t *testing.T, tr *stats.Tracker, row, col int, samples uint64, average float64) {
	t.Helper()
	matrix := tr.StabilizerTransitionStats()
	var total uint64
	for _, cells := range matrix {
		for _, cell := range cells {
			total += cell.Samples
		}
	}
	if cell := matrix[row][col]; cell.Samples != samples || cell.AverageTurns != average || total != samples {
		t.Fatalf("transition [%d][%d] = %+v; total samples=%d, want %d/%.1f", row, col, cell, total, samples, average)
	}
}

func TestStabilizerTransitionActualRetryPreservesEntry(t *testing.T) {
	p := newDeliveryTestPipeline(buffer.NewRingBuffer(4), nil, nil)
	p.tracker = stats.NewTracker()
	p.stabilizerEnabled = true
	p.correctionCfg.StabilizerMaxChecks = 17
	p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 4)
	t.Cleanup(p.telnetStabilizer.Stop)
	s := newDeliveryTestSpot("K1ABC", "N0AAA", time.Now().UTC())
	s.Confidence = "S"
	if _, deliver := p.resolveDeliveryPlan(&outputSpotContext{spot: s}); deliver || p.telnetStabilizer.Pending() != 1 {
		t.Fatal("fixture did not admit S to the real scheduler")
	}
	p.telnetStabilizer.Start()
	first := receiveStabilizerMatrixEnvelope(t, p.telnetStabilizer)
	if first.entryGlyph != "S" || first.checksCompleted != 0 {
		t.Fatalf("initial admission lost entry: %+v", first)
	}
	// Change the live glyph before a re-hold, so recapturing it on enqueue
	// would lose S even if terminal-only mutation tests still passed.
	first.spot.Confidence = "?"
	p.handleStabilizerRelease(first)
	assertStabilizerTransition(t, p.tracker, 1, 3, 0, 0)
	second := receiveStabilizerMatrixEnvelope(t, p.telnetStabilizer)
	if second.entryGlyph != "S" || second.checksCompleted != 1 {
		t.Fatalf("retry lost entry/check count: %+v", second)
	}
	second.spot.Confidence = "V"
	p.handleStabilizerRelease(second)
	assertStabilizerTransition(t, p.tracker, 1, 3, 1, 2)
	if p.telnetStabilizer.Pending() != 0 || p.buf.GetCount() != 1 {
		t.Fatal("terminal V did not finish delivery")
	}
}

func TestStabilizerTransitionTimeoutSeventeenTurns(t *testing.T) {
	for _, action := range []string{stabilizerTimeoutRelease, stabilizerTimeoutSuppress} {
		t.Run(action, func(t *testing.T) {
			p := newDeliveryTestPipeline(buffer.NewRingBuffer(4), nil, nil)
			p.tracker = stats.NewTracker()
			p.stabilizerEnabled = true
			p.correctionCfg.StabilizerMaxChecks = 17
			p.correctionCfg.StabilizerTimeoutAction = action
			p.telnetStabilizer = newTelnetSpotStabilizer(time.Nanosecond, 4)
			t.Cleanup(p.telnetStabilizer.Stop)
			if _, deliver := p.resolveDeliveryPlan(&outputSpotContext{spot: newDeliveryTestSpot("K1ABC", "N0AAA", time.Now().UTC())}); deliver {
				t.Fatal("unknown fixture was not held")
			}
			p.telnetStabilizer.Start()
			for check := 1; check <= 17; check++ {
				envelope := receiveStabilizerMatrixEnvelope(t, p.telnetStabilizer)
				if envelope.entryGlyph != "?" || envelope.checksCompleted != check-1 {
					t.Fatalf("wrong scheduler context on check %d: %+v", check, envelope)
				}
				p.handleStabilizerRelease(envelope)
				if check < 17 {
					assertStabilizerTransition(t, p.tracker, 0, 0, 0, 0)
				}
			}
			assertStabilizerTransition(t, p.tracker, 0, 0, 1, 17)
			wantDelivered := 1
			if action == stabilizerTimeoutSuppress {
				wantDelivered = 0
			}
			if p.buf.GetCount() != wantDelivered || p.telnetStabilizer.Pending() != 0 {
				t.Fatal("terminal timeout changed delivery or left held work")
			}
		})
	}
}

func TestStabilizerTransitionRetryOverflowCountsAdmittedPath(t *testing.T) {
	p := newDeliveryTestPipeline(buffer.NewRingBuffer(4), nil, nil)
	p.tracker = stats.NewTracker()
	p.correctionCfg.StabilizerMaxChecks = 17
	p.telnetStabilizer = newTelnetSpotStabilizer(time.Hour, 1)
	t.Cleanup(p.telnetStabilizer.Stop)
	if !p.telnetStabilizer.Enqueue(newDeliveryTestSpot("K1FILL", "N0AAA", time.Now().UTC())) {
		t.Fatal("could not fill retry capacity")
	}
	p.handleStabilizerRelease(&telnetStabilizerEnvelope{
		spot: newDeliveryTestSpot("K1ABC", "N0AAA", time.Now().UTC()), entryGlyph: "S", checksCompleted: 1,
	})
	assertStabilizerTransition(t, p.tracker, 1, 0, 1, 2)
	if p.tracker.StabilizerOverflowRelease() != 1 || p.buf.GetCount() != 1 {
		t.Fatal("retry overflow did not exercise terminal fail-open")
	}
}

func TestStabilizerTransitionCountsBeforeSecondaryFilter(t *testing.T) {
	p := newDeliveryTestPipeline(buffer.NewRingBuffer(4), nil, nil)
	p.tracker = stats.NewTracker()
	p.secondaryMed = dedup.NewSecondaryDeduper(time.Minute, false)
	s := newDeliveryTestSpot("K1ABC", "N0AAA", time.Now().UTC())
	if !p.secondaryMed.ShouldForward(s) {
		t.Fatal("positive secondary admission failed")
	}
	p.handleStabilizerRelease(&telnetStabilizerEnvelope{spot: s, entryGlyph: "P", checksCompleted: 2})
	assertStabilizerTransition(t, p.tracker, 2, 0, 1, 3)
	processed, duplicates, _ := p.secondaryMed.GetStats()
	if p.buf.GetCount() != 0 || processed != 2 || duplicates != 1 || p.tracker.StabilizerReleasedDelayed() != 0 {
		t.Fatal("fixture must exercise actual final thinning and preserve legacy counters")
	}
}

func TestStabilizerTransitionExcludesImmediateAndAdmissionOverflow(t *testing.T) {
	p := newDeliveryTestPipeline(buffer.NewRingBuffer(4), nil, nil)
	p.tracker = stats.NewTracker()
	p.stabilizerEnabled = true
	p.telnetStabilizer = newTelnetSpotStabilizer(time.Hour, 1)
	t.Cleanup(p.telnetStabilizer.Stop)
	s := newDeliveryTestSpot("K1ABC", "N0AAA", time.Now().UTC())
	s.Confidence = "V"
	if _, deliver := p.resolveDeliveryPlan(&outputSpotContext{spot: s}); !deliver {
		t.Fatal("V must pass through immediately")
	}
	if !p.telnetStabilizer.Enqueue(newDeliveryTestSpot("K1FILL", "N0AAA", time.Now().UTC())) {
		t.Fatal("could not fill actual admission capacity")
	}
	if _, deliver := p.resolveDeliveryPlan(&outputSpotContext{spot: newDeliveryTestSpot("K1OVR", "N0BBB", time.Now().UTC())}); !deliver || p.tracker.StabilizerOverflowRelease() != 1 {
		t.Fatal("did not exercise initial overflow fail-open")
	}
	assertStabilizerTransition(t, p.tracker, 0, 0, 0, 0)
}

func receiveStabilizerMatrixEnvelope(t *testing.T, s *telnetSpotStabilizer) *telnetStabilizerEnvelope {
	t.Helper()
	select {
	case envelope := <-s.ReleaseChan():
		if envelope == nil {
			t.Fatal("scheduler closed before release")
		}
		return envelope
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler did not release held spot")
		return nil
	}
}
