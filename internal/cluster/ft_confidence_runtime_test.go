package cluster

import (
	"testing"
	"time"

	"dxcluster/buffer"
	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/spot"
	"dxcluster/telnet"
)

func newTestFTOutputPipeline(cfg config.CallCorrectionConfig, rb *buffer.RingBuffer) *outputPipeline {
	return &outputPipeline{
		buf:               rb,
		correctionCfg:     cfg,
		ctyLookup:         func() *cty.CTYDatabase { return nil },
		temporal:          newRuntimeTemporalController(cfg),
		ftConfidence:      newFTConfidenceController(cfg),
		ftRecentBandStore: newFTRecentBandStore(cfg),
	}
}

func currentFTObservedTime() time.Time {
	return time.Now().UTC().Truncate(time.Second)
}

func ftTestBurstTiming(t *testing.T) ftConfidenceTiming {
	t.Helper()
	timing, ok := ftConfidenceTimingForMode("FT8", newFTConfidencePolicy(config.CallCorrectionConfig{}))
	if !ok {
		t.Fatal("missing FT timing for FT8")
	}
	return timing
}

func mustBuildFTConfidenceKey(t *testing.T, s *spot.Spot) (ftConfidenceKey, ftConfidenceTiming) {
	t.Helper()
	key, _, ok := buildFTConfidenceKey(s)
	if !ok {
		t.Fatalf("expected FT confidence key for %+v", s)
	}
	timing, ok := ftConfidenceTimingForMode(key.mode, newFTConfidencePolicy(config.CallCorrectionConfig{}))
	if !ok {
		t.Fatalf("missing FT timing for %s", key.mode)
	}
	return key, timing
}

func TestOutputPipelineFTBurstAssignsPAfterRelease(t *testing.T) {
	cfg := config.CallCorrectionConfig{}
	rb := buffer.NewRingBuffer(8)
	pipeline := newTestFTOutputPipeline(cfg, rb)

	observedAt := currentFTObservedTime()
	first := spot.NewSpot("K1ABC", "N0AAA", 14074.1, "FT8")
	first.Time = observedAt
	pipeline.processSpotBody(first, nil)
	if got := rb.GetCount(); got != 0 {
		t.Fatalf("expected first FT spot to be held, got ring count %d", got)
	}

	second := spot.NewSpot("K1ABC", "N0BBB", 14074.1, "FT8")
	second.Time = observedAt.Add(14 * time.Second)
	pipeline.processSpotBody(second, nil)
	if got := rb.GetCount(); got != 0 {
		t.Fatalf("expected corroborating FT spot to be held, got ring count %d", got)
	}

	pipeline.releaseDueFT(time.Now().UTC().Add(30*time.Second), false)
	recent := rb.GetRecent(2)
	if len(recent) != 2 {
		t.Fatalf("expected two released FT spots, got %d", len(recent))
	}
	for _, s := range recent {
		if s.Confidence != "P" {
			t.Fatalf("expected P confidence after two spotters, got %q", s.Confidence)
		}
	}
}

func TestOutputPipelineFTBurstUsesConfiguredThresholdsAndTiming(t *testing.T) {
	cfg := config.CallCorrectionConfig{
		PMinUniqueSpotters: 3,
		VMinUniqueSpotters: 4,
		FT8QuietGapSeconds: 7,
		FT8HardCapSeconds:  14,
	}
	controller := newFTConfidenceController(cfg)
	base := time.Unix(1_700_000_000, 0).UTC()
	firstCtx := outputSpotContext{spot: spot.NewSpot("K1CFG", "N0AAA", 14074.1, "FT8"), modeUpper: "FT8"}
	if held, uniqueCount := controller.Observe(base, firstCtx); !held || uniqueCount != 1 {
		t.Fatalf("expected first configured FT spot to be held with unique count 1, got held=%v unique=%d", held, uniqueCount)
	}
	if due, ok := controller.NextDue(); !ok || !due.Equal(base.Add(7*time.Second)) {
		t.Fatalf("expected configured FT8 due %s, got due=%s ok=%v", base.Add(7*time.Second), due, ok)
	}

	rb := buffer.NewRingBuffer(8)
	pipeline := newTestFTOutputPipeline(cfg, rb)

	observedAt := currentFTObservedTime()
	first := spot.NewSpot("K1CFG", "N0AAA", 14074.1, "FT8")
	second := spot.NewSpot("K1CFG", "N0BBB", 14074.1, "FT8")
	third := spot.NewSpot("K1CFG", "N0CCC", 14074.1, "FT8")
	first.Time = observedAt
	second.Time = observedAt.Add(200 * time.Millisecond)
	third.Time = observedAt.Add(400 * time.Millisecond)

	pipeline.processSpotBody(first, nil)
	pipeline.processSpotBody(second, nil)
	pipeline.processSpotBody(third, nil)
	pipeline.releaseDueFT(observedAt.Add(30*time.Second), false)

	recent := rb.GetRecent(3)
	if len(recent) != 3 {
		t.Fatalf("expected three released FT spots, got %d", len(recent))
	}
	for _, s := range recent {
		if s.Confidence != "P" {
			t.Fatalf("expected configured thresholds to keep three spotters at P, got %q", s.Confidence)
		}
	}
}

func TestOutputPipelineFTRecentSupportPromotesSingleReporterToS(t *testing.T) {
	cfg := config.CallCorrectionConfig{
		RecentBandBonusEnabled:            true,
		RecentBandWindowSeconds:           12 * 60 * 60,
		RecentBandRecordMinUniqueSpotters: 2,
	}
	rb := buffer.NewRingBuffer(8)
	pipeline := newTestFTOutputPipeline(cfg, rb)

	observedAt := currentFTObservedTime()
	first := spot.NewSpot("K1SCP", "N0AAA", 14074.1, "FT8")
	first.Time = observedAt
	second := spot.NewSpot("K1SCP", "N0BBB", 14074.1, "FT8")
	second.Time = observedAt.Add(400 * time.Millisecond)
	pipeline.processSpotBody(first, nil)
	pipeline.processSpotBody(second, nil)
	pipeline.releaseDueFT(observedAt.Add(30*time.Second), false)

	followup := spot.NewSpot("K1SCP", "N0CCC", 14074.2, "FT8")
	followup.Time = observedAt.Add(10 * time.Second)
	pipeline.processSpotBody(followup, nil)
	pipeline.releaseDueFT(observedAt.Add(40*time.Second), false)

	recent := rb.GetRecent(1)
	if len(recent) != 1 {
		t.Fatalf("expected one most-recent released FT spot, got %d", len(recent))
	}
	if recent[0].Confidence != "S" {
		t.Fatalf("expected recent-on-band FT support to promote S, got %q", recent[0].Confidence)
	}
}

func TestApplyResolverStageBypassesFTModesEvenWhenTelnetEnabled(t *testing.T) {
	pipeline := newTestFTOutputPipeline(config.CallCorrectionConfig{}, buffer.NewRingBuffer(4))
	pipeline.telnet = &telnet.Server{}

	ftSpot := spot.NewSpot("K1FT", "N0AAA", 14074.0, "FT8")
	ctx, ok := pipeline.prepareSpotContext(ftSpot)
	if !ok {
		t.Fatal("expected FT spot context")
	}
	if !pipeline.applyResolverStage(&ctx, nil) {
		t.Fatal("expected FT resolver stage bypass to continue")
	}
	if got := ctx.spot.Confidence; got != "" {
		t.Fatalf("expected FT spot confidence to remain blank before FT corroboration, got %q", got)
	}
}

func TestBuildFTConfidenceKeyUsesCanonicalOperationalFrequency(t *testing.T) {
	first := spot.NewSpot("K1CAN", "N0AAA", 14074.0, "FT8")
	first.ObservedFrequency = 14076.11
	second := spot.NewSpot("K1CAN", "N0BBB", 14074.0, "FT8")
	second.ObservedFrequency = 14076.29

	firstKey, _ := mustBuildFTConfidenceKey(t, first)
	secondKey, _ := mustBuildFTConfidenceKey(t, second)
	if firstKey != secondKey {
		t.Fatalf("expected canonical operational frequency to drive FT key, got %+v vs %+v", firstKey, secondKey)
	}
}

func TestBuildFTConfidenceKeyUsesBurstKeying(t *testing.T) {
	cases := []struct {
		mode string
	}{
		{mode: "FT8"},
		{mode: "FT4"},
		{mode: "FT2"},
	}

	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			first := spot.NewSpot("K1BURST", "N0AAA", 14074.0, tc.mode)
			first.Time = time.Unix(1_700_000_000, 0).UTC()
			second := spot.NewSpot("K1BURST", "N0BBB", 14074.0, tc.mode)
			second.Time = first.Time.Add(12 * time.Second)
			third := spot.NewSpot("K1OTHER", "N0CCC", 14074.0, tc.mode)
			third.Time = first.Time

			firstKey, timing := mustBuildFTConfidenceKey(t, first)
			secondKey, secondTiming := mustBuildFTConfidenceKey(t, second)
			thirdKey, _ := mustBuildFTConfidenceKey(t, third)
			if firstKey != secondKey {
				t.Fatalf("expected same-burst %s observations to share a key, got %+v vs %+v", tc.mode, firstKey, secondKey)
			}
			if thirdKey == firstKey {
				t.Fatalf("expected different DX %s observation to change key", tc.mode)
			}
			if timing != secondTiming {
				t.Fatalf("expected %s timing to remain stable, got %+v vs %+v", tc.mode, timing, secondTiming)
			}
		})
	}
}

func TestFTConfidenceControllerExtendsDueWithinBurst(t *testing.T) {
	controller := newFTConfidenceController(config.CallCorrectionConfig{})
	base := time.Unix(1_700_000_000, 0).UTC()
	timing := ftTestBurstTiming(t)

	first := spot.NewSpot("K1LIVE", "N0AAA", 14074.0, "FT8")
	second := spot.NewSpot("K1LIVE", "N0BBB", 14074.0, "FT8")
	first.Time = base
	second.Time = base.Add(14 * time.Second)

	firstHeld, firstUnique := controller.Observe(base, outputSpotContext{spot: first, modeUpper: "FT8"})
	if !firstHeld || firstUnique != 1 {
		t.Fatalf("expected first FT8 spot to be held with unique count 1, got held=%v unique=%d", firstHeld, firstUnique)
	}
	firstDue, ok := controller.NextDue()
	if !ok {
		t.Fatal("expected first due time")
	}
	if want := base.Add(timing.quietGap); !firstDue.Equal(want) {
		t.Fatalf("expected first due %s, got %s", want, firstDue)
	}

	secondArrived := base.Add(1500 * time.Millisecond)
	secondHeld, secondUnique := controller.Observe(secondArrived, outputSpotContext{spot: second, modeUpper: "FT8"})
	if !secondHeld || secondUnique != 2 {
		t.Fatalf("expected second FT8 spot to extend the burst, got held=%v unique=%d", secondHeld, secondUnique)
	}
	secondDue, ok := controller.NextDue()
	if !ok {
		t.Fatal("expected extended due time")
	}
	if want := secondArrived.Add(timing.quietGap); !secondDue.Equal(want) {
		t.Fatalf("expected extended due %s, got %s", want, secondDue)
	}
}

func TestFTConfidenceControllerFlushesOnHardCap(t *testing.T) {
	controller := newFTConfidenceController(config.CallCorrectionConfig{})
	base := time.Unix(1_700_000_000, 0).UTC()
	timing := ftTestBurstTiming(t)

	for i := 0; i < 12; i++ {
		s := spot.NewSpot("K1CAP", "N0AAA", 14074.0, "FT8")
		arrivedAt := base.Add(time.Duration(i) * 1500 * time.Millisecond)
		s.Time = arrivedAt
		held, _ := controller.Observe(arrivedAt, outputSpotContext{spot: s, modeUpper: "FT8"})
		if !held {
			t.Fatalf("expected observation %d to be held", i)
		}
	}
	nextDue, ok := controller.NextDue()
	if !ok {
		t.Fatal("expected hard-cap due")
	}
	if want := base.Add(timing.hardCap); !nextDue.Equal(want) {
		t.Fatalf("expected hard-cap due %s, got %s", want, nextDue)
	}
}

func TestFTConfidenceControllerStartsNewBurstAfterQuietGap(t *testing.T) {
	controller := newFTConfidenceController(config.CallCorrectionConfig{})
	base := time.Unix(1_700_000_000, 0).UTC()
	timing := ftTestBurstTiming(t)

	first := spot.NewSpot("K1SPLIT", "N0AAA", 14074.0, "FT8")
	second := spot.NewSpot("K1SPLIT", "N0BBB", 14074.0, "FT8")
	first.Time = base
	second.Time = base.Add(10 * time.Second)

	if held, _ := controller.Observe(base, outputSpotContext{spot: first, modeUpper: "FT8"}); !held {
		t.Fatal("expected first burst spot to be held")
	}
	releases := controller.Drain(base.Add(timing.quietGap), false)
	if len(releases) != 1 || len(releases[0].spotters) != 1 {
		t.Fatalf("expected first burst release before split, got %+v", releases)
	}
	if held, uniqueCount := controller.Observe(base.Add(timing.quietGap+100*time.Millisecond), outputSpotContext{spot: second, modeUpper: "FT8"}); !held || uniqueCount != 1 {
		t.Fatalf("expected second observation to start a new burst, got held=%v unique=%d", held, uniqueCount)
	}
}

func TestFTConfidenceControllerIgnoresObservedTimeForPSKReporterBursting(t *testing.T) {
	controller := newFTConfidenceController(config.CallCorrectionConfig{})
	base := time.Unix(1_700_000_000, 0).UTC()
	timing := ftTestBurstTiming(t)

	first := spot.NewSpot("K1PSK", "N0AAA", 14074.0, "FT8")
	first.SourceType = spot.SourcePSKReporter
	first.Time = base
	second := spot.NewSpot("K1PSK", "N0BBB", 14074.0, "FT8")
	second.SourceType = spot.SourcePSKReporter
	second.Time = base.Add(14 * time.Second)

	if held, _ := controller.Observe(base, outputSpotContext{spot: first, modeUpper: "FT8"}); !held {
		t.Fatal("expected first PSKReporter FT8 spot to be held")
	}
	secondArrived := base.Add(1200 * time.Millisecond)
	held, uniqueCount := controller.Observe(secondArrived, outputSpotContext{spot: second, modeUpper: "FT8"})
	if !held || uniqueCount != 2 {
		t.Fatalf("expected second PSKReporter FT8 spot to share arrival burst, got held=%v unique=%d", held, uniqueCount)
	}
	releases := controller.Drain(secondArrived.Add(timing.quietGap), false)
	if len(releases) != 1 || len(releases[0].spotters) != 2 {
		t.Fatalf("expected one released PSKReporter burst with two spotters, got %+v", releases)
	}
}

func TestFTConfidenceControllerAllowsCrossSourceBurstCorroboration(t *testing.T) {
	controller := newFTConfidenceController(config.CallCorrectionConfig{})
	base := time.Unix(1_700_000_000, 0).UTC()
	timing := ftTestBurstTiming(t)

	first := spot.NewSpot("K1XSR", "N0AAA", 14074.0, "FT8")
	first.SourceType = spot.SourcePSKReporter
	second := spot.NewSpot("K1XSR", "N0BBB", 14074.0, "FT8")
	second.SourceType = spot.SourceFT8

	if held, _ := controller.Observe(base, outputSpotContext{spot: first, modeUpper: "FT8"}); !held {
		t.Fatal("expected first cross-source FT8 spot to be held")
	}
	held, uniqueCount := controller.Observe(base.Add(800*time.Millisecond), outputSpotContext{spot: second, modeUpper: "FT8"})
	if !held || uniqueCount != 2 {
		t.Fatalf("expected cross-source corroboration, got held=%v unique=%d", held, uniqueCount)
	}
	releases := controller.Drain(base.Add(timing.quietGap+time.Second), false)
	if len(releases) != 1 || len(releases[0].spotters) != 2 {
		t.Fatalf("expected one cross-source release with two spotters, got %+v", releases)
	}
}

func TestFTConfidenceControllerOverflowFailsOpen(t *testing.T) {
	controller := &ftConfidenceController{
		enabled:          true,
		maxPendingGroups: 1,
		maxPendingSpots:  1,
		pending:          make(map[ftConfidenceKey]*ftConfidencePendingGroup),
	}

	base := time.Unix(1_700_000_000, 0).UTC()
	firstSpot := spot.NewSpot("K1OVR", "N0AAA", 14074.1, "FT8")
	firstSpot.Time = base
	firstCtx := outputSpotContext{spot: firstSpot, modeUpper: "FT8"}
	if held, _ := controller.Observe(base, firstCtx); !held {
		t.Fatalf("expected first FT spot to be held")
	}

	secondSpot := spot.NewSpot("K1OVR", "N0BBB", 14074.1, "FT8")
	secondSpot.Time = base.Add(300 * time.Millisecond)
	secondCtx := outputSpotContext{spot: secondSpot, modeUpper: "FT8"}
	held, uniqueCount := controller.Observe(base.Add(300*time.Millisecond), secondCtx)
	if held {
		t.Fatalf("expected overflow path to fail open on second pending FT spot")
	}
	if uniqueCount != 2 {
		t.Fatalf("expected fail-open path to preserve best-known unique count 2, got %d", uniqueCount)
	}
}

func TestFTConfidenceHeapOrdersByDueThenSequence(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	heap := ftConfidenceHeap{}
	heap.push(ftConfidenceItem{key: ftConfidenceKey{call: "LATE"}, due: base.Add(2 * time.Second), seq: 1})
	heap.push(ftConfidenceItem{key: ftConfidenceKey{call: "SECOND"}, due: base.Add(time.Second), seq: 2})
	heap.push(ftConfidenceItem{key: ftConfidenceKey{call: "FIRST"}, due: base.Add(time.Second), seq: 1})

	got := make([]string, 0, 3)
	for heap.Len() > 0 {
		item, ok := heap.pop()
		if !ok {
			t.Fatal("expected heap pop to succeed")
		}
		got = append(got, item.key.call)
	}
	want := []string{"FIRST", "SECOND", "LATE"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected heap order at %d: got %v want %v", i, got, want)
		}
	}
	if _, ok := heap.pop(); ok {
		t.Fatal("expected empty heap pop to report false")
	}
}

func TestFTConfidenceControllerDrainClearsPendingStateAfterReschedule(t *testing.T) {
	controller := newFTConfidenceController(config.CallCorrectionConfig{})
	base := time.Unix(1_700_000_000, 0).UTC()

	first := spot.NewSpot("K1CLR", "N0AAA", 14074.0, "FT8")
	second := spot.NewSpot("K1CLR", "N0BBB", 14074.0, "FT8")
	if held, _ := controller.Observe(base, outputSpotContext{spot: first, modeUpper: "FT8"}); !held {
		t.Fatal("expected first observation to be held")
	}
	if held, _ := controller.Observe(base.Add(time.Second), outputSpotContext{spot: second, modeUpper: "FT8"}); !held {
		t.Fatal("expected second observation to be held")
	}

	releases := controller.Drain(base.Add(30*time.Second), false)
	if len(releases) != 1 {
		t.Fatalf("expected one valid release after stale due-item skip, got %d", len(releases))
	}
	if got := len(releases[0].contexts); got != 2 {
		t.Fatalf("expected two pending contexts in release, got %d", got)
	}
	if len(controller.pending) != 0 || controller.pendingSpots != 0 {
		t.Fatalf("expected pending state to clear, pending=%d pendingSpots=%d", len(controller.pending), controller.pendingSpots)
	}
	if controller.queue.Len() != 0 {
		t.Fatalf("expected stale heap items to be drained, got queue len %d", controller.queue.Len())
	}
}

func BenchmarkOutputPipelineFTHoldAndRelease(b *testing.B) {
	cfg := config.CallCorrectionConfig{
		RecentBandBonusEnabled:            true,
		RecentBandWindowSeconds:           12 * 60 * 60,
		RecentBandRecordMinUniqueSpotters: 2,
	}
	baseTime := time.Unix(1_700_000_000, 0).UTC()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rb := buffer.NewRingBuffer(8)
		pipeline := newTestFTOutputPipeline(cfg, rb)

		first := spot.NewSpot("K1BENCH", "N0AAA", 14074.1, "FT8")
		first.Time = baseTime
		second := spot.NewSpot("K1BENCH", "N0BBB", 14074.1, "FT8")
		second.Time = baseTime.Add(400 * time.Millisecond)
		third := spot.NewSpot("K1BENCH", "N0CCC", 14074.1, "FT8")
		third.Time = baseTime.Add(800 * time.Millisecond)

		pipeline.processSpotBody(first, nil)
		pipeline.processSpotBody(second, nil)
		pipeline.processSpotBody(third, nil)
		pipeline.releaseDueFT(baseTime.Add(30*time.Second), false)
	}
}

func BenchmarkFTConfidenceControllerObserveAndDrain(b *testing.B) {
	baseTime := time.Unix(1_700_000_000, 0).UTC()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		controller := newFTConfidenceController(config.CallCorrectionConfig{})

		first := spot.NewSpot("K1BENCH", "N0AAA", 14074.1, "FT8")
		first.Time = baseTime
		second := spot.NewSpot("K1BENCH", "N0BBB", 14074.1, "FT8")
		second.Time = baseTime.Add(400 * time.Millisecond)
		third := spot.NewSpot("K1BENCH", "N0CCC", 14074.1, "FT8")
		third.Time = baseTime.Add(800 * time.Millisecond)

		controller.Observe(baseTime, outputSpotContext{spot: first, modeUpper: "FT8"})
		controller.Observe(baseTime.Add(400*time.Millisecond), outputSpotContext{spot: second, modeUpper: "FT8"})
		controller.Observe(baseTime.Add(800*time.Millisecond), outputSpotContext{spot: third, modeUpper: "FT8"})
		controller.Drain(baseTime.Add(30*time.Second), false)
	}
}
