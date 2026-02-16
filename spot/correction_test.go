package spot

import (
	"strings"
	"testing"
	"time"

	"dxcluster/bandmap"
)

type captureTraceLogger struct {
	entries []CorrectionLogEntry
}

func (c *captureTraceLogger) Enqueue(entry CorrectionLogEntry) {
	c.entries = append(c.entries, entry)
}

func (c *captureTraceLogger) Close() error {
	return nil
}

func (c *captureTraceLogger) Dropped() int64 {
	return 0
}

func (c *captureTraceLogger) lastTrace(t *testing.T) CorrectionTrace {
	t.Helper()
	if len(c.entries) == 0 {
		t.Fatalf("expected at least one trace entry")
	}
	return c.entries[len(c.entries)-1].Trace
}

func withTestCallQualityStore(t *testing.T, fn func(store *CallQualityStore)) {
	t.Helper()
	old := callQuality.Load()
	store := NewCallQualityStoreWithOptions(CallQualityOptions{
		Shards:          1,
		TTL:             time.Hour,
		MaxEntries:      1024,
		CleanupInterval: time.Hour,
	})
	callQuality.Store(store)
	t.Cleanup(func() {
		callQuality.Store(old)
	})
	fn(store)
}

func withTestRecentBandStore(t *testing.T, fn func(store *RecentBandStore)) {
	t.Helper()
	store := NewRecentBandStoreWithOptions(RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         1024,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})
	fn(store)
}

func TestSuggestCallCorrectionRequiresConsensus(t *testing.T) {
	now := time.Date(2025, 11, 18, 10, 0, 0, 0, time.UTC)
	subject := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 14074.0, Time: now}
	others := []*Spot{
		{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 14074.0, Time: now}, // same reporter, ignored
		{DXCall: "K1A8C", DECall: "W2BBB", Frequency: 14074.0, Time: now},
		{DXCall: "K1A8C", DECall: "W3CCC", Frequency: 14074.1, Time: now},
		{DXCall: "K1A8C", DECall: "W4DDD", Frequency: 14074.0, Time: now.Add(-10 * time.Second)},
	}

	call, supporters, confidence, subjectConfidence, total, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  3,
		MinAdvantage:         1,
		MinConfidencePercent: 50,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
	}, now)
	if !ok {
		t.Fatalf("expected correction suggestion")
	}
	if call != "K1A8C" {
		t.Fatalf("expected K1A8C, got %s", call)
	}
	if supporters != 3 {
		t.Fatalf("expected 3 supporters, got %d", supporters)
	}
	if confidence <= 0 {
		t.Fatalf("expected positive confidence, got %d", confidence)
	}
	if subjectConfidence <= 0 || total == 0 {
		t.Fatalf("expected subject confidence data")
	}
}

func TestSuggestCallCorrectionRespectsRecency(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 14074.0, Time: now}
	stale := now.Add(-2 * time.Minute)
	others := []*Spot{
		{DXCall: "K1A8C", DECall: "W2BBB", Frequency: 14074.0, Time: stale},
		{DXCall: "K1A8C", DECall: "W3CCC", Frequency: 14074.0, Time: stale},
		{DXCall: "K1A8C", DECall: "W4DDD", Frequency: 14074.0, Time: stale},
	}
	if call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  3,
		MinAdvantage:         1,
		MinConfidencePercent: 60,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
	}, now); ok {
		t.Fatalf("expected no correction, got %s", call)
	}
}

func TestSuggestCallCorrectionRequiresUniqueSpotters(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 14074.0, Time: now}
	others := []*Spot{
		{DXCall: "K1XYZ", DECall: "W2BBB", Frequency: 14074.0, Time: now},
		{DXCall: "K1XYZ", DECall: "W2BBB", Frequency: 14074.0, Time: now}, // duplicate reporter
		{DXCall: "K1XYZ", DECall: "W2BBB", Frequency: 14074.0, Time: now},
	}
	if call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  3,
		MinAdvantage:         1,
		MinConfidencePercent: 60,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
	}, now); ok {
		t.Fatalf("expected no correction, got %s", call)
	}
}

func TestSuggestCallCorrectionSkipsSameCall(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 14074.0, Time: now}
	others := []*Spot{
		{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 14074.0, Time: now},
		{DXCall: "K1ABC", DECall: "W3CCC", Frequency: 14074.0, Time: now},
		{DXCall: "K1ABC", DECall: "W4DDD", Frequency: 14074.0, Time: now},
	}
	if call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  3,
		MinAdvantage:         1,
		MinConfidencePercent: 60,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
	}, now); ok {
		t.Fatalf("expected no correction, call=%s", call)
	}
}

func TestSuggestCallCorrectionRequiresAdvantage(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 14074.0, Time: now}
	others := []*Spot{
		{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 14074.0, Time: now},
		{DXCall: "K1XYZ", DECall: "W3CCC", Frequency: 14074.0, Time: now},
		{DXCall: "K1XYZ", DECall: "W4DDD", Frequency: 14074.0, Time: now},
	}
	if call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  2,
		MinAdvantage:         2,
		MinConfidencePercent: 60,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
	}, now); ok {
		t.Fatalf("expected no correction, got %s", call)
	}
}

func TestSuggestCallCorrectionRequiresConfidence(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 14074.0, Time: now}
	others := []*Spot{
		{DXCall: "K1A8C", DECall: "W2BBB", Frequency: 14074.0, Time: now},
		{DXCall: "K1XYZ", DECall: "W3CCC", Frequency: 14074.0, Time: now},
		{DXCall: "K1XYZ", DECall: "W4DDD", Frequency: 14074.0, Time: now},
	}
	if call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  2,
		MinAdvantage:         1,
		MinConfidencePercent: 70,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
	}, now); ok {
		t.Fatalf("expected no correction (confidence too low), got %s", call)
	}
}

func TestSuggestCallCorrectionIgnoresOutOfWindowReporters(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1ABC", Frequency: 14074.0, Time: now}
	others := []*Spot{
		{DXCall: "K1ABD", DECall: "W2BBB", Frequency: 14074.0, Time: now},
		{DXCall: "K9ZZZ", DECall: "W3CCC", Frequency: 18000.0, Time: now.Add(-2 * time.Minute)}, // off-frequency and stale; should not dilute confidence
	}
	call, supporters, confidence, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  1,
		MinAdvantage:         1,
		MinConfidencePercent: 60,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
	}, now)
	if !ok {
		t.Fatalf("expected correction suggestion")
	}
	if call != "K1ABD" {
		t.Fatalf("expected K1ABD, got %s", call)
	}
	if supporters != 1 {
		t.Fatalf("expected 1 supporter, got %d", supporters)
	}
	if confidence != 100 {
		t.Fatalf("expected confidence to ignore stale/off-frequency reporters, got %d", confidence)
	}
}

func TestSuggestCallCorrectionRequiresEditDistance(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 14074.0, Time: now}
	others := []*Spot{
		{DXCall: "ZZ9ZZA", DECall: "W2BBB", Frequency: 14074.0, Time: now},
		{DXCall: "ZZ9ZZA", DECall: "W3CCC", Frequency: 14074.0, Time: now},
		{DXCall: "ZZ9ZZA", DECall: "W4DDD", Frequency: 14074.0, Time: now},
	}
	if call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  3,
		MinAdvantage:         1,
		MinConfidencePercent: 60,
		MaxEditDistance:      1,
		RecencyWindow:        30 * time.Second,
	}, now); ok {
		t.Fatalf("expected no correction due to distance, got %s", call)
	}
}

func TestSuggestCallCorrectionMajorityStrategy(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "BADCALL", DECall: "W1AAA", Frequency: 14074.0, Time: now}
	others := []*Spot{
		{DXCall: "GOOD1", DECall: "W2BBB", Frequency: 14074.0, Time: now},
		{DXCall: "GOOD1", DECall: "W3CCC", Frequency: 14074.0, Time: now},
		{DXCall: "GOOD2", DECall: "W4DDD", Frequency: 14074.0, Time: now}, // tie-breaker stays with lastSeen
	}
	call, supporters, confidence, subjectConfidence, total, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  2,
		MinAdvantage:         1,
		MinConfidencePercent: 40,
		MaxEditDistance:      10,
		RecencyWindow:        30 * time.Second,
	}, now)
	if !ok {
		t.Fatalf("expected majority correction")
	}
	if call != "GOOD1" {
		t.Fatalf("expected GOOD1, got %s", call)
	}
	if supporters != 2 {
		t.Fatalf("expected 2 supporters, got %d", supporters)
	}
	if confidence <= 0 || subjectConfidence < 0 || total != 4 {
		t.Fatalf("unexpected confidence/total values")
	}
}

func TestSuggestCallCorrectionConfusionRankingBreaksTopSupportTie(t *testing.T) {
	model, err := buildConfusionModel(confusionModelFile{
		Modes:       []string{"CW"},
		SNREdges:    []float64{-999, 999},
		Alphabet:    "ABKC18?",
		UnknownChar: "?",
		SubCounts: [][][][]int64{
			{
				{
					/* A */ {0, 1, 1, 1, 1, 1, 1},
					/* B */ {1, 0, 1, 1, 1, 50, 1}, // B->8 modest
					/* K */ {1, 1, 0, 1, 1, 1, 1},
					/* C */ {1, 1, 1, 0, 1, 1, 1},
					/* 1 */ {1, 1, 1, 1, 0, 1, 1},
					/* 8 */ {1, 1, 1, 1, 1, 0, 1},
					/* ? */ {1, 1, 1, 1, 1, 1, 0},
				},
			},
		},
		DelCounts: [][][]int64{
			{
				{1, 1, 1, 1, 1, 1, 1},
			},
		},
		InsCounts: [][][]int64{
			{
				{1, 1, 1, 1, 1, 1, 1},
			},
		},
	})
	if err != nil {
		t.Fatalf("build confusion model: %v", err)
	}
	// Boost X->8 heavily by mapping X through unknown '?' less favorably than B->8.
	// Candidate K1AXC has one unknown char in this synthetic alphabet and should lose.
	// To force a deterministic winner flip, use C->8 as strong signal instead.
	model2, err := buildConfusionModel(confusionModelFile{
		Modes:       []string{"CW"},
		SNREdges:    []float64{-999, 999},
		Alphabet:    "ABKC18?",
		UnknownChar: "?",
		SubCounts: [][][][]int64{
			{
				{
					/* A */ {0, 1, 1, 1, 1, 1, 1},
					/* B */ {1, 0, 1, 1, 1, 5, 1}, // B->8 weak
					/* K */ {1, 1, 0, 1, 1, 1, 1},
					/* C */ {1, 1, 1, 0, 1, 80, 1}, // C->8 strong
					/* 1 */ {1, 1, 1, 1, 0, 1, 1},
					/* 8 */ {1, 1, 1, 1, 1, 0, 1},
					/* ? */ {1, 1, 1, 1, 1, 1, 0},
				},
			},
		},
		DelCounts: [][][]int64{
			{
				{1, 1, 1, 1, 1, 1, 1},
			},
		},
		InsCounts: [][][]int64{
			{
				{1, 1, 1, 1, 1, 1, 1},
			},
		},
	})
	if err != nil {
		t.Fatalf("build confusion model #2: %v", err)
	}

	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now, Report: 20}
	others := []*Spot{
		{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "K1ABC", DECall: "W3CCC", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "K1ACC", DECall: "W4DDD", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "K1ACC", DECall: "W5EEE", Frequency: 7010.0, Mode: "CW", Time: now},
	}

	callBase, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  2,
		MinAdvantage:         1,
		MinConfidencePercent: 40,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
		ConfusionModel:       model,
		ConfusionWeight:      0,
	}, now)
	if !ok {
		t.Fatalf("expected baseline tie winner")
	}
	callWithConfusion, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  2,
		MinAdvantage:         1,
		MinConfidencePercent: 40,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
		ConfusionModel:       model2,
		ConfusionWeight:      2.0,
	}, now)
	if !ok {
		t.Fatalf("expected confusion-ranked tie winner")
	}
	if callBase == callWithConfusion {
		t.Fatalf("expected confusion ranking to break top-support tie differently; both=%q", callBase)
	}
}

func TestSuggestCallCorrectionConfusionRankingDoesNotBypassMinReportsGate(t *testing.T) {
	model, err := buildConfusionModel(confusionModelFile{
		Modes:       []string{"CW"},
		SNREdges:    []float64{-999, 999},
		Alphabet:    "ABKC18?",
		UnknownChar: "?",
		SubCounts: [][][][]int64{
			{
				{
					{0, 1, 1, 1, 1, 1, 1},
					{1, 0, 1, 1, 50, 1, 1},
					{1, 1, 0, 1, 1, 1, 1},
					{1, 1, 1, 0, 1, 1, 1},
					{1, 1, 1, 1, 0, 1, 1},
					{1, 1, 1, 1, 1, 0, 1},
					{1, 1, 1, 1, 1, 1, 0},
				},
			},
		},
		DelCounts: [][][]int64{
			{
				{1, 1, 1, 1, 1, 1, 1},
			},
		},
		InsCounts: [][][]int64{
			{
				{1, 1, 1, 1, 1, 1, 1},
			},
		},
	})
	if err != nil {
		t.Fatalf("build confusion model: %v", err)
	}

	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now, Report: 20}
	others := []*Spot{
		{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now}, // support=1
	}

	_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  2, // hard gate should still reject
		MinAdvantage:         1,
		MinConfidencePercent: 40,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
		ConfusionModel:       model,
		ConfusionWeight:      5.0,
	}, now)
	if ok {
		t.Fatalf("expected min_reports gate to reject even with strong confusion score")
	}
}

func TestSuggestCallCorrectionCandidateEvalTopKFallback(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1A8C", DECall: "", Frequency: 7010.0, Mode: "CW", Time: now}
	others := []*Spot{
		// Ranked #1 by support, but too far by edit distance.
		{DXCall: "ZZZZZZ", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "ZZZZZZ", DECall: "W3CCC", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "ZZZZZZ", DECall: "W4DDD", Frequency: 7010.0, Mode: "CW", Time: now},
		// Ranked #2 and valid correction.
		{DXCall: "K1ABC", DECall: "W5EEE", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "K1ABC", DECall: "W6FFF", Frequency: 7010.0, Mode: "CW", Time: now},
	}

	traceTop1 := &captureTraceLogger{}
	_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  2,
		CandidateEvalTopK:    1, // legacy top-1 only
		MinAdvantage:         1,
		MinConfidencePercent: 30,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
		DebugLog:             true,
		TraceLogger:          traceTop1,
	}, now)
	if ok {
		t.Fatalf("expected no correction with top-1 only")
	}
	lastTop1 := traceTop1.lastTrace(t)
	if lastTop1.Reason != "max_edit_distance" {
		t.Fatalf("expected top-1 rejection by max_edit_distance, got %q", lastTop1.Reason)
	}
	if lastTop1.CandidateRank != 1 {
		t.Fatalf("expected top-1 candidate rank, got %d", lastTop1.CandidateRank)
	}

	traceTop2 := &captureTraceLogger{}
	call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  2,
		CandidateEvalTopK:    2, // allow fallback to #2 candidate
		MinAdvantage:         1,
		MinConfidencePercent: 30,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
		DebugLog:             true,
		TraceLogger:          traceTop2,
	}, now)
	if !ok {
		t.Fatalf("expected correction with top-2 fallback")
	}
	if call != "K1ABC" {
		t.Fatalf("expected fallback correction K1ABC, got %s", call)
	}
	lastTop2 := traceTop2.lastTrace(t)
	if lastTop2.DecisionPath != "consensus" {
		t.Fatalf("expected consensus decision path, got %q", lastTop2.DecisionPath)
	}
	if lastTop2.CandidateRank != 2 {
		t.Fatalf("expected applied candidate rank 2, got %d", lastTop2.CandidateRank)
	}
}

func TestSuggestCallCorrectionPriorBonusOneShortWithSCP(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1A8C", DECall: "", Frequency: 7010.0, Mode: "CW", Time: now}
	others := []*Spot{
		{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
	}
	known := &KnownCallsigns{entries: map[string]struct{}{"K1ABC": {}}}
	trace := &captureTraceLogger{}

	call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:               "majority",
		MinConsensusReports:    2, // candidate is one short
		CandidateEvalTopK:      1,
		MinAdvantage:           1,
		MinConfidencePercent:   40,
		MaxEditDistance:        2,
		RecencyWindow:          30 * time.Second,
		PriorBonusEnabled:      true,
		PriorBonusMax:          1,
		PriorBonusDistanceMax:  1,
		PriorBonusRequiresSCP:  true,
		PriorBonusApplyTo:      "min_reports",
		PriorBonusKnownCallset: known,
		DebugLog:               true,
		TraceLogger:            trace,
	}, now)
	if !ok {
		t.Fatalf("expected prior bonus to satisfy one-short min_reports case")
	}
	if call != "K1ABC" {
		t.Fatalf("expected K1ABC, got %s", call)
	}
	last := trace.lastTrace(t)
	if !last.PriorBonusApplied || last.PriorBonusValue != 1 {
		t.Fatalf("expected prior bonus metadata on applied trace")
	}
	if !strings.Contains(last.DecisionPath, "prior_bonus") {
		t.Fatalf("expected decision path to include prior_bonus, got %q", last.DecisionPath)
	}
}

func TestSuggestCallCorrectionPriorBonusRequiresSCP(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1A8C", DECall: "", Frequency: 7010.0, Mode: "CW", Time: now}
	others := []*Spot{
		{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
	}
	trace := &captureTraceLogger{}

	_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:               "majority",
		MinConsensusReports:    2, // candidate is one short
		CandidateEvalTopK:      1,
		MinAdvantage:           1,
		MinConfidencePercent:   40,
		MaxEditDistance:        2,
		RecencyWindow:          30 * time.Second,
		PriorBonusEnabled:      true,
		PriorBonusMax:          1,
		PriorBonusDistanceMax:  1,
		PriorBonusRequiresSCP:  true,
		PriorBonusApplyTo:      "min_reports",
		PriorBonusKnownCallset: nil,
		DebugLog:               true,
		TraceLogger:            trace,
	}, now)
	if ok {
		t.Fatalf("expected no correction without SCP hit for prior bonus")
	}
	last := trace.lastTrace(t)
	if last.Reason != "min_reports" {
		t.Fatalf("expected min_reports rejection, got %q", last.Reason)
	}
	if last.PriorBonusApplied {
		t.Fatalf("did not expect prior bonus to apply without SCP")
	}
}

func TestSuggestCallCorrectionPriorAndRecentBonusStackToCloseGap(t *testing.T) {
	withTestRecentBandStore(t, func(store *RecentBandStore) {
		now := time.Now().UTC()
		subject := &Spot{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
		others := []*Spot{
			{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W3CCC", Frequency: 7010.0, Mode: "CW", Time: now},
		}
		known := &KnownCallsigns{entries: map[string]struct{}{"K1ABC": {}}}
		store.Record("K1ABC", "40m", "CW", "W8ZZZ", now.Add(-30*time.Minute))
		store.Record("K1ABC", "40m", "CW", "W9YYY", now.Add(-20*time.Minute))
		trace := &captureTraceLogger{}

		call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
			Strategy:                          "majority",
			MinConsensusReports:               4, // support=2; prior +1 and recent +1 should close the gap
			CandidateEvalTopK:                 1,
			MinAdvantage:                      1,
			MinConfidencePercent:              50,
			MaxEditDistance:                   2,
			RecencyWindow:                     30 * time.Second,
			PriorBonusEnabled:                 true,
			PriorBonusMax:                     1,
			PriorBonusDistanceMax:             1,
			PriorBonusRequiresSCP:             true,
			PriorBonusApplyTo:                 "min_reports",
			PriorBonusKnownCallset:            known,
			RecentBandBonusEnabled:            true,
			RecentBandWindow:                  12 * time.Hour,
			RecentBandBonusMax:                1,
			RecentBandRecordMinUniqueSpotters: 2,
			RecentBandStore:                   store,
			DebugLog:                          true,
			TraceLogger:                       trace,
		}, now)
		if !ok {
			t.Fatalf("expected stacked prior+recent bonus to satisfy min_reports")
		}
		if call != "K1ABC" {
			t.Fatalf("expected K1ABC, got %s", call)
		}
		last := trace.lastTrace(t)
		if !last.PriorBonusApplied || last.PriorBonusValue != 1 {
			t.Fatalf("expected prior bonus metadata on applied trace")
		}
		if !strings.Contains(last.DecisionPath, "prior_bonus") || !strings.Contains(last.DecisionPath, "recent_band_bonus") {
			t.Fatalf("expected decision path to include both prior_bonus and recent_band_bonus, got %q", last.DecisionPath)
		}
	})
}

func TestSuggestCallCorrectionPriorAndRecentBonusDoNotBypassAdvantage(t *testing.T) {
	withTestRecentBandStore(t, func(store *RecentBandStore) {
		now := time.Now().UTC()
		subject := &Spot{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
		others := []*Spot{
			{DXCall: "K1A8C", DECall: "W4DDD", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1A8C", DECall: "W5EEE", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W3CCC", Frequency: 7010.0, Mode: "CW", Time: now},
		}
		known := &KnownCallsigns{entries: map[string]struct{}{"K1ABC": {}}}
		store.Record("K1ABC", "40m", "CW", "W8ZZZ", now.Add(-30*time.Minute))
		store.Record("K1ABC", "40m", "CW", "W9YYY", now.Add(-20*time.Minute))
		trace := &captureTraceLogger{}

		_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
			Strategy:                          "majority",
			MinConsensusReports:               4, // support=2; prior +1 and recent +1 can satisfy min_reports
			CandidateEvalTopK:                 1,
			MinAdvantage:                      2, // subject support is 3, so support=2 should still fail advantage
			MinConfidencePercent:              30,
			MaxEditDistance:                   2,
			RecencyWindow:                     30 * time.Second,
			PriorBonusEnabled:                 true,
			PriorBonusMax:                     1,
			PriorBonusDistanceMax:             1,
			PriorBonusRequiresSCP:             true,
			PriorBonusApplyTo:                 "min_reports",
			PriorBonusKnownCallset:            known,
			RecentBandBonusEnabled:            true,
			RecentBandWindow:                  12 * time.Hour,
			RecentBandBonusMax:                1,
			RecentBandRecordMinUniqueSpotters: 2,
			RecentBandStore:                   store,
			DebugLog:                          true,
			TraceLogger:                       trace,
		}, now)
		if ok {
			t.Fatalf("expected no correction because advantage gate should still hold")
		}
		last := trace.lastTrace(t)
		if last.Reason != "advantage" {
			t.Fatalf("expected advantage rejection, got %q", last.Reason)
		}
		if !strings.Contains(last.DecisionPath, "prior_bonus") || !strings.Contains(last.DecisionPath, "recent_band_bonus") {
			t.Fatalf("expected decision path to include both prior_bonus and recent_band_bonus, got %q", last.DecisionPath)
		}
	})
}

func TestSuggestCallCorrectionRecentBandBonusOneShort(t *testing.T) {
	withTestRecentBandStore(t, func(store *RecentBandStore) {
		now := time.Now().UTC()
		subject := &Spot{DXCall: "K1A8C", DECall: "", Frequency: 7010.0, Mode: "CW", Time: now}
		others := []*Spot{
			{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
		}
		store.Record("K1ABC", "40m", "CW", "W8ZZZ", now.Add(-30*time.Minute))
		store.Record("K1ABC", "40m", "CW", "W9YYY", now.Add(-20*time.Minute))
		trace := &captureTraceLogger{}

		call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
			Strategy:                          "majority",
			MinConsensusReports:               2,
			CandidateEvalTopK:                 1,
			MinAdvantage:                      1,
			MinConfidencePercent:              40,
			MaxEditDistance:                   2,
			RecencyWindow:                     30 * time.Second,
			RecentBandBonusEnabled:            true,
			RecentBandWindow:                  12 * time.Hour,
			RecentBandBonusMax:                1,
			RecentBandRecordMinUniqueSpotters: 2,
			RecentBandStore:                   store,
			DebugLog:                          true,
			TraceLogger:                       trace,
		}, now)
		if !ok {
			t.Fatalf("expected recent-on-band bonus to satisfy one-short min_reports case")
		}
		if call != "K1ABC" {
			t.Fatalf("expected K1ABC, got %s", call)
		}
		last := trace.lastTrace(t)
		if !strings.Contains(last.DecisionPath, "recent_band_bonus") {
			t.Fatalf("expected decision path to include recent_band_bonus, got %q", last.DecisionPath)
		}
	})
}

func TestSuggestCallCorrectionRecentBandBonusRequiresAdmission(t *testing.T) {
	withTestRecentBandStore(t, func(store *RecentBandStore) {
		now := time.Now().UTC()
		subject := &Spot{DXCall: "K1A8C", DECall: "", Frequency: 7010.0, Mode: "CW", Time: now}
		others := []*Spot{
			{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
		}
		store.Record("K1ABC", "40m", "CW", "W8ZZZ", now.Add(-30*time.Minute))
		trace := &captureTraceLogger{}

		_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
			Strategy:                          "majority",
			MinConsensusReports:               2,
			CandidateEvalTopK:                 1,
			MinAdvantage:                      1,
			MinConfidencePercent:              40,
			MaxEditDistance:                   2,
			RecencyWindow:                     30 * time.Second,
			RecentBandBonusEnabled:            true,
			RecentBandWindow:                  12 * time.Hour,
			RecentBandBonusMax:                1,
			RecentBandRecordMinUniqueSpotters: 2,
			RecentBandStore:                   store,
			DebugLog:                          true,
			TraceLogger:                       trace,
		}, now)
		if ok {
			t.Fatalf("expected no correction without enough unique recent spotters")
		}
		last := trace.lastTrace(t)
		if last.Reason != "min_reports" {
			t.Fatalf("expected min_reports rejection, got %q", last.Reason)
		}
	})
}

func TestSuggestCallCorrectionRecentBandBonusDoesNotBypassAdvantage(t *testing.T) {
	withTestRecentBandStore(t, func(store *RecentBandStore) {
		now := time.Now().UTC()
		subject := &Spot{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
		others := []*Spot{
			{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
		}
		store.Record("K1ABC", "40m", "CW", "W8ZZZ", now.Add(-30*time.Minute))
		store.Record("K1ABC", "40m", "CW", "W9YYY", now.Add(-20*time.Minute))
		trace := &captureTraceLogger{}

		_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
			Strategy:                          "majority",
			MinConsensusReports:               2,
			CandidateEvalTopK:                 1,
			MinAdvantage:                      1,
			MinConfidencePercent:              40,
			MaxEditDistance:                   2,
			RecencyWindow:                     30 * time.Second,
			RecentBandBonusEnabled:            true,
			RecentBandWindow:                  12 * time.Hour,
			RecentBandBonusMax:                1,
			RecentBandRecordMinUniqueSpotters: 2,
			RecentBandStore:                   store,
			DebugLog:                          true,
			TraceLogger:                       trace,
		}, now)
		if ok {
			t.Fatalf("expected no correction because advantage gate should still hold")
		}
		last := trace.lastTrace(t)
		if last.Reason != "advantage" {
			t.Fatalf("expected advantage rejection, got %q", last.Reason)
		}
		if !strings.Contains(last.DecisionPath, "recent_band_bonus") {
			t.Fatalf("expected decision path to include recent_band_bonus, got %q", last.DecisionPath)
		}
	})
}

func TestSuggestCallCorrectionAnchorRequiresConsensusGates(t *testing.T) {
	withTestCallQualityStore(t, func(store *CallQualityStore) {
		now := time.Now().UTC()
		subject := &Spot{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
		others := []*Spot{
			{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
		}
		// Mark K1ABC as a good anchor candidate in this bin.
		store.Add("K1ABC", subject.Frequency*1000, 500, 3)

		trace := &captureTraceLogger{}
		_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
			Strategy:                  "majority",
			MinConsensusReports:       2, // anchor support is only 1, so this must reject
			MinAdvantage:              1,
			MinConfidencePercent:      50,
			MaxEditDistance:           3,
			RecencyWindow:             30 * time.Second,
			QualityBinHz:              500,
			QualityGoodThreshold:      2,
			QualityNewCallIncrement:   1,
			QualityBustedDecrement:    1,
			DebugLog:                  true,
			TraceLogger:               trace,
			FrequencyToleranceHz:      500,
			FreqGuardMinSeparationKHz: 0.1,
			FreqGuardRunnerUpRatio:    0.5,
		}, now)
		if ok {
			t.Fatalf("expected anchor candidate to be rejected by min_reports gate")
		}
		last := trace.lastTrace(t)
		if last.DecisionPath != "anchor" {
			t.Fatalf("expected anchor decision path, got %q", last.DecisionPath)
		}
		if last.Reason != "min_reports" {
			t.Fatalf("expected min_reports rejection, got %q", last.Reason)
		}
	})
}

func TestSuggestCallCorrectionAnchorFallbacksToConsensus(t *testing.T) {
	withTestCallQualityStore(t, func(store *CallQualityStore) {
		now := time.Now().UTC()
		subject := &Spot{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
		others := []*Spot{
			// Good anchor has only one supporter and should fail min_reports.
			{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
			// Consensus winner has enough support.
			{DXCall: "K1ABD", DECall: "W3CCC", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABD", DECall: "W4DDD", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABD", DECall: "W5EEE", Frequency: 7010.0, Mode: "CW", Time: now},
		}
		store.Add("K1ABC", subject.Frequency*1000, 500, 3)

		trace := &captureTraceLogger{}
		call, supporters, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
			Strategy:                  "majority",
			MinConsensusReports:       2,
			MinAdvantage:              1,
			MinConfidencePercent:      50,
			MaxEditDistance:           3,
			RecencyWindow:             30 * time.Second,
			QualityBinHz:              500,
			QualityGoodThreshold:      2,
			QualityNewCallIncrement:   1,
			QualityBustedDecrement:    1,
			DebugLog:                  true,
			TraceLogger:               trace,
			FrequencyToleranceHz:      500,
			FreqGuardMinSeparationKHz: 0.1,
			FreqGuardRunnerUpRatio:    0.5,
		}, now)
		if !ok {
			t.Fatalf("expected consensus fallback correction")
		}
		if call != "K1ABD" {
			t.Fatalf("expected consensus fallback call K1ABD, got %s", call)
		}
		if supporters != 3 {
			t.Fatalf("expected 3 supporters for consensus winner, got %d", supporters)
		}
		last := trace.lastTrace(t)
		if last.DecisionPath != "consensus" {
			t.Fatalf("expected consensus decision path, got %q", last.DecisionPath)
		}
	})
}

func TestSuggestCallCorrectionAnchorFreqGuardFallbacksToConsensus(t *testing.T) {
	withTestCallQualityStore(t, func(store *CallQualityStore) {
		now := time.Now().UTC()
		subject := &Spot{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
		others := []*Spot{
			{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W3CCC", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABD", DECall: "W4DDD", Frequency: 7010.3, Mode: "CW", Time: now},
			{DXCall: "K1ABD", DECall: "W5EEE", Frequency: 7010.3, Mode: "CW", Time: now},
			{DXCall: "K1ABD", DECall: "W6FFF", Frequency: 7010.3, Mode: "CW", Time: now},
			{DXCall: "K1ABD", DECall: "W7GGG", Frequency: 7010.3, Mode: "CW", Time: now},
		}
		store.Add("K1ABC", subject.Frequency*1000, 500, 3)

		base := CorrectionSettings{
			Strategy:                "majority",
			MinConsensusReports:     2,
			MinAdvantage:            1,
			MinConfidencePercent:    20,
			MaxEditDistance:         3,
			RecencyWindow:           30 * time.Second,
			QualityBinHz:            500,
			QualityGoodThreshold:    2,
			QualityNewCallIncrement: 1,
			QualityBustedDecrement:  1,
			DebugLog:                true,
			FrequencyToleranceHz:    500,
		}

		baselineTrace := &captureTraceLogger{}
		baselineCfg := base
		baselineCfg.TraceLogger = baselineTrace
		baselineCfg.FreqGuardMinSeparationKHz = 1.0
		baselineCfg.FreqGuardRunnerUpRatio = 0.75
		call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), baselineCfg, now)
		if !ok {
			t.Fatalf("expected baseline anchor correction")
		}
		if call != "K1ABC" {
			t.Fatalf("expected baseline anchor call K1ABC, got %s", call)
		}
		baselineLast := baselineTrace.lastTrace(t)
		if baselineLast.DecisionPath != "anchor" {
			t.Fatalf("expected baseline anchor path, got %q", baselineLast.DecisionPath)
		}

		guardTrace := &captureTraceLogger{}
		guardCfg := base
		guardCfg.TraceLogger = guardTrace
		guardCfg.FreqGuardMinSeparationKHz = 0.1
		guardCfg.FreqGuardRunnerUpRatio = 0.75
		call, _, _, _, _, ok = SuggestCallCorrection(subject, toEntries(others), guardCfg, now)
		if !ok {
			t.Fatalf("expected freq_guard fallback to consensus correction")
		}
		if call != "K1ABD" {
			t.Fatalf("expected consensus fallback call K1ABD, got %s", call)
		}
		guardLast := guardTrace.lastTrace(t)
		if guardLast.DecisionPath != "consensus" {
			t.Fatalf("expected consensus path after anchor freq_guard rejection, got %q", guardLast.DecisionPath)
		}
	})
}

func TestSuggestCallCorrectionAnchorHonorsCooldown(t *testing.T) {
	withTestCallQualityStore(t, func(store *CallQualityStore) {
		now := time.Now().UTC()
		subject := &Spot{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
		others := []*Spot{
			{DXCall: "K1A8C", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1A8C", DECall: "W3CCC", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W4DDD", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W5EEE", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W6FFF", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W7GGG", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W8HHH", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W9III", Frequency: 7010.0, Mode: "CW", Time: now},
		}
		store.Add("K1ABC", subject.Frequency*1000, 500, 3)
		cooldown := NewCallCooldown(CallCooldownConfig{
			Enabled:      true,
			MinReporters: 3,
			Duration:     2 * time.Minute,
			TTL:          5 * time.Minute,
			BinHz:        500,
			MaxReporters: 16,
		})

		trace := &captureTraceLogger{}
		_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
			Strategy:                  "majority",
			MinConsensusReports:       1,
			MinAdvantage:              1,
			MinConfidencePercent:      50,
			MaxEditDistance:           3,
			RecencyWindow:             30 * time.Second,
			QualityBinHz:              500,
			QualityGoodThreshold:      2,
			QualityNewCallIncrement:   1,
			QualityBustedDecrement:    1,
			DebugLog:                  true,
			TraceLogger:               trace,
			FrequencyToleranceHz:      500,
			FreqGuardMinSeparationKHz: 0.1,
			FreqGuardRunnerUpRatio:    0.5,
			Cooldown:                  cooldown,
			CooldownMinReporters:      3,
		}, now)
		if ok {
			t.Fatalf("expected anchor candidate to be blocked by cooldown")
		}
		last := trace.lastTrace(t)
		if last.DecisionPath != "anchor" {
			t.Fatalf("expected anchor decision path, got %q", last.DecisionPath)
		}
		if last.Reason != "cooldown" {
			t.Fatalf("expected cooldown rejection, got %q", last.Reason)
		}
	})
}

func TestSuggestCallCorrectionAnchorCooldownFallbacksToConsensus(t *testing.T) {
	withTestCallQualityStore(t, func(store *CallQualityStore) {
		now := time.Now().UTC()
		subject := &Spot{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
		others := []*Spot{
			{DXCall: "K1A8C", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1A8C", DECall: "W3CCC", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W4DDD", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W5EEE", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W6FFF", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W7GGG", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABD", DECall: "W8HHH", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABD", DECall: "W9III", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABD", DECall: "W0JJJ", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABD", DECall: "W0KKK", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABD", DECall: "W0LLL", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABD", DECall: "W0MMM", Frequency: 7010.0, Mode: "CW", Time: now},
		}
		store.Add("K1ABC", subject.Frequency*1000, 500, 3)

		base := CorrectionSettings{
			Strategy:                  "majority",
			MinConsensusReports:       2,
			MinAdvantage:              1,
			MinConfidencePercent:      20,
			MaxEditDistance:           3,
			RecencyWindow:             30 * time.Second,
			QualityBinHz:              500,
			QualityGoodThreshold:      2,
			QualityNewCallIncrement:   1,
			QualityBustedDecrement:    1,
			DebugLog:                  true,
			FrequencyToleranceHz:      500,
			FreqGuardMinSeparationKHz: 1.0,
			FreqGuardRunnerUpRatio:    2.0,
		}

		baselineTrace := &captureTraceLogger{}
		baselineCfg := base
		baselineCfg.TraceLogger = baselineTrace
		call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), baselineCfg, now)
		if !ok {
			t.Fatalf("expected baseline anchor correction")
		}
		if call != "K1ABC" {
			t.Fatalf("expected baseline anchor call K1ABC, got %s", call)
		}
		baselineLast := baselineTrace.lastTrace(t)
		if baselineLast.DecisionPath != "anchor" {
			t.Fatalf("expected baseline anchor path, got %q", baselineLast.DecisionPath)
		}

		cooldown := NewCallCooldown(CallCooldownConfig{
			Enabled:          true,
			MinReporters:     3,
			Duration:         2 * time.Minute,
			TTL:              5 * time.Minute,
			BinHz:            500,
			MaxReporters:     16,
			BypassAdvantage:  2,
			BypassConfidence: 10,
		})

		cooldownTrace := &captureTraceLogger{}
		cooldownCfg := base
		cooldownCfg.TraceLogger = cooldownTrace
		cooldownCfg.Cooldown = cooldown
		cooldownCfg.CooldownMinReporters = 3
		call, _, _, _, _, ok = SuggestCallCorrection(subject, toEntries(others), cooldownCfg, now)
		if !ok {
			t.Fatalf("expected cooldown fallback to consensus correction")
		}
		if call != "K1ABD" {
			t.Fatalf("expected consensus fallback call K1ABD, got %s", call)
		}
		cooldownLast := cooldownTrace.lastTrace(t)
		if cooldownLast.DecisionPath != "consensus" {
			t.Fatalf("expected consensus path after anchor cooldown rejection, got %q", cooldownLast.DecisionPath)
		}
	})
}

func TestSuggestCallCorrectionFreqGuardUsesTrueRunnerUp(t *testing.T) {
	withTestCallQualityStore(t, func(_ *CallQualityStore) {
		now := time.Now().UTC()
		subject := &Spot{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
		others := []*Spot{
			{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W3CCC", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W4DDD", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W5EEE", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W6FFF", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABQ", DECall: "W7GGG", Frequency: 7010.3, Mode: "CW", Time: now},
			{DXCall: "K1ABQ", DECall: "W8HHH", Frequency: 7010.3, Mode: "CW", Time: now},
			{DXCall: "K1ABQ", DECall: "W9III", Frequency: 7010.3, Mode: "CW", Time: now},
			{DXCall: "K1ABR", DECall: "W0JJJ", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABR", DECall: "W0KKK", Frequency: 7010.0, Mode: "CW", Time: now},
		}

		for i := 0; i < 32; i++ {
			trace := &captureTraceLogger{}
			_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
				Strategy:                  "majority",
				MinConsensusReports:       2,
				MinAdvantage:              1,
				MinConfidencePercent:      40,
				MaxEditDistance:           3,
				RecencyWindow:             30 * time.Second,
				DebugLog:                  true,
				TraceLogger:               trace,
				FrequencyToleranceHz:      500,
				FreqGuardMinSeparationKHz: 0.1,
				FreqGuardRunnerUpRatio:    0.5,
			}, now)
			if ok {
				t.Fatalf("expected freq_guard rejection (iteration %d)", i)
			}
			last := trace.lastTrace(t)
			if last.Reason != "freq_guard" {
				t.Fatalf("expected freq_guard reason, got %q (iteration %d)", last.Reason, i)
			}
		}
	})
}

func TestSuggestCallCorrectionDecisionPathConsensus(t *testing.T) {
	withTestCallQualityStore(t, func(_ *CallQualityStore) {
		now := time.Now().UTC()
		subject := &Spot{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
		others := []*Spot{
			{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
			{DXCall: "K1ABC", DECall: "W3CCC", Frequency: 7010.0, Mode: "CW", Time: now},
		}

		trace := &captureTraceLogger{}
		_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
			Strategy:                  "majority",
			MinConsensusReports:       2,
			MinAdvantage:              1,
			MinConfidencePercent:      50,
			MaxEditDistance:           3,
			RecencyWindow:             30 * time.Second,
			DebugLog:                  true,
			TraceLogger:               trace,
			FrequencyToleranceHz:      500,
			FreqGuardMinSeparationKHz: 0.1,
			FreqGuardRunnerUpRatio:    0.5,
		}, now)
		if !ok {
			t.Fatalf("expected consensus correction")
		}
		last := trace.lastTrace(t)
		if last.DecisionPath != "consensus" {
			t.Fatalf("expected consensus path, got %q", last.DecisionPath)
		}
	})
}

func TestSuggestCallCorrectionAppliesSpotterReliabilityFloor(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
	others := []*Spot{
		{DXCall: "K1ABD", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
	}

	_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:              "majority",
		MinConsensusReports:   1,
		MinAdvantage:          1,
		MinConfidencePercent:  50,
		MaxEditDistance:       2,
		RecencyWindow:         30 * time.Second,
		SpotterReliability:    SpotterReliability{"W2BBB": 0.2},
		MinSpotterReliability: 0.5,
	}, now)
	if ok {
		t.Fatalf("expected low-reliability reporter to be ignored")
	}
}

func TestSuggestCallCorrectionUsesCWModeSpecificReliability(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
	others := []*Spot{
		{DXCall: "K1ABD", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
	}

	_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:              "majority",
		MinConsensusReports:   1,
		MinAdvantage:          1,
		MinConfidencePercent:  50,
		MaxEditDistance:       2,
		RecencyWindow:         30 * time.Second,
		SpotterReliability:    SpotterReliability{"W2BBB": 0.9},
		SpotterReliabilityCW:  SpotterReliability{"W2BBB": 0.2},
		MinSpotterReliability: 0.5,
	}, now)
	if ok {
		t.Fatalf("expected CW mode-specific low reliability to override global map and reject")
	}
}

func TestSuggestCallCorrectionFallsBackToGlobalReliabilityWhenModeMapMissing(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
	others := []*Spot{
		{DXCall: "K1ABD", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "K1ABD", DECall: "W3CCC", Frequency: 7010.0, Mode: "CW", Time: now},
	}

	call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:              "majority",
		MinConsensusReports:   2,
		MinAdvantage:          1,
		MinConfidencePercent:  50,
		MaxEditDistance:       2,
		RecencyWindow:         30 * time.Second,
		SpotterReliability:    SpotterReliability{"W2BBB": 0.9},
		MinSpotterReliability: 0.5,
	}, now)
	if !ok {
		t.Fatalf("expected global reliability fallback to allow correction")
	}
	if call != "K1ABD" {
		t.Fatalf("expected K1ABD via global fallback, got %q", call)
	}
}

func TestSuggestCallCorrectionUsesRTTYModeSpecificReliability(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 14080.0, Mode: "RTTY", Time: now}
	others := []*Spot{
		{DXCall: "K1ABD", DECall: "W2BBB", Frequency: 14080.0, Mode: "RTTY", Time: now},
	}

	_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:               "majority",
		MinConsensusReports:    1,
		MinAdvantage:           1,
		MinConfidencePercent:   50,
		MaxEditDistance:        2,
		RecencyWindow:          30 * time.Second,
		SpotterReliability:     SpotterReliability{"W2BBB": 0.9},
		SpotterReliabilityRTTY: SpotterReliability{"W2BBB": 0.2},
		MinSpotterReliability:  0.5,
	}, now)
	if ok {
		t.Fatalf("expected RTTY mode-specific low reliability to override global map and reject")
	}
}

func TestSuggestCallCorrectionSlashPrecedenceDropsBareCall(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "W1AW", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
	others := []*Spot{
		{DXCall: "W1AW/1", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "W1AW/1", DECall: "W3CCC", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "W1AW/1", DECall: "W4DDD", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "W1AW/1", DECall: "W5EEE", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "W1AW", DECall: "W6FFF", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "W1AW", DECall: "W7GGG", Frequency: 7010.0, Mode: "CW", Time: now},
	}
	trace := &captureTraceLogger{}
	call, supporters, confidence, subjectConf, total, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  3,
		MinAdvantage:         1,
		MinConfidencePercent: 70,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
		DebugLog:             true,
		TraceLogger:          trace,
	}, now)
	if !ok {
		t.Fatalf("expected slash correction to apply")
	}
	if call != "W1AW/1" {
		t.Fatalf("expected W1AW/1 winner, got %q", call)
	}
	if supporters != 4 {
		t.Fatalf("expected 4 slash supporters, got %d", supporters)
	}
	if confidence != 100 || subjectConf != 0 || total != 4 {
		t.Fatalf("unexpected confidence tuple got winner=%d subject=%d total=%d", confidence, subjectConf, total)
	}
	last := trace.lastTrace(t)
	if last.DecisionPath != "consensus+slash_precedence" {
		t.Fatalf("expected slash precedence decision path, got %q", last.DecisionPath)
	}
}

func TestSuggestCallCorrectionSlashPrecedenceRequiresCredibleSlashSupport(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "W1AW", DECall: "W1AAA", Frequency: 7010.0, Mode: "CW", Time: now}
	others := []*Spot{
		{DXCall: "W1AW/1", DECall: "W2BBB", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "W1AW", DECall: "W3CCC", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "W1AW", DECall: "W4DDD", Frequency: 7010.0, Mode: "CW", Time: now},
		{DXCall: "W1AW", DECall: "W5EEE", Frequency: 7010.0, Mode: "CW", Time: now},
	}
	trace := &captureTraceLogger{}
	_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  3,
		MinAdvantage:         1,
		MinConfidencePercent: 70,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
		DebugLog:             true,
		TraceLogger:          trace,
	}, now)
	if ok {
		t.Fatalf("expected no correction when slash support is not credible")
	}
	last := trace.lastTrace(t)
	if strings.Contains(last.DecisionPath, "slash_precedence") {
		t.Fatalf("did not expect slash precedence path, got %q", last.DecisionPath)
	}
}

func TestSuggestCallCorrectionSlashRegionalPrefixSuffixEquivalent(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "W1AW", DECall: "W1AAA", Frequency: 14032.0, Mode: "CW", Time: now}
	others := []*Spot{
		{DXCall: "KH6/W1AW", DECall: "W2BBB", Frequency: 14032.0, Mode: "CW", Time: now},
		{DXCall: "W1AW/KH6", DECall: "W3CCC", Frequency: 14032.0, Mode: "CW", Time: now},
		{DXCall: "W1AW/KH6", DECall: "W4DDD", Frequency: 14032.0, Mode: "CW", Time: now},
	}
	call, supporters, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  3,
		MinAdvantage:         1,
		MinConfidencePercent: 70,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
	}, now)
	if !ok {
		t.Fatalf("expected merged KH6 variant to correct subject")
	}
	if supporters != 3 {
		t.Fatalf("expected merged support=3, got %d", supporters)
	}
	if call != "W1AW/KH6" {
		t.Fatalf("expected most-supported display variant W1AW/KH6, got %q", call)
	}
}

func TestSuggestCallCorrectionConflictingSlashVariantsCanReject(t *testing.T) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "W1AW", DECall: "W1AAA", Frequency: 14032.0, Mode: "CW", Time: now}
	others := []*Spot{
		{DXCall: "W1AW/6", DECall: "W2BBB", Frequency: 14032.0, Mode: "CW", Time: now},
		{DXCall: "W1AW/6", DECall: "W3CCC", Frequency: 14032.0, Mode: "CW", Time: now},
		{DXCall: "W1AW/KH6", DECall: "W4DDD", Frequency: 14032.0, Mode: "CW", Time: now},
		{DXCall: "KH6/W1AW", DECall: "W5EEE", Frequency: 14032.0, Mode: "CW", Time: now},
		{DXCall: "W1AW", DECall: "W6FFF", Frequency: 14032.0, Mode: "CW", Time: now},
		{DXCall: "W1AW", DECall: "W7GGG", Frequency: 14032.0, Mode: "CW", Time: now},
	}
	trace := &captureTraceLogger{}
	_, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  2,
		MinAdvantage:         1,
		MinConfidencePercent: 60,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
		DebugLog:             true,
		TraceLogger:          trace,
	}, now)
	if ok {
		t.Fatalf("expected conflicting slash variants to fail confidence gate")
	}
	last := trace.lastTrace(t)
	if last.Reason != "confidence" {
		t.Fatalf("expected confidence rejection, got %q", last.Reason)
	}
	if last.DecisionPath != "consensus+slash_precedence" {
		t.Fatalf("expected slash precedence path on rejection, got %q", last.DecisionPath)
	}
}

func TestSuggestCallCorrectionSlashPrecedenceAppliesToAnchorPath(t *testing.T) {
	withTestCallQualityStore(t, func(store *CallQualityStore) {
		now := time.Now().UTC()
		subject := &Spot{DXCall: "W1AW", DECall: "W1AAA", Frequency: 14032.0, Mode: "CW", Time: now}
		others := []*Spot{
			{DXCall: "W1AW/1", DECall: "W2BBB", Frequency: 14032.0, Mode: "CW", Time: now},
			{DXCall: "W1AW/1", DECall: "W3CCC", Frequency: 14032.0, Mode: "CW", Time: now},
			{DXCall: "W1AW/1", DECall: "W4DDD", Frequency: 14032.0, Mode: "CW", Time: now},
			{DXCall: "W1AW", DECall: "W5EEE", Frequency: 14032.0, Mode: "CW", Time: now},
			{DXCall: "W1AW", DECall: "W6FFF", Frequency: 14032.0, Mode: "CW", Time: now},
		}
		store.Add("W1AW/1", subject.Frequency*1000, 500, 3)

		trace := &captureTraceLogger{}
		call, _, _, _, _, ok := SuggestCallCorrection(subject, toEntries(others), CorrectionSettings{
			Strategy:                "majority",
			MinConsensusReports:     3,
			MinAdvantage:            1,
			MinConfidencePercent:    70,
			MaxEditDistance:         2,
			RecencyWindow:           30 * time.Second,
			QualityBinHz:            500,
			QualityGoodThreshold:    2,
			QualityNewCallIncrement: 1,
			QualityBustedDecrement:  1,
			DebugLog:                true,
			TraceLogger:             trace,
		}, now)
		if !ok {
			t.Fatalf("expected anchor-driven slash correction")
		}
		if call != "W1AW/1" {
			t.Fatalf("expected W1AW/1 from anchor path, got %q", call)
		}
		last := trace.lastTrace(t)
		if last.DecisionPath != "anchor+slash_precedence" {
			t.Fatalf("expected anchor+slash_precedence path, got %q", last.DecisionPath)
		}
	})
}

func TestCallDistanceToggle(t *testing.T) {
	plain := callDistance("E1A", "H1A", "CW", "plain", "plain")
	morse := callDistance("E1A", "H1A", "CW", "morse", "plain")
	if morse <= plain {
		t.Fatalf("expected morse distance (%d) to exceed plain (%d)", morse, plain)
	}
}

func TestCallDistanceNonCWStaysPlain(t *testing.T) {
	dist := callDistance("K1ABC", "K1A8C", "SSB", "morse", "baudot")
	if dist != 1 {
		t.Fatalf("expected non-CW to use plain distance, got %d", dist)
	}
}

func TestCallDistanceRTTYUsesBaudot(t *testing.T) {
	plain := callDistance("K1AB6C", "K1A86C", "RTTY", "plain", "plain")
	baudot := callDistance("K1AB6C", "K1A86C", "RTTY", "plain", "baudot")
	if baudot <= plain {
		t.Fatalf("expected baudot distance (%d) to exceed plain (%d)", baudot, plain)
	}
}

func BenchmarkSuggestCallCorrectionSlashPrecedence(b *testing.B) {
	now := time.Now().UTC()
	subject := &Spot{DXCall: "W1AW", DECall: "W1AAA", Frequency: 14032.0, Mode: "CW", Time: now}
	others := toEntries([]*Spot{
		{DXCall: "W1AW/6", DECall: "W2BBB", Frequency: 14032.0, Mode: "CW", Time: now},
		{DXCall: "W1AW/6", DECall: "W3CCC", Frequency: 14032.0, Mode: "CW", Time: now},
		{DXCall: "W1AW/KH6", DECall: "W4DDD", Frequency: 14032.0, Mode: "CW", Time: now},
		{DXCall: "KH6/W1AW", DECall: "W5EEE", Frequency: 14032.0, Mode: "CW", Time: now},
		{DXCall: "W1AW", DECall: "W6FFF", Frequency: 14032.0, Mode: "CW", Time: now},
		{DXCall: "W1AW", DECall: "W7GGG", Frequency: 14032.0, Mode: "CW", Time: now},
	})
	settings := CorrectionSettings{
		Strategy:             "majority",
		MinConsensusReports:  2,
		MinAdvantage:         1,
		MinConfidencePercent: 60,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, _, _, _ = SuggestCallCorrection(subject, others, settings, now)
	}
}

func toEntries(spots []*Spot) []bandmap.SpotEntry {
	out := make([]bandmap.SpotEntry, 0, len(spots))
	for _, s := range spots {
		if s == nil {
			continue
		}
		out = append(out, bandmap.SpotEntry{
			Call:    s.DXCall,
			Spotter: s.DECall,
			Mode:    s.Mode,
			FreqHz:  uint32(s.Frequency*1000 + 0.5),
			Time:    s.Time.Unix(),
			SNR:     s.Report,
		})
	}
	return out
}
