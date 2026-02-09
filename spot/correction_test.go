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
