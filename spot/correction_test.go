package spot

import (
	"testing"
	"time"
)

func TestSuggestCallCorrectionRequiresConsensus(t *testing.T) {
	now := time.Date(2025, 11, 18, 10, 0, 0, 0, time.UTC)
	subject := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 14074.0, Time: now}
	others := []*Spot{
		{DXCall: "K1A8C", DECall: "W1AAA", Frequency: 14074.0, Time: now}, // same reporter, ignored
		{DXCall: "K1A8C", DECall: "W2BBB", Frequency: 14074.0, Time: now},
		{DXCall: "K1A8C", DECall: "W3CCC", Frequency: 14074.1, Time: now},
		{DXCall: "K1A8C", DECall: "W4DDD", Frequency: 14074.0, Time: now.Add(-10 * time.Second)},
	}

	call, supporters, confidence, subjectConfidence, total, ok := SuggestCallCorrection(subject, others, CorrectionSettings{
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
	if call, _, _, _, _, ok := SuggestCallCorrection(subject, others, CorrectionSettings{
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
	if call, _, _, _, _, ok := SuggestCallCorrection(subject, others, CorrectionSettings{
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
	if call, _, _, _, _, ok := SuggestCallCorrection(subject, others, CorrectionSettings{
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
	if call, _, _, _, _, ok := SuggestCallCorrection(subject, others, CorrectionSettings{
		MinConsensusReports:  2,
		MinAdvantage:         1,
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
	if call, _, _, _, _, ok := SuggestCallCorrection(subject, others, CorrectionSettings{
		MinConsensusReports:  2,
		MinAdvantage:         1,
		MinConfidencePercent: 70,
		MaxEditDistance:      2,
		RecencyWindow:        30 * time.Second,
	}, now); ok {
		t.Fatalf("expected no correction (confidence too low), got %s", call)
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
	if call, _, _, _, _, ok := SuggestCallCorrection(subject, others, CorrectionSettings{
		MinConsensusReports:  3,
		MinAdvantage:         1,
		MinConfidencePercent: 60,
		MaxEditDistance:      1,
		RecencyWindow:        30 * time.Second,
	}, now); ok {
		t.Fatalf("expected no correction due to distance, got %s", call)
	}
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
