package spot

import (
	"testing"
	"time"
)

func TestHarmonicDetectorDropsSecondHarmonic(t *testing.T) {
	settings := HarmonicSettings{
		Enabled:              true,
		RecencyWindow:        2 * time.Minute,
		MaxHarmonicMultiple:  4,
		FrequencyToleranceHz: 25,
		MinReportDelta:       6,
	}
	detector := NewHarmonicDetector(settings)
	now := time.Now().UTC()

	fundamental := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 7011.0, Report: 20, Mode: "CW", Time: now}
	if drop, _ := detector.ShouldDrop(fundamental, now); drop {
		t.Fatalf("fundamental should not be dropped")
	}

	harmonic := &Spot{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 14022.0, Report: 10, Mode: "CW", Time: now.Add(5 * time.Second)}
	drop, fundamentalFreq := detector.ShouldDrop(harmonic, now.Add(5*time.Second))
	if !drop {
		t.Fatalf("expected harmonic to be dropped")
	}
	if fundamentalFreq != 7011.0 {
		t.Fatalf("expected fundamental 7011.0, got %.1f", fundamentalFreq)
	}
}

func TestHarmonicDetectorKeepsStrongerSpot(t *testing.T) {
	settings := HarmonicSettings{
		Enabled:              true,
		RecencyWindow:        2 * time.Minute,
		MaxHarmonicMultiple:  4,
		FrequencyToleranceHz: 25,
		MinReportDelta:       6,
	}
	detector := NewHarmonicDetector(settings)
	now := time.Now().UTC()

	fundamental := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 7011.0, Report: 10, Mode: "SSB", Time: now}
	detector.ShouldDrop(fundamental, now)

	strongHarmonic := &Spot{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 14022.0, Report: 20, Mode: "SSB", Time: now.Add(10 * time.Second)}
	if drop, _ := detector.ShouldDrop(strongHarmonic, now.Add(10*time.Second)); drop {
		t.Fatalf("harmonic with stronger report should not be dropped")
	}
}

func TestHarmonicDetectorRequiresMultipleRatio(t *testing.T) {
	settings := HarmonicSettings{
		Enabled:              true,
		RecencyWindow:        2 * time.Minute,
		MaxHarmonicMultiple:  4,
		FrequencyToleranceHz: 10,
		MinReportDelta:       3,
	}
	detector := NewHarmonicDetector(settings)
	now := time.Now().UTC()

	fundamental := &Spot{DXCall: "K1ABC", DECall: "W1AAA", Frequency: 7010.0, Report: 15, Mode: "RTTY", Time: now}
	detector.ShouldDrop(fundamental, now)

	offFrequency := &Spot{DXCall: "K1ABC", DECall: "W2BBB", Frequency: 15000.0, Report: 5, Mode: "RTTY", Time: now.Add(5 * time.Second)}
	if drop, _ := detector.ShouldDrop(offFrequency, now.Add(5*time.Second)); drop {
		t.Fatalf("spot not near integer multiple should not be dropped")
	}
}
