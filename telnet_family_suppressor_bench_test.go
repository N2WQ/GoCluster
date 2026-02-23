package main

import (
	"fmt"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func benchmarkTelnetFamilyPolicy() spot.CorrectionFamilyPolicy {
	return spot.CorrectionFamilyPolicy{
		Configured:                 true,
		TruncationEnabled:          true,
		TruncationMaxLengthDelta:   1,
		TruncationMinShorterLength: 3,
		TruncationAllowPrefix:      true,
		TruncationAllowSuffix:      true,
	}
}

func benchmarkCallCorrectionConfig() config.CallCorrectionConfig {
	return config.CallCorrectionConfig{
		FrequencyToleranceHz:      500,
		VoiceFrequencyToleranceHz: 2000,
	}
}

func benchmarkMakeSpots(prefix string, count int, startFreq, step float64) []*spot.Spot {
	spots := make([]*spot.Spot, 0, count)
	for i := 0; i < count; i++ {
		call := fmt.Sprintf("%s%05d", prefix, i)
		spots = append(spots, spot.NewSpot(call, "BENCH", startFreq+float64(i)*step, "CW"))
	}
	return spots
}

func BenchmarkTelnetFamilySuppressorShouldSuppressAtCapacityEvict(b *testing.B) {
	const maxEntries = 2048
	cfg := benchmarkCallCorrectionConfig()
	suppressor := newTelnetFamilySuppressor(10*time.Minute, maxEntries, benchmarkTelnetFamilyPolicy(), 500)
	spotPool := benchmarkMakeSpots("K1", maxEntries*8, 7000.0, 1.0)
	nowBase := time.Unix(0, 0).UTC()

	for i := 0; i < maxEntries; i++ {
		suppressor.ShouldSuppress(spotPool[i], cfg, nowBase.Add(time.Duration(i)*time.Millisecond))
	}

	idx := maxEntries
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if idx >= len(spotPool) {
			idx = maxEntries
		}
		suppressor.ShouldSuppress(spotPool[idx], cfg, nowBase.Add(time.Duration(maxEntries+i)*time.Millisecond))
		idx++
	}
}

func BenchmarkTelnetFamilySuppressorShouldSuppressMixedAtHalfCapacity(b *testing.B) {
	const maxEntries = 4096
	const warmEntries = 1536
	cfg := benchmarkCallCorrectionConfig()
	suppressor := newTelnetFamilySuppressor(10*time.Minute, maxEntries, benchmarkTelnetFamilyPolicy(), 500)
	nowBase := time.Unix(0, 0).UTC()

	warm := benchmarkMakeSpots("W1", warmEntries, 10000.0, 1.0)
	for i := range warm {
		suppressor.ShouldSuppress(warm[i], cfg, nowBase.Add(time.Duration(i)*time.Millisecond))
	}

	background := benchmarkMakeSpots("B1", 512, 12000.0, 1.0)
	familyOps := []*spot.Spot{
		spot.NewSpot("WA1ABC", "BENCH1", 7010.0, "CW"),
		spot.NewSpot("A1ABC", "BENCH2", 7010.0, "CW"),
		spot.NewSpot("W1AB", "BENCH3", 7010.0, "CW"),
		spot.NewSpot("W1ABC", "BENCH4", 7010.0, "CW"),
		spot.NewSpot("KP4/N2WQ", "BENCH5", 7010.0, "CW"),
		spot.NewSpot("N2WQ", "BENCH6", 7010.0, "CW"),
	}

	bgIdx := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var sp *spot.Spot
		switch i % 8 {
		case 0, 1, 2, 3, 4, 5:
			sp = familyOps[i%len(familyOps)]
		default:
			sp = background[bgIdx%len(background)]
			bgIdx++
		}
		suppressor.ShouldSuppress(sp, cfg, nowBase.Add(time.Duration(warmEntries+i)*time.Millisecond))
	}
}
