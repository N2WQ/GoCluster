package main

import (
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

func newTestTelnetFamilySuppressor() *telnetFamilySuppressor {
	return newTelnetFamilySuppressor(30*time.Second, 32, spot.CorrectionFamilyPolicy{
		Configured:                 true,
		TruncationEnabled:          true,
		TruncationMaxLengthDelta:   1,
		TruncationMinShorterLength: 3,
		TruncationAllowPrefix:      true,
		TruncationAllowSuffix:      true,
	}, 500)
}

func TestTelnetFamilySuppressorSuppressesBareAfterSlashVariant(t *testing.T) {
	suppressor := newTestTelnetFamilySuppressor()
	cfg := config.CallCorrectionConfig{
		FrequencyToleranceHz:      500,
		VoiceFrequencyToleranceHz: 2000,
	}
	now := time.Now().UTC()

	slash := spot.NewSpot("KP4/N2WQ", "W1AAA", 7010.0, "CW")
	if suppressor.ShouldSuppress(slash, cfg, now) {
		t.Fatalf("did not expect first slash spot to be suppressed")
	}

	bare := spot.NewSpot("N2WQ", "W2BBB", 7010.0, "CW")
	if !suppressor.ShouldSuppress(bare, cfg, now.Add(2*time.Second)) {
		t.Fatalf("expected bare variant to be suppressed after slash variant")
	}
}

func TestTelnetFamilySuppressorSuppressesTruncationAfterLongerForm(t *testing.T) {
	suppressor := newTestTelnetFamilySuppressor()
	cfg := config.CallCorrectionConfig{
		FrequencyToleranceHz:      500,
		VoiceFrequencyToleranceHz: 2000,
	}
	now := time.Now().UTC()

	longer := spot.NewSpot("WA1ABC", "W1AAA", 7010.0, "CW")
	if suppressor.ShouldSuppress(longer, cfg, now) {
		t.Fatalf("did not expect first longer form to be suppressed")
	}

	shorter := spot.NewSpot("A1ABC", "W2BBB", 7010.0, "CW")
	if !suppressor.ShouldSuppress(shorter, cfg, now.Add(2*time.Second)) {
		t.Fatalf("expected shorter truncation form to be suppressed")
	}
}

func TestTelnetFamilySuppressorPromotesMoreSpecificForm(t *testing.T) {
	suppressor := newTestTelnetFamilySuppressor()
	cfg := config.CallCorrectionConfig{
		FrequencyToleranceHz:      500,
		VoiceFrequencyToleranceHz: 2000,
	}
	now := time.Now().UTC()

	shorter := spot.NewSpot("W1AB", "W1AAA", 7010.0, "CW")
	if suppressor.ShouldSuppress(shorter, cfg, now) {
		t.Fatalf("did not expect initial shorter form to be suppressed")
	}

	longer := spot.NewSpot("W1ABC", "W2BBB", 7010.0, "CW")
	if suppressor.ShouldSuppress(longer, cfg, now.Add(time.Second)) {
		t.Fatalf("did not expect longer form promotion to be suppressed")
	}

	if !suppressor.ShouldSuppress(shorter, cfg, now.Add(2*time.Second)) {
		t.Fatalf("expected shorter form to be suppressed after longer form promotion")
	}
}

func TestTelnetFamilySuppressorDoesNotCrossFrequencyBuckets(t *testing.T) {
	suppressor := newTestTelnetFamilySuppressor()
	cfg := config.CallCorrectionConfig{
		FrequencyToleranceHz:      500,
		VoiceFrequencyToleranceHz: 2000,
	}
	now := time.Now().UTC()

	longer := spot.NewSpot("W1ABC", "W1AAA", 7010.0, "CW")
	if suppressor.ShouldSuppress(longer, cfg, now) {
		t.Fatalf("did not expect initial longer form to be suppressed")
	}

	shorterFar := spot.NewSpot("W1AB", "W2BBB", 7012.0, "CW")
	if suppressor.ShouldSuppress(shorterFar, cfg, now.Add(2*time.Second)) {
		t.Fatalf("did not expect suppression across different frequency buckets")
	}
}

func TestTelnetFamilySuppressorChecksAdjacentBinsWithinTolerance(t *testing.T) {
	suppressor := newTestTelnetFamilySuppressor()
	cfg := config.CallCorrectionConfig{
		FrequencyToleranceHz:      1000,
		VoiceFrequencyToleranceHz: 2000,
	}
	now := time.Now().UTC()

	longer := spot.NewSpot("W1ABC", "W1AAA", 7010.5, "CW")
	if suppressor.ShouldSuppress(longer, cfg, now) {
		t.Fatalf("did not expect initial longer form to be suppressed")
	}

	// Adjacent rounded bin, but still within absolute tolerance.
	shorter := spot.NewSpot("W1AB", "W2BBB", 7011.5, "CW")
	if !suppressor.ShouldSuppress(shorter, cfg, now.Add(2*time.Second)) {
		t.Fatalf("expected suppression across adjacent bins when within tolerance")
	}
}

func TestTelnetFamilySuppressorAdjacentBinsHonorAbsoluteTolerance(t *testing.T) {
	suppressor := newTestTelnetFamilySuppressor()
	cfg := config.CallCorrectionConfig{
		FrequencyToleranceHz:      1000,
		VoiceFrequencyToleranceHz: 2000,
	}
	now := time.Now().UTC()

	longer := spot.NewSpot("W1ABC", "W1AAA", 7010.5, "CW")
	if suppressor.ShouldSuppress(longer, cfg, now) {
		t.Fatalf("did not expect initial longer form to be suppressed")
	}

	// Adjacent rounded bin, but beyond absolute tolerance.
	shorter := spot.NewSpot("W1AB", "W2BBB", 7011.6, "CW")
	if suppressor.ShouldSuppress(shorter, cfg, now.Add(2*time.Second)) {
		t.Fatalf("did not expect suppression when adjacent bins exceed absolute tolerance")
	}
}

func TestTelnetFamilySuppressorEvictsOldestAtCapacity(t *testing.T) {
	suppressor := newTestTelnetFamilySuppressor()
	suppressor.maxEntries = 2
	cfg := config.CallCorrectionConfig{
		FrequencyToleranceHz:      500,
		VoiceFrequencyToleranceHz: 2000,
	}
	now := time.Now().UTC()

	longer := spot.NewSpot("W1ABC", "W1AAA", 7010.0, "CW")
	if suppressor.ShouldSuppress(longer, cfg, now) {
		t.Fatalf("did not expect initial longer form to be suppressed")
	}
	unrelated := spot.NewSpot("K1XYZ", "W2BBB", 7011.0, "CW")
	if suppressor.ShouldSuppress(unrelated, cfg, now.Add(time.Second)) {
		t.Fatalf("did not expect unrelated call to be suppressed")
	}
	if suppressor.totalEntries != 2 {
		t.Fatalf("expected cap to hold 2 entries, got %d", suppressor.totalEntries)
	}

	newest := spot.NewSpot("N2AAA", "W3CCC", 7012.0, "CW")
	if suppressor.ShouldSuppress(newest, cfg, now.Add(2*time.Second)) {
		t.Fatalf("did not expect newest call to be suppressed")
	}
	if suppressor.totalEntries != 2 {
		t.Fatalf("expected cap-enforced entry count 2, got %d", suppressor.totalEntries)
	}

	// The original longer form should have been evicted as the oldest entry.
	shorter := spot.NewSpot("W1AB", "W4DDD", 7010.0, "CW")
	if suppressor.ShouldSuppress(shorter, cfg, now.Add(3*time.Second)) {
		t.Fatalf("expected no suppression after oldest eviction removed the longer form")
	}
}

func TestTelnetFamilySuppressorPrunesExpiredEntriesAtCapacity(t *testing.T) {
	suppressor := newTestTelnetFamilySuppressor()
	suppressor.window = 2 * time.Second
	suppressor.maxEntries = 2
	cfg := config.CallCorrectionConfig{
		FrequencyToleranceHz:      500,
		VoiceFrequencyToleranceHz: 2000,
	}
	now := time.Now().UTC()

	longer := spot.NewSpot("W1ABC", "W1AAA", 7010.0, "CW")
	if suppressor.ShouldSuppress(longer, cfg, now) {
		t.Fatalf("did not expect initial longer form to be suppressed")
	}
	unrelated := spot.NewSpot("K1XYZ", "W2BBB", 7011.0, "CW")
	if suppressor.ShouldSuppress(unrelated, cfg, now.Add(time.Second)) {
		t.Fatalf("did not expect unrelated call to be suppressed")
	}
	if suppressor.totalEntries != 2 {
		t.Fatalf("expected cap to hold 2 entries, got %d", suppressor.totalEntries)
	}

	// At t+3s the original longer form (t0) is expired for a 2s window.
	shorter := spot.NewSpot("W1AB", "W4DDD", 7010.0, "CW")
	if suppressor.ShouldSuppress(shorter, cfg, now.Add(3*time.Second)) {
		t.Fatalf("expected no suppression after expiry pruned the older longer form")
	}
	if suppressor.totalEntries != 2 {
		t.Fatalf("expected expiry prune + insert to keep entry count bounded at 2, got %d", suppressor.totalEntries)
	}
}
