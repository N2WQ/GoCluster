package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
	"dxcluster/ui"
)

type captureSurface struct {
	unlicensed []string
}

func (c *captureSurface) WaitReady() {}
func (c *captureSurface) Stop()      {}
func (c *captureSurface) SetStats(lines []string) {
}
func (c *captureSurface) UpdateNetworkStatus(summaryLine string, clientLines []string) {
}
func (c *captureSurface) AppendDropped(line string) {
}
func (c *captureSurface) AppendCall(line string) {
}
func (c *captureSurface) AppendUnlicensed(line string) {
	c.unlicensed = append(c.unlicensed, line)
}
func (c *captureSurface) AppendHarmonic(line string) {
}
func (c *captureSurface) AppendReputation(line string) {
}
func (c *captureSurface) AppendSystem(line string) {
}
func (c *captureSurface) SystemWriter() io.Writer { return nil }
func (c *captureSurface) SetSnapshot(snapshot ui.Snapshot) {
}

func TestEventFormattersEmitPlainText(t *testing.T) {
	tests := []string{
		formatUnlicensedDropMessage("DX", "K1ABC", "RBN", "CW", 14020.1),
		formatHarmonicSuppressedMessage("K1ABC", 14020.1, 7010.0, 3, 18),
		formatCallCorrectedMessage("K1A8C", "K1ABC", 7012.3, 4, 92),
	}
	for _, line := range tests {
		if strings.Contains(line, "[") || strings.Contains(line, "]") {
			t.Fatalf("expected plain text message without color tags, got %q", line)
		}
	}
}

func TestMakeUnlicensedReporterEmitsPlainTextToSurface(t *testing.T) {
	surface := &captureSurface{}
	reporter := makeUnlicensedReporter(surface, nil, nil)
	reporter("rbn", "dx", "k1abc", "cw", 7029.5)

	if len(surface.unlicensed) != 1 {
		t.Fatalf("expected one unlicensed message, got %d", len(surface.unlicensed))
	}
	got := surface.unlicensed[0]
	if strings.Contains(got, "[") || strings.Contains(got, "]") {
		t.Fatalf("expected plain text unlicensed message, got %q", got)
	}
	if !strings.Contains(got, "K1ABC") {
		t.Fatalf("expected normalized callsign in message, got %q", got)
	}
}

func TestCloneSpotForPeerPublishAddsModeWhenCommentEmpty(t *testing.T) {
	src := spot.NewSpot("K1ABC", "W1XYZ", 7074.0, "")
	src.Mode = "FT8"
	src.Comment = ""

	peerSpot := cloneSpotForPeerPublish(src)
	if peerSpot == nil {
		t.Fatalf("expected peer spot, got nil")
	}
	if peerSpot == src {
		t.Fatalf("expected a cloned spot when adding inferred mode to comment")
	}
	if peerSpot.Comment != "FT8" {
		t.Fatalf("expected comment to carry inferred mode, got %q", peerSpot.Comment)
	}
	if src.Comment != "" {
		t.Fatalf("expected original comment to remain empty, got %q", src.Comment)
	}
}

func TestCloneSpotForPeerPublishPassthroughWhenCommentPresent(t *testing.T) {
	src := spot.NewSpot("K1ABC", "W1XYZ", 7074.0, "")
	src.Mode = "FT8"
	src.Comment = "cq test"

	peerSpot := cloneSpotForPeerPublish(src)
	if peerSpot != src {
		t.Fatalf("expected passthrough when comment present")
	}
}

func TestShouldBufferSpotSkipsTestSpotter(t *testing.T) {
	spotTest := spot.NewSpot("K1ABC", "K1TEST", 7074.0, "FT8")
	spotTest.IsTestSpotter = true
	if shouldBufferSpot(spotTest) {
		t.Fatalf("expected test spotter to skip ring buffer")
	}
	spotNormal := spot.NewSpot("K1ABC", "W1XYZ", 7074.0, "FT8")
	if !shouldBufferSpot(spotNormal) {
		t.Fatalf("expected normal spot to enter ring buffer")
	}
}

func TestShouldArchiveSpotSkipsTestSpotter(t *testing.T) {
	spotTest := spot.NewSpot("K1ABC", "K1TEST", 7074.0, "FT8")
	spotTest.IsTestSpotter = true
	if shouldArchiveSpot(spotTest) {
		t.Fatalf("expected test spotter to skip archive")
	}
	spotNormal := spot.NewSpot("K1ABC", "W1XYZ", 7074.0, "FT8")
	if !shouldArchiveSpot(spotNormal) {
		t.Fatalf("expected normal spot to archive")
	}
}

func TestShouldPublishToPeersSkipsTestSpotter(t *testing.T) {
	spotTest := spot.NewSpot("K1ABC", "K1TEST", 7074.0, "FT8")
	spotTest.SourceType = spot.SourceManual
	spotTest.IsTestSpotter = true
	if shouldPublishToPeers(spotTest) {
		t.Fatalf("expected test spotter to skip peering")
	}
	spotNormal := spot.NewSpot("K1ABC", "W1XYZ", 7074.0, "FT8")
	spotNormal.SourceType = spot.SourceManual
	if !shouldPublishToPeers(spotNormal) {
		t.Fatalf("expected manual spot to publish to peers")
	}
	spotUpstream := spot.NewSpot("K1ABC", "W1XYZ", 7074.0, "FT8")
	spotUpstream.SourceType = spot.SourceUpstream
	if shouldPublishToPeers(spotUpstream) {
		t.Fatalf("expected upstream spot to skip peering")
	}
	spotPeer := spot.NewSpot("K1ABC", "W1XYZ", 7074.0, "FT8")
	spotPeer.SourceType = spot.SourcePeer
	if shouldPublishToPeers(spotPeer) {
		t.Fatalf("expected peer spot to skip peering")
	}
}

// Purpose: Verify gridDBCheckOnMissEnabled defaults to true.
// Key aspects: Clears env override before test.
// Upstream: go test execution.
// Downstream: gridDBCheckOnMissEnabled.
func TestGridDBCheckOnMissEnabled_DefaultsTrue(t *testing.T) {
	t.Setenv(envGridDBCheckOnMiss, "")

	got, source := gridDBCheckOnMissEnabled(&config.Config{})
	if !got {
		t.Fatalf("expected default grid DB check to be enabled, got %v (source=%s)", got, source)
	}
}

// Purpose: Verify config can disable grid DB check.
// Key aspects: Uses explicit config override.
// Upstream: go test execution.
// Downstream: gridDBCheckOnMissEnabled.
func TestGridDBCheckOnMissEnabled_ConfigFalse(t *testing.T) {
	t.Setenv(envGridDBCheckOnMiss, "")
	cfg := &config.Config{GridDBCheckOnMiss: boolPtr(false)}

	got, source := gridDBCheckOnMissEnabled(cfg)
	if got {
		t.Fatalf("expected grid DB check to be disabled by config, got %v (source=%s)", got, source)
	}
}

// Purpose: Verify env override takes precedence over config.
// Key aspects: Sets env to false and checks source.
// Upstream: go test execution.
// Downstream: gridDBCheckOnMissEnabled.
func TestGridDBCheckOnMissEnabled_EnvOverridesConfig(t *testing.T) {
	cfg := &config.Config{GridDBCheckOnMiss: boolPtr(true)}
	t.Setenv(envGridDBCheckOnMiss, "false")

	got, source := gridDBCheckOnMissEnabled(cfg)
	if got {
		t.Fatalf("expected env override to disable grid DB check, got %v (source=%s)", got, source)
	}
	if source != envGridDBCheckOnMiss {
		t.Fatalf("expected source=%q, got %q", envGridDBCheckOnMiss, source)
	}
}

// Purpose: Verify invalid env override is ignored.
// Key aspects: Uses non-boolean env value.
// Upstream: go test execution.
// Downstream: gridDBCheckOnMissEnabled.
func TestGridDBCheckOnMissEnabled_InvalidEnvIgnored(t *testing.T) {
	cfg := &config.Config{GridDBCheckOnMiss: boolPtr(false)}
	t.Setenv(envGridDBCheckOnMiss, "notabool")

	got, _ := gridDBCheckOnMissEnabled(cfg)
	if got {
		t.Fatalf("expected invalid env override to be ignored, got %v", got)
	}
}

// Purpose: Verify LoadedFrom is reported as the config source.
// Key aspects: Leaves env unset to test config source reporting.
// Upstream: go test execution.
// Downstream: gridDBCheckOnMissEnabled.
func TestGridDBCheckOnMissEnabled_UsesLoadedFromWhenSet(t *testing.T) {
	cfg := &config.Config{GridDBCheckOnMiss: boolPtr(true), LoadedFrom: "data/config"}
	t.Setenv(envGridDBCheckOnMiss, "")

	_, source := gridDBCheckOnMissEnabled(cfg)
	if source != "data/config" {
		t.Fatalf("expected source=data/config, got %s", source)
	}
}

// Purpose: Verify block profile rate parsing accepts duration strings.
// Key aspects: Uses Go-style duration values and validates nanoseconds conversion.
// Upstream: go test execution.
// Downstream: parseBlockProfileRate.
func TestParseBlockProfileRateDuration(t *testing.T) {
	got, err := parseBlockProfileRate("10ms")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 10*time.Millisecond {
		t.Fatalf("expected 10ms, got %s", got)
	}
	got, err = parseBlockProfileRate("10 ms")
	if err != nil {
		t.Fatalf("unexpected error for spaced duration: %v", err)
	}
	if got != 10*time.Millisecond {
		t.Fatalf("expected 10ms for spaced duration, got %s", got)
	}
}

// Purpose: Verify block profile rate parsing accepts integer nanoseconds.
// Key aspects: Uses an integer string to represent nanoseconds.
// Upstream: go test execution.
// Downstream: parseBlockProfileRate.
func TestParseBlockProfileRateNanos(t *testing.T) {
	got, err := parseBlockProfileRate("10000000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 10*time.Millisecond {
		t.Fatalf("expected 10ms, got %s", got)
	}
}

// Purpose: Verify invalid block profile rates are rejected.
// Key aspects: Covers negative and non-duration values.
// Upstream: go test execution.
// Downstream: parseBlockProfileRate.
func TestParseBlockProfileRateInvalid(t *testing.T) {
	if _, err := parseBlockProfileRate("-1ms"); err == nil {
		t.Fatalf("expected error for negative duration")
	}
	if _, err := parseBlockProfileRate("notaduration"); err == nil {
		t.Fatalf("expected error for invalid duration")
	}
}

// Purpose: Verify mutex profile fraction parsing.
// Key aspects: Accepts integer values and rejects negatives.
// Upstream: go test execution.
// Downstream: parseMutexProfileFraction.
func TestParseMutexProfileFraction(t *testing.T) {
	got, err := parseMutexProfileFraction("10")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 10 {
		t.Fatalf("expected 10, got %d", got)
	}
	got, err = parseMutexProfileFraction("0")
	if err != nil {
		t.Fatalf("unexpected error for zero: %v", err)
	}
	if got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
	if _, err := parseMutexProfileFraction("-1"); err == nil {
		t.Fatalf("expected error for negative fraction")
	}
	if _, err := parseMutexProfileFraction("notanint"); err == nil {
		t.Fatalf("expected error for invalid fraction")
	}
}

func TestLookupGridUnifiedUsesSyncThenAsync(t *testing.T) {
	syncCalls := 0
	asyncCalls := 0
	syncFn := func(call string) (string, bool, bool) {
		syncCalls++
		return "", false, false
	}
	asyncFn := func(call string) (string, bool, bool) {
		asyncCalls++
		return "FN20", true, true
	}
	grid, derived, ok := lookupGridUnified("K1ABC", syncFn, asyncFn)
	if !ok || grid != "FN20" || !derived {
		t.Fatalf("expected async fallback grid FN20 derived=true, got %q ok=%v derived=%v", grid, ok, derived)
	}
	if syncCalls != 1 || asyncCalls != 1 {
		t.Fatalf("expected sync=1 async=1, got sync=%d async=%d", syncCalls, asyncCalls)
	}

	syncCalls = 0
	asyncCalls = 0
	syncFn = func(call string) (string, bool, bool) {
		syncCalls++
		return "EM12", false, true
	}
	asyncFn = func(call string) (string, bool, bool) {
		asyncCalls++
		return "", false, false
	}
	grid, derived, ok = lookupGridUnified("K1ABC", syncFn, asyncFn)
	if !ok || grid != "EM12" || derived {
		t.Fatalf("expected sync grid EM12 derived=false, got %q ok=%v derived=%v", grid, ok, derived)
	}
	if syncCalls != 1 || asyncCalls != 0 {
		t.Fatalf("expected sync=1 async=0, got sync=%d async=%d", syncCalls, asyncCalls)
	}
}

func TestFormatGridLineIncludesLookupRateAndDrops(t *testing.T) {
	metrics := &gridMetrics{}
	metrics.learnedTotal.Store(5)
	metrics.cacheLookups.Store(160)
	metrics.cacheHits.Store(80)
	metrics.asyncDrops.Store(12)
	metrics.syncDrops.Store(3)
	now := time.Now().UTC()
	metrics.rateMu.Lock()
	metrics.lastLookupCount = 100
	metrics.lastSample = now.Add(-time.Minute)
	metrics.rateMu.Unlock()

	line := formatGridLine(metrics, nil, nil)
	if !strings.Contains(line, "Grids:") {
		t.Fatalf("expected Grids label, got %q", line)
	}
	if !strings.Contains(line, " / 60 | Drop a12 s3") {
		t.Fatalf("expected lookup rate and drop counts, got %q", line)
	}
}

func TestRestoreGridStoreFromPathReplacesDB(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pebble")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("mkdir db: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dbPath, "old.txt"), []byte("old"), 0o644); err != nil {
		t.Fatalf("write old: %v", err)
	}
	checkpointPath := filepath.Join(dir, "checkpoint")
	if err := os.MkdirAll(checkpointPath, 0o755); err != nil {
		t.Fatalf("mkdir checkpoint: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkpointPath, "new.txt"), []byte("new"), 0o644); err != nil {
		t.Fatalf("write new: %v", err)
	}
	if err := restoreGridStoreFromPath(context.Background(), dbPath, checkpointPath); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dbPath, "new.txt")); err != nil {
		t.Fatalf("expected restored file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dbPath, "old.txt")); err == nil {
		t.Fatalf("expected old file to be removed")
	}
}

func TestRestoreGridStoreFromPathCancelLeavesDBIntact(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pebble")
	if err := os.MkdirAll(dbPath, 0o755); err != nil {
		t.Fatalf("mkdir db: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dbPath, "old.txt"), []byte("old"), 0o644); err != nil {
		t.Fatalf("write old: %v", err)
	}
	checkpointPath := filepath.Join(dir, "checkpoint")
	if err := os.MkdirAll(checkpointPath, 0o755); err != nil {
		t.Fatalf("mkdir checkpoint: %v", err)
	}
	if err := os.WriteFile(filepath.Join(checkpointPath, "new.txt"), []byte("new"), 0o644); err != nil {
		t.Fatalf("write new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := restoreGridStoreFromPath(ctx, dbPath, checkpointPath); err == nil {
		t.Fatalf("expected cancel error")
	}
	if _, err := os.Stat(filepath.Join(dbPath, "old.txt")); err != nil {
		t.Fatalf("expected old file to remain: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dbPath, "new.txt")); err == nil {
		t.Fatalf("did not expect new file to appear")
	}
}

// Purpose: Ensure SCP-known calls promote '?' confidence to 'S' after correction.
// Key aspects: Applies the known-call floor only when confidence is '?'.
// Upstream: go test execution.
// Downstream: applyKnownCallFloor and spot.LoadKnownCallsigns.
func TestApplyKnownCallFloorPromotesKnownDX(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "known.txt")
	if err := os.WriteFile(path, []byte("K1KI\n"), 0o644); err != nil {
		t.Fatalf("write known calls: %v", err)
	}
	known, err := spot.LoadKnownCallsigns(path)
	if err != nil {
		t.Fatalf("load known calls: %v", err)
	}
	var knownPtr atomic.Pointer[spot.KnownCallsigns]
	knownPtr.Store(known)

	s := spot.NewSpot("K1KI", "W2TT", 1831.3, "CW")
	s.Confidence = "?"

	if !applyKnownCallFloor(s, &knownPtr) {
		t.Fatalf("expected known-call floor to mark confidence")
	}
	if s.Confidence != "S" {
		t.Fatalf("expected confidence S, got %q", s.Confidence)
	}
}

// Purpose: Ensure SCP floor does not override explicit P/V/C confidence.
// Key aspects: Keeps existing confidence when it is not '?'.
// Upstream: go test execution.
// Downstream: applyKnownCallFloor.
func TestApplyKnownCallFloorKeepsNonUnknownConfidence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "known.txt")
	if err := os.WriteFile(path, []byte("K1KI\n"), 0o644); err != nil {
		t.Fatalf("write known calls: %v", err)
	}
	known, err := spot.LoadKnownCallsigns(path)
	if err != nil {
		t.Fatalf("load known calls: %v", err)
	}
	var knownPtr atomic.Pointer[spot.KnownCallsigns]
	knownPtr.Store(known)

	s := spot.NewSpot("K1KI", "W2TT", 1831.3, "CW")
	s.Confidence = "P"

	if applyKnownCallFloor(s, &knownPtr) {
		t.Fatalf("did not expect known-call floor to override P")
	}
	if s.Confidence != "P" {
		t.Fatalf("expected confidence P, got %q", s.Confidence)
	}
}

// Purpose: Ensure SCP floor ignores modes without confidence glyphs.
// Key aspects: FT8 remains without S promotion even when known.
// Upstream: go test execution.
// Downstream: applyKnownCallFloor.
func TestApplyKnownCallFloorSkipsUnsupportedMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "known.txt")
	if err := os.WriteFile(path, []byte("K1KI\n"), 0o644); err != nil {
		t.Fatalf("write known calls: %v", err)
	}
	known, err := spot.LoadKnownCallsigns(path)
	if err != nil {
		t.Fatalf("load known calls: %v", err)
	}
	var knownPtr atomic.Pointer[spot.KnownCallsigns]
	knownPtr.Store(known)

	s := spot.NewSpot("K1KI", "W2TT", 14074.0, "FT8")
	s.Confidence = "?"

	if applyKnownCallFloor(s, &knownPtr) {
		t.Fatalf("did not expect known-call floor to apply to FT8")
	}
	if s.Confidence != "?" {
		t.Fatalf("expected confidence ?, got %q", s.Confidence)
	}
}

func TestBuildCorrectionSettingsMapsConfigFields(t *testing.T) {
	cfg := config.CallCorrectionConfig{
		MinAdvantage:              2,
		MinConfidencePercent:      65,
		MaxEditDistance:           3,
		Strategy:                  "majority",
		MinSNRCW:                  4,
		MinSNRRTTY:                3,
		MinSNRVoice:               1,
		DistanceModelCW:           "morse",
		DistanceModelRTTY:         "baudot",
		Distance3ExtraReports:     1,
		Distance3ExtraAdvantage:   1,
		Distance3ExtraConfidence:  5,
		DebugLog:                  true,
		FreqGuardMinSeparationKHz: 0.2,
		FreqGuardRunnerUpRatio:    0.6,
		QualityGoodThreshold:      3,
		QualityNewCallIncrement:   2,
		QualityBustedDecrement:    2,
		CandidateEvalTopK:         3,
		MinSpotterReliability:     0.4,
		ConfusionModelWeight:      0.25,
		PriorBonusEnabled:         true,
		PriorBonusMax:             1,
		PriorBonusDistanceMax:     1,
		PriorBonusRequiresSCP:     true,
		PriorBonusApplyTo:         "min_reports",
	}
	window := 75 * time.Second
	reliability := spot.SpotterReliability{"W2BBB": 0.7}
	reliabilityCW := spot.SpotterReliability{"W2BBB": 0.8}
	reliabilityRTTY := spot.SpotterReliability{"W2BBB": 0.9}
	knownCallset := &spot.KnownCallsigns{}
	got := buildCorrectionSettings(
		cfg,
		5,
		6,
		window,
		900,
		400,
		nil,
		nil,
		reliability,
		reliabilityCW,
		reliabilityRTTY,
		nil,
		knownCallset,
		nil,
	)

	if got.MinConsensusReports != 5 {
		t.Fatalf("expected min reports 5, got %d", got.MinConsensusReports)
	}
	if got.CooldownMinReporters != 6 {
		t.Fatalf("expected cooldown min reporters 6, got %d", got.CooldownMinReporters)
	}
	if got.RecencyWindow != window {
		t.Fatalf("expected window %s, got %s", window, got.RecencyWindow)
	}
	if got.FrequencyToleranceHz != 900 {
		t.Fatalf("expected frequency tolerance 900Hz, got %.1f", got.FrequencyToleranceHz)
	}
	if got.QualityBinHz != 400 {
		t.Fatalf("expected quality bin 400Hz, got %d", got.QualityBinHz)
	}
	if got.FreqGuardMinSeparationKHz != cfg.FreqGuardMinSeparationKHz {
		t.Fatalf("expected freq guard separation %.3f, got %.3f", cfg.FreqGuardMinSeparationKHz, got.FreqGuardMinSeparationKHz)
	}
	if got.FreqGuardRunnerUpRatio != cfg.FreqGuardRunnerUpRatio {
		t.Fatalf("expected freq guard runner ratio %.3f, got %.3f", cfg.FreqGuardRunnerUpRatio, got.FreqGuardRunnerUpRatio)
	}
	if got.MinAdvantage != cfg.MinAdvantage ||
		got.MinConfidencePercent != cfg.MinConfidencePercent ||
		got.MaxEditDistance != cfg.MaxEditDistance ||
		got.Strategy != cfg.Strategy ||
		got.MinSNRCW != cfg.MinSNRCW ||
		got.MinSNRRTTY != cfg.MinSNRRTTY ||
		got.MinSNRVoice != cfg.MinSNRVoice ||
		got.DistanceModelCW != cfg.DistanceModelCW ||
		got.DistanceModelRTTY != cfg.DistanceModelRTTY ||
		got.Distance3ExtraReports != cfg.Distance3ExtraReports ||
		got.Distance3ExtraAdvantage != cfg.Distance3ExtraAdvantage ||
		got.Distance3ExtraConfidence != cfg.Distance3ExtraConfidence ||
		got.QualityGoodThreshold != cfg.QualityGoodThreshold ||
		got.QualityNewCallIncrement != cfg.QualityNewCallIncrement ||
		got.QualityBustedDecrement != cfg.QualityBustedDecrement ||
		got.CandidateEvalTopK != cfg.CandidateEvalTopK ||
		got.ConfusionWeight != cfg.ConfusionModelWeight ||
		got.PriorBonusEnabled != cfg.PriorBonusEnabled ||
		got.PriorBonusMax != cfg.PriorBonusMax ||
		got.PriorBonusDistanceMax != cfg.PriorBonusDistanceMax ||
		got.PriorBonusRequiresSCP != cfg.PriorBonusRequiresSCP ||
		got.PriorBonusApplyTo != cfg.PriorBonusApplyTo ||
		got.MinSpotterReliability != cfg.MinSpotterReliability {
		t.Fatalf("expected correction settings to mirror config fields")
	}
	if got.PriorBonusKnownCallset != knownCallset {
		t.Fatalf("expected known callset pointer to be preserved")
	}
	if got.SpotterReliability["W2BBB"] != 0.7 {
		t.Fatalf("expected spotter reliability map to be preserved")
	}
	if got.SpotterReliabilityCW["W2BBB"] != 0.8 {
		t.Fatalf("expected CW spotter reliability map to be preserved")
	}
	if got.SpotterReliabilityRTTY["W2BBB"] != 0.9 {
		t.Fatalf("expected RTTY spotter reliability map to be preserved")
	}
}

func TestCallCorrectionWindowForModeUsesOverrides(t *testing.T) {
	cfg := config.CallCorrectionConfig{
		RecencySeconds:     120,
		RecencySecondsCW:   45,
		RecencySecondsRTTY: 90,
	}

	if got := callCorrectionWindowForMode(cfg, "CW"); got != 45*time.Second {
		t.Fatalf("expected CW window 45s, got %s", got)
	}
	if got := callCorrectionWindowForMode(cfg, "RTTY"); got != 90*time.Second {
		t.Fatalf("expected RTTY window 90s, got %s", got)
	}
	if got := callCorrectionWindowForMode(cfg, "USB"); got != 120*time.Second {
		t.Fatalf("expected USB/base window 120s, got %s", got)
	}
}

func TestCallCorrectionCleanupWindowUsesMaxRecency(t *testing.T) {
	cfg := config.CallCorrectionConfig{
		RecencySeconds:     60,
		RecencySecondsCW:   180,
		RecencySecondsRTTY: 90,
	}
	if got := callCorrectionCleanupWindow(cfg); got != 180*time.Second {
		t.Fatalf("expected cleanup window 180s, got %s", got)
	}
}

// Purpose: Validate SSID collapsing rules for broadcast formatting.
// Key aspects: Covers numeric, non-numeric, and composite suffixes.
// Upstream: go test execution.
// Downstream: collapseSSIDForBroadcast.
func TestCollapseSSIDForBroadcast(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"N2WQ-1-#", "N2WQ-#"},
		{"N2WQ-#", "N2WQ-#"},
		{"N2WQ-1", "N2WQ"},
		{"N2WQ-12", "N2WQ"},
		{"N2WQ-TEST", "N2WQ-TEST"},
		{"N2WQ-1/P", "N2WQ-1/P"},
		{"", ""},
	}

	for _, tc := range cases {
		got := collapseSSIDForBroadcast(tc.input)
		if got != tc.want {
			t.Fatalf("collapseSSIDForBroadcast(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// Purpose: Ensure metadata lookups strip skimmer and numeric SSID suffixes only.
// Key aspects: Preserves portable segments while normalizing skimmer suffixes.
// Upstream: go test execution.
// Downstream: normalizeCallForMetadata.
func TestNormalizeCallForMetadata(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"VE6WZ-#", "VE6WZ"},
		{"VE6WZ-1-#", "VE6WZ"},
		{"VE6WZ-1", "VE6WZ"},
		{"VE6WZ-TEST", "VE6WZ"},
		{"VE6WZ/P", "VE6WZ/P"},
		{"VE6WZ-1/P", "VE6WZ-1/P"},
		{"K1ABC/VE3-#", "K1ABC/VE3"},
		{"", ""},
	}

	for _, tc := range cases {
		got := normalizeCallForMetadata(tc.input)
		if got != tc.want {
			t.Fatalf("normalizeCallForMetadata(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// Purpose: Helper to allocate a bool pointer.
// Key aspects: Avoids inline address-of literals.
// Upstream: grid DB check tests in this file.
// Downstream: None (returns pointer).
func boolPtr(v bool) *bool {
	b := v
	return &b
}
