package main

import (
	"bytes"
	"context"
	"io"
	"log"
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

// Purpose: Ensure confidence bucketing uses '?' only for single-reporter cases.
// Key aspects: Multi-reporter inputs always map to P/V by percentage.
func TestFormatConfidenceSingleReporterOnlyUnknown(t *testing.T) {
	tests := []struct {
		name           string
		percent        int
		totalReporters int
		want           string
	}{
		{name: "single reporter unknown", percent: 100, totalReporters: 1, want: "?"},
		{name: "zero percent still probable with multiple reporters", percent: 0, totalReporters: 2, want: "P"},
		{name: "mid percent probable", percent: 45, totalReporters: 3, want: "P"},
		{name: "majority very likely", percent: 51, totalReporters: 3, want: "V"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if got := formatConfidence(tc.percent, tc.totalReporters); got != tc.want {
				t.Fatalf("formatConfidence(%d, %d) = %q, want %q", tc.percent, tc.totalReporters, got, tc.want)
			}
		})
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

func TestForwardSpotsDropsNilAndStale(t *testing.T) {
	spotChan := make(chan *spot.Spot, 3)
	ingest := make(chan *spot.Spot, 3)
	policy := config.SpotPolicy{MaxAgeSeconds: 1}

	stale := spot.NewSpot("K1ABC", "W1XYZ", 7000, "CW")
	stale.Time = time.Now().Add(-2 * time.Second)
	fresh := spot.NewSpot("K1DEF", "W1XYZ", 7000, "CW")
	fresh.Time = time.Now().UTC()

	spotChan <- nil
	spotChan <- stale
	spotChan <- fresh
	close(spotChan)

	forwardSpots(spotChan, ingest, "TEST", policy, nil)
	close(ingest)

	var got []*spot.Spot
	for s := range ingest {
		got = append(got, s)
	}
	if len(got) != 1 {
		t.Fatalf("expected only one forwarded spot, got %d", len(got))
	}
	if got[0] == nil || got[0].DXCall != "K1DEF" {
		t.Fatalf("expected fresh spot to be forwarded, got %+v", got[0])
	}
}

func TestForwardSpotsAppliesHumanTransform(t *testing.T) {
	spotChan := make(chan *spot.Spot, 1)
	ingest := make(chan *spot.Spot, 1)
	policy := config.SpotPolicy{}

	in := spot.NewSpot("K1ABC", "W1XYZ", 7000, "")
	in.Mode = ""
	in.SourceNode = ""
	spotChan <- in
	close(spotChan)

	forwardSpots(spotChan, ingest, "HUMAN-TELNET", policy, func(sp *spot.Spot) {
		sp.IsHuman = true
		sp.SourceType = spot.SourceUpstream
		if strings.TrimSpace(sp.SourceNode) == "" {
			sp.SourceNode = "HUMAN-TELNET"
		}
		if strings.TrimSpace(sp.Mode) == "" {
			sp.Mode = "RTTY"
			sp.EnsureNormalized()
		}
	})
	close(ingest)

	out := <-ingest
	if out == nil {
		t.Fatalf("expected transformed spot")
	}
	if !out.IsHuman {
		t.Fatalf("expected IsHuman=true")
	}
	if out.SourceType != spot.SourceUpstream {
		t.Fatalf("expected SourceType=upstream, got %q", out.SourceType)
	}
	if out.SourceNode != "HUMAN-TELNET" {
		t.Fatalf("expected default SourceNode, got %q", out.SourceNode)
	}
	if strings.TrimSpace(out.Mode) == "" {
		t.Fatalf("expected Mode to be defaulted")
	}
}

func TestForwardSpotsRateLimitsDropLogs(t *testing.T) {
	oldInterval := ingestForwardDropLogInterval
	ingestForwardDropLogInterval = time.Hour
	defer func() { ingestForwardDropLogInterval = oldInterval }()

	oldWriter := log.Writer()
	oldFlags := log.Flags()
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
	}()

	spotChan := make(chan *spot.Spot, 2)
	ingest := make(chan *spot.Spot) // unbuffered, no receiver: always drop
	spotChan <- spot.NewSpot("K1ABC", "W1XYZ", 7000, "CW")
	spotChan <- spot.NewSpot("K1DEF", "W1XYZ", 7001, "CW")
	close(spotChan)

	forwardSpots(spotChan, ingest, "TEST", config.SpotPolicy{}, nil)

	logs := buf.String()
	if strings.Count(logs, "Ingest input full, dropping spot") != 1 {
		t.Fatalf("expected exactly one throttled drop log, got logs:\n%s", logs)
	}
	if !strings.Contains(logs, "TEST: Spot processing stopped") {
		t.Fatalf("expected stop log line, got logs:\n%s", logs)
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

	if !applyKnownCallFloor(s, &knownPtr, nil, config.CallCorrectionConfig{}) {
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

	if applyKnownCallFloor(s, &knownPtr, nil, config.CallCorrectionConfig{}) {
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

	if applyKnownCallFloor(s, &knownPtr, nil, config.CallCorrectionConfig{}) {
		t.Fatalf("did not expect known-call floor to apply to FT8")
	}
	if s.Confidence != "?" {
		t.Fatalf("expected confidence ?, got %q", s.Confidence)
	}
}

func TestApplyKnownCallFloorPromotesRecentOnBandDX(t *testing.T) {
	store := spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})
	now := time.Now().UTC()
	s := spot.NewSpot("K1REC", "W2TT", 7010.0, "CW")
	s.Confidence = "?"
	store.Record("K1REC", s.BandNorm, "CW", "N0AAA", now.Add(-10*time.Minute))
	store.Record("K1REC", s.BandNorm, "CW", "N0BBB", now.Add(-5*time.Minute))

	if !applyKnownCallFloor(s, nil, store, config.CallCorrectionConfig{
		RecentBandBonusEnabled:            true,
		RecentBandRecordMinUniqueSpotters: 2,
	}) {
		t.Fatalf("expected recent-on-band floor to mark confidence")
	}
	if s.Confidence != "S" {
		t.Fatalf("expected confidence S, got %q", s.Confidence)
	}
}

func TestApplyKnownCallFloorRecentOnBandHonorsModeAndFlag(t *testing.T) {
	store := spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})
	now := time.Now().UTC()
	s := spot.NewSpot("K1REC", "W2TT", 7010.0, "CW")
	s.Confidence = "?"
	store.Record("K1REC", s.BandNorm, "RTTY", "N0AAA", now.Add(-10*time.Minute))
	store.Record("K1REC", s.BandNorm, "RTTY", "N0BBB", now.Add(-5*time.Minute))

	if applyKnownCallFloor(s, nil, store, config.CallCorrectionConfig{
		RecentBandBonusEnabled:            true,
		RecentBandRecordMinUniqueSpotters: 2,
	}) {
		t.Fatalf("did not expect recent-on-band floor from mismatched mode")
	}
	if s.Confidence != "?" {
		t.Fatalf("expected confidence ?, got %q", s.Confidence)
	}

	store.Record("K1REC", s.BandNorm, "CW", "N0AAA", now.Add(-10*time.Minute))
	store.Record("K1REC", s.BandNorm, "CW", "N0BBB", now.Add(-5*time.Minute))
	if applyKnownCallFloor(s, nil, store, config.CallCorrectionConfig{
		RecentBandBonusEnabled:            false,
		RecentBandRecordMinUniqueSpotters: 2,
	}) {
		t.Fatalf("did not expect recent-on-band floor when feature is disabled")
	}
	if s.Confidence != "?" {
		t.Fatalf("expected confidence ?, got %q", s.Confidence)
	}
}

func TestShouldDelayTelnetByStabilizerDelaysUnknownWithoutRecentSupport(t *testing.T) {
	store := spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})
	s := spot.NewSpot("K1RISK", "W2TT", 7010.0, "CW")
	s.Confidence = "?"

	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:                 true,
		RecentBandRecordMinUniqueSpotters: 2,
	}
	if !shouldDelayTelnetByStabilizer(s, store, cfg, time.Now().UTC()) {
		t.Fatalf("expected unknown, not-recent call to be delayed")
	}
}

func TestShouldDelayTelnetByStabilizerSkipsPConfidence(t *testing.T) {
	store := spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})
	s := spot.NewSpot("K1PASS", "W2TT", 7010.0, "CW")
	s.Confidence = "P"

	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:                 true,
		RecentBandRecordMinUniqueSpotters: 2,
	}
	if shouldDelayTelnetByStabilizer(s, store, cfg, time.Now().UTC()) {
		t.Fatalf("did not expect P-confidence call to be delayed")
	}
}

func TestShouldDelayTelnetByStabilizerSkipsRecentSupport(t *testing.T) {
	store := spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})
	now := time.Now().UTC()
	s := spot.NewSpot("K1REC", "W2TT", 7010.0, "CW")
	s.Confidence = "?"
	store.Record("K1REC", s.BandNorm, "CW", "N0AAA", now.Add(-10*time.Minute))
	store.Record("K1REC", s.BandNorm, "CW", "N0BBB", now.Add(-5*time.Minute))

	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:                 true,
		RecentBandRecordMinUniqueSpotters: 2,
	}
	if shouldDelayTelnetByStabilizer(s, store, cfg, now) {
		t.Fatalf("did not expect recent-on-band call to be delayed")
	}
}

func TestShouldDelayTelnetByStabilizerUsesSlashFamilyRecentSupport(t *testing.T) {
	store := spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})
	now := time.Now().UTC()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:                 true,
		RecentBandRecordMinUniqueSpotters: 2,
	}

	// Record slash-explicit observations through the normal admission path so
	// family keys are inserted consistently.
	recordRecentBandObservation(spot.NewSpot("W1AW/5", "N0AAA", 7010.0, "CW"), store, cfg)
	recordRecentBandObservation(spot.NewSpot("W1AW/5", "N0BBB", 7010.0, "CW"), store, cfg)

	bare := spot.NewSpot("W1AW", "W2TT", 7010.0, "CW")
	bare.Confidence = "?"
	if shouldDelayTelnetByStabilizer(bare, store, cfg, now) {
		t.Fatalf("did not expect bare call to be delayed when slash family is recent")
	}
}

func TestShouldRetryTelnetByStabilizerEligibility(t *testing.T) {
	base := spot.NewSpot("K1RISK", "W2TT", 7010.0, "CW")
	base.Confidence = "?"
	if !shouldRetryTelnetByStabilizer(base, true, 1, 2) {
		t.Fatalf("expected retry when risky, unknown confidence, and checks remain")
	}
	if shouldRetryTelnetByStabilizer(base, true, 1, 1) {
		t.Fatalf("did not expect retry when max_checks=1 (legacy single-check behavior)")
	}
	if shouldRetryTelnetByStabilizer(base, true, 2, 2) {
		t.Fatalf("did not expect retry once max checks are exhausted")
	}
	if shouldRetryTelnetByStabilizer(base, false, 1, 2) {
		t.Fatalf("did not expect retry when spot is no longer risky")
	}
}

func TestShouldRetryTelnetByStabilizerSkipsPVCConfidence(t *testing.T) {
	for _, confidence := range []string{"P", "V", "C"} {
		s := spot.NewSpot("K1RISK", "W2TT", 7010.0, "CW")
		s.Confidence = confidence
		if shouldRetryTelnetByStabilizer(s, true, 1, 3) {
			t.Fatalf("did not expect retry for confidence %s", confidence)
		}
	}
}

func TestApplyKnownCallFloorPromotesViaSlashFamilyRecentSupport(t *testing.T) {
	store := spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})
	cfg := config.CallCorrectionConfig{
		RecentBandBonusEnabled:            true,
		RecentBandRecordMinUniqueSpotters: 2,
	}
	recordRecentBandObservation(spot.NewSpot("W1AW/5", "N0AAA", 7010.0, "CW"), store, cfg)
	recordRecentBandObservation(spot.NewSpot("W1AW/5", "N0BBB", 7010.0, "CW"), store, cfg)

	s := spot.NewSpot("W1AW", "W2TT", 7010.0, "CW")
	s.Confidence = "?"
	if !applyKnownCallFloor(s, nil, store, cfg) {
		t.Fatalf("expected recent-on-band floor to use slash family support")
	}
	if s.Confidence != "S" {
		t.Fatalf("expected confidence S, got %q", s.Confidence)
	}
}

func TestRecentBandAdmissionDelayedReleaseWaitsForOutcome(t *testing.T) {
	store := newRecentBandStoreForStabilizerAdmissionTests()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled: true,
	}
	now := time.Now().UTC()
	s := spot.NewSpot("K1RISK", "W2TT", 7010.0, "CW")

	if shouldRecordRecentBandInMainLoop(true, true) {
		recordRecentBandObservation(s, store, cfg)
	}
	if count := recentBandSupportCountForSpot(store, s, now); count != 0 {
		t.Fatalf("expected no recent support before delayed release, got %d", count)
	}

	if !shouldRecordRecentBandAfterStabilizerDelay(stabilizerTimeoutRelease, true) {
		t.Fatalf("expected release timeout action to admit delayed spot")
	}
	recordRecentBandObservation(s, store, cfg)
	if count := recentBandSupportCountForSpot(store, s, now); count != 1 {
		t.Fatalf("expected one recent support after delayed release, got %d", count)
	}
}

func TestRecentBandAdmissionDelayedSuppressSkipsRecord(t *testing.T) {
	store := newRecentBandStoreForStabilizerAdmissionTests()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled: true,
	}
	now := time.Now().UTC()
	s := spot.NewSpot("K1RISK", "W2TT", 7010.0, "CW")

	if shouldRecordRecentBandInMainLoop(true, true) {
		recordRecentBandObservation(s, store, cfg)
	}
	if count := recentBandSupportCountForSpot(store, s, now); count != 0 {
		t.Fatalf("expected no recent support before timeout decision, got %d", count)
	}

	if shouldRecordRecentBandAfterStabilizerDelay(stabilizerTimeoutSuppress, true) {
		t.Fatalf("expected suppress timeout action to skip delayed admission")
	}
	if count := recentBandSupportCountForSpot(store, s, now); count != 0 {
		t.Fatalf("expected no recent support after suppressed timeout, got %d", count)
	}
}

func TestRecentBandAdmissionOverflowReleaseRecordsInMainPath(t *testing.T) {
	store := newRecentBandStoreForStabilizerAdmissionTests()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled: true,
	}
	now := time.Now().UTC()
	s := spot.NewSpot("K1OVR", "W2TT", 7010.0, "CW")

	if !shouldRecordRecentBandInMainLoop(true, false) {
		t.Fatalf("expected overflow fail-open path to record in main loop")
	}
	recordRecentBandObservation(s, store, cfg)
	if count := recentBandSupportCountForSpot(store, s, now); count != 1 {
		t.Fatalf("expected one recent support for overflow release, got %d", count)
	}
}

func TestRecentBandAdmissionNonStabilizerRecordsInMainPath(t *testing.T) {
	store := newRecentBandStoreForStabilizerAdmissionTests()
	cfg := config.CallCorrectionConfig{
		RecentBandBonusEnabled: true,
	}
	now := time.Now().UTC()
	s := spot.NewSpot("K1MAIN", "W2TT", 7010.0, "CW")

	if !shouldRecordRecentBandInMainLoop(false, false) {
		t.Fatalf("expected non-stabilizer path to record in main loop")
	}
	recordRecentBandObservation(s, store, cfg)
	if count := recentBandSupportCountForSpot(store, s, now); count != 1 {
		t.Fatalf("expected one recent support on non-stabilizer path, got %d", count)
	}
}

func TestBuildResolverEvidenceSnapshotCapturesPreMutationCall(t *testing.T) {
	cfg := config.CallCorrectionConfig{
		Enabled:                   true,
		MinConsensusReports:       3,
		MinAdvantage:              2,
		MinConfidencePercent:      70,
		RecencySeconds:            45,
		FrequencyToleranceHz:      500,
		VoiceFrequencyToleranceHz: 2000,
	}
	s := spot.NewSpot("DL6LD", "K1AAA", 7010.0, "CW")
	s.EnsureNormalized()

	now := time.Now().UTC()
	ev, ok := buildResolverEvidenceSnapshot(s, cfg, nil, now)
	if !ok {
		t.Fatalf("expected resolver evidence snapshot")
	}
	if ev.DXCall != "DL6LD" {
		t.Fatalf("expected pre-mutation call DL6LD, got %q", ev.DXCall)
	}

	s.DXCall = "DL6LN"
	s.DXCallNorm = ""
	s.EnsureNormalized()
	if ev.DXCall != "DL6LD" {
		t.Fatalf("snapshot should remain immutable after mutation, got %q", ev.DXCall)
	}
}

func newRecentBandStoreForStabilizerAdmissionTests() *spot.RecentBandStore {
	return spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})
}

func recentBandSupportCountForSpot(store *spot.RecentBandStore, s *spot.Spot, now time.Time) int {
	if store == nil || s == nil {
		return 0
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	mode := s.ModeNorm
	if mode == "" {
		mode = s.Mode
	}
	band := s.BandNorm
	if band == "" || band == "???" {
		band = spot.FreqToBand(s.Frequency)
	}
	return store.RecentSupportCount(call, band, mode, now)
}

func TestBuildOverviewLinesIncludesRecentOnBandCalls(t *testing.T) {
	now := time.Now().UTC()
	recentBandStore := spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})
	recentBandStore.Record("K1ABC", "40m", "CW", "W1AAA", now.Add(-10*time.Minute))
	recentBandStore.Record("K1ABC", "20m", "CW", "W2BBB", now.Add(-5*time.Minute))
	recentBandStore.Record("N0XYZ", "40m", "CW", "W3CCC", now.Add(-6*time.Minute))

	lines := buildOverviewLines(
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		recentBandStore,
		nil,
		"",
		nil,
		nil,
		nil,
		nil,
		nil,
		"N2WQ-2",
		false,
		false,
		false,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		"Path: n/a",
		"",
		nil,
		"n/a",
	)

	found := false
	for _, line := range lines {
		if strings.Contains(line, "[yellow]Recent on band[-]: 2") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected recent-on-band line with count 2 in overview lines, got %v", lines)
	}
	perBandFound := false
	for _, line := range lines {
		if strings.Contains(line, "[yellow]40m[-]: 2") && strings.Contains(line, "[yellow]20m[-]: 1") {
			perBandFound = true
			break
		}
	}
	if !perBandFound {
		t.Fatalf("expected per-band recent-on-band counts in overview lines, got %v", lines)
	}
}

func TestBuildCorrectionSettingsMapsConfigFields(t *testing.T) {
	cfg := config.CallCorrectionConfig{
		FamilyPolicy: config.CallCorrectionFamilyPolicyConfig{
			SlashPrecedenceMinReports: 2,
			Truncation: config.CallCorrectionTruncationFamilyConfig{
				Enabled:          true,
				MaxLengthDelta:   1,
				MinShorterLength: 3,
				AllowPrefixMatch: true,
				AllowSuffixMatch: true,
				RelaxAdvantage: config.CallCorrectionTruncationAdvantageConfig{
					Enabled:                   true,
					MinAdvantage:              0,
					RequireCandidateValidated: true,
					RequireSubjectUnvalidated: true,
				},
				LengthBonus: config.CallCorrectionTruncationLengthBonusConfig{
					Enabled:                   true,
					Max:                       2,
					RequireCandidateValidated: true,
					RequireSubjectUnvalidated: true,
				},
				Delta2Rails: config.CallCorrectionTruncationDelta2RailsConfig{
					Enabled:                   true,
					ExtraConfidencePercent:    12,
					RequireCandidateValidated: true,
					RequireSubjectUnvalidated: false,
				},
			},
		},
		MinAdvantage:                      2,
		MinConfidencePercent:              65,
		MaxEditDistance:                   3,
		Strategy:                          "majority",
		MinSNRCW:                          4,
		MinSNRRTTY:                        3,
		MinSNRVoice:                       1,
		DistanceModelCW:                   "morse",
		DistanceModelRTTY:                 "baudot",
		Distance3ExtraReports:             1,
		Distance3ExtraAdvantage:           1,
		Distance3ExtraConfidence:          5,
		DebugLog:                          true,
		FreqGuardMinSeparationKHz:         0.2,
		FreqGuardRunnerUpRatio:            0.6,
		QualityGoodThreshold:              3,
		QualityNewCallIncrement:           2,
		QualityBustedDecrement:            2,
		CandidateEvalTopK:                 3,
		MinSpotterReliability:             0.4,
		ConfusionModelWeight:              0.25,
		RecentBandBonusEnabled:            true,
		RecentBandWindowSeconds:           43200,
		RecentBandBonusMax:                1,
		RecentBandRecordMinUniqueSpotters: 2,
		PriorBonusEnabled:                 true,
		PriorBonusMax:                     1,
		PriorBonusDistanceMax:             1,
		PriorBonusRequiresSCP:             true,
		PriorBonusApplyTo:                 "min_reports",
	}
	window := 75 * time.Second
	reliability := spot.SpotterReliability{"W2BBB": 0.7}
	reliabilityCW := spot.SpotterReliability{"W2BBB": 0.8}
	reliabilityRTTY := spot.SpotterReliability{"W2BBB": 0.9}
	recentBandStore := spot.NewRecentBandStore(12 * time.Hour)
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
		recentBandStore,
		knownCallset,
		nil,
	)

	if got.MinConsensusReports != 5 {
		t.Fatalf("expected min reports 5, got %d", got.MinConsensusReports)
	}
	if got.SlashPrecedenceMinReports != cfg.FamilyPolicy.SlashPrecedenceMinReports {
		t.Fatalf("expected slash min reports %d, got %d", cfg.FamilyPolicy.SlashPrecedenceMinReports, got.SlashPrecedenceMinReports)
	}
	if !got.FamilyPolicy.Configured ||
		!got.FamilyPolicy.TruncationEnabled ||
		got.FamilyPolicy.TruncationMaxLengthDelta != cfg.FamilyPolicy.Truncation.MaxLengthDelta ||
		got.FamilyPolicy.TruncationMinShorterLength != cfg.FamilyPolicy.Truncation.MinShorterLength ||
		got.FamilyPolicy.TruncationAllowPrefix != cfg.FamilyPolicy.Truncation.AllowPrefixMatch ||
		got.FamilyPolicy.TruncationAllowSuffix != cfg.FamilyPolicy.Truncation.AllowSuffixMatch {
		t.Fatalf("expected family policy mapping to be preserved")
	}
	if !got.TruncationAdvantagePolicy.Configured ||
		!got.TruncationAdvantagePolicy.Enabled ||
		got.TruncationAdvantagePolicy.MinAdvantage != cfg.FamilyPolicy.Truncation.RelaxAdvantage.MinAdvantage ||
		got.TruncationAdvantagePolicy.RequireCandidateValidated != cfg.FamilyPolicy.Truncation.RelaxAdvantage.RequireCandidateValidated ||
		got.TruncationAdvantagePolicy.RequireSubjectUnvalidated != cfg.FamilyPolicy.Truncation.RelaxAdvantage.RequireSubjectUnvalidated {
		t.Fatalf("expected truncation advantage policy mapping to be preserved")
	}
	if got.TruncationLengthBonusEnabled != cfg.FamilyPolicy.Truncation.LengthBonus.Enabled ||
		got.TruncationLengthBonusMax != cfg.FamilyPolicy.Truncation.LengthBonus.Max ||
		got.TruncationLengthBonusRequireCandidateValidated != cfg.FamilyPolicy.Truncation.LengthBonus.RequireCandidateValidated ||
		got.TruncationLengthBonusRequireSubjectUnvalidated != cfg.FamilyPolicy.Truncation.LengthBonus.RequireSubjectUnvalidated {
		t.Fatalf("expected truncation length bonus mapping to be preserved")
	}
	if got.TruncationDelta2RailsEnabled != cfg.FamilyPolicy.Truncation.Delta2Rails.Enabled ||
		got.TruncationDelta2ExtraConfidence != cfg.FamilyPolicy.Truncation.Delta2Rails.ExtraConfidencePercent ||
		got.TruncationDelta2RequireCandidateValidated != cfg.FamilyPolicy.Truncation.Delta2Rails.RequireCandidateValidated ||
		got.TruncationDelta2RequireSubjectUnvalidated != cfg.FamilyPolicy.Truncation.Delta2Rails.RequireSubjectUnvalidated {
		t.Fatalf("expected truncation delta-2 rail mapping to be preserved")
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
		got.RecentBandBonusEnabled != cfg.RecentBandBonusEnabled ||
		got.RecentBandWindow != 12*time.Hour ||
		got.RecentBandBonusMax != cfg.RecentBandBonusMax ||
		got.RecentBandRecordMinUniqueSpotters != cfg.RecentBandRecordMinUniqueSpotters ||
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
	if got.RecentBandStore != recentBandStore {
		t.Fatalf("expected recent-band store pointer to be preserved")
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
