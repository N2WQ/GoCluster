package cluster

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/cty"
	"dxcluster/internal/voacap"
	"dxcluster/pathreliability"
	"dxcluster/spot"
	"dxcluster/stats"
	"dxcluster/telnet"
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
		t.Run(tc.name, func(t *testing.T) {
			if got := formatConfidence(tc.percent, tc.totalReporters); got != tc.want {
				t.Fatalf("formatConfidence(%d, %d) = %q, want %q", tc.percent, tc.totalReporters, got, tc.want)
			}
		})
	}
}

func TestMakeUnlicensedReporterEmitsPlainTextToSurface(t *testing.T) {
	surface := &captureSurface{}
	reporter := makeUnlicensedReporter(surface, nil, nil, nil)
	reporter("rbn", "dx", "k1abc", "n0call", "k1abc", "cw", 7029.5)

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

func TestPeerPublishCommentAddsModeWhenCommentEmpty(t *testing.T) {
	src := spot.NewSpot("K1ABC", "W1XYZ", 7074.0, "")
	src.Mode = "FT8"
	src.Comment = ""

	comment := peerPublishComment(src)
	if comment != "FT8" {
		t.Fatalf("expected comment to carry inferred mode, got %q", comment)
	}
	if src.Comment != "" {
		t.Fatalf("expected original comment to remain empty, got %q", src.Comment)
	}
}

func TestPeerPublishCommentPassthroughWhenCommentPresent(t *testing.T) {
	src := spot.NewSpot("K1ABC", "W1XYZ", 7074.0, "")
	src.Mode = "FT8"
	src.Comment = "cq test"

	comment := peerPublishComment(src)
	if comment != "cq test" {
		t.Fatalf("expected passthrough comment, got %q", comment)
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

func openTestCustomSCPStore(t *testing.T) *spot.CustomSCPStore {
	t.Helper()
	store, err := spot.OpenCustomSCPStore(spot.CustomSCPOptions{
		Path:           filepath.Join(t.TempDir(), "custom_scp"),
		CoreMinScore:   1,
		CoreMinH3Cells: 1,
	})
	if err != nil {
		t.Fatalf("open custom_scp: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func recordCustomSCPStatic(store *spot.CustomSCPStore, call string, freq float64, mode string) {
	s := spot.NewSpot(call, "N0AAA", freq, mode)
	s.Confidence = "V"
	s.Time = time.Now().UTC()
	store.RecordSpot(s)
}

func TestApplySupportFloorPromotesCustomSCPStaticMembership(t *testing.T) {
	store := openTestCustomSCPStore(t)
	recordCustomSCPStatic(store, "K1KI", 1831.3, "CW")

	s := spot.NewSpot("K1KI", "W2TT", 1831.3, "CW")
	s.Confidence = "?"

	if !applySupportFloor(s, nil, store, nil, config.CallCorrectionConfig{
		CustomSCP: config.CallCorrectionCustomSCPConfig{Enabled: true},
	}) {
		t.Fatalf("expected custom_scp static floor to mark confidence")
	}
	if s.Confidence != "S" {
		t.Fatalf("expected confidence S, got %q", s.Confidence)
	}
}

func TestApplySupportFloorKeepsNonUnknownConfidence(t *testing.T) {
	store := openTestCustomSCPStore(t)
	recordCustomSCPStatic(store, "K1KI", 1831.3, "CW")

	s := spot.NewSpot("K1KI", "W2TT", 1831.3, "CW")
	s.Confidence = "P"

	if applySupportFloor(s, nil, store, nil, config.CallCorrectionConfig{
		CustomSCP: config.CallCorrectionCustomSCPConfig{Enabled: true},
	}) {
		t.Fatalf("did not expect floor to override P")
	}
	if s.Confidence != "P" {
		t.Fatalf("expected confidence P, got %q", s.Confidence)
	}
}

func TestApplySupportFloorPromotesSupportedFTModeViaCustomSCPStatic(t *testing.T) {
	store := openTestCustomSCPStore(t)
	recordCustomSCPStatic(store, "K1KI", 14074.0, "FT8")

	s := spot.NewSpot("K1KI", "W2TT", 14074.0, "FT8")
	s.Confidence = "?"

	if !applySupportFloor(s, nil, store, nil, config.CallCorrectionConfig{
		CustomSCP: config.CallCorrectionCustomSCPConfig{Enabled: true},
	}) {
		t.Fatalf("expected static custom_scp floor to apply to FT8")
	}
	if s.Confidence != "S" {
		t.Fatalf("expected confidence S, got %q", s.Confidence)
	}
}

func TestApplySupportFloorPromotesRecentOnBandDX(t *testing.T) {
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

	if !applySupportFloor(s, store, nil, nil, config.CallCorrectionConfig{
		RecentBandBonusEnabled:            true,
		RecentBandRecordMinUniqueSpotters: 2,
	}) {
		t.Fatalf("expected recent-on-band floor to mark confidence")
	}
	if s.Confidence != "S" {
		t.Fatalf("expected confidence S, got %q", s.Confidence)
	}
}

func TestApplySupportFloorRecentOnBandHonorsModeAndFlag(t *testing.T) {
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

	if applySupportFloor(s, store, nil, nil, config.CallCorrectionConfig{
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
	if applySupportFloor(s, store, nil, nil, config.CallCorrectionConfig{
		RecentBandBonusEnabled:            false,
		RecentBandRecordMinUniqueSpotters: 2,
	}) {
		t.Fatalf("did not expect recent-on-band floor when feature is disabled")
	}
	if s.Confidence != "?" {
		t.Fatalf("expected confidence ?, got %q", s.Confidence)
	}
}

func TestApplySupportFloorUsesFTRecentOverrideWhenCustomSCPEnabled(t *testing.T) {
	store := spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})
	now := time.Now().UTC()
	s := spot.NewSpot("K1REC", "W2TT", 14074.0, "FT8")
	s.Confidence = "?"
	store.Record("K1REC", s.BandNorm, "FT8", "N0AAA", now.Add(-10*time.Minute))
	store.Record("K1REC", s.BandNorm, "FT8", "N0BBB", now.Add(-5*time.Minute))

	if !applySupportFloor(s, nil, &spot.CustomSCPStore{}, store, config.CallCorrectionConfig{
		RecentBandBonusEnabled: true,
		CustomSCP: config.CallCorrectionCustomSCPConfig{
			Enabled: true,
		},
		RecentBandRecordMinUniqueSpotters: 2,
	}) {
		t.Fatalf("expected FT recent-on-band override to mark confidence")
	}
	if s.Confidence != "S" {
		t.Fatalf("expected confidence S, got %q", s.Confidence)
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
	recordRecentBandObservation(spot.NewSpot("W1AW/5", "N0AAA", 7010.0, "CW"), store, nil, cfg)
	recordRecentBandObservation(spot.NewSpot("W1AW/5", "N0BBB", 7010.0, "CW"), store, nil, cfg)

	bare := spot.NewSpot("W1AW", "W2TT", 7010.0, "CW")
	bare.Confidence = "?"
	if shouldDelayTelnetByStabilizer(bare, store, cfg, now) {
		t.Fatalf("did not expect bare call to be delayed when slash family is recent")
	}
}

func TestShouldRetryTelnetByStabilizerEligibility(t *testing.T) {
	decision := stabilizerDelayDecision{
		ShouldDelay: true,
		Reason:      stabilizerDelayReasonUnknownOrNonRecent,
		MaxChecks:   2,
	}
	if !shouldRetryTelnetByStabilizer(decision, 1) {
		t.Fatalf("expected retry when risky, unknown confidence, and checks remain")
	}
	decision.MaxChecks = 1
	if shouldRetryTelnetByStabilizer(decision, 1) {
		t.Fatalf("did not expect retry when max_checks=1 (legacy single-check behavior)")
	}
	decision.MaxChecks = 2
	if shouldRetryTelnetByStabilizer(decision, 2) {
		t.Fatalf("did not expect retry once max checks are exhausted")
	}
	decision.ShouldDelay = false
	if shouldRetryTelnetByStabilizer(decision, 1) {
		t.Fatalf("did not expect retry when spot is no longer risky")
	}
}

func TestShouldRetryTelnetByStabilizerUsesReasonScopedChecks(t *testing.T) {
	decision := stabilizerDelayDecision{
		ShouldDelay: true,
		Reason:      stabilizerDelayReasonPLowConfidence,
		MaxChecks:   3,
	}
	if !shouldRetryTelnetByStabilizer(decision, 2) {
		t.Fatalf("expected retry while checks remain for low-confidence P policy")
	}
	if shouldRetryTelnetByStabilizer(decision, 3) {
		t.Fatalf("did not expect retry once low-confidence P checks are exhausted")
	}
}

func TestEvaluateTelnetStabilizerDelayUsesAmbiguousResolverPolicy(t *testing.T) {
	store := newRecentBandStoreForStabilizerAdmissionTests()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:            true,
		StabilizerMaxChecks:          5,
		StabilizerAmbiguousMaxChecks: 2,
	}
	s := spot.NewSpot("K1AMB", "W2TT", 7010.0, "CW")
	s.Confidence = "S"
	s.EnsureNormalized()

	snapshot := spot.ResolverSnapshot{
		State:          spot.ResolverStateSplit,
		TotalReporters: 5,
	}
	decision := evaluateTelnetStabilizerDelay(s, store, cfg, time.Now().UTC(), snapshot, true)
	if !decision.ShouldDelay {
		t.Fatalf("expected ambiguous resolver state to trigger delay")
	}
	if decision.Reason != stabilizerDelayReasonAmbiguous {
		t.Fatalf("expected ambiguous reason, got %q", decision.Reason.String())
	}
	if decision.MaxChecks != 2 {
		t.Fatalf("expected ambiguous max checks 2, got %d", decision.MaxChecks)
	}
}

func TestEvaluateTelnetStabilizerDelayUsesPLowConfidencePolicy(t *testing.T) {
	store := newRecentBandStoreForStabilizerAdmissionTests()
	now := time.Now().UTC()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:                 true,
		StabilizerMaxChecks:               5,
		StabilizerPDelayConfidencePercent: 25,
		StabilizerPDelayMaxChecks:         2,
		RecentBandRecordMinUniqueSpotters: 2,
	}
	s := spot.NewSpot("K1PLOW", "W2TT", 7010.0, "CW")
	s.Confidence = "P"
	s.EnsureNormalized()
	store.Record("K1PLOW", s.BandNorm, "CW", "N0AAA", now.Add(-2*time.Minute))
	store.Record("K1PLOW", s.BandNorm, "CW", "N0BBB", now.Add(-1*time.Minute))

	snapshot := spot.ResolverSnapshot{
		State:                     spot.ResolverStateProbable,
		TotalWeightedSupportMilli: 1000,
		CandidateRanks: []spot.ResolverCandidateSupport{
			{Call: "K1PLOW", WeightedSupportMilli: 200},
		},
	}
	decision := evaluateTelnetStabilizerDelay(s, store, cfg, now, snapshot, true)
	if !decision.ShouldDelay {
		t.Fatalf("expected low-confidence P policy to trigger delay")
	}
	if decision.Reason != stabilizerDelayReasonPLowConfidence {
		t.Fatalf("expected P-low-confidence reason, got %q", decision.Reason.String())
	}
	if decision.MaxChecks != 2 {
		t.Fatalf("expected P-low-confidence max checks 2, got %d", decision.MaxChecks)
	}
}

func TestEvaluateTelnetStabilizerDelayPLowConfidenceFailsOpenWithoutSnapshot(t *testing.T) {
	store := newRecentBandStoreForStabilizerAdmissionTests()
	now := time.Now().UTC()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:                 true,
		StabilizerPDelayConfidencePercent: 25,
		StabilizerPDelayMaxChecks:         2,
		RecentBandRecordMinUniqueSpotters: 2,
	}
	s := spot.NewSpot("K1PLOW", "W2TT", 7010.0, "CW")
	s.Confidence = "P"
	s.EnsureNormalized()
	store.Record("K1PLOW", s.BandNorm, "CW", "N0AAA", now.Add(-2*time.Minute))
	store.Record("K1PLOW", s.BandNorm, "CW", "N0BBB", now.Add(-1*time.Minute))

	decision := evaluateTelnetStabilizerDelay(s, store, cfg, now, spot.ResolverSnapshot{}, false)
	if decision.ShouldDelay {
		t.Fatalf("expected fail-open behavior when low-confidence P has no snapshot evidence")
	}
}

func TestEvaluateTelnetStabilizerDelayUsesEditNeighborPolicy(t *testing.T) {
	store := newRecentBandStoreForStabilizerAdmissionTests()
	now := time.Now().UTC()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:                 true,
		StabilizerMaxChecks:               5,
		StabilizerEditNeighborEnabled:     true,
		StabilizerEditNeighborMaxChecks:   3,
		StabilizerEditNeighborMinSpotters: 2,
	}
	s := spot.NewSpot("K1ABC", "W2TT", 7010.0, "CW")
	s.Confidence = "S"
	s.EnsureNormalized()
	store.Record("K1ABC", s.BandNorm, "CW", "N0AAA", now.Add(-2*time.Minute))
	store.Record("K1ABC", s.BandNorm, "CW", "N0AAB", now.Add(-90*time.Second))
	store.Record("K1ABD", s.BandNorm, "CW", "N0AAC", now.Add(-80*time.Second))
	store.Record("K1ABD", s.BandNorm, "CW", "N0AAD", now.Add(-70*time.Second))

	decision := evaluateTelnetStabilizerDelay(s, store, cfg, now, spot.ResolverSnapshot{}, false)
	if !decision.ShouldDelay {
		t.Fatalf("expected edit-neighbor policy to trigger delay")
	}
	if decision.Reason != stabilizerDelayReasonEditNeighbor {
		t.Fatalf("expected edit-neighbor reason, got %q", decision.Reason.String())
	}
	if decision.MaxChecks != 3 {
		t.Fatalf("expected edit-neighbor max checks 3, got %d", decision.MaxChecks)
	}
}

func TestEvaluateTelnetStabilizerDelayVPassesThrough(t *testing.T) {
	store := newRecentBandStoreForStabilizerAdmissionTests()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:            true,
		StabilizerMaxChecks:          5,
		StabilizerAmbiguousMaxChecks: 2,
	}
	s := spot.NewSpot("K1VOK", "W2TT", 7010.0, "CW")
	s.Confidence = "V"
	s.EnsureNormalized()

	snapshot := spot.ResolverSnapshot{
		State:          spot.ResolverStateSplit,
		TotalReporters: 5,
	}
	decision := evaluateTelnetStabilizerDelay(s, store, cfg, time.Now().UTC(), snapshot, true)
	if decision.ShouldDelay {
		t.Fatalf("did not expect V glyph to be delayed")
	}
	if decision.Reason != stabilizerDelayReasonNone {
		t.Fatalf("expected none reason, got %q", decision.Reason.String())
	}
}

func TestEvaluateTelnetStabilizerDelayLocalSelfSpotPassesThrough(t *testing.T) {
	store := newRecentBandStoreForStabilizerAdmissionTests()
	cfg := config.CallCorrectionConfig{
		StabilizerEnabled:            true,
		StabilizerMaxChecks:          5,
		StabilizerAmbiguousMaxChecks: 2,
	}
	s := spot.NewSpot("K1SELF", "K1SELF", 28400.0, "SSB")
	s.Confidence = "S"
	s.EnsureNormalized()

	snapshot := spot.ResolverSnapshot{
		State:          spot.ResolverStateSplit,
		TotalReporters: 5,
	}
	decision := evaluateTelnetStabilizerDelay(s, store, cfg, time.Now().UTC(), snapshot, true)
	if decision.ShouldDelay {
		t.Fatalf("did not expect local self spot to be delayed")
	}
	if decision.Reason != stabilizerDelayReasonNone {
		t.Fatalf("expected none reason, got %q", decision.Reason.String())
	}
}

func TestLocalSelfSpotResolverStageForcesVAndAdmitsCustomSCP(t *testing.T) {
	store, err := spot.OpenCustomSCPStore(spot.CustomSCPOptions{
		Path:           filepath.Join(t.TempDir(), "scp"),
		CoreMinScore:   1,
		CoreMinH3Cells: 1,
	})
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	cfg := config.CallCorrectionConfig{
		Enabled: true,
		CustomSCP: config.CallCorrectionCustomSCPConfig{
			Enabled: true,
		},
		TemporalDecoder: config.CallCorrectionTemporalDecoderConfig{
			Enabled:             true,
			Scope:               "all_correction_candidates",
			LagSeconds:          2,
			MaxWaitSeconds:      6,
			BeamSize:            8,
			MaxObsCandidates:    8,
			StayBonus:           120,
			SwitchPenalty:       160,
			FamilySwitchPenalty: 60,
			Edit1SwitchPenalty:  90,
			MinScore:            0,
			MinMarginScore:      0,
			OverflowAction:      "fallback_resolver",
			MaxPending:          100,
			MaxActiveKeys:       16,
			MaxEventsPerKey:     32,
		},
	}
	pipeline := &outputPipeline{
		telnet:        &telnet.Server{},
		correctionCfg: cfg,
		ctyLookup: func() *cty.CTYDatabase {
			return nil
		},
		temporal: newRuntimeTemporalController(cfg),
	}

	s := spot.NewSpot("K1SELF", "K1SELF", 28400.0, "SSB")
	ctx, ok := pipeline.prepareSpotContext(s)
	if !ok {
		t.Fatalf("expected self spot context")
	}
	if !pipeline.applyResolverStage(&ctx, nil) {
		t.Fatalf("expected self spot to pass resolver stage")
	}
	if s.Confidence != "V" {
		t.Fatalf("expected V confidence, got %q", s.Confidence)
	}
	if _, pending := pipeline.temporal.NextDue(); pending {
		t.Fatalf("did not expect self spot to enter temporal hold")
	}

	recordRecentBandObservation(s, nil, store, cfg)
	if !store.StaticContains("K1SELF") {
		t.Fatalf("expected V self spot to enter custom_scp static membership")
	}
}

func TestApplySupportFloorPromotesViaSlashFamilyRecentSupport(t *testing.T) {
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
	recordRecentBandObservation(spot.NewSpot("W1AW/5", "N0AAA", 7010.0, "CW"), store, nil, cfg)
	recordRecentBandObservation(spot.NewSpot("W1AW/5", "N0BBB", 7010.0, "CW"), store, nil, cfg)

	s := spot.NewSpot("W1AW", "W2TT", 7010.0, "CW")
	s.Confidence = "?"
	if !applySupportFloor(s, store, nil, nil, cfg) {
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
		recordRecentBandObservation(s, store, nil, cfg)
	}
	if count := recentBandSupportCountForSpot(store, s, now); count != 0 {
		t.Fatalf("expected no recent support before delayed release, got %d", count)
	}

	if !shouldRecordRecentBandAfterStabilizerDelay(stabilizerTimeoutRelease, true) {
		t.Fatalf("expected release timeout action to admit delayed spot")
	}
	recordRecentBandObservation(s, store, nil, cfg)
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
		recordRecentBandObservation(s, store, nil, cfg)
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
	recordRecentBandObservation(s, store, nil, cfg)
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
	recordRecentBandObservation(s, store, nil, cfg)
	if count := recentBandSupportCountForSpot(store, s, now); count != 1 {
		t.Fatalf("expected one recent support on non-stabilizer path, got %d", count)
	}
}

func TestRecordWhoSpotsMeObservationRecordsAcceptedSpot(t *testing.T) {
	store := spot.NewWhoSpotsMeStoreWithOptions(spot.WhoSpotsMeOptions{
		Window:               10 * time.Second,
		Shards:               1,
		MaxEntries:           16,
		MaxCountriesPerEntry: 8,
		CleanupInterval:      time.Hour,
	})
	now := time.Unix(500, 0).UTC()
	s := spot.NewSpot("K1ABC", "DL1AAA", 14020.0, "CW")
	s.DEMetadata.ADIF = 230
	s.DEMetadata.Continent = "EU"
	s.EnsureNormalized()

	recordWhoSpotsMeObservation(s, store, now)
	got := store.CountryCountsByContinent("K1ABC", "20m", now)
	want := map[string][]spot.WhoSpotsMeCountryCount{
		"EU": {{ADIF: 230, Count: 1}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("who-spots-me counts mismatch: got %#v want %#v", got, want)
	}
}

func TestRecordWhoSpotsMeObservationSkipsRejectedShapes(t *testing.T) {
	store := spot.NewWhoSpotsMeStoreWithOptions(spot.WhoSpotsMeOptions{
		Window:               10 * time.Second,
		Shards:               1,
		MaxEntries:           16,
		MaxCountriesPerEntry: 8,
		CleanupInterval:      time.Hour,
	})
	now := time.Unix(600, 0).UTC()

	testSpotter := spot.NewSpot("K1ABC", "DL1AAA", 14020.0, "CW")
	testSpotter.DEMetadata.ADIF = 230
	testSpotter.DEMetadata.Continent = "EU"
	testSpotter.IsTestSpotter = true
	testSpotter.EnsureNormalized()
	recordWhoSpotsMeObservation(testSpotter, store, now)

	beacon := spot.NewSpot("K1ABC/B", "DL1AAA", 14020.0, "CW")
	beacon.DEMetadata.ADIF = 230
	beacon.DEMetadata.Continent = "EU"
	beacon.IsBeacon = true
	beacon.EnsureNormalized()
	recordWhoSpotsMeObservation(beacon, store, now)

	if got := store.CountryCountsByContinent("K1ABC", "20m", now); got != nil {
		t.Fatalf("expected rejected shapes to be skipped, got %#v", got)
	}
}

func TestStabilizerImmediateCountEligible(t *testing.T) {
	if stabilizerImmediateCountEligible(nil) {
		t.Fatalf("nil spot should not be stabilizer-immediate eligible")
	}

	ft8 := spot.NewSpot("K1ABC", "N2WQ", 14074.0, "FT8")
	ft8.EnsureNormalized()
	if stabilizerImmediateCountEligible(ft8) {
		t.Fatalf("FT8 should not be stabilizer-immediate eligible")
	}

	cw := spot.NewSpot("K1ABC", "N2WQ", 7010.0, "CW")
	cw.EnsureNormalized()
	if !stabilizerImmediateCountEligible(cw) {
		t.Fatalf("CW should be stabilizer-immediate eligible")
	}

	usbFallback := &spot.Spot{Mode: "USB"}
	if !stabilizerImmediateCountEligible(usbFallback) {
		t.Fatalf("USB mode fallback should be stabilizer-immediate eligible")
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

func TestResolverConfidenceGlyphFromSnapshot(t *testing.T) {
	if got := resolverConfidenceGlyph(spot.ResolverSnapshot{}, false, "K1ABC"); got != "?" {
		t.Fatalf("expected no-snapshot confidence ?, got %q", got)
	}

	splitLikely := spot.ResolverSnapshot{
		State:          spot.ResolverStateSplit,
		WinnerSupport:  2,
		TotalReporters: 4,
	}
	if got := resolverConfidenceGlyph(splitLikely, true, "K1ABC"); got != "P" {
		t.Fatalf("expected split multi-reporter confidence P, got %q", got)
	}

	splitStrong := spot.ResolverSnapshot{
		State:          spot.ResolverStateSplit,
		WinnerSupport:  3,
		TotalReporters: 4,
	}
	if got := resolverConfidenceGlyph(splitStrong, true, "K1ABC"); got != "P" {
		t.Fatalf("expected split multi-reporter confidence P, got %q", got)
	}

	weightedLikely := spot.ResolverSnapshot{
		State:                      spot.ResolverStateProbable,
		Winner:                     "K1ABC",
		WinnerSupport:              1,
		TotalReporters:             2,
		WinnerWeightedSupportMilli: 600,
		TotalWeightedSupportMilli:  1600,
	}
	if got := resolverConfidenceGlyph(weightedLikely, true, "K1ABC"); got != "P" {
		t.Fatalf("expected weighted confidence P, got %q", got)
	}

	weightedStrong := spot.ResolverSnapshot{
		State:                      spot.ResolverStateConfident,
		Winner:                     "K1ABC",
		WinnerSupport:              1,
		TotalReporters:             2,
		WinnerWeightedSupportMilli: 950,
		TotalWeightedSupportMilli:  1550,
	}
	if got := resolverConfidenceGlyph(weightedStrong, true, "K1ABC"); got != "V" {
		t.Fatalf("expected weighted confidence V, got %q", got)
	}

	callSpecific := spot.ResolverSnapshot{
		State:                     spot.ResolverStateConfident,
		Winner:                    "K1ABC",
		TotalReporters:            3,
		TotalWeightedSupportMilli: 1200,
		CandidateRanks: []spot.ResolverCandidateSupport{
			{Call: "K1ABC", Support: 2, WeightedSupportMilli: 900},
			{Call: "K1ABD", Support: 1, WeightedSupportMilli: 300},
		},
	}
	if got := resolverConfidenceGlyph(callSpecific, true, "K1ABD"); got != "P" {
		t.Fatalf("expected emitted runner-up confidence P, got %q", got)
	}
	if got := resolverConfidenceGlyph(callSpecific, true, "K1ABC"); got != "V" {
		t.Fatalf("expected emitted winner confidence V, got %q", got)
	}
}

func TestMaybeApplyResolverCorrectionAppliesWinner(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    16,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      1,
		FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance:        3,
		DistanceModelCW:        "morse",
		DistanceModelRTTY:      "baudot",
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)

	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	now := time.Now().UTC()
	evidence := []spot.ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "K1ABC-2", Spotter: "N0AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC-2", Spotter: "N0BBB", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0CCC", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range evidence {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue resolver evidence")
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap, ok := resolver.Lookup(key)
		if ok && spot.NormalizeSpotDXCallsign(snap.Winner) == "K1ABC" && snap.WinnerSupport >= 2 && (snap.State == spot.ResolverStateConfident || snap.State == spot.ResolverStateProbable) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	s := spot.NewSpot("K1ABD", "W1XYZ", 7010.0, "CW")
	s.EnsureNormalized()

	suppress := false
	applyDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(applyDeadline) {
		suppress = maybeApplyResolverCorrection(
			s,
			resolver,
			spot.ResolverEvidence{Key: key},
			true,
			config.CallCorrectionConfig{
				Enabled:              true,
				MaxEditDistance:      6,
				MinConsensusReports:  1,
				MinAdvantage:         0,
				MinConfidencePercent: 0,
				DistanceModelCW:      "morse",
				DistanceModelRTTY:    "baudot",
				InvalidAction:        "broadcast",
			},
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
		if s.DXCallNorm == "K1ABC" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if suppress {
		t.Fatalf("did not expect suppress")
	}
	if got := s.DXCallNorm; got != "K1ABC" {
		t.Fatalf("expected resolver winner K1ABC, got %q", got)
	}
	if got := strings.TrimSpace(s.Confidence); got != "C" {
		t.Fatalf("expected confidence C after resolver correction, got %q", got)
	}
}

func TestMaybeApplyResolverCorrectionNoApplyUsesEmittedCallConfidence(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    16,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      1,
		FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance:        3,
		DistanceModelCW:        "morse",
		DistanceModelRTTY:      "baudot",
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)

	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	now := time.Now().UTC()
	evidence := []spot.ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0BBB", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0CCC", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range evidence {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue resolver evidence")
		}
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		snap, ok := resolver.Lookup(key)
		if ok && snap.Winner == "K1ABC" && snap.WinnerSupport >= 2 && (snap.State == spot.ResolverStateConfident || snap.State == spot.ResolverStateProbable) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	tracker := stats.NewTracker()
	s := spot.NewSpot("K1ABD", "W1XYZ", 7010.0, "CW")
	s.EnsureNormalized()
	suppress := maybeApplyResolverCorrection(
		s,
		resolver,
		spot.ResolverEvidence{Key: key},
		true,
		config.CallCorrectionConfig{
			Enabled:              true,
			MaxEditDistance:      6,
			MinConsensusReports:  1,
			MinAdvantage:         5, // force gate rejection on advantage
			MinConfidencePercent: 0,
			DistanceModelCW:      "morse",
			DistanceModelRTTY:    "baudot",
			InvalidAction:        "broadcast",
		},
		nil,
		nil,
		tracker,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	if suppress {
		t.Fatalf("did not expect suppress")
	}
	if got := s.DXCallNorm; got != "K1ABD" {
		t.Fatalf("expected call to remain K1ABD, got %q", got)
	}
	if got := strings.TrimSpace(s.Confidence); got != "P" {
		t.Fatalf("expected emitted-call confidence P, got %q", got)
	}
	if !s.ConfidencePercentOK || s.ConfidencePercent != 33 {
		t.Fatalf("expected emitted-call confidence percent 33, got percent=%d ok=%v", s.ConfidencePercent, s.ConfidencePercentOK)
	}
	if got := tracker.CorrectionDecisionReasons()["resolver_gate_advantage"]; got != 1 {
		t.Fatalf("expected resolver_gate_advantage=1, got %d", got)
	}
}

func TestMaybeApplyResolverCorrectionUsesAdaptiveMinReports(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    16,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      1,
		FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance:        3,
		DistanceModelCW:        "morse",
		DistanceModelRTTY:      "baudot",
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)

	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	now := time.Now().UTC()
	evidence := []spot.ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAB", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0AAC", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range evidence {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue resolver evidence")
		}
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		snap, ok := resolver.Lookup(key)
		if ok && snap.Winner == "K1ABC" && snap.WinnerSupport >= 2 && (snap.State == spot.ResolverStateConfident || snap.State == spot.ResolverStateProbable) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	adaptive := spot.NewAdaptiveMinReports(config.CallCorrectionConfig{
		Enabled:             true,
		MinConsensusReports: 1,
		AdaptiveMinReports: config.AdaptiveMinReportsConfig{
			Enabled:                 true,
			WindowMinutes:           10,
			EvaluationPeriodSeconds: 1,
			HysteresisWindows:       1,
			Groups: []config.AdaptiveMinReportsGroup{
				{
					Name:             "midbands",
					Bands:            []string{"40m"},
					QuietBelow:       1000,
					BusyAbove:        2000,
					QuietMinReports:  4,
					NormalMinReports: 4,
					BusyMinReports:   4,
				},
			},
		},
	})
	if adaptive == nil {
		t.Fatalf("expected adaptive min-reports controller")
	}

	tracker := stats.NewTracker()
	s := spot.NewSpot("K1ABD", "W1XYZ", 7010.0, "CW")
	s.EnsureNormalized()
	suppress := maybeApplyResolverCorrection(
		s,
		resolver,
		spot.ResolverEvidence{Key: key},
		true,
		config.CallCorrectionConfig{
			Enabled:              true,
			MaxEditDistance:      6,
			MinConsensusReports:  1,
			MinAdvantage:         0,
			MinConfidencePercent: 0,
			DistanceModelCW:      "morse",
			DistanceModelRTTY:    "baudot",
			InvalidAction:        "broadcast",
		},
		nil,
		nil,
		tracker,
		nil,
		nil,
		adaptive,
		nil,
		nil,
		nil,
		nil,
	)
	if suppress {
		t.Fatalf("did not expect suppress")
	}
	if got := s.DXCallNorm; got != "K1ABD" {
		t.Fatalf("expected adaptive min_reports to block correction, got %q", got)
	}
	if got := tracker.CorrectionDecisionReasons()["resolver_gate_min_reports"]; got != 1 {
		t.Fatalf("expected resolver_gate_min_reports=1, got %d", got)
	}
}

func TestMaybeApplyResolverCorrectionUsesNeighborhoodWinner(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    16,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      1,
		FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance:        3,
		DistanceModelCW:        "morse",
		DistanceModelRTTY:      "baudot",
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)

	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	neighborKey := spot.NewResolverSignalKey(7009.5, "40m", "CW", 500)
	now := time.Now().UTC()
	events := []spot.ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAB", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAC", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAD", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range events {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue resolver evidence")
		}
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mainSnap, mainOK := resolver.Lookup(key)
		neighborSnap, neighborOK := resolver.Lookup(neighborKey)
		if mainOK &&
			neighborOK &&
			mainSnap.Winner == "K1ABD" &&
			neighborSnap.Winner == "K1ABC" &&
			mainSnap.WinnerSupport >= 1 &&
			neighborSnap.WinnerSupport >= 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	tracker := stats.NewTracker()
	s := spot.NewSpot("K1ABD", "W1XYZ", 7010.0, "CW")
	s.EnsureNormalized()
	suppress := maybeApplyResolverCorrection(
		s,
		resolver,
		spot.ResolverEvidence{Key: key},
		true,
		config.CallCorrectionConfig{
			Enabled:                          true,
			MaxEditDistance:                  6,
			MinConsensusReports:              1,
			MinAdvantage:                     0,
			MinConfidencePercent:             0,
			DistanceModelCW:                  "morse",
			DistanceModelRTTY:                "baudot",
			InvalidAction:                    "broadcast",
			ResolverNeighborhoodEnabled:      true,
			ResolverNeighborhoodBucketRadius: 1,
			FreqGuardRunnerUpRatio:           0.6,
		},
		nil,
		nil,
		tracker,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	if suppress {
		t.Fatalf("did not expect suppress")
	}
	if got := s.DXCallNorm; got != "K1ABC" {
		t.Fatalf("expected neighborhood winner K1ABC, got %q", got)
	}
	if got := tracker.CorrectionDecisionAppliedReasons()[resolverDecisionAppliedNeighbor]; got != 1 {
		t.Fatalf("expected %s=1, got %d", resolverDecisionAppliedNeighbor, got)
	}
}

func TestMaybeApplyResolverCorrectionRejectsNeighborhoodConflict(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    16,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      1,
		FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance:        3,
		DistanceModelCW:        "morse",
		DistanceModelRTTY:      "baudot",
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)

	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	neighborKey := spot.NewResolverSignalKey(7009.5, "40m", "CW", 500)
	now := time.Now().UTC()
	events := []spot.ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAB", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAC", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAD", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0AAE", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0AAF", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0AAG", FrequencyKHz: 7009.5, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range events {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue resolver evidence")
		}
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		mainSnap, mainOK := resolver.Lookup(key)
		neighborSnap, neighborOK := resolver.Lookup(neighborKey)
		if mainOK &&
			neighborOK &&
			mainSnap.Winner == "K1ABC" &&
			neighborSnap.Winner == "K1ABD" &&
			mainSnap.WinnerSupport >= 4 &&
			neighborSnap.WinnerSupport >= 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	tracker := stats.NewTracker()
	s := spot.NewSpot("K1ABD", "W1XYZ", 7010.0, "CW")
	s.EnsureNormalized()
	suppress := maybeApplyResolverCorrection(
		s,
		resolver,
		spot.ResolverEvidence{Key: key},
		true,
		config.CallCorrectionConfig{
			Enabled:                          true,
			MaxEditDistance:                  6,
			MinConsensusReports:              1,
			MinAdvantage:                     0,
			MinConfidencePercent:             0,
			DistanceModelCW:                  "morse",
			DistanceModelRTTY:                "baudot",
			InvalidAction:                    "broadcast",
			ResolverNeighborhoodEnabled:      true,
			ResolverNeighborhoodBucketRadius: 1,
			FreqGuardRunnerUpRatio:           0.7,
		},
		nil,
		nil,
		tracker,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	if suppress {
		t.Fatalf("did not expect suppress")
	}
	if got := s.DXCallNorm; got != "K1ABD" {
		t.Fatalf("expected neighborhood conflict to block correction, got %q", got)
	}
	if got := tracker.CorrectionDecisionReasons()[resolverDecisionNeighborConflict]; got != 1 {
		t.Fatalf("expected %s=1, got %d", resolverDecisionNeighborConflict, got)
	}
}

func TestMaybeApplyResolverCorrectionRecordsNoSnapshotReason(t *testing.T) {
	tracker := stats.NewTracker()
	s := spot.NewSpot("K1ABC", "W1XYZ", 7010.0, "CW")
	s.EnsureNormalized()

	suppress := maybeApplyResolverCorrection(
		s,
		nil,
		spot.ResolverEvidence{},
		false,
		config.CallCorrectionConfig{
			Enabled: true,
		},
		nil,
		nil,
		tracker,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	if suppress {
		t.Fatalf("did not expect suppress")
	}
	if got := strings.TrimSpace(s.Confidence); got != "?" {
		t.Fatalf("expected confidence ?, got %q", got)
	}
	if got := tracker.CorrectionDecisionReasons()[resolverDecisionNoSnapshot]; got != 1 {
		t.Fatalf("expected %s=1, got %d", resolverDecisionNoSnapshot, got)
	}
}

func TestMaybeApplyResolverCorrectionHonorsMaxEditDistance(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    16,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      1,
		FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance:        6,
		DistanceModelCW:        "morse",
		DistanceModelRTTY:      "baudot",
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)

	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	now := time.Now().UTC()
	evidence := []spot.ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "WQ5W", Spotter: "N0AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "WA2CNJ", Spotter: "N0BBB", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "WA2CNJ", Spotter: "N0CCC", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range evidence {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue resolver evidence")
		}
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		snap, ok := resolver.Lookup(key)
		if ok && snap.Winner == "WA2CNJ" && snap.WinnerSupport >= 2 && (snap.State == spot.ResolverStateConfident || snap.State == spot.ResolverStateProbable) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	s := spot.NewSpot("WQ5W", "W1XYZ", 7010.0, "CW")
	s.EnsureNormalized()
	original := s.DXCallNorm
	suppress := maybeApplyResolverCorrection(
		s,
		resolver,
		spot.ResolverEvidence{Key: key},
		true,
		config.CallCorrectionConfig{
			Enabled:         true,
			MaxEditDistance: 1,
			InvalidAction:   "broadcast",
		},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	if suppress {
		t.Fatalf("did not expect suppress")
	}
	if got := s.DXCallNorm; got != original {
		t.Fatalf("expected max_edit_distance to block correction, got %q", got)
	}
}

func TestMaybeApplyResolverCorrectionHonorsDistance3ExtraRails(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    16,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      1,
		FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance:        6,
		DistanceModelCW:        "morse",
		DistanceModelRTTY:      "baudot",
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)

	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	now := time.Now().UTC()
	evidence := []spot.ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "WQ5W", Spotter: "N0AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "WA2CNJ", Spotter: "N0BBB", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "WA2CNJ", Spotter: "N0CCC", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range evidence {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue resolver evidence")
		}
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		snap, ok := resolver.Lookup(key)
		if ok && snap.Winner == "WA2CNJ" && snap.WinnerSupport >= 2 && (snap.State == spot.ResolverStateConfident || snap.State == spot.ResolverStateProbable) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	s := spot.NewSpot("WQ5W", "W1XYZ", 7010.0, "CW")
	s.EnsureNormalized()
	original := s.DXCallNorm
	suppress := maybeApplyResolverCorrection(
		s,
		resolver,
		spot.ResolverEvidence{Key: key},
		true,
		config.CallCorrectionConfig{
			Enabled:                  true,
			MaxEditDistance:          6,
			MinConsensusReports:      1,
			MinAdvantage:             0,
			MinConfidencePercent:     0,
			Distance3ExtraReports:    5,
			Distance3ExtraAdvantage:  0,
			Distance3ExtraConfidence: 0,
			DistanceModelCW:          "morse",
			DistanceModelRTTY:        "baudot",
			InvalidAction:            "broadcast",
		},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	if suppress {
		t.Fatalf("did not expect suppress")
	}
	if got := s.DXCallNorm; got != original {
		t.Fatalf("expected distance-3 extra rails to block correction, got %q", got)
	}
}

func TestMaybeApplyResolverCorrectionAppliesTruncationLengthBonusParity(t *testing.T) {
	recentStore := spot.NewRecentBandStoreWithOptions(spot.RecentBandOptions{
		Window:             12 * time.Hour,
		Shards:             1,
		MaxEntries:         128,
		CleanupInterval:    time.Hour,
		MaxSpottersPerCall: 8,
	})
	now := time.Now().UTC()
	recentStore.Record("VE3NNT", "40m", "CW", "N0AAA", now.Add(-10*time.Minute))
	recentStore.Record("VE3NNT", "40m", "CW", "N0BBB", now.Add(-5*time.Minute))

	settings := spot.CorrectionSettings{
		MinConsensusReports:  3,
		MinAdvantage:         1,
		MinConfidencePercent: 45,
		MaxEditDistance:      3,
		DistanceModelCW:      "morse",
		DistanceModelRTTY:    "baudot",
		FamilyPolicy: spot.CorrectionFamilyPolicy{
			Configured:                 true,
			TruncationEnabled:          true,
			TruncationMaxLengthDelta:   1,
			TruncationMinShorterLength: 3,
			TruncationAllowPrefix:      true,
			TruncationAllowSuffix:      true,
		},
		TruncationLengthBonusEnabled:                   true,
		TruncationLengthBonusMax:                       1,
		TruncationLengthBonusRequireCandidateValidated: true,
		TruncationLengthBonusRequireSubjectUnvalidated: true,
		RecentBandStore:                                recentStore,
		RecentBandRecordMinUniqueSpotters:              2,
	}
	if relation, ok := spot.DetectCorrectionFamilyWithPolicy("VE3NN", "VE3NNT", settings.FamilyPolicy); !ok || relation.Kind != spot.CorrectionFamilyTruncation {
		t.Fatalf("expected truncation family relation, got ok=%t relation=%+v", ok, relation)
	} else if relation.MoreSpecific != "VE3NNT" || relation.LessSpecific != "VE3NN" {
		t.Fatalf("expected VE3NNT to be more specific, got relation=%+v", relation)
	}

	result := spot.EvaluateResolverPrimaryGates("VE3NN", "VE3NNT", "40m", "CW", 1, 2, 66, 1000, 2000, settings, now, spot.ResolverPrimaryGateOptions{})
	if !result.Allow {
		t.Fatalf("expected truncation length bonus parity to admit VE3NNT, reason=%q result=%+v", result.Reason, result)
	}
	if result.LengthBonus != 1 || result.EffectiveSupport != 3 {
		t.Fatalf("expected length bonus 1 and effective support 3, got bonus=%d effective=%d", result.LengthBonus, result.EffectiveSupport)
	}
}

func TestMaybeApplyResolverCorrectionHonorsTruncationDelta2ValidationRail(t *testing.T) {
	settings := spot.CorrectionSettings{
		MinConsensusReports:  1,
		MinAdvantage:         0,
		MinConfidencePercent: 0,
		MaxEditDistance:      3,
		DistanceModelCW:      "morse",
		DistanceModelRTTY:    "baudot",
		FamilyPolicy: spot.CorrectionFamilyPolicy{
			Configured:                 true,
			TruncationEnabled:          true,
			TruncationMaxLengthDelta:   2,
			TruncationMinShorterLength: 3,
			TruncationAllowPrefix:      true,
			TruncationAllowSuffix:      true,
		},
		TruncationDelta2RailsEnabled:              true,
		TruncationDelta2ExtraConfidence:           0,
		TruncationDelta2RequireCandidateValidated: true,
		TruncationDelta2RequireSubjectUnvalidated: false,
	}
	if relation, ok := spot.DetectCorrectionFamilyWithPolicy("VE3NN", "VE3NNTT", settings.FamilyPolicy); !ok || relation.Kind != spot.CorrectionFamilyTruncation {
		t.Fatalf("expected truncation family relation, got ok=%t relation=%+v", ok, relation)
	} else if relation.MoreSpecific != "VE3NNTT" || relation.LessSpecific != "VE3NN" {
		t.Fatalf("expected VE3NNTT to be more specific, got relation=%+v", relation)
	}

	result := spot.EvaluateResolverPrimaryGates("VE3NN", "VE3NNTT", "40m", "CW", 1, 2, 66, 1000, 2000, settings, time.Now().UTC(), spot.ResolverPrimaryGateOptions{})
	if result.Allow {
		t.Fatalf("expected delta-2 validation rail to block correction result=%+v", result)
	}
	if result.Reason != "truncation_delta2_candidate_unvalidated" {
		t.Fatalf("expected truncation_delta2_candidate_unvalidated, got %q", result.Reason)
	}
}

func TestEvaluateResolverPrimaryGatesAppliesRecentPlus1OneShort(t *testing.T) {
	now := time.Now().UTC()
	recent := newRecentBandStoreForStabilizerAdmissionTests()
	recent.Record("K1ABC", "40m", "CW", "N0AAA", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N0AAB", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N0AAC", now.Add(-time.Minute))
	recent.Record("K1ABD", "40m", "CW", "N0AAD", now.Add(-time.Minute))

	settings := spot.CorrectionSettings{
		MinConsensusReports:                     3,
		MinAdvantage:                            1,
		MinConfidencePercent:                    45,
		MaxEditDistance:                         3,
		DistanceModelCW:                         "morse",
		DistanceModelRTTY:                       "baudot",
		RecentBandStore:                         recent,
		RecentBandRecordMinUniqueSpotters:       2,
		ResolverRecentPlus1Enabled:              true,
		ResolverRecentPlus1MinUniqueWinner:      3,
		ResolverRecentPlus1RequireSubjectWeaker: true,
		ResolverRecentPlus1MaxDistance:          1,
		ResolverRecentPlus1AllowTruncation:      true,
	}
	result := spot.EvaluateResolverPrimaryGates(
		"K1ABD",
		"K1ABC",
		"40m",
		"CW",
		1,
		2,
		66,
		1000,
		2000,
		settings,
		now,
		spot.ResolverPrimaryGateOptions{},
	)
	if !result.Allow {
		t.Fatalf("expected resolver recent plus1 to admit one-short winner, result=%+v", result)
	}
	if !result.RecentPlus1Considered || !result.RecentPlus1Applied {
		t.Fatalf("expected resolver recent plus1 considered+applied, result=%+v", result)
	}
	if result.EffectiveSupport != 3 {
		t.Fatalf("expected effective support 3, got %d", result.EffectiveSupport)
	}
}

func TestEvaluateResolverPrimaryGatesRejectsRecentPlus1WhenSubjectNotWeaker(t *testing.T) {
	now := time.Now().UTC()
	recent := newRecentBandStoreForStabilizerAdmissionTests()
	recent.Record("K1ABC", "40m", "CW", "N0AAA", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N0AAB", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N0AAC", now.Add(-time.Minute))
	recent.Record("K1ABD", "40m", "CW", "N0AAD", now.Add(-time.Minute))
	recent.Record("K1ABD", "40m", "CW", "N0AAE", now.Add(-time.Minute))
	recent.Record("K1ABD", "40m", "CW", "N0AAF", now.Add(-time.Minute))

	settings := spot.CorrectionSettings{
		MinConsensusReports:                     3,
		MinAdvantage:                            1,
		MinConfidencePercent:                    45,
		MaxEditDistance:                         3,
		DistanceModelCW:                         "morse",
		DistanceModelRTTY:                       "baudot",
		RecentBandStore:                         recent,
		RecentBandRecordMinUniqueSpotters:       2,
		ResolverRecentPlus1Enabled:              true,
		ResolverRecentPlus1MinUniqueWinner:      3,
		ResolverRecentPlus1RequireSubjectWeaker: true,
		ResolverRecentPlus1MaxDistance:          1,
		ResolverRecentPlus1AllowTruncation:      true,
	}
	result := spot.EvaluateResolverPrimaryGates(
		"K1ABD",
		"K1ABC",
		"40m",
		"CW",
		1,
		2,
		66,
		1000,
		2000,
		settings,
		now,
		spot.ResolverPrimaryGateOptions{},
	)
	if result.Allow {
		t.Fatalf("expected subject_not_weaker to block plus1 path, result=%+v", result)
	}
	if !result.RecentPlus1Considered || result.RecentPlus1Applied {
		t.Fatalf("expected resolver recent plus1 considered but not applied, result=%+v", result)
	}
	if result.RecentPlus1Reject != "subject_not_weaker" {
		t.Fatalf("expected subject_not_weaker reject, got %q", result.RecentPlus1Reject)
	}
}

func TestEvaluateResolverPrimaryGatesAppliesBayesReportBonusOneShort(t *testing.T) {
	now := time.Now().UTC()
	recent := newRecentBandStoreForStabilizerAdmissionTests()
	recent.Record("K1ABC", "40m", "CW", "N0AAA", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N0AAB", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N0AAC", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N0AAD", now.Add(-time.Minute))
	recent.Record("K1ABD", "40m", "CW", "N0AAE", now.Add(-time.Minute))

	settings := spot.CorrectionSettings{
		MinConsensusReports:               3,
		MinAdvantage:                      1,
		MinConfidencePercent:              45,
		MaxEditDistance:                   3,
		DistanceModelCW:                   "morse",
		DistanceModelRTTY:                 "baudot",
		RecentBandStore:                   recent,
		RecentBandRecordMinUniqueSpotters: 2,
		ResolverRecentPlus1Enabled:        false,
		BayesBonusPolicy: spot.CorrectionBayesBonusPolicy{
			Configured:                              true,
			Enabled:                                 true,
			WeightDistance1Milli:                    350,
			WeightDistance2Milli:                    200,
			AdvantageThresholdDistance1Milli:        450,
			AdvantageMinWeightedDeltaDistance1Milli: 200,
			AdvantageExtraConfidenceDistance1:       3,
		},
	}
	result := spot.EvaluateResolverPrimaryGates(
		"K1ABD",
		"K1ABC",
		"40m",
		"CW",
		1,
		2,
		66,
		1000,
		3000,
		settings,
		now,
		spot.ResolverPrimaryGateOptions{},
	)
	if !result.Allow {
		t.Fatalf("expected bayes report bonus to admit one-short winner, result=%+v", result)
	}
	if !result.BayesReportBonusConsidered || !result.BayesReportBonusApplied {
		t.Fatalf("expected bayes report bonus considered+applied, result=%+v", result)
	}
	if result.EffectiveSupport != 3 {
		t.Fatalf("expected effective support 3, got %d", result.EffectiveSupport)
	}
}

func TestEvaluateResolverPrimaryGatesRejectsBayesReportBonusScoreBelowThreshold(t *testing.T) {
	now := time.Now().UTC()
	recent := newRecentBandStoreForStabilizerAdmissionTests()
	recent.Record("K1ABC", "40m", "CW", "N0AAA", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N0AAB", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N0AAC", now.Add(-time.Minute))
	recent.Record("K1ABD", "40m", "CW", "N0AAD", now.Add(-time.Minute))
	recent.Record("K1ABD", "40m", "CW", "N0AAE", now.Add(-time.Minute))

	settings := spot.CorrectionSettings{
		MinConsensusReports:               3,
		MinAdvantage:                      1,
		MinConfidencePercent:              45,
		MaxEditDistance:                   3,
		DistanceModelCW:                   "morse",
		DistanceModelRTTY:                 "baudot",
		RecentBandStore:                   recent,
		RecentBandRecordMinUniqueSpotters: 2,
		ResolverRecentPlus1Enabled:        false,
		BayesBonusPolicy: spot.CorrectionBayesBonusPolicy{
			Configured:                              true,
			Enabled:                                 true,
			WeightDistance1Milli:                    350,
			WeightDistance2Milli:                    200,
			AdvantageThresholdDistance1Milli:        450,
			AdvantageMinWeightedDeltaDistance1Milli: 200,
			AdvantageExtraConfidenceDistance1:       3,
		},
	}
	result := spot.EvaluateResolverPrimaryGates(
		"K1ABD",
		"K1ABC",
		"40m",
		"CW",
		1,
		2,
		66,
		1800,
		2000,
		settings,
		now,
		spot.ResolverPrimaryGateOptions{},
	)
	if result.Allow {
		t.Fatalf("expected bayes score rail to block one-short report bonus, result=%+v", result)
	}
	if result.Reason != "min_reports" {
		t.Fatalf("expected min_reports reject, got %q", result.Reason)
	}
	if !result.BayesReportBonusConsidered || result.BayesReportBonusApplied {
		t.Fatalf("expected bayes report bonus considered but not applied, result=%+v", result)
	}
	if result.BayesReportBonusReject != "score_below_threshold" {
		t.Fatalf("expected score_below_threshold reject, got %q", result.BayesReportBonusReject)
	}
}

func TestEvaluateResolverPrimaryGatesAppliesBayesAdvantageTieBreak(t *testing.T) {
	now := time.Now().UTC()
	recent := newRecentBandStoreForStabilizerAdmissionTests()
	recent.Record("K1ABC", "40m", "CW", "N0AAA", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N0AAB", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N0AAC", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N0AAD", now.Add(-time.Minute))
	recent.Record("K1ABD", "40m", "CW", "N0AAE", now.Add(-time.Minute))

	settings := spot.CorrectionSettings{
		MinConsensusReports:               3,
		MinAdvantage:                      1,
		MinConfidencePercent:              45,
		MaxEditDistance:                   3,
		DistanceModelCW:                   "morse",
		DistanceModelRTTY:                 "baudot",
		RecentBandStore:                   recent,
		RecentBandRecordMinUniqueSpotters: 2,
		ResolverRecentPlus1Enabled:        false,
		BayesBonusPolicy: spot.CorrectionBayesBonusPolicy{
			Configured:                              true,
			Enabled:                                 true,
			WeightDistance1Milli:                    350,
			WeightDistance2Milli:                    200,
			AdvantageThresholdDistance1Milli:        450,
			AdvantageMinWeightedDeltaDistance1Milli: 200,
			AdvantageExtraConfidenceDistance1:       3,
		},
	}
	result := spot.EvaluateResolverPrimaryGates(
		"K1ABD",
		"K1ABC",
		"40m",
		"CW",
		2,
		2,
		70,
		1500,
		2300,
		settings,
		now,
		spot.ResolverPrimaryGateOptions{},
	)
	if !result.Allow {
		t.Fatalf("expected bayes advantage tie-break to admit winner, result=%+v", result)
	}
	if !result.BayesReportBonusApplied {
		t.Fatalf("expected bayes report bonus to support one-short min_reports, result=%+v", result)
	}
	if !result.BayesAdvantageConsidered || !result.BayesAdvantageApplied {
		t.Fatalf("expected bayes advantage bonus considered+applied, result=%+v", result)
	}
	if result.EffectiveAdvantageSupport != 3 {
		t.Fatalf("expected effective advantage support 3, got %d", result.EffectiveAdvantageSupport)
	}
}

func TestResolverBayesDecisionReasonReportReject(t *testing.T) {
	reason, ok := resolverBayesDecisionReason(spot.ResolverPrimaryGateResult{
		Reason:                     "min_reports",
		BayesReportBonusConsidered: true,
		BayesReportBonusApplied:    false,
		BayesReportBonusReject:     "score_below_threshold",
	})
	if !ok {
		t.Fatalf("expected bayes report reject reason")
	}
	want := resolverDecisionBayesReportRejectPrefix + "score_below_threshold"
	if reason != want {
		t.Fatalf("expected %q, got %q", want, reason)
	}
}

func TestResolverBayesDecisionReasonAdvantageReject(t *testing.T) {
	reason, ok := resolverBayesDecisionReason(spot.ResolverPrimaryGateResult{
		Reason:                   "advantage",
		BayesAdvantageConsidered: true,
		BayesAdvantageApplied:    false,
		BayesAdvantageReject:     "weighted_delta_insufficient",
	})
	if !ok {
		t.Fatalf("expected bayes advantage reject reason")
	}
	want := resolverDecisionBayesAdvantageRejectPrefix + "weighted_delta_insufficient"
	if reason != want {
		t.Fatalf("expected %q, got %q", want, reason)
	}
}

func TestMaybeApplyResolverCorrectionUsesRecentPlus1AppliedReason(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    16,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      1,
		FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance:        3,
		DistanceModelCW:        "morse",
		DistanceModelRTTY:      "baudot",
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)

	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	now := time.Now().UTC()
	events := []spot.ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAB", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0AAC", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range events {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue resolver evidence")
		}
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if snap, ok := resolver.Lookup(key); ok && snap.Winner == "K1ABC" && snap.WinnerSupport >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	recent := newRecentBandStoreForStabilizerAdmissionTests()
	recent.Record("K1ABC", "40m", "CW", "N1AAA", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N1AAB", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N1AAC", now.Add(-time.Minute))
	recent.Record("K1ABD", "40m", "CW", "N1AAD", now.Add(-time.Minute))

	tracker := stats.NewTracker()
	s := spot.NewSpot("K1ABD", "W1XYZ", 7010.0, "CW")
	s.EnsureNormalized()
	suppress := maybeApplyResolverCorrection(
		s,
		resolver,
		spot.ResolverEvidence{Key: key},
		true,
		config.CallCorrectionConfig{
			Enabled:                                 true,
			MaxEditDistance:                         3,
			MinConsensusReports:                     3,
			MinAdvantage:                            1,
			MinConfidencePercent:                    45,
			DistanceModelCW:                         "morse",
			DistanceModelRTTY:                       "baudot",
			InvalidAction:                           "broadcast",
			ResolverRecentPlus1Enabled:              true,
			ResolverRecentPlus1MinUniqueWinner:      3,
			ResolverRecentPlus1RequireSubjectWeaker: true,
			ResolverRecentPlus1MaxDistance:          1,
			ResolverRecentPlus1AllowTruncation:      true,
		},
		nil,
		nil,
		tracker,
		nil,
		recent,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	if suppress {
		t.Fatalf("did not expect suppress")
	}
	if got := s.DXCallNorm; got != "K1ABC" {
		t.Fatalf("expected recent plus1 correction to K1ABC, got %q", got)
	}
	if got := tracker.CorrectionDecisionAppliedReasons()[resolverDecisionAppliedRecentPlus1]; got != 1 {
		t.Fatalf("expected %s=1, got %d", resolverDecisionAppliedRecentPlus1, got)
	}
}

func TestMaybeApplyResolverCorrectionRejectsRecentPlus1WhenSubjectNotWeaker(t *testing.T) {
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:              64,
		MaxActiveKeys:          16,
		MaxCandidatesPerKey:    8,
		MaxReportersPerCand:    16,
		InactiveTTL:            time.Minute,
		EvalMinInterval:        5 * time.Millisecond,
		SweepInterval:          5 * time.Millisecond,
		HysteresisWindows:      1,
		FreqGuardRunnerUpRatio: 0.6,
		MaxEditDistance:        3,
		DistanceModelCW:        "morse",
		DistanceModelRTTY:      "baudot",
	})
	resolver.Start()
	t.Cleanup(resolver.Stop)

	key := spot.NewResolverSignalKey(7010.0, "40m", "CW", 500)
	now := time.Now().UTC()
	events := []spot.ResolverEvidence{
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAA", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABC", Spotter: "N0AAB", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
		{ObservedAt: now, Key: key, DXCall: "K1ABD", Spotter: "N0AAC", FrequencyKHz: 7010.0, RecencyWindow: 30 * time.Second},
	}
	for _, ev := range events {
		if ok := resolver.Enqueue(ev); !ok {
			t.Fatalf("failed to enqueue resolver evidence")
		}
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if snap, ok := resolver.Lookup(key); ok && snap.Winner == "K1ABC" && snap.WinnerSupport >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	recent := newRecentBandStoreForStabilizerAdmissionTests()
	recent.Record("K1ABC", "40m", "CW", "N1AAA", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N1AAB", now.Add(-time.Minute))
	recent.Record("K1ABC", "40m", "CW", "N1AAC", now.Add(-time.Minute))
	recent.Record("K1ABD", "40m", "CW", "N1AAD", now.Add(-time.Minute))
	recent.Record("K1ABD", "40m", "CW", "N1AAE", now.Add(-time.Minute))
	recent.Record("K1ABD", "40m", "CW", "N1AAF", now.Add(-time.Minute))

	tracker := stats.NewTracker()
	s := spot.NewSpot("K1ABD", "W1XYZ", 7010.0, "CW")
	s.EnsureNormalized()
	suppress := maybeApplyResolverCorrection(
		s,
		resolver,
		spot.ResolverEvidence{Key: key},
		true,
		config.CallCorrectionConfig{
			Enabled:                                 true,
			MaxEditDistance:                         3,
			MinConsensusReports:                     3,
			MinAdvantage:                            1,
			MinConfidencePercent:                    45,
			DistanceModelCW:                         "morse",
			DistanceModelRTTY:                       "baudot",
			InvalidAction:                           "broadcast",
			ResolverRecentPlus1Enabled:              true,
			ResolverRecentPlus1MinUniqueWinner:      3,
			ResolverRecentPlus1RequireSubjectWeaker: true,
			ResolverRecentPlus1MaxDistance:          1,
			ResolverRecentPlus1AllowTruncation:      true,
		},
		nil,
		nil,
		tracker,
		nil,
		recent,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	if suppress {
		t.Fatalf("did not expect suppress")
	}
	if got := s.DXCallNorm; got != "K1ABD" {
		t.Fatalf("expected plus1 rejection to keep K1ABD, got %q", got)
	}
	if got := tracker.CorrectionDecisionReasons()[resolverDecisionRecentPlus1RejectPrefix+"subject_not_weaker"]; got != 1 {
		t.Fatalf("expected %ssubject_not_weaker=1, got %d", resolverDecisionRecentPlus1RejectPrefix, got)
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

func TestBuildOverviewLinesIncludesRecentSupportByBand(t *testing.T) {
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
		recentBandStore,
		nil,
		nil,
		nil,
		nil,
		nil,
		voacap.SunspotMonitorSnapshot{},
		pathreliability.VOACAPClosedFallbackSnapshot{},
		nil,
		nil,
		"N2WQ-2",
		false,
		false,
		false,
		nil,
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
		if strings.Contains(line, "[yellow]Recent support[-]: 2") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected recent-support line with count 2 in overview lines, got %v", lines)
	}
	perBandFound := false
	blankBeforeKnown := false
	for _, line := range lines {
		if strings.Contains(line, "[yellow]40m[-]: 2") && strings.Contains(line, "[yellow]20m[-]: 1") {
			perBandFound = true
		}
	}
	for i, line := range lines {
		if !strings.Contains(line, "[yellow]Recent support[-]: 2") {
			continue
		}
		if i > 0 && strings.TrimSpace(lines[i-1]) == "" {
			blankBeforeKnown = true
		}
		break
	}
	if !perBandFound {
		t.Fatalf("expected per-band recent-support counts in overview lines, got %v", lines)
	}
	if !blankBeforeKnown {
		t.Fatalf("expected blank spacer line before recent-support section, got %v", lines)
	}
	for _, line := range lines {
		if strings.Contains(line, "Evidence policy") {
			t.Fatalf("did not expect evidence policy line in overview lines, got %v", lines)
		}
		if strings.Contains(line, "Custom SCP calls") {
			t.Fatalf("did not expect custom SCP static count for legacy recent-band store, got %v", lines)
		}
	}
}

func TestFormatRecentSupportByBandLinesIncludesCustomSCPStaticCalls(t *testing.T) {
	store, err := spot.OpenCustomSCPStore(spot.CustomSCPOptions{
		Path:                   filepath.Join(t.TempDir(), "scp"),
		HorizonDays:            60,
		StaticHorizonDays:      395,
		MaxSpottersPerKey:      4,
		CoreMinScore:           1,
		CoreMinH3Cells:         1,
		SFloorMinScore:         1,
		SFloorExactMinH3Cells:  1,
		SFloorFamilyMinH3Cells: 1,
	})
	if err != nil {
		t.Fatalf("open custom store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	now := time.Date(2026, 4, 19, 12, 0, 0, 0, time.UTC)
	record := func(call, band, spotter string, minutesAgo int) {
		store.RecordSpot(&spot.Spot{
			DXCall:     call,
			DXCallNorm: call,
			DECall:     spotter,
			DECallNorm: spotter,
			DEGridNorm: "FN20",
			Frequency:  7020,
			Band:       band,
			BandNorm:   band,
			Mode:       "CW",
			ModeNorm:   "CW",
			HasReport:  true,
			Report:     12,
			Time:       now.Add(-time.Duration(minutesAgo) * time.Minute),
			Confidence: "V",
		})
	}
	record("K1ABC", "40m", "W1AAA", 10)
	record("K1ABC", "20m", "W2BBB", 5)
	record("N0XYZ", "40m", "W3CCC", 6)

	lines := formatRecentSupportByBandLines(store, now, 0)
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "[yellow]Custom SCP[-]: 2 (R) / 2 (S)") {
		t.Fatalf("expected combined custom SCP recent/static count line, got %v", lines)
	}
	if !strings.Contains(joined, "[yellow]40m[-]: 2") || !strings.Contains(joined, "[yellow]20m[-]: 1") {
		t.Fatalf("expected recent per-band support rows, got %v", lines)
	}
	customIdx := -1
	for i, line := range lines {
		if strings.Contains(line, "Custom SCP[-]") {
			customIdx = i
		}
	}
	if customIdx != 0 {
		t.Fatalf("expected custom SCP summary before per-band rows, got %v", lines)
	}
	if len(lines) < 3 || strings.TrimSpace(lines[1]) != "" {
		t.Fatalf("expected blank spacer between custom SCP summary and per-band rows, got %v", lines)
	}
	for _, forbidden := range []string{"Custom SCP calls", "Recent support"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("did not expect old custom SCP label %q, got %v", forbidden, lines)
		}
	}
}

func TestFormatIngestSourceLinesIncludesPeerSSIDs(t *testing.T) {
	lines := formatIngestSourceLines([]dashboardIngestSource{
		{Label: "RBN", Enabled: true, Connected: true},
		{Label: "RBN-FT", Enabled: true, Connected: true},
		{Label: "PSKReporter", Enabled: true, Connected: true},
		{Label: "Peers", Enabled: true, Connected: true, Details: []string{"KM3T-44", "N2WQ-73"}},
	})
	if len(lines) == 0 {
		t.Fatal("expected ingest source lines")
	}
	if !strings.Contains(lines[0], "4 / 4 connected") {
		t.Fatalf("expected 4/4 connected summary, got %q", lines[0])
	}
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "KM3T-44") || !strings.Contains(joined, "N2WQ-73") {
		t.Fatalf("expected peer SSIDs in ingest source list, got %v", lines)
	}
	if strings.Contains(joined, "Peers (2)") {
		t.Fatalf("did not expect peer-count fallback when SSIDs are available, got %v", lines)
	}
}

func TestFormatIngestSourceLinesFallsBackToPeerCount(t *testing.T) {
	lines := formatIngestSourceLines([]dashboardIngestSource{
		{Label: "Peers", Enabled: true, Connected: true, Details: []string{"Peers (2)"}},
	})
	if len(lines) == 0 {
		t.Fatal("expected ingest source lines")
	}
	if !strings.Contains(lines[0], "1 / 1 connected") {
		t.Fatalf("expected peer-only connected summary, got %q", lines[0])
	}
	joined := strings.Join(lines, "\n")
	if !strings.Contains(joined, "Peers (2)") {
		t.Fatalf("expected peer-count fallback when SSIDs are unavailable, got %v", lines)
	}
}

func TestDiffSourceModesPSK31AndPSK63Only(t *testing.T) {
	current := map[string]uint64{
		sourceModeKey("PSKREPORTER", "PSK31"):  12,
		sourceModeKey("PSKREPORTER", "PSK63"):  8,
		sourceModeKey("PSKREPORTER", "PSK125"): 20,
		sourceModeKey("PSKREPORTER", "PSK"):    40,
	}
	previous := map[string]uint64{
		sourceModeKey("PSKREPORTER", "PSK31"):  3,
		sourceModeKey("PSKREPORTER", "PSK63"):  2,
		sourceModeKey("PSKREPORTER", "PSK125"): 10,
		sourceModeKey("PSKREPORTER", "PSK"):    35,
	}

	got := diffSourceModes(current, previous, "PSKREPORTER", "PSK31", "PSK63")
	const want = 15 // (12-3) + (8-2)
	if got != want {
		t.Fatalf("expected PSK31/63 delta %d, got %d", want, got)
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
		MinAdvantage:                            2,
		MinConfidencePercent:                    65,
		MaxEditDistance:                         3,
		DistanceModelCW:                         "morse",
		DistanceModelRTTY:                       "baudot",
		Distance3ExtraReports:                   1,
		Distance3ExtraAdvantage:                 1,
		Distance3ExtraConfidence:                5,
		FreqGuardMinSeparationKHz:               0.2,
		FreqGuardRunnerUpRatio:                  0.6,
		RecentBandRecordMinUniqueSpotters:       2,
		ResolverRecentPlus1Enabled:              true,
		ResolverRecentPlus1MinUniqueWinner:      3,
		ResolverRecentPlus1RequireSubjectWeaker: true,
		ResolverRecentPlus1MaxDistance:          1,
		ResolverRecentPlus1AllowTruncation:      true,
		BayesBonus: config.CallCorrectionBayesBonusConfig{
			Enabled:                                 true,
			WeightDistance1Milli:                    330,
			WeightDistance2Milli:                    180,
			WeightedSmoothingMilli:                  1200,
			RecentSmoothing:                         3,
			ObsLogCapMilli:                          400,
			PriorLogMinMilli:                        -250,
			PriorLogMaxMilli:                        700,
			ReportThresholdDistance1Milli:           500,
			ReportThresholdDistance2Milli:           700,
			AdvantageThresholdDistance1Milli:        750,
			AdvantageThresholdDistance2Milli:        980,
			AdvantageMinWeightedDeltaDistance1Milli: 250,
			AdvantageMinWeightedDeltaDistance2Milli: 350,
			AdvantageExtraConfidenceDistance1:       4,
			AdvantageExtraConfidenceDistance2:       6,
			RequireCandidateValidated:               false,
			RequireSubjectUnvalidatedDistance2:      false,
		},
	}
	window := 75 * time.Second
	recentBandStore := spot.NewRecentBandStore(12 * time.Hour)
	got := buildCorrectionSettings(
		cfg,
		5,
		window,
		900,
		recentBandStore,
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
	if got.RecencyWindow != window {
		t.Fatalf("expected window %s, got %s", window, got.RecencyWindow)
	}
	if got.FrequencyToleranceHz != 900 {
		t.Fatalf("expected frequency tolerance 900Hz, got %.1f", got.FrequencyToleranceHz)
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
		got.DistanceModelCW != cfg.DistanceModelCW ||
		got.DistanceModelRTTY != cfg.DistanceModelRTTY ||
		got.Distance3ExtraReports != cfg.Distance3ExtraReports ||
		got.Distance3ExtraAdvantage != cfg.Distance3ExtraAdvantage ||
		got.Distance3ExtraConfidence != cfg.Distance3ExtraConfidence ||
		got.RecentBandRecordMinUniqueSpotters != cfg.RecentBandRecordMinUniqueSpotters ||
		got.ResolverRecentPlus1Enabled != cfg.ResolverRecentPlus1Enabled ||
		got.ResolverRecentPlus1MinUniqueWinner != cfg.ResolverRecentPlus1MinUniqueWinner ||
		got.ResolverRecentPlus1RequireSubjectWeaker != cfg.ResolverRecentPlus1RequireSubjectWeaker ||
		got.ResolverRecentPlus1MaxDistance != cfg.ResolverRecentPlus1MaxDistance ||
		got.ResolverRecentPlus1AllowTruncation != cfg.ResolverRecentPlus1AllowTruncation {
		t.Fatalf("expected correction settings to mirror config fields")
	}
	if !got.BayesBonusPolicy.Configured ||
		got.BayesBonusPolicy.Enabled != cfg.BayesBonus.Enabled ||
		got.BayesBonusPolicy.WeightDistance1Milli != cfg.BayesBonus.WeightDistance1Milli ||
		got.BayesBonusPolicy.WeightDistance2Milli != cfg.BayesBonus.WeightDistance2Milli ||
		got.BayesBonusPolicy.WeightedSmoothingMilli != cfg.BayesBonus.WeightedSmoothingMilli ||
		got.BayesBonusPolicy.RecentSmoothing != cfg.BayesBonus.RecentSmoothing ||
		got.BayesBonusPolicy.ObsLogCapMilli != cfg.BayesBonus.ObsLogCapMilli ||
		got.BayesBonusPolicy.PriorLogMinMilli != cfg.BayesBonus.PriorLogMinMilli ||
		got.BayesBonusPolicy.PriorLogMaxMilli != cfg.BayesBonus.PriorLogMaxMilli ||
		got.BayesBonusPolicy.ReportThresholdDistance1Milli != cfg.BayesBonus.ReportThresholdDistance1Milli ||
		got.BayesBonusPolicy.ReportThresholdDistance2Milli != cfg.BayesBonus.ReportThresholdDistance2Milli ||
		got.BayesBonusPolicy.AdvantageThresholdDistance1Milli != cfg.BayesBonus.AdvantageThresholdDistance1Milli ||
		got.BayesBonusPolicy.AdvantageThresholdDistance2Milli != cfg.BayesBonus.AdvantageThresholdDistance2Milli ||
		got.BayesBonusPolicy.AdvantageMinWeightedDeltaDistance1Milli != cfg.BayesBonus.AdvantageMinWeightedDeltaDistance1Milli ||
		got.BayesBonusPolicy.AdvantageMinWeightedDeltaDistance2Milli != cfg.BayesBonus.AdvantageMinWeightedDeltaDistance2Milli ||
		got.BayesBonusPolicy.AdvantageExtraConfidenceDistance1 != cfg.BayesBonus.AdvantageExtraConfidenceDistance1 ||
		got.BayesBonusPolicy.AdvantageExtraConfidenceDistance2 != cfg.BayesBonus.AdvantageExtraConfidenceDistance2 ||
		got.BayesBonusPolicy.RequireCandidateValidated != cfg.BayesBonus.RequireCandidateValidated ||
		got.BayesBonusPolicy.RequireSubjectUnvalidatedDistance2 != cfg.BayesBonus.RequireSubjectUnvalidatedDistance2 {
		t.Fatalf("expected bayes bonus mapping to be preserved")
	}
	if got.RecentBandStore != recentBandStore {
		t.Fatalf("expected recent-band store pointer to be preserved")
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
		{"EA5JLX-1-@", "EA5JLX-@"},
		{"EA5JLX-@", "EA5JLX-@"},
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
		{"EA5JLX-@", "EA5JLX"},
		{"EA5JLX-1-@", "EA5JLX"},
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
