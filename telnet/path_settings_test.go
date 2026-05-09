package telnet

import (
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/pathreliability"
	"dxcluster/spot"
)

func TestHandlePathSettingsNoiseIndustrial(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	server := &Server{
		noiseModel: cfg.NoiseModel(),
	}
	client := &Client{}
	resp, handled := server.handlePathSettingsCommand(client, "SET NOISE INDUSTRIAL")
	if !handled {
		t.Fatalf("expected SET NOISE to be handled")
	}
	if !strings.Contains(resp, "Noise class set to INDUSTRIAL") {
		t.Fatalf("unexpected response: %q", resp)
	}
	if client.noiseClass != "INDUSTRIAL" {
		t.Fatalf("expected noise class INDUSTRIAL, got %q", client.noiseClass)
	}
}

func TestHandlePathSettingsPathSamplesStricterThanDefault(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	cfg.MinObservationCount = 19
	server := &Server{
		pathPredictor: pathreliability.NewPredictor(cfg, []string{"20m"}),
	}
	client := &Client{}

	resp, handled := server.handlePathSettingsCommand(client, "SET PATHSAMPLES 19")
	if !handled {
		t.Fatalf("expected SET PATHSAMPLES to be handled")
	}
	if !strings.Contains(resp, "greater than cluster default 19") {
		t.Fatalf("expected default floor rejection, got %q", resp)
	}

	resp, handled = server.handlePathSettingsCommand(client, "SET PATHSAMPLES 30")
	if !handled {
		t.Fatalf("expected SET PATHSAMPLES to be handled")
	}
	if !strings.Contains(resp, "Path sample minimum set to 30 (cluster default 19)") {
		t.Fatalf("unexpected response: %q", resp)
	}
	if client.pathMinObservationCount != 30 {
		t.Fatalf("expected client path sample floor 30, got %d", client.pathMinObservationCount)
	}

	resp, handled = server.handlePathSettingsCommand(client, "SET PATHSAMPLES DEFAULT")
	if !handled {
		t.Fatalf("expected SET PATHSAMPLES DEFAULT to be handled")
	}
	if !strings.Contains(resp, "reset to cluster default 19") {
		t.Fatalf("unexpected reset response: %q", resp)
	}
	if client.pathMinObservationCount != 0 {
		t.Fatalf("expected client path sample floor reset, got %d", client.pathMinObservationCount)
	}
}

func TestNoisePenaltyForClassBand(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	server := &Server{noiseModel: cfg.NoiseModel()}
	if got := server.noisePenaltyForClassBand("URBAN", "160m"); got != 0 {
		t.Fatalf("expected urban 160m penalty 0, got %v", got)
	}
	if got := server.noisePenaltyForClassBand("URBAN", "6m"); got != 0 {
		t.Fatalf("expected urban 6m penalty 0, got %v", got)
	}
}

func TestPathPredictionUsesZeroNoisePenalty(t *testing.T) {
	requireH3Mappings(t)
	cfg := pathreliability.DefaultConfig()
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 1
	predictor := pathreliability.NewPredictor(cfg, []string{"160m", "6m"})

	userCell := pathreliability.EncodeCell("FN31")
	dxCell := pathreliability.EncodeCell("FN32")
	userCoarse := pathreliability.EncodeCoarseCell("FN31")
	dxCoarse := pathreliability.EncodeCoarseCell("FN32")
	now := time.Now().UTC()
	for _, band := range []string{"160m", "6m"} {
		predictor.Update(pathreliability.BucketCombined, userCell, dxCell, userCoarse, dxCoarse, band, -5, 1, now, false)
	}

	server := &Server{
		pathPredictor: predictor,
		pathDisplay:   true,
		noiseModel:    cfg.NoiseModel(),
	}
	client := &Client{
		grid:           "FN31",
		gridCell:       userCell,
		gridCoarseCell: userCoarse,
		noiseClass:     "URBAN",
	}

	lowBandSpot := spot.NewSpot("DX1AA", "DE1AA", 1810, "FT8")
	lowBandSpot.BandNorm = "160m"
	lowBandSpot.DXMetadata.Grid = "FN32"
	highBandSpot := spot.NewSpot("DX1AA", "DE1AA", 50125, "FT8")
	highBandSpot.BandNorm = "6m"
	highBandSpot.DXMetadata.Grid = "FN32"

	if got := server.pathGlyphsForClient(client, lowBandSpot); got != cfg.GlyphSymbols.High {
		t.Fatalf("expected 160m zero penalty to preserve high glyph, got %q", got)
	}
	if got := server.pathGlyphsForClient(client, highBandSpot); got != cfg.GlyphSymbols.High {
		t.Fatalf("expected 6m zero penalty to preserve high glyph, got %q", got)
	}
	if got := server.pathClassForClient(client, lowBandSpot); got != filter.PathClassHigh {
		t.Fatalf("expected 160m PATH class high, got %q", got)
	}
	if got := server.pathClassForClient(client, highBandSpot); got != filter.PathClassHigh {
		t.Fatalf("expected 6m PATH class high, got %q", got)
	}
}

func TestPathPredictionStaleEvidenceIsInsufficientForDisplayAndFilter(t *testing.T) {
	requireH3Mappings(t)
	cfg := pathreliability.DefaultConfig()
	cfg.BandHalfLifeSec = map[string]int{"20m": 10}
	cfg.StaleAfterHalfLifeMultiplier = 100
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 1
	cfg.MaxPredictionAgeHalfLifeMultiplier = 1
	predictor := pathreliability.NewPredictor(cfg, []string{"20m"})

	userCell := pathreliability.EncodeCell("FN31")
	dxCell := pathreliability.EncodeCell("FN32")
	userCoarse := pathreliability.EncodeCoarseCell("FN31")
	dxCoarse := pathreliability.EncodeCoarseCell("FN32")
	now := time.Now().UTC()
	predictor.Update(pathreliability.BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", 25, 10, now.Add(-20*time.Second), false)

	server := &Server{
		pathPredictor: predictor,
		pathDisplay:   true,
		noiseModel:    cfg.NoiseModel(),
		nowFn:         func() time.Time { return now },
	}
	client := &Client{
		grid:           "FN31",
		gridCell:       userCell,
		gridCoarseCell: userCoarse,
		noiseClass:     "QUIET",
	}
	sp := spot.NewSpot("DX1AA", "DE1AA", 14074, "FT8")
	sp.BandNorm = "20m"
	sp.DXMetadata.Grid = "FN32"

	if got := server.pathGlyphsForClient(client, sp); got != cfg.GlyphSymbols.Insufficient {
		t.Fatalf("expected stale display glyph to be insufficient %q, got %q", cfg.GlyphSymbols.Insufficient, got)
	}
	if got := server.pathClassForClient(client, sp); got != filter.PathClassInsufficient {
		t.Fatalf("expected stale PATH class insufficient, got %q", got)
	}
	stats := server.PathPredictionStatsSnapshot()
	if stats.Stale != 1 || stats.NoSample != 0 || stats.LowWeight != 0 {
		t.Fatalf("expected stale stats only, got %+v", stats)
	}
}

func TestPathSamplesOverrideAppliesToDisplayFilterAndDiag(t *testing.T) {
	requireH3Mappings(t)
	cfg := pathreliability.DefaultConfig()
	cfg.MinEffectiveWeight = 0.1
	cfg.MinObservationCount = 2
	predictor := pathreliability.NewPredictor(cfg, []string{"20m"})

	userCell := pathreliability.EncodeCell("FN31")
	dxCell := pathreliability.EncodeCell("FN32")
	userCoarse := pathreliability.EncodeCoarseCell("FN31")
	dxCoarse := pathreliability.EncodeCoarseCell("FN32")
	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		predictor.Update(pathreliability.BucketCombined, userCell, dxCell, userCoarse, dxCoarse, "20m", -5, 10, now, false)
	}

	server := &Server{
		pathPredictor: predictor,
		pathDisplay:   true,
		noiseModel:    cfg.NoiseModel(),
		nowFn:         func() time.Time { return now },
	}
	client := &Client{
		grid:                    "FN31",
		gridCell:                userCell,
		gridCoarseCell:          userCoarse,
		noiseClass:              "QUIET",
		pathMinObservationCount: 4,
	}
	sp := spot.NewSpot("DX1AA", "DE1AA", 14074, "FT8")
	sp.BandNorm = "20m"
	sp.DXMetadata.Grid = "FN32"

	if got := server.pathGlyphsForClient(client, sp); got != cfg.GlyphSymbols.Insufficient {
		t.Fatalf("expected user path sample floor to return insufficient glyph %q, got %q", cfg.GlyphSymbols.Insufficient, got)
	}
	if got := server.pathClassForClient(client, sp); got != filter.PathClassInsufficient {
		t.Fatalf("expected user path sample floor to return insufficient class, got %q", got)
	}
	client.setDiagMode(diagModePath)
	line := server.formatSpotForClient(client, sp)
	if !strings.Contains(line, "n3|lown") {
		t.Fatalf("expected low-count diagnostic from user floor, got %q", line)
	}
}
