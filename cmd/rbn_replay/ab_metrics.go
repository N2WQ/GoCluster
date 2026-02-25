package main

import (
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
	"dxcluster/strutil"
)

// replayConfidenceCounts tracks final confidence glyph frequencies.
// "unknown" corresponds to "?".
type replayConfidenceCounts struct {
	Samples uint64 `json:"samples"`
	Unknown uint64 `json:"unknown"`
	S       uint64 `json:"s"`
	P       uint64 `json:"p"`
	V       uint64 `json:"v"`
	C       uint64 `json:"c"`
	B       uint64 `json:"b"`
	Other   uint64 `json:"other"`
}

func (c *replayConfidenceCounts) Observe(glyph string) {
	if c == nil {
		return
	}
	c.Samples++
	switch normalizeConfidenceGlyph(glyph) {
	case "?":
		c.Unknown++
	case "S":
		c.S++
	case "P":
		c.P++
	case "V":
		c.V++
	case "C":
		c.C++
	case "B":
		c.B++
	default:
		c.Other++
	}
}

type replayConfidenceOutcome struct {
	Final       string
	LegacyFinal string
}

type replayCurrentPathABMetrics struct {
	ConfidenceCounts  replayConfidenceCounts `json:"confidence_counts"`
	LegacyUnknownNowP uint64                 `json:"legacy_unknown_now_p"`
}

type replayResolverStateCounts struct {
	Samples   uint64 `json:"samples"`
	Confident uint64 `json:"confident"`
	Probable  uint64 `json:"probable"`
	Uncertain uint64 `json:"uncertain"`
	Split     uint64 `json:"split"`
}

func (c *replayResolverStateCounts) Observe(state spot.ResolverState) {
	if c == nil {
		return
	}
	c.Samples++
	switch state {
	case spot.ResolverStateConfident:
		c.Confident++
	case spot.ResolverStateProbable:
		c.Probable++
	case spot.ResolverStateUncertain:
		c.Uncertain++
	case spot.ResolverStateSplit:
		c.Split++
	}
}

type replayResolverABMetrics struct {
	Samples                   uint64                    `json:"samples"`
	MissingSnapshot           uint64                    `json:"missing_snapshot"`
	StateCounts               replayResolverStateCounts `json:"state_counts"`
	ProjectedConfidenceCounts replayConfidenceCounts    `json:"projected_confidence_counts"`
	LegacyUnknownNowP         uint64                    `json:"legacy_unknown_now_p"`
}

type replayStabilizerDelayProxyMetrics struct {
	Eligible                    uint64 `json:"eligible"`
	WouldDelayOld               uint64 `json:"would_delay_old"`
	WouldDelayNew               uint64 `json:"would_delay_new"`
	NewlyNotDelayedUnderNewRule uint64 `json:"newly_not_delayed_under_new_rule"`
	NewlyDelayedUnderNewRule    uint64 `json:"newly_delayed_under_new_rule"`
	DelayDelta                  int64  `json:"delay_delta"`
}

func (m *replayStabilizerDelayProxyMetrics) Observe(oldDelay bool, newDelay bool) {
	if m == nil {
		return
	}
	m.Eligible++
	if oldDelay {
		m.WouldDelayOld++
	}
	if newDelay {
		m.WouldDelayNew++
	}
	if oldDelay && !newDelay {
		m.NewlyNotDelayedUnderNewRule++
	}
	if !oldDelay && newDelay {
		m.NewlyDelayedUnderNewRule++
	}
	m.DelayDelta = int64(m.WouldDelayNew) - int64(m.WouldDelayOld)
}

type replayABMetrics struct {
	CurrentPath          replayCurrentPathABMetrics        `json:"current_path"`
	Resolver             replayResolverABMetrics           `json:"resolver"`
	StabilizerDelayProxy replayStabilizerDelayProxyMetrics `json:"stabilizer_delay_proxy"`
}

func (m *replayABMetrics) ObserveCurrentPath(outcome replayConfidenceOutcome) {
	if m == nil {
		return
	}
	newGlyph := normalizeConfidenceGlyph(outcome.Final)
	legacyGlyph := normalizeConfidenceGlyph(outcome.LegacyFinal)
	if legacyGlyph == "" {
		legacyGlyph = newGlyph
	}
	m.CurrentPath.ConfidenceCounts.Observe(newGlyph)
	if legacyGlyph == "?" && newGlyph == "P" {
		m.CurrentPath.LegacyUnknownNowP++
	}
}

func (m *replayABMetrics) ObserveResolverSnapshot(snap spot.ResolverSnapshot, ok bool) {
	if m == nil {
		return
	}
	m.Resolver.Samples++
	if !ok {
		m.Resolver.MissingSnapshot++
		return
	}
	m.Resolver.StateCounts.Observe(snap.State)
	projected := projectedResolverConfidenceOutcome(snap)
	m.Resolver.ProjectedConfidenceCounts.Observe(projected.Final)
	if projected.LegacyFinal == "?" && projected.Final == "P" {
		m.Resolver.LegacyUnknownNowP++
	}
}

func projectedResolverConfidenceOutcome(snap spot.ResolverSnapshot) replayConfidenceOutcome {
	if snap.State != spot.ResolverStateConfident && snap.State != spot.ResolverStateProbable {
		return replayConfidenceOutcome{Final: "?", LegacyFinal: "?"}
	}
	if strings.TrimSpace(snap.Winner) == "" || snap.WinnerSupport <= 0 || snap.TotalReporters <= 0 {
		return replayConfidenceOutcome{Final: "?", LegacyFinal: "?"}
	}
	percent := (snap.WinnerSupport * 100) / snap.TotalReporters
	newGlyph := formatConfidence(percent, snap.TotalReporters)
	legacyGlyph := formatConfidenceLegacy(percent, snap.TotalReporters)
	return replayConfidenceOutcome{
		Final:       normalizeConfidenceGlyph(newGlyph),
		LegacyFinal: normalizeConfidenceGlyph(legacyGlyph),
	}
}

func normalizeConfidenceGlyph(value string) string {
	trimmed := strutil.NormalizeUpper(strings.TrimSpace(value))
	switch trimmed {
	case "S", "P", "V", "C", "B":
		return trimmed
	case "?":
		return "?"
	case "":
		return ""
	default:
		return trimmed
	}
}

// formatConfidenceLegacy mirrors the pre-change confidence bucketing:
// multi-reporter confidence under 25% was '?' instead of 'P'.
func formatConfidenceLegacy(percent int, totalReporters int) string {
	if totalReporters <= 1 {
		return "?"
	}
	value := percent
	if value < 0 {
		value = 0
	}
	if value > 100 {
		value = 100
	}
	switch {
	case value >= 51:
		return "V"
	case value >= 25:
		return "P"
	default:
		return "?"
	}
}

func stabilizerDelayProxyEligible(s *spot.Spot, store *spot.RecentBandStore, cfg config.CallCorrectionConfig) bool {
	if s == nil || store == nil || !cfg.StabilizerEnabled || s.IsBeacon {
		return false
	}
	mode := s.ModeNorm
	if mode == "" {
		mode = s.Mode
	}
	if !spot.IsCallCorrectionCandidate(mode) {
		return false
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	if strings.TrimSpace(call) == "" {
		return false
	}
	band := s.BandNorm
	if strings.TrimSpace(band) == "" || band == "???" {
		band = spot.FreqToBand(s.Frequency)
	}
	band = spot.NormalizeBand(band)
	return band != "" && band != "???"
}

func shouldDelayTelnetByStabilizerReplay(s *spot.Spot, store *spot.RecentBandStore, cfg config.CallCorrectionConfig, now time.Time) bool {
	if s == nil || store == nil || !cfg.StabilizerEnabled || s.IsBeacon {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(s.Confidence), "P") {
		return false
	}
	mode := s.ModeNorm
	if mode == "" {
		mode = s.Mode
	}
	if !spot.IsCallCorrectionCandidate(mode) {
		return false
	}
	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	if strings.TrimSpace(call) == "" {
		return false
	}
	band := s.BandNorm
	if strings.TrimSpace(band) == "" || band == "???" {
		band = spot.FreqToBand(s.Frequency)
	}
	minUnique := cfg.RecentBandRecordMinUniqueSpotters
	if minUnique <= 0 {
		minUnique = 2
	}
	return !hasRecentSupportForCallFamily(store, call, band, mode, minUnique, now)
}

func wouldDelayTelnetByStabilizerWithConfidence(
	s *spot.Spot,
	store *spot.RecentBandStore,
	cfg config.CallCorrectionConfig,
	now time.Time,
	confidence string,
) bool {
	if s == nil {
		return false
	}
	originalConfidence := s.Confidence
	s.Confidence = normalizeConfidenceGlyph(confidence)
	delayed := shouldDelayTelnetByStabilizerReplay(s, store, cfg, now)
	s.Confidence = originalConfidence
	return delayed
}

func hasRecentSupportForCallFamily(store *spot.RecentBandStore, call, band, mode string, minUnique int, now time.Time) bool {
	if store == nil {
		return false
	}
	keys := spot.CorrectionFamilyKeys(call)
	if len(keys) == 0 {
		return store.HasRecentSupport(call, band, mode, minUnique, now)
	}
	for _, key := range keys {
		if store.HasRecentSupport(key, band, mode, minUnique, now) {
			return true
		}
	}
	return false
}
