package correctionflow

import (
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// StabilizerDelayReason explains why a spot is held by the telnet stabilizer.
type StabilizerDelayReason string

const (
	StabilizerDelayReasonNone               StabilizerDelayReason = "none"
	StabilizerDelayReasonUnknownOrNonRecent StabilizerDelayReason = "unknown_or_nonrecent"
	StabilizerDelayReasonAmbiguous          StabilizerDelayReason = "ambiguous_resolver"
	StabilizerDelayReasonPLowConfidence     StabilizerDelayReason = "p_low_confidence"
	StabilizerDelayReasonEditNeighbor       StabilizerDelayReason = "edit_neighbor_contested"
)

func (r StabilizerDelayReason) String() string {
	v := strings.TrimSpace(string(r))
	if v == "" {
		return string(StabilizerDelayReasonNone)
	}
	return v
}

// StabilizerDelayDecision is one stabilizer-gating verdict for a spot.
type StabilizerDelayDecision struct {
	ShouldDelay bool
	Reason      StabilizerDelayReason
	MaxChecks   int
}

// ShouldDelayTelnetByStabilizer reports baseline stabilizer hold eligibility
// without resolver evidence.
func ShouldDelayTelnetByStabilizer(s *spot.Spot, store spot.RecentSupportStore, cfg config.CallCorrectionConfig, now time.Time) bool {
	decision := EvaluateStabilizerDelay(s, store, cfg, now, spot.ResolverSnapshot{}, false)
	return decision.ShouldDelay
}

// EvaluateStabilizerDelay computes stabilizer delay decision and retry budget.
func EvaluateStabilizerDelay(
	s *spot.Spot,
	store spot.RecentSupportStore,
	cfg config.CallCorrectionConfig,
	now time.Time,
	resolverSnapshot spot.ResolverSnapshot,
	resolverSnapshotOK bool,
) StabilizerDelayDecision {
	decision := StabilizerDelayDecision{
		ShouldDelay: false,
		Reason:      StabilizerDelayReasonNone,
		MaxChecks:   normalizeStabilizerChecks(cfg.StabilizerMaxChecks, 1),
	}
	if s == nil || store == nil || !cfg.StabilizerEnabled || s.IsBeacon {
		return decision
	}

	mode := s.ModeNorm
	if mode == "" {
		mode = s.Mode
	}
	if !spot.IsCallCorrectionCandidate(mode) {
		return decision
	}

	call := s.DXCallNorm
	if call == "" {
		call = s.DXCall
	}
	if strings.TrimSpace(call) == "" {
		return decision
	}

	band := s.BandNorm
	if strings.TrimSpace(band) == "" || band == "???" {
		band = spot.FreqToBand(s.Frequency)
	}
	minUnique := cfg.RecentBandRecordMinUniqueSpotters
	if minUnique <= 0 {
		minUnique = 2
	}
	glyph := stabilizerConfidenceGlyph(s.Confidence)
	if !stabilizerDelayEligibleGlyph(glyph) {
		return decision
	}

	if resolverSnapshotOK && (resolverSnapshot.State == spot.ResolverStateSplit || resolverSnapshot.State == spot.ResolverStateUncertain) {
		decision.ShouldDelay = true
		decision.Reason = StabilizerDelayReasonAmbiguous
		decision.MaxChecks = normalizeStabilizerChecks(cfg.StabilizerAmbiguousMaxChecks, decision.MaxChecks)
		return decision
	}

	if glyph == "P" {
		if cfg.StabilizerPDelayConfidencePercent > 0 && cfg.StabilizerPDelayMaxChecks > 0 {
			if percent, ok := stabilizerCallConfidencePercent(s, resolverSnapshot, resolverSnapshotOK); ok && percent < cfg.StabilizerPDelayConfidencePercent {
				decision.ShouldDelay = true
				decision.Reason = StabilizerDelayReasonPLowConfidence
				decision.MaxChecks = normalizeStabilizerChecks(cfg.StabilizerPDelayMaxChecks, decision.MaxChecks)
				return decision
			}
		}
		// Preserve legacy P pass-through when low-confidence P policy is not
		// configured or confidence evidence is unavailable.
		return decision
	}

	if cfg.StabilizerEditNeighborEnabled {
		minSpotters := cfg.StabilizerEditNeighborMinSpotters
		if minSpotters <= 0 {
			minSpotters = minUnique
		}
		if hasRecentSupportForEditNeighbor(store, call, band, mode, minSpotters, now) {
			decision.ShouldDelay = true
			decision.Reason = StabilizerDelayReasonEditNeighbor
			decision.MaxChecks = normalizeStabilizerChecks(cfg.StabilizerEditNeighborMaxChecks, decision.MaxChecks)
			return decision
		}
	}

	if !HasRecentSupportForCallFamily(store, call, band, mode, minUnique, now) {
		decision.ShouldDelay = true
		decision.Reason = StabilizerDelayReasonUnknownOrNonRecent
		return decision
	}

	return decision
}

// ShouldRetryStabilizerDelay reports whether a delayed spot should be held for
// another stabilizer cycle.
func ShouldRetryStabilizerDelay(decision StabilizerDelayDecision, checksCompleted int) bool {
	if !decision.ShouldDelay {
		return false
	}
	maxChecks := normalizeStabilizerChecks(decision.MaxChecks, 1)
	return checksCompleted < maxChecks
}

// StabilizerReleaseReason returns the final reason label for delayed release
// counters, preferring the current decision when available.
func StabilizerReleaseReason(decision StabilizerDelayDecision, prior string) string {
	if decision.Reason != StabilizerDelayReasonNone {
		return decision.Reason.String()
	}
	trimmed := strings.TrimSpace(prior)
	if trimmed == "" {
		return StabilizerDelayReasonNone.String()
	}
	return trimmed
}

// HasRecentSupportForCallFamily checks recent support across family identities.
func HasRecentSupportForCallFamily(store spot.RecentSupportStore, call, band, mode string, minUnique int, now time.Time) bool {
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

func normalizeStabilizerChecks(preferred int, fallback int) int {
	if preferred > 0 {
		return preferred
	}
	if fallback > 0 {
		return fallback
	}
	return 1
}

func stabilizerCallConfidencePercent(s *spot.Spot, snapshot spot.ResolverSnapshot, snapshotOK bool) (int, bool) {
	if s == nil || !snapshotOK {
		return 0, false
	}
	call := NormalizedDXCall(s)
	if call == "" {
		return 0, false
	}
	return ResolverCallConfidencePercent(snapshot, call)
}

func hasRecentSupportForEditNeighbor(store spot.RecentSupportStore, call, band, mode string, minUnique int, now time.Time) bool {
	if store == nil {
		return false
	}
	if minUnique <= 0 {
		minUnique = 2
	}
	ownSupport := maxRecentSupportForCallFamily(store, call, band, mode, now)
	for _, variant := range EditDistanceOneSubstitutionVariants(call) {
		support := store.RecentSupportCount(variant, band, mode, now)
		if support < minUnique {
			continue
		}
		if support >= ownSupport {
			return true
		}
	}
	return false
}

func maxRecentSupportForCallFamily(store spot.RecentSupportStore, call, band, mode string, now time.Time) int {
	if store == nil {
		return 0
	}
	maxSupport := 0
	keys := spot.CorrectionFamilyKeys(call)
	if len(keys) == 0 {
		keys = []string{spot.CorrectionVoteKey(call)}
	}
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			continue
		}
		support := store.RecentSupportCount(key, band, mode, now)
		if support > maxSupport {
			maxSupport = support
		}
	}
	return maxSupport
}

func stabilizerConfidenceGlyph(confidence string) string {
	trimmed := strings.ToUpper(strings.TrimSpace(confidence))
	if trimmed == "" {
		return "?"
	}
	return trimmed[:1]
}

func stabilizerDelayEligibleGlyph(glyph string) bool {
	switch glyph {
	case "?", "S", "P":
		return true
	default:
		return false
	}
}
