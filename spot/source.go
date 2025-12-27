package spot

// Purpose: Determine whether a source type represents a skimmer feed.
// Key aspects: Only RBN/FT8/FT4/PSKReporter are treated as skimmers.
// Upstream: mode inference and source classification.
// Downstream: None (pure predicate).
// IsSkimmerSource reports whether a spot came from automated skimmer feeds.
// Only RBN and PSKReporter-originated sources are treated as skimmers.
func IsSkimmerSource(source SourceType) bool {
	switch source {
	case SourceRBN, SourceFT8, SourceFT4, SourcePSKReporter:
		return true
	default:
		return false
	}
}

// Purpose: Set IsHuman based solely on SourceType.
// Key aspects: Human is the inverse of IsSkimmerSource.
// Upstream: ingest and output pipelines.
// Downstream: IsSkimmerSource.
// ApplySourceHumanFlag enforces the source-based human/skimmer contract on a spot.
func ApplySourceHumanFlag(s *Spot) {
	if s == nil {
		return
	}
	s.IsHuman = !IsSkimmerSource(s.SourceType)
}
