package spot

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

// ApplySourceHumanFlag enforces the source-based human/skimmer contract on a spot.
func ApplySourceHumanFlag(s *Spot) {
	if s == nil {
		return
	}
	s.IsHuman = !IsSkimmerSource(s.SourceType)
}
