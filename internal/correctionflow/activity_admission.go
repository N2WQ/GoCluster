// File role: Defines the immutable activity population shared by live primary
// admission and historical replay. Correction retries only query this population.
package correctionflow

import (
	"time"

	"dxcluster/spot"
)

// AdmitResolverActivity records eligible activity exactly when admit succeeds.
// Call only after validation and flood policy. The sink must be a nonblocking
// primary-input send without other locks, callbacks, or adaptive queries.
// Capturing the tuple before publication makes downstream mutation race-safe.
// A nil controller means adaptation is disabled; correction's enabled setting
// deliberately does not control incoming-activity accounting.
func AdmitResolverActivity(s *spot.Spot, adaptive *spot.AdaptiveMinReports, clock func() time.Time, admit func() bool) bool {
	if adaptive == nil {
		return admit()
	}
	band, reporter, eligible := resolverActivityTuple(s)
	if !eligible {
		return admit()
	}
	return adaptive.AdmitObservation(band, reporter, clock, admit)
}

// ObserveResolverActivity is the synchronous replay admission counterpart.
// at is the original historical event time, never a later retry/release time.
func ObserveResolverActivity(s *spot.Spot, adaptive *spot.AdaptiveMinReports, at time.Time) {
	if adaptive == nil {
		return
	}
	band, reporter, eligible := resolverActivityTuple(s)
	if eligible {
		adaptive.Observe(band, reporter, at)
	}
}

func resolverActivityTuple(s *spot.Spot) (band, reporter string, eligible bool) {
	if s == nil || s.IsBeacon || spot.IsLocalSelfSpot(s) {
		return "", "", false
	}
	mode := s.ModeNorm
	if mode == "" {
		mode = s.Mode
	}
	// Mode inference remains downstream. Unknown ingress modes do not count.
	if !spot.SourceSkewCorrectedMode(mode) {
		return "", "", false
	}
	band = s.Band
	if band == "" {
		band = spot.FreqToBand(s.Frequency)
	}
	reporter = s.DECallNorm
	if reporter == "" {
		reporter = s.DECall
	}
	return band, reporter, band != "" && reporter != ""
}
