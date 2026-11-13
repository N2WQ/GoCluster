package voacap

import "time"

// Solar input identifiers describe the numeric provenance saved with a forecast.
// Zero identifiers and flags mean unknown provenance, such as SSN-only providers.
const (
	SolarSourceNOAANoonF107 uint16 = 1
	SolarUnitsSFU           uint16 = 1
	SolarConversionITU      uint16 = 1
)

const (
	SolarInputRecentPresent uint16 = 1 << iota
	SolarInputMeanPresent
	SolarInputBlendPresent
)

// SolarInputProvenance is an 80-byte, pointer-free copy of the accepted solar
// input. Flux values use solar flux units (10^-22 W m^-2 Hz^-1), not SSN. Unix
// timestamps describe observations; InputUpdatedUnix is local acceptance time.
// The presence flags distinguish missing components from valid numeric zero.
// No observation history or mutable monitor state escapes through this value.
type SolarInputProvenance struct {
	RecentObservedUnix  int64 `json:"recent_observed_unix"`
	MeanObservedUnix    int64 `json:"mean_observed_unix"`
	InputUpdatedUnix    int64 `json:"input_updated_unix"`
	EWMAHalfLifeSeconds int64 `json:"ewma_half_life_seconds"`

	LatestNoonFluxSFU    float64 `json:"latest_noon_flux_sfu"`
	RecentEWMAFluxSFU    float64 `json:"recent_ewma_flux_sfu"`
	PublishedMeanFluxSFU float64 `json:"published_mean_flux_sfu"`
	BlendedFluxSFU       float64 `json:"blended_flux_sfu"`
	RecentWeight         float64 `json:"recent_weight"`

	SourceID     uint16 `json:"source_id"`
	UnitsID      uint16 `json:"units_id"`
	ConversionID uint16 `json:"conversion_id"`
	Flags        uint16 `json:"flags"`
}

// SolarInputSnapshot captures SSN and its actual input together under the monitor
// lock. A forecast job must copy this value once, rather than resampling input
// metadata after separately obtaining an SSN. Its 64-bit Go size is 88 bytes.
type SolarInputSnapshot struct {
	SSN        int                  `json:"ssn"`
	Provenance SolarInputProvenance `json:"provenance"`
}

// solarInputFromState preserves zero-weight semantics explicitly: absent or
// invalid noncontributing data must not enter arithmetic through 0*value.
func solarInputFromState(s sunspotMonitorState, now time.Time) (SolarInputSnapshot, bool, error) {
	w := s.Identity.RecentWeight
	if (w > 0 && !s.EWMAInitialized) || (w < 1 && !s.MeanInitialized) {
		return SolarInputSnapshot{}, false, nil
	}
	blend := s.PublishedMeanSFU
	if w == 1 {
		blend = s.RecentFluxEWMA
	} else if w != 0 {
		blend = w*s.RecentFluxEWMA + (1-w)*s.PublishedMeanSFU
	}
	ssn, err := ssnFromSolarFlux(blend)
	if err != nil {
		return SolarInputSnapshot{}, false, err
	}
	p := SolarInputProvenance{
		InputUpdatedUnix: now.Unix(), EWMAHalfLifeSeconds: s.Identity.EWMAHalfLifeSeconds,
		LatestNoonFluxSFU: s.LastNoonFluxSFU, RecentEWMAFluxSFU: s.RecentFluxEWMA,
		PublishedMeanFluxSFU: s.PublishedMeanSFU, BlendedFluxSFU: blend, RecentWeight: w,
		SourceID: SolarSourceNOAANoonF107, UnitsID: SolarUnitsSFU, ConversionID: SolarConversionITU,
		Flags: SolarInputBlendPresent,
	}
	if s.EWMAInitialized {
		p.RecentObservedUnix = s.LastObservedAtUTC.Unix()
		p.Flags |= SolarInputRecentPresent
	}
	if s.MeanInitialized {
		p.MeanObservedUnix = s.MeanObservedAtUTC.Unix()
		p.Flags |= SolarInputMeanPresent
	}
	return SolarInputSnapshot{SSN: ssn, Provenance: p}, true, nil
}
