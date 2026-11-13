package voacap

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

const (
	SolarSourceNOAANoonF107       uint16 = 1
	SolarSourceNOAASunspotReport  uint16 = 2
	SolarUnitsSFU                 uint16 = 1
	SolarUnitsLocalSunspotIndex   uint16 = 2
	SolarConversionITU            uint16 = 1
	SolarConversionLocalCountEWMA uint16 = 2
)
const (
	SolarInputRecentPresent uint16 = 1 << iota
	SolarInputMeanPresent
	SolarInputBlendPresent
)

// SolarInputProvenance is an 80-byte pointer-free value. SourceID determines
// the units and meaning of its scalar slots. Legacy flux values remain SFU;
// count-source values are the local reported-group index, not official SSN.
// JSON adapters retain the original flux keys without labeling counts as SFU.
// LatestValue/SmoothedValue hold raw/recent values for both sources. The three
// remaining value slots and ReferenceObservedUnix are legacy-flux-only: the
// published mean, blended flux and recent share, respectively; counts leave
// them zero. InputUpdatedUnix records local acceptance, not observation age.
type SolarInputProvenance struct {
	RecentObservedUnix    int64
	ReferenceObservedUnix int64
	InputUpdatedUnix      int64
	EWMAHalfLifeSeconds   int64
	LatestValue           float64
	SmoothedValue         float64
	ReferenceValue        float64
	EffectiveValue        float64
	ReferenceWeight       float64
	SourceID              uint16
	UnitsID               uint16
	ConversionID          uint16
	Flags                 uint16
}

// SolarInputSnapshot copies SSN and provenance together under the monitor lock.
// Its 64-bit Go size is 88 bytes; it owns no source history or mutable state.
type SolarInputSnapshot struct {
	SSN        int                  `json:"ssn"`
	Provenance SolarInputProvenance `json:"provenance"`
}

// Valid checks the source-tagged value without allocating on forecast paths.
func (p SolarInputProvenance) Valid() bool {
	for _, v := range [...]float64{p.LatestValue, p.SmoothedValue, p.ReferenceValue, p.EffectiveValue, p.ReferenceWeight} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return false
		}
	}
	switch p.SourceID {
	case 0:
		return p == (SolarInputProvenance{})
	case SolarSourceNOAANoonF107:
		return p.UnitsID == SolarUnitsSFU && p.ConversionID == SolarConversionITU && p.Flags & ^uint16(7) == 0 && p.ReferenceWeight <= 1
	case SolarSourceNOAASunspotReport:
		return p.UnitsID == SolarUnitsLocalSunspotIndex && p.ConversionID == SolarConversionLocalCountEWMA && p.Flags == SolarInputRecentPresent && p.ReferenceObservedUnix == 0 && p.ReferenceValue == 0 && p.EffectiveValue == 0 && p.ReferenceWeight == 0 && p.RecentObservedUnix > 0 && p.InputUpdatedUnix >= p.RecentObservedUnix && p.EWMAHalfLifeSeconds > 0 && validRawCount(p.LatestValue) && finiteNonnegativeCount(p.SmoothedValue) && p.SmoothedValue >= 10
	default:
		return false
	}
}

type solarCommonWire struct {
	RecentObservedUnix  int64  `json:"recent_observed_unix"`
	InputUpdatedUnix    int64  `json:"input_updated_unix"`
	EWMAHalfLifeSeconds int64  `json:"ewma_half_life_seconds"`
	SourceID            uint16 `json:"source_id"`
	UnitsID             uint16 `json:"units_id"`
	ConversionID        uint16 `json:"conversion_id"`
	Flags               uint16 `json:"flags"`
}
type solarFluxWire struct {
	solarCommonWire
	ReferenceObservedUnix int64   `json:"mean_observed_unix"`
	LatestValue           float64 `json:"latest_noon_flux_sfu"`
	SmoothedValue         float64 `json:"recent_ewma_flux_sfu"`
	ReferenceValue        float64 `json:"published_mean_flux_sfu"`
	EffectiveValue        float64 `json:"blended_flux_sfu"`
	ReferenceWeight       float64 `json:"recent_weight"`
}
type solarCountWire struct {
	solarCommonWire
	LatestValue   float64 `json:"latest_raw_count_index"`
	SmoothedValue float64 `json:"smoothed_count_index"`
}

func (p SolarInputProvenance) commonWire() solarCommonWire {
	return solarCommonWire{p.RecentObservedUnix, p.InputUpdatedUnix, p.EWMAHalfLifeSeconds, p.SourceID, p.UnitsID, p.ConversionID, p.Flags}
}
func (p SolarInputProvenance) MarshalJSON() ([]byte, error) {
	if !p.Valid() {
		return nil, errors.New("invalid solar input provenance")
	}
	if p.SourceID == SolarSourceNOAANoonF107 {
		return json.Marshal(solarFluxWire{p.commonWire(), p.ReferenceObservedUnix, p.LatestValue, p.SmoothedValue, p.ReferenceValue, p.EffectiveValue, p.ReferenceWeight})
	}
	if p.SourceID == SolarSourceNOAASunspotReport {
		return json.Marshal(solarCountWire{p.commonWire(), p.LatestValue, p.SmoothedValue})
	}
	return json.Marshal(p.commonWire())
}
func (p *SolarInputProvenance) UnmarshalJSON(body []byte) error {
	fields, err := uniqueJSONObject(body)
	if err != nil {
		return err
	}
	var common solarCommonWire
	if err = json.Unmarshal(body, &common); err != nil {
		return err
	}
	next := SolarInputProvenance{RecentObservedUnix: common.RecentObservedUnix, InputUpdatedUnix: common.InputUpdatedUnix, EWMAHalfLifeSeconds: common.EWMAHalfLifeSeconds, SourceID: common.SourceID, UnitsID: common.UnitsID, ConversionID: common.ConversionID, Flags: common.Flags}
	allowed := map[string]bool{"recent_observed_unix": true, "input_updated_unix": true, "ewma_half_life_seconds": true, "source_id": true, "units_id": true, "conversion_id": true, "flags": true}
	switch common.SourceID {
	case SolarSourceNOAANoonF107:
		var w solarFluxWire
		if err = json.Unmarshal(body, &w); err != nil {
			return err
		}
		next.ReferenceObservedUnix, next.LatestValue, next.SmoothedValue, next.ReferenceValue, next.EffectiveValue, next.ReferenceWeight = w.ReferenceObservedUnix, w.LatestValue, w.SmoothedValue, w.ReferenceValue, w.EffectiveValue, w.ReferenceWeight
		for _, k := range []string{"mean_observed_unix", "latest_noon_flux_sfu", "recent_ewma_flux_sfu", "published_mean_flux_sfu", "blended_flux_sfu", "recent_weight"} {
			allowed[k] = true
		}
	case SolarSourceNOAASunspotReport:
		var w solarCountWire
		if err = json.Unmarshal(body, &w); err != nil {
			return err
		}
		next.LatestValue, next.SmoothedValue = w.LatestValue, w.SmoothedValue
		allowed["latest_raw_count_index"], allowed["smoothed_count_index"] = true, true
	case 0:
		// Older SSN-only providers wrote all legacy scalar keys as zero.
		var w solarFluxWire
		if err = json.Unmarshal(body, &w); err != nil {
			return err
		}
		if w.ReferenceObservedUnix != 0 || w.LatestValue != 0 || w.SmoothedValue != 0 || w.ReferenceValue != 0 || w.EffectiveValue != 0 || w.ReferenceWeight != 0 {
			return errors.New("unknown solar provenance contains values")
		}
		for _, k := range []string{"mean_observed_unix", "latest_noon_flux_sfu", "recent_ewma_flux_sfu", "published_mean_flux_sfu", "blended_flux_sfu", "recent_weight"} {
			allowed[k] = true
		}
	default:
		return errors.New("unsupported solar input source")
	}
	if common.SourceID != 0 {
		for key := range allowed {
			if _, ok := fields[key]; !ok {
				return fmt.Errorf("missing solar provenance field %q", key)
			}
		}
	}
	for k, v := range fields {
		if !allowed[k] || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return fmt.Errorf("unsupported solar provenance field %q", k)
		}
	}
	if !next.Valid() {
		return errors.New("invalid solar input provenance")
	}
	*p = next
	return nil
}

// uniqueJSONObject rejects repeated members at persistence boundaries.
func uniqueJSONObject(body []byte) (map[string]json.RawMessage, error) {
	fields, duplicates, err := decodeJSONObject(body)
	if err != nil {
		return nil, err
	}
	for key := range duplicates {
		return nil, fmt.Errorf("duplicate JSON member %q", key)
	}
	return fields, nil
}

// decodeJSONObject preserves duplicate evidence without selecting a last value.
// Source parsing can isolate a bad group only if report identity is unambiguous.
func decodeJSONObject(body []byte) (map[string]json.RawMessage, map[string]bool, error) {
	d := json.NewDecoder(bytes.NewReader(body))
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		return nil, nil, errors.New("expected JSON object")
	}
	fields := make(map[string]json.RawMessage)
	duplicates := make(map[string]bool)
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return nil, nil, err
		}
		name, ok := key.(string)
		if !ok {
			return nil, nil, errors.New("invalid JSON member")
		}
		var raw json.RawMessage
		if err = d.Decode(&raw); err != nil {
			return nil, nil, err
		}
		if _, exists := fields[name]; exists {
			duplicates[name] = true
		} else {
			fields[name] = raw
		}
	}
	if _, err = d.Token(); err != nil {
		return nil, nil, err
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, nil, errors.New("trailing JSON content")
	}
	return fields, duplicates, nil
}

// Valid preserves unknown SSN-only providers and checks count-source arithmetic.
// Legacy flux provenance is never recomputed or relabeled during restoration.
func (s SolarInputSnapshot) Valid() bool {
	if s.SSN < 0 || !s.Provenance.Valid() {
		return false
	}
	if s.Provenance.SourceID == SolarSourceNOAASunspotReport {
		ssn, err := roundedCountSSN(s.Provenance.SmoothedValue)
		return err == nil && ssn == s.SSN
	}
	return true
}

func solarInputFromState(s sunspotMonitorState, now time.Time) (SolarInputSnapshot, bool, error) {
	if !s.EWMAInitialized {
		return SolarInputSnapshot{}, false, nil
	}
	ssn, err := roundedCountSSN(s.SmoothedCountIndex)
	if err != nil {
		return SolarInputSnapshot{}, false, err
	}
	p := SolarInputProvenance{RecentObservedUnix: s.LastObservedAtUTC.Unix(), InputUpdatedUnix: now.Unix(), EWMAHalfLifeSeconds: s.Identity.EWMAHalfLifeSeconds, LatestValue: s.LastRawCountIndex, SmoothedValue: s.SmoothedCountIndex, SourceID: SolarSourceNOAASunspotReport, UnitsID: SolarUnitsLocalSunspotIndex, ConversionID: SolarConversionLocalCountEWMA, Flags: SolarInputRecentPresent}
	return SolarInputSnapshot{SSN: ssn, Provenance: p}, true, nil
}
