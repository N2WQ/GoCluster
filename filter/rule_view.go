// File role: Evaluation-local lazy normalization; spots remain immutable.
package filter

import (
	"math"
	"strings"

	"dxcluster/pathreliability"
	"dxcluster/spot"
	"dxcluster/strutil"
)

// One view belongs to one spot/user evaluation across all PASS and REJECT rules, never
// to a program or global cache. Empty normalized text means unknown. The loaded
// bits also cache that absence, so repeated unknown fields do not rescan input.
type ruleView struct {
	spot                 *spot.Spot
	pathClass            string
	fine, coarse         pathreliability.CellID
	nearbyLoaded, nearby bool
	loaded               uint32
	text                 [ruleFieldCount]string
}

func newRuleView(s *spot.Spot, pathClass string) ruleView {
	return ruleView{spot: s, pathClass: pathClass}
}

// EvaluationContext is captured by the caller for one user. Its zero value
// explicitly supplies unavailable location and path prediction. It is never
// retained by a prepared rule or used to read mutable client state.
type EvaluationContext struct {
	PathClass            string
	FineCell, CoarseCell pathreliability.CellID
}

func (v *ruleView) nearbyValue() bool {
	if !v.nearbyLoaded {
		v.nearbyLoaded = true
		v.nearby = matchesNearbyCells(v.spot, v.fine, v.coarse, "")
	}
	return v.nearby
}

func (v *ruleView) textValue(field ruleField) string {
	bit := uint32(1) << field
	if v.loaded&bit == 0 {
		v.loaded |= bit
		if v.spot != nil {
			v.text[field] = v.loadText(field)
		}
	}
	return v.text[field]
}

func (v *ruleView) loadText(field ruleField) string {
	s := v.spot
	switch field {
	case ruleFieldBand:
		band := spot.NormalizeBand(s.BandNorm)
		if band == "" {
			band = spot.NormalizeBand(s.Band)
		}
		if spot.IsValidBand(band) {
			return band
		}
	case ruleFieldMode:
		mode := canonicalModeToken(modeFilterTokenForSpot(s))
		if IsSupportedMode(mode) {
			return mode
		}
	case ruleFieldSource:
		if s.IsHuman {
			return "HUMAN"
		}
		return "SKIMMER"
	case ruleFieldDXCall:
		return strings.ToUpper(s.DXCall)
	case ruleFieldDECall:
		if s.DECallStripped != "" {
			return strings.ToUpper(s.DECallStripped)
		}
		return strings.ToUpper(s.DECall)
	case ruleFieldDXCont:
		return ruleContinentValue(s.DXContinentNorm, s.DXMetadata.Continent)
	case ruleFieldDECont:
		return ruleContinentValue(s.DEContinentNorm, s.DEMetadata.Continent)
	case ruleFieldDXState:
		if spot.IsState(s.DXMetadata.State) {
			return s.DXMetadata.State
		}
	case ruleFieldDEState:
		if spot.IsState(s.DEMetadata.State) {
			return s.DEMetadata.State
		}
	case ruleFieldDXGrid2:
		return ruleGrid2Value(s.DXGrid2, s.DXMetadata.Grid)
	case ruleFieldDEGrid2:
		return ruleGrid2Value(s.DEGrid2, s.DEMetadata.Grid)
	case ruleFieldConf:
		if !isConfidenceExemptMode(v.textValue(ruleFieldMode)) {
			return normalizeConfidenceSymbol(s.Confidence)
		}
	case ruleFieldPath:
		class := normalizePathClass(v.pathClass)
		if class == "" {
			return PathClassInsufficient
		}
		return class
	case ruleFieldComment:
		return s.Comment
	}
	return ""
}

func ruleContinentValue(normalized, raw string) string {
	if normalized == "" {
		normalized = raw
	}
	normalized = strutil.NormalizeUpper(normalized)
	if IsSupportedContinent(normalized) {
		return normalized
	}
	return ""
}

func ruleGrid2Value(normalized, raw string) string {
	if normalized == "" {
		normalized = raw
	}
	normalized = normalizeGrid2Token(normalized)
	if ValidGrid2Prefix(normalized) {
		return normalized
	}
	return ""
}

func (v *ruleView) integerValue(field ruleField) (int, bool) {
	if v.spot == nil {
		return 0, false
	}
	s := v.spot
	switch field {
	case ruleFieldSNR:
		return s.Report, s.HasReport
	case ruleFieldDXZone:
		return s.DXMetadata.CQZone, IsSupportedZone(s.DXMetadata.CQZone)
	case ruleFieldDEZone:
		return s.DEMetadata.CQZone, IsSupportedZone(s.DEMetadata.CQZone)
	case ruleFieldDXDXCC:
		return s.DXMetadata.ADIF, s.DXMetadata.ADIF > 0
	case ruleFieldDEDXCC:
		return s.DEMetadata.ADIF, s.DEMetadata.ADIF > 0
	default:
		return 0, false
	}
}

func (v *ruleView) frequencyValue() (float64, bool) {
	if v.spot == nil {
		return 0, false
	}
	frequency := v.spot.Frequency
	return frequency, frequency > 0 && !math.IsNaN(frequency) && !math.IsInf(frequency, 0)
}
