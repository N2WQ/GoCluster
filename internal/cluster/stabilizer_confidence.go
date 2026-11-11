// File role: Keeps held display confidence separate from fresh resolver evidence.
// The finite scalar follows the existing bounded scheduler ownership; it owns
// no callsign, history, allocation, or cleanup dependency. Entry telemetry is
// independent. Only the same literal identity can retain ordinary confidence.
// Related docs: spot/README.md#live-stabilizer-output, ui/README.md#stabilizer-matrix.
// Related tests: internal/cluster/stabilizer_confidence_test.go.
package cluster

import "strings"

type stabilizerOrdinaryConfidence uint8

const (
	stabilizerConfidenceUnknown stabilizerOrdinaryConfidence = iota
	stabilizerConfidenceSupported
	stabilizerConfidencePartial
	stabilizerConfidenceVerified
)

func ordinaryStabilizerConfidence(glyph string) (stabilizerOrdinaryConfidence, bool) {
	switch strings.ToUpper(strings.TrimSpace(glyph)) {
	case "", "?":
		return stabilizerConfidenceUnknown, true
	case "S":
		return stabilizerConfidenceSupported, true
	case "P":
		return stabilizerConfidencePartial, true
	case "V":
		return stabilizerConfidenceVerified, true
	default:
		return stabilizerConfidenceUnknown, false
	}
}

// terminalStabilizerConfidence computes output without changing the working
// Spot. Callers must finish fresh validation, holding and history admission
// before projecting this result; neither percentages nor C/B are retained.
func terminalStabilizerConfidence(fresh string, best stabilizerOrdinaryConfidence) (string, bool) {
	rank, ordinary := ordinaryStabilizerConfidence(fresh)
	if !ordinary || best <= rank || best > stabilizerConfidenceVerified {
		return fresh, false
	}
	switch best {
	case stabilizerConfidenceSupported:
		return "S", true
	case stabilizerConfidencePartial:
		return "P", true
	case stabilizerConfidenceVerified:
		return "V", true
	default:
		return fresh, false
	}
}
