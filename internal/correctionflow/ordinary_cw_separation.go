// File role: Tests positive current-signal separation for the offline ordinary
// CW safeguard. Crawler notes: The replay caller supplies its frozen family
// mapping and the existing read-only frequency sources. No row escapes.
package correctionflow

import (
	"math/bits"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// OrdinaryCWSeparationInput borrows bounded receiver views from the replay
// owner. ReceiverFamily must return the frozen family for a reporter.
type OrdinaryCWSeparationInput struct {
	Key              spot.ResolverSignalKey
	Config           config.CallCorrectionConfig
	Sources          [5]FrequencySource
	Original, Winner string
	FocalKHz         float64
	At               time.Time
	FocalFamily      string
	ReceiverFamily   func(string) string
}

// OrdinaryCWSeparation requires distinct occupied groups and two families
// reporting both specific identities within one minute. It also requires two
// families other than the incoming reporter to have heard the original in the
// last minute. Missing or inconsistent evidence leaves ordinary authority intact.
func OrdinaryCWSeparation(in OrdinaryCWSeparationInput) bool {
	if in.Key.Mode != "CW" || in.Original == in.Winner || !frequencyModelCall(in.Original) ||
		!frequencyModelCall(in.Winner) || strings.Contains(in.Original, "/") || strings.Contains(in.Winner, "/") ||
		in.At.IsZero() || in.FocalFamily == "" || in.ReceiverFamily == nil || !positiveFrequency(in.FocalKHz) {
		return false
	}
	var assessment FrequencyAssessment
	rows, _, refusal := frequencySourceRows(in.Key, in.Sources, in.Config, &assessment)
	if refusal != "" {
		return false
	}
	_, masks, refusal := partitionFrequencyRows(rows, in.FocalKHz, 150, &assessment)
	if refusal != "" {
		return false
	}
	originalGroup := uint16(1) << assessment.AssociatedComponent
	winnerGroups := masks[in.Winner]
	if masks[in.Original] != originalGroup || bits.OnesCount16(winnerGroups) != 1 || winnerGroups == originalGroup {
		return false
	}
	return ordinaryCWFamilyWitnesses(in, rows)
}

func ordinaryCWFamilyWitnesses(in OrdinaryCWSeparationInput, rows []frequencyRow) bool {
	original := frequencyComponent{receivers: make(map[string][]time.Time)}
	winner := frequencyComponent{receivers: make(map[string][]time.Time)}
	fresh := make(map[string]struct{})
	for _, row := range rows {
		r := row.observation
		if r.ObservedAt.IsZero() || r.ObservedAt.After(in.At) {
			return false
		}
		if strings.Contains(row.call, "/") {
			for part := range strings.SplitSeq(row.call, "/") {
				if part == in.Original || part == in.Winner {
					return false
				}
			}
		}
		if row.call != in.Original && row.call != in.Winner {
			continue
		}
		if !knownFrequency(r) {
			return false
		}
		age := in.At.Sub(r.ObservedAt)
		if r.WeightMilli <= 0 || age > 5*time.Minute {
			continue
		}
		family := in.ReceiverFamily(r.Receiver)
		if family == "" {
			return false
		}
		if row.call == in.Winner {
			winner.receivers[family] = append(winner.receivers[family], r.ObservedAt)
		} else {
			original.receivers[family] = append(original.receivers[family], r.ObservedAt)
			if age <= time.Minute && family != in.FocalFamily {
				fresh[family] = struct{}{}
			}
		}
	}
	return len(fresh) >= 2 && frequencyPairSeparated(original, winner, time.Minute)
}
