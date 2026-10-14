// Test probes calculate intermediate quantiles through the same common-time
// capture and bin-selection code used in production. Connected Predictor tests
// additionally exercise the final-only quantile route.
package pathreliability

import "time"

func selectSampleWithDistribution(fine sampleWithBins, coarse sampleWithBins, minFineWeight float64, fineOnlyWeight float64) sampleWithBins {
	selected := selectSampleBins(fine, coarse, minFineWeight, fineOnlyWeight)
	return observedSampleDistribution(selected.Sample, selected.snrBins, selected.distributionUnavailable)
}

func (s *Store) lookupWithDistribution(receiverCell, senderCell CellID, receiverCoarse, senderCoarse CellID, band string, now time.Time) (fine sampleWithBins, coarse sampleWithBins) {
	candidates := s.lookupCandidateSet(receiverCell, senderCell, receiverCoarse, senderCoarse, band, now, false, sampleQuantile)
	return candidates[0], candidates[1]
}
