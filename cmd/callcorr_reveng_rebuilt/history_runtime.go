// File role: Constructs reverse-engineering resolver/history owners only after
// validating the gates this tool actually consumes against its legacy store.
package main

import (
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

// newRevengHistoryOwners preserves this tool's legacy backend even when the
// configuration selects CustomSCP. It runs resolver gates, not live S floors
// or stabilizer scheduling. Reject impossible consumed requirements before
// constructing any relevant owner; earlier file/model loading remains separate.
func newRevengHistoryOwners(c config.CallCorrectionConfig, reliability, cw, rtty spot.SpotterReliability, model *spot.ConfusionModel) (*spot.SignalResolver, *spot.SignalResolverDriver, *spot.RecentBandStore, error) {
	useHistory := c.Enabled && (c.RecentBandBonusEnabled || c.StabilizerEnabled)
	if useHistory {
		if err := c.ValidateLegacyHistoryCapacity("reverse-engineering legacy recent-band", spot.RecentBandDefaultMaxSpottersPerCall,
			config.LegacyHistoryConsumers{Resolver: true}); err != nil {
			return nil, nil, nil, err
		}
	}
	resolver := spot.NewSignalResolver(spot.SignalResolverConfig{
		QueueSize:                 resolverQueueSize,
		MaxActiveKeys:             resolverMaxActiveKeys,
		MaxCandidatesPerKey:       resolverMaxCandidatesPerKey,
		MaxReportersPerCand:       resolverMaxReportersPerCand,
		InactiveTTL:               resolverInactiveTTL,
		EvalMinInterval:           resolverEvalMinInterval,
		SweepInterval:             resolverSweepInterval,
		HysteresisWindows:         resolverHysteresisWindows,
		FreqGuardMinSeparationKHz: c.FreqGuardMinSeparationKHz,
		FreqGuardRunnerUpRatio:    c.FreqGuardRunnerUpRatio,
		MaxEditDistance:           c.MaxEditDistance,
		DistanceModelCW:           c.DistanceModelCW,
		DistanceModelRTTY:         c.DistanceModelRTTY,
		SpotterReliability:        reliability,
		SpotterReliabilityCW:      cw,
		SpotterReliabilityRTTY:    rtty,
		MinSpotterReliability:     c.MinSpotterReliability,
		ConfusionModel:            model,
		ConfusionWeight:           c.ConfusionModelWeight,
		FamilyPolicy: spot.CorrectionFamilyPolicy{
			Configured:                 true,
			TruncationEnabled:          c.FamilyPolicy.Truncation.Enabled,
			TruncationMaxLengthDelta:   c.FamilyPolicy.Truncation.MaxLengthDelta,
			TruncationMinShorterLength: c.FamilyPolicy.Truncation.MinShorterLength,
			TruncationAllowPrefix:      c.FamilyPolicy.Truncation.AllowPrefixMatch,
			TruncationAllowSuffix:      c.FamilyPolicy.Truncation.AllowSuffixMatch,
		},
	})
	driver, err := spot.NewSignalResolverDriver(resolver)
	if err != nil {
		return nil, nil, nil, err
	}
	var history *spot.RecentBandStore
	if useHistory {
		history = spot.NewRecentBandStore(time.Duration(c.RecentBandWindowSeconds) * time.Second)
	}
	return resolver, driver, history, nil
}
