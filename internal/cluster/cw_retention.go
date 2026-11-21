// File role: Preserve an eligible original RBN CW call at the ordinary mutation
// boundary. A retention is terminal for extra rescue and never confirms a rename.
package cluster

import (
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

// An independently retained call could be renamed again by optional Method C.
// The same-release certificate is only valid when that extra rescue is off.
func cwBurstComparisonEnabled(cfg *config.CallCorrectionConfig) bool {
	return cfg.Enabled && cfg.CWBurstWPMEnabled && !cfg.SelectiveRescueEnabled
}

const (
	cwFrequencyRetained = "cw_frequency_separation_retained"
	cwBurstRetained     = "cw_burst_wpm_decay_retained"
)

type cwRetentionRequest struct {
	pipeline   *outputPipeline
	context    *outputSpotContext
	immediate  bool
	admittedAt time.Time
}

func (r *cwRetentionRequest) evaluate(key spot.ResolverSignalKey, original, winner string, at time.Time) string {
	p, ctx := r.pipeline, r.context
	cfg := &p.correctionCfg
	s := ctx.spot
	if key.Mode != "CW" || (!cfg.CWFrequencySeparationEnabled && !cfg.CWBurstWPMEnabled) ||
		s.SourceType != spot.SourceRBN || s.RBNCQReceivedAt.IsZero() || s.IsHuman || s.IsBeacon {
		return ""
	}
	if cfg.CWFrequencySeparationEnabled {
		sources := correctionflow.ReadFrequencySources(p.signalResolver, key, *cfg, at)
		// These independent read-only publications can overlap an owner update.
		// A newer read cannot establish separation for an earlier release.
		current := true
		for i := range sources {
			if sources[i].Present && sources[i].Snapshot.EvaluatedAt.After(at) {
				current = false
				break
			}
		}
		if current && correctionflow.OrdinaryCWSeparation(correctionflow.OrdinaryCWSeparationInput{
			Key: key, Config: *cfg, Sources: sources, Original: original, Winner: winner,
			FocalKHz: s.Frequency, At: at, FocalFamily: correctionflow.CWReceiverFamily(s.DECall),
			ReceiverFamily: correctionflow.CWReceiverFamily,
		}) && !replacementLicenseRejected(s, original, ctx.ctyDB, p.metaCache) {
			return cwFrequencyRetained
		}
	}
	if cfg.CWBurstWPMEnabled && r.immediate && original == s.CWBurstFocal.Original &&
		p.cwBursts.Retain(s.CWBurstFocal, winner) {
		if snapshot, ok := p.cwDecayRetainsImmediate(ctx, key, r.admittedAt); ok {
			setResolverConfidence(s, snapshot, true, original)
			return cwBurstRetained
		}
	}
	return ""
}
