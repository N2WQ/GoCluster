// File role: Certify that a separate CW decay decision keeps a raw spelling
// on the same immediate release. No model confirmations, history writes,
// hold admissions or output counters are performed for the comparison.
package cluster

import (
	"time"

	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

// cwDecayRetainsImmediate uses a sufficient subset of the native terminal
// KEEP outcomes. A released ordinary hold cannot prove that the independent
// temporal or stabilizer arm ever admitted that same hold. An overflow is
// similarly insufficient. Dispatch and queueing remain owned by the live arm.
func (p *outputPipeline) cwDecayRetainsImmediate(ctx *outputSpotContext, key spot.ResolverSignalKey, now time.Time) (spot.ResolverSnapshot, bool) {
	if p == nil || ctx == nil || ctx.spot == nil || p.signalResolver == nil || key.Mode != "CW" ||
		now.IsZero() || ctx.temporalResult != nil || !ctx.resolverReleaseAt.IsZero() || ctx.stabilizerHeld ||
		p.correctionCfg.SelectiveRescueEnabled {
		return spot.ResolverSnapshot{}, false
	}
	subject := normalizedDXCall(ctx.spot)
	selection := correctionflow.SelectCWDecaySnapshotForCallAt(p.signalResolver, ctx.cwDecayPrior, key, p.correctionCfg, subject, now)
	snapshot := selection.Snapshot
	// Neighborhood selections keep merged receiver tallies and a generation
	// witness per admitted bucket. They intentionally omit a raw receiver view.
	if !selection.SnapshotOK || snapshot.EvidenceLoss || snapshot.TotalWeightedSupportMilli <= 0 ||
		correctionflow.ResolverWeightedSupportForCall(snapshot, subject) <= 0 ||
		p.temporal != nil && p.temporal.Enabled() && p.temporal.decoder.ShouldHoldSelection(selection) {
		return spot.ResolverSnapshot{}, false
	}
	if !p.cwDecayKeepsIdentity(ctx.spot, key, selection, now) {
		return spot.ResolverSnapshot{}, false
	}
	// Replay records original licensing and only *predicts* stabilizer delay.
	// The live stabilizer will read a newer ordinary exact publication. Even
	// a P which passes on the decayed view might be held as ambiguous there.
	// V passes independently of the publication and learned call history.
	if p.stabilizerEnabled && p.recentBandStore != nil && resolverConfidenceGlyph(snapshot, true, subject) != "V" {
		return spot.ResolverSnapshot{}, false
	}
	if ctx.ctyDB != nil {
		probe := ctx.spot.Clone()
		setResolverConfidence(probe, snapshot, true, subject)
		if applyLicenseGate(probe, ctx.ctyDB, p.metaCache, nil) {
			return spot.ResolverSnapshot{}, false
		}
	}
	return snapshot, true
}

func (p *outputPipeline) cwDecayKeepsIdentity(s *spot.Spot, key spot.ResolverSignalKey, selection correctionflow.ResolverPrimarySelection, now time.Time) bool {
	snap, subject := selection.Snapshot, normalizedDXCall(s)
	_, _, pending := snap.PendingTransition()
	if selection.NeighborhoodSplit || pending || (snap.State != spot.ResolverStateProbable && snap.State != spot.ResolverStateConfident) || snap.Winner == "" {
		return false
	}
	if spot.CorrectionVoteKey(subject) == spot.CorrectionVoteKey(snap.Winner) {
		return true
	}
	// A separate history would have learned some different emitted calls.
	// Use the common gate implementation and accept only a veto that survives
	// every possible Recent+1, truncation and Bayes history value.
	settings := correctionflow.BuildCorrectionSettings(correctionflow.BuildSettingsInput{Cfg: p.correctionCfg})
	confidence := correctionflow.ResolverWinnerConfidence(snap)
	gate := spot.EvaluateResolverPrimaryGates(subject, snap.Winner, key.Band, key.Mode,
		correctionflow.ResolverSupportForCall(snap, subject), correctionflow.ResolverSupportForCall(snap, snap.Winner), confidence,
		correctionflow.ResolverWeightedSupportForCall(snap, subject), correctionflow.ResolverWeightedSupportForCall(snap, snap.Winner),
		settings, now, spot.ResolverPrimaryGateOptions{})
	return gate.Reason == "max_edit_distance" || gate.Reason == "confidence" || confidence < settings.MinConfidencePercent
}
