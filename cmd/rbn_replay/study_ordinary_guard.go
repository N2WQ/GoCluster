// File role: Owns the exposed-only ordinary CW safeguard in offline replay.
// The replay owner evaluates it synchronously after replacement validation.
package main

import (
	"errors"
	"time"

	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

const studyOrdinaryGuardRetained = "ordinary_cw_guard_retained_original"

func validateOrdinaryGuardRequest(req studyCLIRequest) error {
	if !req.OrdinaryGuard {
		return nil
	}
	if req.Arm != "repaired_off" || req.Phase != "tuning" || req.Compare != "" ||
		req.DiagnosticSelection != "" || req.DiagnosticFreeze != "" ||
		req.MethodPlan != "" || req.MethodFreeze != "" || req.MethodCandidate != "" {
		return errors.New("ordinary CW guard requires an exposed repaired_off capture without other study modes")
	}
	_, err := studyDiagnosticCaseDate(req.Date)
	return err
}

func (r *replayRunner) retainOrdinaryCWOriginal(s *spot.Spot, key spot.ResolverSignalKey, winner string, now time.Time) bool {
	subject := normalizedDXCall(s)
	if rejectReplayDXLicense(r.ctyDB, subject, s.IsBeacon || spot.IsBeaconCall(subject), now) {
		return false
	}
	// A replay Step cannot interleave these reads with the ordinary decision.
	// Queued focal evidence is drained only after the terminal output path.
	sources := correctionflow.ReadFrequencySources(r.resolver, key, r.cfg.CallCorrection, now)
	return correctionflow.OrdinaryCWSeparation(correctionflow.OrdinaryCWSeparationInput{
		Key: key, Config: r.cfg.CallCorrection, Sources: sources,
		Original: subject, Winner: winner, FocalKHz: s.Frequency, At: now,
		FocalFamily: r.study.pending[s].ReceiverFamily, ReceiverFamily: r.study.day.family,
	})
}
