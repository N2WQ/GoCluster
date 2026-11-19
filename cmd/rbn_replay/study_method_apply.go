// File role: Apply frequency-backed rescue only inside explicit method replay.
// Ordinary correction runs first. Request-local group numbers never replace
// the ordinary/public/temporal selection; validation is the mutation boundary.
package main

import (
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

func studyMethodCLIFlag(argument string) bool {
	if !strings.HasPrefix(argument, "-") {
		return false
	}
	name := strings.TrimPrefix(strings.TrimPrefix(argument, "-"), "-")
	name, _, _ = strings.Cut(name, "=")
	return name == "study-method-plan" || name == "study-method-freeze" || name == "study-method-candidate"
}

type studyMethodGateTrial struct {
	Proposal correctionflow.FrequencyProposal `json:"proposal"`
	Gate     spot.ResolverPrimaryGateResult   `json:"gate"`
}

type studyMethodReceipt struct {
	RawID                        string                                     `json:"raw_id"`
	ObservedAt                   time.Time                                  `json:"observed_at"`
	DecisionAt                   time.Time                                  `json:"decision_at"`
	CandidateID                  string                                     `json:"candidate_id"`
	Subject                      string                                     `json:"subject"`
	PermittedTemporalIdentity    string                                     `json:"permitted_temporal_identity"`
	LifecycleCurrent             bool                                       `json:"lifecycle_current"`
	Assessment                   correctionflow.FrequencyAssessment         `json:"assessment"`
	SelectiveDeclines            []correctionflow.SelectiveFrequencyDecline `json:"selective_declines,omitempty"`
	OrdinalAvailable             bool                                       `json:"ordinal_available"`
	Trials                       []studyMethodGateTrial                     `json:"trials"`
	Applied                      bool                                       `json:"applied"`
	Suppress                     bool                                       `json:"suppress"`
	ReplacementRejected          bool                                       `json:"replacement_rejected"`
	FinalObserved                string                                     `json:"final_observed"`
	OrdinaryPublicPercent        int                                        `json:"ordinary_public_percent"`
	OrdinaryPublicPercentPresent bool                                       `json:"ordinary_public_percent_present"`
	OrdinaryGate                 spot.ResolverPrimaryGateResult             `json:"ordinary_gate"`
}

func (r *replayRunner) methodEnabled() bool { return r.method != nil && r.method.prepared.Plan.Enabled }

// saved is supplied only for a held Apply whose existing release guards passed.
// Fallback is a new current resolver decision. Abstain/bypass/overflow never
// reach this function through the experimental route.
func (r *replayRunner) applyMethodRescue(s *spot.Spot, evidence spot.ResolverEvidence, hasEvidence bool, now time.Time,
	outcome replayResolverApplyOutcome, permitted string, saved *correctionflow.FrequencyLifecycleVector) replayResolverApplyOutcome {
	if !r.methodEnabled() || s == nil || !hasEvidence || !r.cfg.CallCorrection.Enabled || evidence.Key.Mode != "CW" ||
		outcome.Applied || outcome.Suppress || outcome.replacementRejected {
		return outcome
	}
	subject := normalizedDXCall(s)
	candidate := r.method.prepared.Candidate
	maxDistance := candidate.CWDistances[len(candidate.CWDistances)-1]
	sources := correctionflow.ReadFrequencySources(r.resolver, evidence.Key, r.cfg.CallCorrection, now)
	receipt := studyMethodReceipt{ObservedAt: evidence.ObservedAt, DecisionAt: now, CandidateID: candidate.ID, Subject: subject,
		PermittedTemporalIdentity: permitted, LifecycleCurrent: true, OrdinaryGate: outcome.Gate}
	if r.study != nil {
		if pending := r.study.pending[s]; pending != nil {
			receipt.RawID = pending.RawID
		}
	}
	if saved != nil && *saved != correctionflow.FrequencySourceLifecycles(sources) {
		receipt.LifecycleCurrent = false
		receipt.Assessment.Reason = "lifecycle_changed"
	} else {
		options := correctionflow.FrequencyProposalOptions{GapHz: candidate.GapHz, MaxWitnessSeparation: time.Duration(candidate.MaxWitnessSeconds) * time.Second, MaxDistance: maxDistance}
		if r.method.prepared.Plan.SchemaVersion == 2 {
			selective := correctionflow.AssessSelectiveFrequencyProposals(evidence.Key, subject, s.Frequency, outcome.Selection, sources, r.cfg.CallCorrection, options)
			receipt.Assessment, receipt.SelectiveDeclines = selective.FrequencyAssessment, selective.Declines
		} else {
			receipt.Assessment = correctionflow.AssessFrequencyProposals(evidence.Key, subject, s.Frequency, outcome.Selection, sources, r.cfg.CallCorrection, options)
		}
		proposals := receipt.Assessment.Proposals
		if permitted != "" {
			proposals = nil
			for _, p := range receipt.Assessment.Proposals {
				if p.Identity == spot.CorrectionVoteKey(permitted) {
					proposals = append(proposals, p)
				}
			}
		}
		if candidate.Ranking == "ordinal" {
			proposals, receipt.OrdinalAvailable = r.method.prepared.Model.RankFrequencyProposals(proposals, spot.CorrectionVoteKey(subject), evidence.Key.Mode, evidence.Report, evidence.HasReport)
		}
		for _, proposal := range proposals[:min(8, len(proposals))] {
			gate := r.evaluateMethodGate(s, subject, proposal, now)
			receipt.Trials = append(receipt.Trials, studyMethodGateTrial{proposal, gate})
			outcome.Gate, outcome.GateEvaluated = gate, true
			if !gate.Allow {
				continue
			}
			outcome.methodWinner = proposal.Call
			applied, suppress, reason := applyReplayReplacement(s, proposal.Call, r.cfg.CallCorrection, r.ctyDB, now)
			outcome.Applied, outcome.Suppress, outcome.replacementRejected = applied, suppress, !applied
			outcome.Confidence.Final = normalizeConfidenceGlyph(s.Confidence)
			if applied {
				outcome.Winner = proposal.Call
				if r.method.prepared.Plan.SchemaVersion == 2 {
					s.ConfidencePercent, s.ConfidencePercentOK = correctionflow.ResolverCallConfidencePercent(outcome.Selection.Snapshot, proposal.Call)
				}
				if r.tracker != nil {
					r.tracker.IncrementCallCorrections()
				}
				outcome.methodReason = "frequency_applied"
			} else {
				outcome.methodReason = "frequency_" + reason
			}
			break // First gate allowance alone crosses the final validation boundary.
		}
	}
	if outcome.methodReason == "" {
		outcome.methodReason = "frequency_" + receipt.Assessment.Reason
		if len(receipt.Trials) > 0 {
			outcome.methodReason = "frequency_gate_" + outcome.Gate.Reason
		}
	}
	receipt.Applied, receipt.Suppress, receipt.ReplacementRejected = outcome.Applied, outcome.Suppress, outcome.replacementRejected
	receipt.FinalObserved = normalizedDXCall(s)
	receipt.OrdinaryPublicPercent, receipt.OrdinaryPublicPercentPresent = correctionflow.ResolverCallConfidencePercent(outcome.Selection.Snapshot, receipt.FinalObserved)
	if err := r.method.record(receipt); err != nil {
		r.method.files.latch(err)
	}
	return outcome
}

func (r *replayRunner) evaluateMethodGate(s *spot.Spot, subject string, p correctionflow.FrequencyProposal, now time.Time) spot.ResolverPrimaryGateResult {
	cfg := r.cfg.CallCorrection
	runtime := correctionflow.ResolveRuntimeSettings(cfg, s, r.adaptiveMinReports, now, false)
	settings := correctionflow.BuildCorrectionSettings(correctionflow.BuildSettingsInput{Cfg: cfg, MinReports: runtime.MinReports, Window: runtime.Window,
		FreqToleranceHz: runtime.FreqToleranceHz, RecentBandStore: r.recentBandStore})
	band, mode := s.BandNorm, s.ModeNorm
	if band == "" || band == "???" {
		band = spot.FreqToBand(s.Frequency)
	}
	if mode == "" {
		mode = s.Mode
	}
	options := spot.ResolverPrimaryGateOptions{}
	// Preserve V1's frozen replay contract. New V2 consumers use the complete
	// ordinary comparator restriction, including structurally enabled Bayes.
	comparatorEnabled := cfg.ResolverRecentPlus1Enabled
	if r.method != nil && r.method.prepared.Plan.SchemaVersion == 2 {
		comparatorEnabled = comparatorEnabled || config.BayesBonusModeEnabled(cfg.BayesBonus.Enabled, cfg.BayesBonus.EnabledDistancesByMode, mode, cfg.MaxEditDistance)
	}
	if comparatorEnabled && p.ComparableEditNeighbor {
		options.RecentPlus1DisallowReason = resolverRecentPlus1DisallowEditNeighborGate
	}
	return spot.EvaluateResolverPrimaryGates(subject, p.Call, band, mode, p.SubjectSupport, p.Support, p.ConfidencePercent,
		p.SubjectWeightedSupportMilli, p.WeightedSupportMilli, settings, now, options)
}
