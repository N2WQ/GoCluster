// File role: Copy the decoder's actual event universe and executed cutoffs.
// Evaluate owns these bounded copies only until returning them to its caller.
// No callback, formatter, writer, extra clock or decoder rule runs for capture.
package correctionflow

import (
	"strings"
	"time"
)

type DiagnosticTemporalRank struct {
	Position                                         int
	Call                                             string
	Status                                           string
	Checked, Allowed, Admitted, Deduplicated, Cutoff bool
}

type DiagnosticTemporalExtra struct {
	Kind, Call string
	Added      bool
}

type DiagnosticTemporalEvent struct {
	ID            uint64
	ObservedAt    time.Time
	Subject       string
	Selection     DiagnosticSelection
	Ranks         []DiagnosticTemporalRank
	Extras        []DiagnosticTemporalExtra
	Candidates    []string
	AdmittedRanks int
	Cutoff        bool
}

type DiagnosticTemporalStartScore struct {
	Call  string
	Score int
}

type TemporalEvaluateDiagnostics struct {
	ID                                                                                                        uint64
	RequestedAt, EffectiveAt, RequestObservedAt, LagAt, MaxAt, WindowEnd                                      time.Time
	ClockRead, Force, RequestFound, KeyStateFound, StartEventFound, ThresholdChecked, ThresholdPass, TimedOut bool
	Key                                                                                                       DiagnosticKey
	MaxObsCandidates, MaxEventsPerKey                                                                         int
	Window                                                                                                    []DiagnosticTemporalEvent
	StartScores                                                                                               []DiagnosticTemporalStartScore
	Transform                                                                                                 DiagnosticTransform
	Exit                                                                                                      string
	Action                                                                                                    TemporalDecisionAction
	Reason, Winner                                                                                            string
	BestScore, MarginScore, CandidateCount                                                                    int
	CommitLatency                                                                                             time.Duration
	Output                                                                                                    DiagnosticSelection
	budget                                                                                                    diagnosticBudget
}

// EvaluateWithDiagnostics returns the unchanged normal decision even when the
// capture exceeds its independent bounds. Callers handle the error after normal
// retirement/output work; they must not convert it into a fallback decision.
func (d *TemporalDecoder) EvaluateWithDiagnostics(id uint64, now time.Time, force bool, dst *TemporalEvaluateDiagnostics) (TemporalDecision, error) {
	if dst != nil {
		*dst = TemporalEvaluateDiagnostics{ID: id, RequestedAt: now.UTC(), Force: force}
		dst.budget.take(diagnosticScaffoldBytes)
	}
	decision := d.evaluateDiagnostics(id, now, force, dst)
	if dst == nil {
		return decision, nil
	}
	return decision, dst.budget.err
}

// EncodedByteUpperBound covers event/rank/alias copies and fixed scaffolding.
func (d *TemporalEvaluateDiagnostics) EncodedByteUpperBound() int { return d.budget.bytes }

func finishTemporalDiagnostic(d *TemporalEvaluateDiagnostics, decision TemporalDecision) TemporalDecision {
	if d != nil {
		d.Exit = decision.Reason
		d.Action = TemporalDecisionAction(d.budget.text(string(decision.Action)))
		d.Reason = d.budget.text(decision.Reason)
		d.Winner = d.budget.text(decision.Winner)
		d.BestScore, d.MarginScore, d.CandidateCount = decision.BestScore, decision.MarginScore, decision.CandidateCount
		d.CommitLatency = decision.CommitLatency
		d.Output = cloneDiagnosticSelection(&d.budget, decision.Selection)
	}
	return decision
}

func (d *TemporalEvaluateDiagnostics) window(events []temporalEvent) {
	if d.budget.err != nil {
		return
	}
	if len(events) > diagnosticMaxEvents {
		d.budget.fail("diagnostic temporal window exceeds 32 events")
		return
	}
	if !d.budget.take(len(events) * 256) {
		return
	}
	d.Window = make([]DiagnosticTemporalEvent, len(events))
	for i := range events {
		event := &events[i]
		out := &d.Window[i]
		out.ID, out.ObservedAt, out.Subject = event.id, event.observedAt.UTC(), d.budget.text(event.subject)
		out.Selection = cloneDiagnosticSelection(&d.budget, event.selection)
		if d.budget.err != nil {
			return
		}
		ranks := event.selection.Snapshot.CandidateRanks
		for j := range ranks {
			if !d.budget.take(256 + 6*len(ranks[j].Call)) {
				return
			}
		}
		out.Ranks = make([]DiagnosticTemporalRank, len(ranks))
		for j := range ranks {
			out.Ranks[j] = DiagnosticTemporalRank{Position: j, Call: out.Selection.Snapshot.Candidates[j].Call, Status: "not_reached"}
		}
	}
}

func (d *TemporalEvaluateDiagnostics) event(index int) *DiagnosticTemporalEvent {
	if d == nil || d.budget.err != nil || index >= len(d.Window) {
		return nil
	}
	return &d.Window[index]
}

func diagnosticTemporalRankStatus(capture *DiagnosticTemporalEvent, index int, allowed, admitted, deduplicated, cutoff bool) {
	if capture == nil {
		return
	}
	r := &capture.Ranks[index]
	r.Checked, r.Allowed, r.Admitted, r.Deduplicated, r.Cutoff = true, allowed, admitted, deduplicated, cutoff
	switch {
	case cutoff:
		r.Status = "checked_post_limit_cutoff"
		capture.Cutoff = true
		for i := index + 1; i < len(capture.Ranks); i++ {
			capture.Ranks[i].Status = "not_visited_after_cutoff"
		}
	case !allowed:
		r.Status = "rejected"
	case deduplicated:
		r.Status = "admitted_duplicate"
	default:
		r.Status = "admitted"
	}
}

func diagnosticTemporalExtra(capture *DiagnosticTemporalEvent, budget *diagnosticBudget, kind, call string, added bool) {
	if capture == nil || budget.err != nil {
		return
	}
	if len(capture.Extras) >= 3 {
		budget.fail("diagnostic temporal extras exceed three")
		return
	}
	if !budget.take(128) {
		return
	}
	capture.Extras = append(capture.Extras, DiagnosticTemporalExtra{Kind: kind, Call: budget.text(call), Added: added})
}

func diagnosticTemporalCandidates(capture *DiagnosticTemporalEvent, budget *diagnosticBudget, candidates []string, admitted int) {
	if capture == nil || budget.err != nil {
		return
	}
	if len(candidates) > diagnosticMaxEventCandidates {
		budget.fail("diagnostic temporal candidates exceed 8+3")
		return
	}
	for _, call := range candidates {
		if !budget.label(call) || !budget.take(6*len(call)+8) {
			return
		}
	}
	capture.Candidates = make([]string, len(candidates))
	for i := range candidates {
		capture.Candidates[i] = strings.Clone(candidates[i])
	}
	capture.AdmittedRanks = admitted
}

func (d *TemporalEvaluateDiagnostics) scores(scores []temporalScore) {
	if d.budget.err != nil {
		return
	}
	if len(scores) > diagnosticMaxEventCandidates {
		d.budget.fail("diagnostic temporal start universe exceeds 8+3")
		return
	}
	for _, row := range scores {
		if !d.budget.label(row.call) || !d.budget.take(128+6*len(row.call)) {
			return
		}
	}
	d.StartScores = make([]DiagnosticTemporalStartScore, len(scores))
	for i := range scores {
		d.StartScores[i] = DiagnosticTemporalStartScore{Call: strings.Clone(scores[i].call), Score: scores[i].score}
	}
}

func setDiagnosticTransformDisplay(d *DiagnosticTransform, budget *diagnosticBudget, display string) {
	if d != nil {
		d.Display = budget.text(display)
	}
}
