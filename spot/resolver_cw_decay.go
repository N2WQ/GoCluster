// File role: CW comparison authority owned by the ordinary resolver key. Its
// hysteresis advances on the owner's evaluation clock, never on lookup.
package spot

import (
	"math"
	"slices"
	"time"
)

type resolverCWDecayAuthority struct {
	stable, pending string
	wins            int
}

// Sentinel for the ordinary owner's uncontested authority. A reader still
// computes fresh decayed weights from the original reports at the focal clock.
var cwDecayUsesSingleIdentity ResolverSnapshot

func (a *resolverCWDecayAuthority) invalidate(identity string) {
	if a.stable == identity {
		*a = resolverCWDecayAuthority{}
	} else if a.pending == identity {
		a.pending, a.wins = "", 0
	}
}

// Always reprice original reliability milliweights, including separate
// observations by the same receiver for competing spellings.
type cwDecayWorkspace struct {
	view ResolverEvidenceView
	rows []ResolverReceiverObservation
}

func (w *cwDecayWorkspace) reset() {
	clear(w.rows)
	clear(w.view.candidates)
	clear(w.view.canonical)
}

func (w *cwDecayWorkspace) project(raw *ResolverEvidenceView, at time.Time) *ResolverEvidenceView {
	v := &w.view
	v.window, v.count = raw.window, raw.count
	v.candidates = slices.Grow(v.candidates[:0], len(raw.candidates))[:len(raw.candidates)]
	w.rows = slices.Grow(w.rows[:0], raw.count)[:raw.count]
	storage := w.rows
	for i, candidate := range raw.candidates {
		size := len(candidate.receivers)
		rows := storage[:size:size]
		storage = storage[size:]
		copy(rows, candidate.receivers)
		for j := range rows {
			row := &rows[j]
			age := max(at.Sub(row.ObservedAt), 0)
			row.WeightMilli = int(math.Round(float64(row.WeightMilli) * math.Exp2(-age.Seconds()/60)))
		}
		v.candidates[i] = resolverEvidenceCandidate{call: candidate.call, receivers: rows}
	}
	singletonsInOrder := len(raw.canonical) == len(raw.candidates)
	for i := 0; singletonsInOrder && i < len(raw.canonical); i++ {
		singletonsInOrder = raw.canonical[i].display == raw.candidates[i].call
	}
	if singletonsInOrder {
		// With one literal per group, semantic receiver order, observed
		// representative, recency and slash variants are weight-independent.
		// Slash normalization can reorder groups, so verify the alignment.
		v.canonical = slices.Grow(v.canonical[:0], len(raw.canonical))[:len(raw.canonical)]
		copy(v.canonical, raw.canonical)
		for i := range v.canonical {
			group := &v.canonical[i]
			group.receivers = v.candidates[i].receivers
			group.weighted = 0
			for _, row := range group.receivers {
				group.weighted += row.WeightMilli
			}
		}
	} else {
		v.canonical = buildCanonicalResolverCandidates(v.candidates)
	}
	return v
}

func (r *SignalResolver) rankCWDecaySnapshot(st *resolverKeyState, ordinary ResolverSnapshot, raw *ResolverEvidenceView, now time.Time) *ResolverSnapshot {
	if st.cwDecay == nil {
		st.cwDecay = &resolverCWDecayAuthority{}
	}
	a := st.cwDecay
	if len(raw.candidates) == 1 && a.pending == "" && (a.stable == "" || a.stable == CorrectionVoteKey(ordinary.Winner)) &&
		!ordinary.EvidenceLoss && ordinary.State == ResolverStateConfident &&
		ordinary.Winner != "" && raw.window <= 5*time.Minute && hasPositiveCWDecayWeight(raw, now) {
		// A single positive-weight identity has the same 100% owner authority
		// at either weighting. Defer its actual tally until a neighborhood
		// needs it. Cloning the raw rows for every uncontested CQ is costly.
		a.stable, a.pending, a.wins = CorrectionVoteKey(ordinary.Winner), "", 0
		return &cwDecayUsesSingleIdentity
	}
	v := r.cwDecayScratch.project(raw, now)
	state := resolverKeyState{key: st.key, stableWinner: a.stable, pendingWinner: a.pending,
		pendingWins: a.wins, rankedScratch: st.rankedScratch, candidateRanksScratch: st.candidateRanksScratch}
	result := r.rankResolverSnapshot(&state, v, now, true)
	r.cwDecayScratch.reset()
	a.stable, a.pending, a.wins = state.stableWinner, state.pendingWinner, state.pendingWins
	st.rankedScratch, st.candidateRanksScratch = state.rankedScratch, state.candidateRanksScratch
	// Fresh readers project from the ordinary publication's original rows.
	// The shadow only needs sealed rank order and hysteresis between reads.
	result.receiverEvidence = nil
	result.Generation = st.generation
	result.ordinaryWinner, result.ordinaryState = CorrectionVoteKey(result.Winner), result.State
	if a.pending != "" && a.wins > 0 {
		result.pendingIncumbent, result.pendingChallenger = a.stable, a.pending
	}
	return &result
}

func hasPositiveCWDecayWeight(v *ResolverEvidenceView, at time.Time) bool {
	// The key keeps at most five half-lives here. One positive effective
	// observation is sufficient for this one-candidate 100% share.
	for _, row := range v.candidates[0].receivers {
		if row.WeightMilli >= 16 && !row.ObservedAt.Before(at.Add(-5*time.Minute)) {
			return true
		}
	}
	return false
}

// LookupCWDecayEligible returns the independent 60-second model at the
// specified release clock. It projects original retained weights, validates
// the key lifecycle and fences incomplete cap-loss control publications.
func (r *SignalResolver) LookupCWDecayEligible(key ResolverSignalKey, now time.Time) (ResolverSnapshot, bool) {
	if r == nil || !r.cfg.CWDecayComparison || key.Mode != "CW" || now.IsZero() {
		return ResolverSnapshot{}, false
	}
	ordinary, cell, ok := r.lookupSnapshot(key)
	if !ok || !r.cellCurrent(cell) {
		return ResolverSnapshot{}, false
	}
	if !ordinary.EvaluatedAt.Before(now) {
		return ResolverSnapshot{Key: key, Generation: ordinary.Generation, EvidenceLoss: true, State: ResolverStateUncertain}, true
	}
	return r.projectCWDecayAt(ordinary, cell, now)
}

// CWDecayPrior pins an exact-key observation before the focal is queued. Its
// lifetime is one immediate request, including when the owner ingests the
// focal before the ordinary rename reaches its mutation boundary.
type CWDecayPrior struct {
	cell     *resolverSnapshotCell
	snapshot *ResolverSnapshot
}

// CaptureCWDecayPrior pins the current exact-slot immutable publication
// before a CW spot is enqueued. Missing evidence cannot authorize retention.
func (r *SignalResolver) CaptureCWDecayPrior(key ResolverSignalKey) CWDecayPrior {
	if r == nil || !r.cfg.CWDecayComparison || key.Mode != "CW" {
		return CWDecayPrior{}
	}
	value, _ := r.snapshots.Load(key)
	cell, ok := value.(*resolverSnapshotCell)
	if !ok || cell == nil {
		return CWDecayPrior{}
	}
	return CWDecayPrior{cell: cell, snapshot: cell.snapshot.Load()}
}

// LookupCWDecayPrior revalues the pinned population if its key lifecycle is
// still current. Return an explicit loss when a neighborhood must abstain.
func (r *SignalResolver) LookupCWDecayPrior(prior CWDecayPrior, key ResolverSignalKey, now time.Time) (ResolverSnapshot, bool) {
	if r == nil || prior.snapshot == nil || prior.snapshot.Key != key ||
		now.IsZero() || now.Before(prior.snapshot.EvaluatedAt) || !r.cellCurrent(prior.cell) {
		return ResolverSnapshot{Key: key, State: ResolverStateUncertain, EvidenceLoss: true}, true
	}
	return r.projectCWDecayAt(*prior.snapshot, prior.cell, now)
}

func (r *SignalResolver) projectCWDecayAt(ordinary ResolverSnapshot, cell *resolverSnapshotCell, now time.Time) (ResolverSnapshot, bool) {
	if ordinary.cwDecay == nil || ordinary.receiverEvidence == nil || resolverSnapshotLossEligible(ordinary, now) {
		return ResolverSnapshot{Key: ordinary.Key, Generation: ordinary.Generation, EvidenceLoss: true, State: ResolverStateUncertain}, true
	}
	shadow := *ordinary.cwDecay
	if ordinary.cwDecay == &cwDecayUsesSingleIdentity {
		shadow = ordinary
		shadow.cwDecay = nil
	}
	raw := ordinary.receiverEvidence
	var local cwDecayWorkspace
	shadow = r.projectEligibleView(shadow, now, local.project(raw, now))
	if !r.cellCurrent(cell) {
		return ResolverSnapshot{Key: ordinary.Key, State: ResolverStateUncertain, EvidenceLoss: true}, true
	}
	return shadow, true
}
