// File role: Owns constant-size cap-loss and process-idle metadata for each
// resolver generation. Cells and loss bounds retire with the existing key;
// controls publish immutable current rows without advancing model hysteresis.
package spot

import (
	"sort"
	"sync/atomic"
	"time"
)

const resolverLossIdleLimit = 10 * time.Minute

type resolverProcessClock struct {
	epoch   time.Time
	manual  atomic.Bool
	elapsed atomic.Int64
}

func (r *SignalResolver) processElapsed() time.Duration {
	if r.processClock.manual.Load() {
		return time.Duration(r.processClock.elapsed.Load())
	}
	if r.processClock.epoch.IsZero() {
		return 0 // Externally constructed private test states have no epoch.
	}
	return time.Since(r.processClock.epoch)
}

func (r *SignalResolver) attachKeyGeneration(st *resolverKeyState) {
	if st.cell != nil {
		return
	}
	// The owner allocates a new cell for every lifecycle. Never reuse an old
	// cell: a reader may still hold it after physical deletion/recreation.
	if value, ok := r.snapshots.Load(st.key); ok {
		if old, valid := value.(*resolverSnapshotCell); valid && old != nil {
			old.terminal.Store(true)
			old.snapshot.Store(nil)
		}
	}
	r.nextGeneration++
	st.generation = r.nextGeneration
	st.cell = &resolverSnapshotCell{}
	r.snapshots.Store(st.key, st.cell)
}

func (r *SignalResolver) retireKey(states map[ResolverSignalKey]*resolverKeyState, key ResolverSignalKey, st *resolverKeyState) {
	if st != nil && st.cell != nil {
		st.cell.terminal.Store(true)
		st.cell.snapshot.Store(nil)
	} else if value, ok := r.snapshots.Load(key); ok {
		if cell, valid := value.(*resolverSnapshotCell); valid && cell != nil {
			cell.terminal.Store(true)
			cell.snapshot.Store(nil)
		}
	}
	delete(states, key)
	r.snapshots.Delete(key)
}

func (r *SignalResolver) lookupSnapshot(key ResolverSignalKey) (ResolverSnapshot, *resolverSnapshotCell, bool) {
	if r != nil {
		if value, ok := r.snapshots.Load(key); ok {
			if cell, valid := value.(*resolverSnapshotCell); valid && cell != nil {
				if snapshot := cell.snapshot.Load(); snapshot != nil {
					return *snapshot, cell, true
				}
			}
		}
	}
	return ResolverSnapshot{}, nil, false
}

func (r *SignalResolver) cellIdleExpired(cell *resolverSnapshotCell, elapsed time.Duration) bool {
	return cell != nil && cell.lossAffected.Load() && elapsed-time.Duration(cell.activity.Load()) >= resolverLossIdleLimit
}

func (r *SignalResolver) cellCurrent(cell *resolverSnapshotCell) bool {
	if cell == nil || cell.terminal.Load() {
		return false
	}
	if cell.lossAffected.Load() && r.cellIdleExpired(cell, r.processElapsed()) {
		return false
	}
	// Validate the captured cell after reading its snapshot/activity, including
	// after a freshness projection. Retirement always marks it before deletion.
	return !cell.terminal.Load()
}

func recordResolverCapLoss(st *resolverKeyState, observedAt time.Time) {
	if !st.cell.lossAffected.Load() {
		st.lossMinAt, st.lossMaxAt = observedAt, observedAt
	} else {
		if observedAt.Before(st.lossMinAt) {
			st.lossMinAt = observedAt
		}
		if observedAt.After(st.lossMaxAt) {
			st.lossMaxAt = observedAt
		}
	}
	st.cell.lossAffected.Store(true)
}

func resolverSnapshotLossEligible(snapshot ResolverSnapshot, now time.Time) bool {
	if snapshot.lossMinAt.IsZero() {
		return snapshot.EvidenceLoss
	}
	return snapshot.lossWindow > 0 && !snapshot.lossMinAt.After(now) && !snapshot.lossMaxAt.Before(now.Add(-snapshot.lossWindow))
}

func (r *SignalResolver) publishKeySnapshot(st *resolverKeyState, snapshot ResolverSnapshot, now time.Time, ordinary bool) {
	r.attachKeyGeneration(st)
	if ordinary {
		st.ordinaryWinner, st.ordinaryState = snapshot.Winner, snapshot.State
	}
	snapshot.Generation = st.generation
	snapshot.lossMinAt, snapshot.lossMaxAt, snapshot.lossWindow = st.lossMinAt, st.lossMaxAt, st.recencyWindow
	snapshot.ordinaryWinner, snapshot.ordinaryState = st.ordinaryWinner, st.ordinaryState
	snapshot.EvidenceLoss = resolverSnapshotLossEligible(snapshot, now)
	r.publishSnapshot(st.key, snapshot)
}

// publishLossControl accounts for the complete retained population. The prior
// ordinary tie order and authority hint can preserve that same settled winner;
// this path never evaluates a new model winner or advances pendingWins.
func (r *SignalResolver) publishLossControl(st *resolverKeyState, now time.Time) {
	view := publishedResolverEvidence(st)
	snapshot := ResolverSnapshot{Key: st.key, EvaluatedAt: st.lastEvalAt, State: ResolverStateUncertain,
		receiverEvidence: view, ordinaryWinner: st.ordinaryWinner, ordinaryState: st.ordinaryState,
		TotalReporters: len(st.reporterRefs), TotalWeightedSupportMilli: totalResolverReporterWeightMilli(st.reporterRefs)}
	previousOrder := make(map[string]int, len(st.publishedCandidateRanks))
	for i, previous := range st.publishedCandidateRanks {
		previousOrder[previous.Call] = i
	}
	snapshot.CandidateRanks = make([]ResolverCandidateSupport, 0, len(st.candidates))
	for call, candidate := range st.candidates {
		if candidate != nil && len(candidate.reporters) > 0 {
			snapshot.CandidateRanks = append(snapshot.CandidateRanks, ResolverCandidateSupport{Call: call,
				Support: len(candidate.reporters), WeightedSupportMilli: weightedSupportForResolverCandidate(candidate, st.reporterRefs)})
		}
	}
	sort.Slice(snapshot.CandidateRanks, func(i, j int) bool {
		left, right := snapshot.CandidateRanks[i], snapshot.CandidateRanks[j]
		if left.WeightedSupportMilli != right.WeightedSupportMilli {
			return left.WeightedSupportMilli > right.WeightedSupportMilli
		}
		if left.Support != right.Support {
			return left.Support > right.Support
		}
		a, aOK := previousOrder[left.Call]
		b, bOK := previousOrder[right.Call]
		if aOK != bOK {
			return aOK
		}
		if aOK && a != b {
			return a < b
		}
		return left.Call < right.Call
	})
	snapshot = r.projectResolverAuthority(snapshot, snapshot)
	r.publishKeySnapshot(st, snapshot, now, false)
}
