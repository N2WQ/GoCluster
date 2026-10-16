package spot

import (
	"reflect"
	"testing"
	"time"
)

type sweepCapturedRow struct {
	call string
	row  ResolverReceiverObservation
}

func sweepSnapshotRows(s ResolverSnapshot) []sweepCapturedRow {
	var rows []sweepCapturedRow
	s.ReceiverEvidence().Range(func(call string, row ResolverReceiverObservation) bool {
		rows = append(rows, sweepCapturedRow{call: call, row: row})
		return true
	})
	return rows
}

// This coherent private fixture includes a shared receiver whose stale A row
// must expire without deleting its fresh B reference. The cutoff row is inclusive.
func sweepBoundaryFixture(t testing.TB) (*SignalResolver, *SignalResolverDriver, *resolverKeyState, time.Time) {
	t.Helper()
	r, d, key, at := lossTestDriver(t, SignalResolverConfig{EvalMinInterval: time.Second, SweepInterval: time.Second})
	st := &resolverKeyState{
		key: key, recencyWindow: 10 * time.Second, lastSeen: at, dirty: true,
		candidates: make(map[string]*resolverCandidate), reporterRefs: make(map[string]resolverReporterRef),
		stableWinner: "K1ABE", pendingWinner: "K1ABC", pendingWins: 1,
	}
	old := at.Add(-10*time.Second - time.Nanosecond)
	for _, row := range []struct {
		call, receiver string
		observedAt     time.Time
	}{
		{"K1ABC", "N0SHARED", old},
		{"K1ABC", "N0KEEP", at},
		{"K1ABC", "N0BOUND", at.Add(-10 * time.Second)},
		{"K1ABD", "N0SHARED", at},
		{"K1ABE", "N0STALE", old},
	} {
		c := st.candidates[row.call]
		if c == nil {
			c = &resolverCandidate{reporters: make(map[string]resolverReporterObservation),
				lastFreqKHz: 7010.1, identity: normalizeCorrectionCallIdentity(row.call), callRunes: []rune(row.call)}
			st.candidates[row.call] = c
		}
		upsertResolverReporter(st, c, row.receiver, row.observedAt, 7010.1, 1000)
		if row.observedAt.After(c.lastSeen) {
			c.lastSeen = row.observedAt
		}
	}
	d.states[key] = st
	return r, d, st, at
}

func TestResolverSweepPreparedEvaluation(t *testing.T) {
	for _, standalone := range []bool{false, true} {
		name := "sweep"
		if standalone {
			name = "standalone"
		}
		t.Run(name, func(t *testing.T) {
			r, d, st, at := sweepBoundaryFixture(t)
			if standalone {
				r.evaluateKey(nil, at)
				r.evaluateKey(st, at)
			} else {
				d.Sweep(at)
				if st.dirty || st.nextEvalAt != at.Add(time.Second) || r.MetricsSnapshot().ActiveKeys != 1 {
					t.Fatal("sweep did not finish scheduling the evaluated key")
				}
			}
			s := lossTestSnapshot(t, r, st.key)
			wantRanks := []ResolverCandidateSupport{{Call: "K1ABC", Support: 2, WeightedSupportMilli: 2000}, {Call: "K1ABD", Support: 1, WeightedSupportMilli: 1000}}
			if s.Key != st.key || s.Generation != 1 || s.EvaluatedAt != at || s.State != ResolverStateConfident ||
				s.Winner != "K1ABC" || s.RunnerUp != "K1ABD" || s.WinnerSupport != 2 || s.RunnerSupport != 1 ||
				s.Margin != 1 || s.TotalReporters != 3 || s.WinnerWeightedSupportMilli != 2000 ||
				s.RunnerWeightedSupportMilli != 1000 || s.TotalWeightedSupportMilli != 3000 || s.EvidenceLoss ||
				s.SupportBasis != ResolverSupportBasisWeighted || !reflect.DeepEqual(s.CandidateRanks, wantRanks) {
				t.Fatalf("pruned evaluation changed literal result: %+v", s)
			}
			wantRefs := map[string]resolverReporterRef{
				"N0KEEP": {refCount: 1, weightMilli: 1000}, "N0BOUND": {refCount: 1, weightMilli: 1000}, "N0SHARED": {refCount: 1, weightMilli: 1000},
			}
			if len(st.candidates) != 2 || !reflect.DeepEqual(st.reporterRefs, wantRefs) || st.lastEvalAt != at ||
				st.stableWinner != "K1ABC" || st.pendingWinner != "" || st.pendingWins != 0 {
				t.Fatal("pruning did not repair references and stale winner state")
			}
			wantRows := []ResolverReceiverObservation{
				{Receiver: "N0BOUND", ObservedAt: at.Add(-10 * time.Second), FrequencyKHz: 7010.1, WeightMilli: 1000},
				{Receiver: "N0KEEP", ObservedAt: at, FrequencyKHz: 7010.1, WeightMilli: 1000},
				{Receiver: "N0SHARED", ObservedAt: at, FrequencyKHz: 7010.1, WeightMilli: 1000},
			}
			var rows []ResolverReceiverObservation
			var calls []string
			s.ReceiverEvidence().Range(func(call string, row ResolverReceiverObservation) bool {
				calls = append(calls, call)
				rows = append(rows, row)
				return true
			})
			if !reflect.DeepEqual(rows, wantRows) || !reflect.DeepEqual(calls, []string{"K1ABC", "K1ABC", "K1ABD"}) || s.ReceiverEvidence().RecencyWindow() != 10*time.Second {
				t.Fatalf("wrong retained rows: %v / %v", calls, rows)
			}
		})
	}
}

func TestResolverSweepSchedulingBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name        string
		nextOffset  time.Duration
		lastAge     time.Duration
		dirty, loss bool
		evaluate    bool
		control     bool
	}{
		{"not_due_dirty", time.Nanosecond, time.Second, true, false, false, false},
		{"due_clean_before_interval", 0, time.Second - time.Nanosecond, false, false, false, false},
		{"due_clean_at_interval", 0, time.Second, false, false, true, false},
		{"due_dirty_at_equality", 0, time.Nanosecond, true, false, true, false},
		{"not_due_loss_control", time.Nanosecond, time.Second, true, true, false, true},
		{"due_clean_loss_no_control", 0, time.Second - time.Nanosecond, false, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, d, st, at := sweepBoundaryFixture(t)
			// Publish all five rows without pruning them. Sweep must prune even
			// when scheduling declines ordinary evaluation.
			r.attachKeyGeneration(st)
			st.lastEvalAt, st.nextEvalAt = at.Add(-tc.lastAge), at.Add(tc.nextOffset)
			st.dirty = tc.dirty
			if tc.loss {
				r.recordResolverCapLoss(st, at)
			}
			r.publishLossControl(st, at)
			before := st.cell.snapshot.Load()
			beforeValue := *before
			beforeValue.CandidateRanks = append([]ResolverCandidateSupport(nil), before.CandidateRanks...)
			beforeRows := sweepSnapshotRows(*before)
			lastEval, nextEval := st.lastEvalAt, st.nextEvalAt
			d.Sweep(at)
			after := st.cell.snapshot.Load()
			if len(st.candidates) != 2 || len(st.reporterRefs) != 3 {
				t.Fatal("scheduling skipped required pruning")
			}
			if tc.evaluate {
				if st.lastEvalAt != at || st.nextEvalAt != at.Add(time.Second) || st.dirty || after == before || after.EvaluatedAt != at || after.Winner != "K1ABC" {
					t.Fatalf("due evaluation/scheduling wrong: %+v", after)
				}
			} else {
				if st.lastEvalAt != lastEval || st.nextEvalAt != nextEval || st.dirty != tc.dirty || after.EvaluatedAt != lastEval {
					t.Fatal("declined evaluation advanced ordinary authority/scheduling")
				}
				if (after != before) != tc.control {
					t.Fatalf("control publication changed: published=%t want=%t", after != before, tc.control)
				}
				if tc.control && (after.TotalReporters != 3 || after.ReceiverEvidence().Len() != 3 || !after.EvidenceLoss || st.pendingWins != 0) {
					t.Fatalf("not-due control did not publish coherent pruned evidence: %+v", after)
				}
			}
			if len(beforeRows) != 5 || !reflect.DeepEqual(*before, beforeValue) || !reflect.DeepEqual(sweepSnapshotRows(*before), beforeRows) {
				t.Fatal("sweep mutated captured receiver generation")
			}
		})
	}
}

func TestResolverSweepRetirementBoundaries(t *testing.T) {
	for _, reason := range []string{"nil", "empty", "ttl", "loss_idle"} {
		t.Run(reason, func(t *testing.T) {
			r, d, key, at := lossTestDriver(t, SignalResolverConfig{InactiveTTL: time.Second})
			lossTestAdmit(t, r, d, key, "K1ABC", "N0KEEP", at, 10*time.Second)
			before := lossTestSnapshot(t, r, key)
			beforeRows := sweepSnapshotRows(before)
			st := d.states[key]
			cell := st.cell
			query := at
			switch reason {
			case "nil":
				d.states[key] = nil
			case "empty":
				st.recencyWindow = time.Nanosecond
				query = at.Add(2 * time.Nanosecond)
			case "ttl":
				d.Sweep(at.Add(time.Second))
				if len(d.states) != 1 {
					t.Fatal("inactive TTL equality retired early")
				}
				query = at.Add(time.Second + time.Nanosecond)
			case "loss_idle":
				r.recordResolverCapLoss(st, at)
				if err := d.SetProcessElapsed(10*time.Minute - time.Nanosecond); err != nil {
					t.Fatal(err)
				}
				d.Sweep(at)
				if len(d.states) != 1 {
					t.Fatal("process idle retired early")
				}
				if err := d.SetProcessElapsed(10 * time.Minute); err != nil {
					t.Fatal(err)
				}
			}
			d.Sweep(query)
			if len(d.states) != 0 || r.MetricsSnapshot().ActiveKeys != 0 || !cell.terminal.Load() || cell.snapshot.Load() != nil {
				t.Fatal("sweep failed to retire owner/captured cell")
			}
			if _, ok := r.snapshots.Load(key); ok {
				t.Fatal("retired generation retained in snapshot index")
			}
			if before.Generation != 1 || before.Winner != "K1ABC" || len(beforeRows) != 1 || !reflect.DeepEqual(sweepSnapshotRows(before), beforeRows) {
				t.Fatal("retirement mutated captured snapshot")
			}
			lossTestAdmit(t, r, d, key, "K1ABD", "N0NEW", query, 10*time.Second)
			after := lossTestSnapshot(t, r, key)
			if after.Generation != 2 || after.Winner != "K1ABD" || after.TotalReporters != 1 || after.EvidenceLoss || d.states[key].cell == cell {
				t.Fatalf("retirement/recreation reused old generation: %+v", after)
			}
		})
	}
}

func TestResolverSweepAdvancesHysteresisOnce(t *testing.T) {
	r, d, key, at := lossTestDriver(t, SignalResolverConfig{HysteresisWindows: 2})
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A1", at, time.Minute)
	for _, receiver := range []string{"N0B1", "N0B2", "N0B3"} {
		lossTestAdmit(t, r, d, key, "K1ABD", receiver, at.Add(time.Millisecond), time.Minute)
	}
	st := d.states[key]
	firstAt := at.Add(500 * time.Millisecond)
	d.Sweep(firstAt)
	first := lossTestSnapshot(t, r, key)
	if first.Winner != "K1ABC" || first.State != ResolverStateUncertain || st.pendingWinner != "K1ABD" || st.pendingWins != 1 || first.EvaluatedAt != firstAt {
		t.Fatalf("first sweep must advance exactly one window: %+v pending=%s/%d", first, st.pendingWinner, st.pendingWins)
	}
	secondAt := firstAt.Add(500 * time.Millisecond)
	d.Sweep(secondAt)
	second := lossTestSnapshot(t, r, key)
	if second.Winner != "K1ABD" || second.State != ResolverStateConfident || st.pendingWinner != "" || st.pendingWins != 0 || second.EvaluatedAt != secondAt {
		t.Fatalf("second sweep did not settle challenger: %+v", second)
	}
}
