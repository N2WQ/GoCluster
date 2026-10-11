package spot

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestResolverLossConcurrentReadersRetainImmutableGenerations(t *testing.T) {
	r, d, key, at := lossTestDriver(t, SignalResolverConfig{MaxCandidatesPerKey: 2, MaxReportersPerCand: 2})
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A1", at.Add(-100*time.Second), 2*time.Minute)
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A2", at, 2*time.Minute)
	lossTestAdmit(t, r, d, key, "K1ABD", "N0B1", at, 2*time.Minute)
	d.Sweep(at.Add(time.Second))
	clean := lossTestSnapshot(t, r, key)
	lossTestAdmit(t, r, d, key, "K1ABC", "N0A3", at.Add(time.Second+time.Millisecond), 2*time.Minute)
	quarantined := lossTestSnapshot(t, r, key)
	if clean.EvidenceLoss || !quarantined.EvidenceLoss || clean.Generation != quarantined.Generation {
		t.Fatal("immutable reader fixtures do not straddle loss")
	}
	stop := make(chan struct{})
	ready := make(chan struct{}, 4)
	failures := make(chan string, 4)
	type phaseExpectation struct {
		loss   bool
		window time.Duration
	}
	var phases [4]chan phaseExpectation
	acknowledged := make(chan int, 4)
	reportFailure := func(message string) {
		select {
		case failures <- message:
		default:
		}
	}
	var readers sync.WaitGroup
	var reads atomic.Int64
	for reader := 0; reader < 4; reader++ {
		phases[reader] = make(chan phaseExpectation, 1)
		readers.Add(1)
		go func(reader int) {
			defer readers.Done()
			announced := false
			for {
				select {
				case <-stop:
					return
				case phase := <-phases[reader]:
					for _, lookup := range []func() (ResolverSnapshot, bool){func() (ResolverSnapshot, bool) { return r.Lookup(key) }, func() (ResolverSnapshot, bool) { return r.LookupEligible(key, at.Add(2*time.Second)) }} {
						s, ok := lookup()
						if !ok || s.EvidenceLoss != phase.loss || s.Generation != clean.Generation || s.TotalReporters != 3 || s.TotalWeightedSupportMilli != 3000 || s.ReceiverEvidence().Len() != 3 || s.ReceiverEvidence().RecencyWindow() != phase.window || s.Compact().EvidenceLoss != phase.loss {
							reportFailure("reader missed literal loss/window publication phase")
						}
						if row, exists := lossTestRow(s, "K1ABC", "N0A3"); !exists || row.WeightMilli != 1000 {
							reportFailure("phase reader lost retained newest A receiver")
						}
					}
					acknowledged <- reader
				default:
				}
				for _, retained := range []ResolverSnapshot{clean, quarantined} {
					compact := retained.Compact()
					if compact.Generation != clean.Generation || compact.EvidenceLoss != retained.EvidenceLoss || compact.ReceiverEvidence() != nil || retained.TotalReporters != 3 || retained.TotalWeightedSupportMilli != 3000 || retained.ReceiverEvidence().Len() != 3 {
						reportFailure("retained reader metadata changed")
					}
					if row, exists := lossTestRow(retained, "K1ABD", "N0B1"); !exists || row.ObservedAt != at || row.WeightMilli != 1000 || row.FrequencyKHz != 7010.1 {
						reportFailure("retained dissent changed")
					}
				}
				if row, exists := lossTestRow(clean, "K1ABC", "N0A1"); !exists || row.ObservedAt != at.Add(-100*time.Second) {
					reportFailure("clean old receiver changed")
				}
				if row, exists := lossTestRow(quarantined, "K1ABC", "N0A3"); !exists || row.ObservedAt != at.Add(time.Second+time.Millisecond) {
					reportFailure("quarantined incoming receiver changed")
				}
				for _, lookup := range []func() (ResolverSnapshot, bool){func() (ResolverSnapshot, bool) { return r.Lookup(key) }, func() (ResolverSnapshot, bool) { return r.LookupEligible(key, at.Add(2*time.Second)) }} {
					s, ok := lookup()
					if !ok {
						reportFailure("live generation vanished during owner churn")
					}
					if s.Generation != clean.Generation || s.TotalReporters > 4 || len(s.CandidateRanks) > 2 || s.ReceiverEvidence().Len() > 4 || s.TotalWeightedSupportMilli != s.TotalReporters*1000 {
						reportFailure("live publication exceeded bounds or lost exact totals")
					}
					if s.Compact().EvidenceLoss != s.EvidenceLoss {
						reportFailure("live Compact discarded loss marker")
					}
				}
				reads.Add(1)
				if !announced {
					ready <- struct{}{}
					announced = true
				}
			}
		}(reader)
	}
	for reader := 0; reader < 4; reader++ {
		<-ready
	}
	// The owner waits for every reader to observe all three literal publications;
	// no sleep or scheduler timing can substitute for active/shrink/reactivation.
	for index, phase := range []phaseExpectation{{true, 2 * time.Minute}, {false, 20 * time.Second}, {true, 2 * time.Minute}} {
		if index > 0 {
			lossTestAdmit(t, r, d, key, "K1ABC", "N0A3", at.Add(time.Second+time.Duration(index+1)*time.Millisecond), phase.window)
		}
		for reader := range phases {
			phases[reader] <- phase
		}
		seen := map[int]bool{}
		for reader := 0; reader < 4; reader++ {
			seen[<-acknowledged] = true
		}
		if len(seen) != 4 {
			t.Error("publication phase lacked an acknowledgement from each reader")
		}
	}
	// These exact controls deactivate/reactivate the initial old loss while
	// reader-held clean and quarantined populations remain immutable.
	for i := 0; i < 64; i++ {
		window := 20 * time.Second
		if i%2 != 0 {
			window = 2 * time.Minute
		}
		lossTestAdmit(t, r, d, key, "K1ABC", "N0A3", at.Add(time.Second+2*time.Millisecond+time.Duration(i)*time.Nanosecond), window)
		s := lossTestSnapshot(t, r, key)
		if s.EvidenceLoss != (i%2 != 0) || s.ReceiverEvidence().RecencyWindow() != window || s.Generation != clean.Generation || s.TotalReporters != 3 {
			t.Errorf("literal control boundary %+v", s)
			break
		}
	}
	// Actual candidate and receiver eviction then replace the owner population
	// under the same live UID while concurrent readers continue to project it.
	for i := 0; i < 64; i++ {
		call := []string{"K1ABC", "K1ABD", "K1ABE"}[i%3]
		lossTestAdmit(t, r, d, key, call, fmt.Sprintf("N0Z%d", i), at.Add(time.Second+3*time.Millisecond), 2*time.Minute)
	}
	close(stop)
	readers.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	if reads.Load() == 0 {
		t.Fatal("no concurrent reader completed")
	}
	if metrics := r.MetricsSnapshot(); metrics.EvictedCandidates == 0 || metrics.EvictedReporters < 2 || metrics.PublishedReceiverRecords > 4 {
		t.Fatalf("owner churn did not exercise both caps: %+v", metrics)
	}
	if clean.EvidenceLoss || clean.lossWindow != 2*time.Minute || quarantined.lossMinAt != at.Add(-100*time.Second) || quarantined.lossMaxAt != at.Add(-100*time.Second) || !quarantined.EvidenceLoss {
		t.Fatal("retained scalar metadata mutated")
	}
	lossTestRank(t, clean, "K1ABC", 2, 2000)
	lossTestRank(t, clean, "K1ABD", 1, 1000)
	lossTestRank(t, quarantined, "K1ABC", 2, 2000)
	lossTestRank(t, quarantined, "K1ABD", 1, 1000)
}

func TestResolverCapturedReaderCellDeclinesAfterOwnerRetirement(t *testing.T) {
	r, d, key, at, old := lossTestQuarantine(t, SignalResolverConfig{})
	snapshot, cell, ok := r.lookupSnapshot(key)
	if !ok || snapshot.Generation != old.Generation {
		t.Fatal("capture publication before retirement")
	}
	if err := d.SetProcessElapsed(10 * time.Minute); err != nil {
		t.Fatal(err)
	}
	d.Sweep(at)
	lossTestAdmit(t, r, d, key, "K1ABD", "N0NEW", at, time.Hour)
	newSnapshot := lossTestSnapshot(t, r, key)
	// This is the read ordering used by LookupEligible: immutable projection
	// first, captured-cell final validation second. The replacement's activity
	// and fresh loss-free snapshot cannot make the captured old cell current.
	snapshot.EvidenceLoss = resolverSnapshotLossEligible(snapshot, at)
	projected := r.projectEligibleSnapshot(snapshot, at)
	if r.cellCurrent(cell) || cell.snapshot.Load() != nil || !cell.terminal.Load() || projected.Generation != old.Generation || !projected.EvidenceLoss || newSnapshot.Generation == projected.Generation {
		t.Fatal("old reader gained authority from replacement generation")
	}
	if _, exists := lossTestRow(projected, "K1ABC", "N0A2"); !exists {
		t.Fatal("retirement mutated reader's immutable evidence")
	}
}
