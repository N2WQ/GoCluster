package spot

import (
	"fmt"
	"testing"
	"time"
)

const cwCapacityTestWindow = 5 * time.Minute

func cwCapacityDriver(t testing.TB, limit int, expand bool, mode string) (*SignalResolver, *SignalResolverDriver, ResolverSignalKey) {
	t.Helper()
	r := NewSignalResolver(SignalResolverConfig{
		ExpandCWReporterCapacity: expand, MaxCandidatesPerKey: limit, MaxReportersPerCand: 64,
		EvalMinInterval: time.Millisecond, SweepInterval: time.Millisecond, InactiveTTL: 10 * time.Minute,
	})
	d, err := NewSignalResolverDriver(r)
	if err != nil {
		t.Fatal(err)
	}
	return r, d, NewResolverSignalKey(14020, "20m", mode, 400)
}

func cwCapacityAdmit(t testing.TB, d *SignalResolverDriver, key ResolverSignalKey, at time.Time, call int, first, count int) {
	t.Helper()
	for i := first; i < first+count; i++ {
		e := ResolverEvidence{Key: key, ObservedAt: at, DXCall: fmt.Sprintf("K1CAP%d", call), Spotter: fmt.Sprintf("N0R%d-%d", call, i),
			FrequencyKHz: 14020, RecencyWindow: cwCapacityTestWindow}
		if !d.resolver.Enqueue(e) || d.Drain() != 1 {
			t.Fatal("failed to admit receiver")
		}
	}
}

func TestCWResolverReceiverBudget(t *testing.T) {
	at := time.Date(2026, 11, 28, 3, 0, 0, 0, time.UTC)
	fresh := at.Add(time.Minute)
	r, d, key := cwCapacityDriver(t, 16, true, "CW")

	cwCapacityAdmit(t, d, key, at, 0, 0, 256)
	cwCapacityAdmit(t, d, key, fresh, 0, 0, 1) // Refresh a receiver without charging another row.
	cwCapacityAdmit(t, d, key, fresh, 1, 0, 128)
	for call := 2; call < 16; call++ {
		// The bucket's extra budget is exhausted, but every new callsign
		// can still add its full base of 64 unique receivers.
		cwCapacityAdmit(t, d, key, fresh, call, 0, 64)
	}
	d.Sweep(fresh.Add(time.Millisecond))
	st := d.states[key]
	snap, ok := r.Lookup(key)
	if !ok || snap.EvidenceLoss || snap.ReceiverEvidence().Len() != 1280 || st.cwExtraRows != 256 {
		t.Fatalf("full clean bucket: evidence=%t loss=%t rows=%d extra=%d", ok, snap.EvidenceLoss, snap.ReceiverEvidence().Len(), st.cwExtraRows)
	}

	checkOverflow := func(call, first, retained int, evictions uint64) {
		t.Helper()
		cwCapacityAdmit(t, d, key, fresh, call, first, 1)
		snap, ok := r.Lookup(key)
		m := r.MetricsSnapshot()
		if !ok || !snap.EvidenceLoss || snap.ReceiverEvidence().Len() != 1280 || st.cwExtraRows != 256 ||
			len(st.candidates[fmt.Sprintf("K1CAP%d", call)].reporters) != retained ||
			m.EvictedReporters != evictions || m.CapLossDiscards != evictions {
			t.Fatalf("overflow failed to retain the bounded population and publish loss: metrics=%+v", m)
		}
	}
	checkOverflow(0, 256, 256, 1)
	checkOverflow(2, 64, 64, 2)
	// The old observations of call 0 expire first. The entry remains
	// active, but call 2 can use its returned expansion capacity immediately.
	expire := at.Add(cwCapacityTestWindow + time.Millisecond)
	d.Sweep(expire)
	if len(st.candidates["K1CAP0"].reporters) != 2 || st.cwExtraRows != 64 {
		t.Fatalf("age prune did not return expansion budget: extra=%d", st.cwExtraRows)
	}
	cwCapacityAdmit(t, d, key, expire, 2, 65, 128)
	snap, ok = r.Lookup(key)
	if len(st.candidates["K1CAP2"].reporters) != 192 || st.cwExtraRows != 192 || !ok || !snap.EvidenceLoss {
		t.Fatalf("returning capacity must neither pin old allocations nor clear still-eligible loss")
	}
}

func TestCWResolverReceiverCapacityCompaction(t *testing.T) {
	at := time.Date(2026, 11, 28, 3, 0, 0, 0, time.UTC)
	r, d, key := cwCapacityDriver(t, 1, true, "CW")
	for cycle := range 4 {
		start := at.Add(time.Duration(cycle) * 7 * time.Minute)
		cwCapacityAdmit(t, d, key, start, cycle, 0, 256)
		cwCapacityAdmit(t, d, key, start.Add(time.Minute), cycle, 0, 1)
		st := d.states[key]
		c := st.candidates[fmt.Sprintf("K1CAP%d", cycle)]
		if st.cwExtraRows != 192 || c.reporterPeak != 256 || st.reporterRefsPeak != 256 {
			t.Fatalf("cycle %d did not charge or observe the full expanded maps", cycle)
		}
		oldReporters, oldRefs := c.reporters, st.reporterRefs
		d.Sweep(start.Add(cwCapacityTestWindow + time.Millisecond))
		if st.cwExtraRows != 0 || c.reporterPeak != 1 || st.reporterRefsPeak != 1 {
			t.Fatalf("cycle %d retained the expanded backing after pruning", cycle)
		}
		// Prove both maps were rebuilt, with the survivor's reference intact.
		probe := fmt.Sprintf("N0R%d-0", cycle)
		if len(c.reporters) != 1 || len(st.reporterRefs) != 1 || st.reporterRefs[probe].refCount != 1 {
			t.Fatalf("cycle %d lost or duplicated the surviving membership", cycle)
		}
		delete(oldReporters, probe)
		delete(oldRefs, probe)
		if len(c.reporters) != 1 || len(st.reporterRefs) != 1 {
			t.Fatal("compaction kept stale backing maps")
		}
	}
	if r.MetricsSnapshot().CapLossDiscards != 0 {
		t.Fatal("ordinary expiry must never count as cap loss")
	}
}

func TestCWResolverReceiverCapacityScopeAndEviction(t *testing.T) {
	at := time.Date(2026, 11, 28, 3, 0, 0, 0, time.UTC)
	for _, mode := range []string{"RTTY", "USB", "LSB", "CW"} {
		t.Run(mode, func(t *testing.T) {
			// Explicit opt-in still leaves other modes at 64. Unexpanded
			// CW mirrors the frozen study and direct/default callers.
			r, d, key := cwCapacityDriver(t, 16, mode != "CW", mode)
			cwCapacityAdmit(t, d, key, at, 0, 0, 65)
			if d.states[key].cwExtraRows != 0 || r.MetricsSnapshot().EvictedReporters != 1 {
				t.Fatal("non-expanded mode exceeded its existing cap")
			}
		})
	}
	r, d, key := cwCapacityDriver(t, 1, true, "CW")
	cwCapacityAdmit(t, d, key, at, 0, 0, 256)
	cwCapacityAdmit(t, d, key, at.Add(time.Second), 1, 0, 256)
	if st := d.states[key]; st.cwExtraRows != 192 || len(st.candidates["K1CAP1"].reporters) != 256 ||
		r.MetricsSnapshot().EvictedCandidates != 1 || r.MetricsSnapshot().EvictedReporters != 0 {
		t.Fatal("candidate eviction did not return its entire expansion budget")
	}
}
