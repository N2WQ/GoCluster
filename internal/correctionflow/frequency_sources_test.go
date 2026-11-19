package correctionflow

import (
	"testing"
	"time"

	"dxcluster/spot"
)

func TestCCV2F2CurrentReadWithoutDrain(t *testing.T) {
	f := newB31GenerationFixture(t, 4)
	f.cfg.ResolverNeighborhoodEnabled = true
	f.cfg.ResolverNeighborhoodBucketRadius = 1
	f.add(t, 0, "K1ABC", "N0AAA", f.at)
	f.add(t, 0, "K1ABC", "N0BBB", f.at)
	f.add(t, 0, "K1ABD", "N0CCC", f.at)
	f.driver.Sweep(f.at.Add(time.Millisecond))
	before := ReadFrequencySources(f.resolver, f.key, f.cfg, f.at.Add(time.Second))
	if before[2].Evidence.Len() != 3 {
		t.Fatal("positive published control missing")
	}
	if !f.resolver.Enqueue(spot.ResolverEvidence{Key: f.key, ObservedAt: f.at.Add(time.Second), DXCall: "K1ABD", Spotter: "N0DDD", FrequencyKHz: 14020, RecencyWindow: time.Minute}) {
		t.Fatal("enqueue failed")
	}
	current := ReadFrequencySources(f.resolver, f.key, f.cfg, f.at.Add(time.Second))
	if current[2].Evidence.Len() != 3 || f.driver.Drain() != 1 {
		t.Fatal("read forced queued evidence into publication")
	}
	f.driver.Sweep(f.at.Add(time.Second + time.Millisecond))
	fresh := ReadFrequencySources(f.resolver, f.key, f.cfg, f.at.Add(2*time.Second))
	if fresh[2].Evidence.Len() != 4 || FrequencySourceLifecycles(before) != FrequencySourceLifecycles(fresh) {
		t.Fatal("same-lifecycle row changes were cached or relabeled")
	}
	vec := FrequencySourceLifecycles(before)
	if vec.ReadMask != 14 || vec.PresentMask != 4 {
		t.Fatalf("read/presence=%b/%b", vec.ReadMask, vec.PresentMask)
	}
	f.add(t, 1, "W9XYZ", "N0EEE", f.at)
	f.driver.Sweep(f.at.Add(2 * time.Second))
	created := FrequencySourceLifecycles(ReadFrequencySources(f.resolver, f.key, f.cfg, f.at.Add(2*time.Second)))
	if vec == created || created.PresentMask != 12 {
		t.Fatal("formerly excluded new source presence was lost")
	}
	if !f.resolver.Enqueue(spot.ResolverEvidence{Key: f.key, ObservedAt: f.at.Add(3 * time.Second), DXCall: "K1ABC", Spotter: "N0AAA", FrequencyKHz: 14020, RecencyWindow: time.Minute, Report: 18, HasReport: true}) {
		t.Fatal("SNR update refused")
	}
	if f.driver.Drain() != 1 {
		t.Fatal("SNR update missing")
	}
	f.driver.Sweep(f.at.Add(3*time.Second + time.Millisecond))
	snrFresh := ReadFrequencySources(f.resolver, f.key, f.cfg, f.at.Add(4*time.Second))
	found := false
	snrFresh[2].Evidence.Range(func(call string, row spot.ResolverReceiverObservation) bool {
		if call == "K1ABC" && row.Receiver == "N0AAA" {
			found = row.HasReport && row.Report == 18
		}
		return true
	})
	if !found || FrequencySourceLifecycles(snrFresh) != created {
		t.Fatal("same-generation receiver SNR update was cached or confused with lifecycle change")
	}
	var synthetic [5]FrequencySource
	synthetic[2] = FrequencySource{Read: true, Present: true}
	zero := FrequencySourceLifecycles(synthetic)
	synthetic[2].Present = false
	if zero == FrequencySourceLifecycles(synthetic) {
		t.Fatal("present generation zero became missing")
	}
	if expired := ReadFrequencySources(f.resolver, f.key, f.cfg, f.at.Add(2*time.Minute)); expired[2].Evidence.Len() != 0 {
		t.Fatal("clock expiry retained rows")
	}
}

func TestCCV2F2ReleaseAuthorityResult(t *testing.T) {
	f := newB31GenerationFixture(t, 4)
	f.add(t, 0, "K1ABC", "N0AAA", f.at)
	f.add(t, 0, "K1ABC", "N0BBB", f.at)
	f.add(t, 0, "K1ABD", "N0CCC", f.at)
	f.driver.Sweep(f.at.Add(time.Millisecond))
	saved := f.selection(f.at.Add(time.Second))
	decision := TemporalDecision{Action: TemporalDecisionActionApply, Winner: "K1ABD", Selection: saved}
	released, allowed := SelectTemporalReleaseSnapshotWithAuthority(f.resolver, f.key, f.cfg, "K1ABE", decision, f.at.Add(time.Second))
	if !allowed || released.Snapshot.Winner != "K1ABD" || released.Snapshot.TotalReporters != 3 {
		t.Fatalf("existing release transform unreachable: %+v allowed=%v", released, allowed)
	}
	decision.Winner = "W1XYZ"
	if _, allowed := SelectTemporalReleaseSnapshotWithAuthority(f.resolver, f.key, f.cfg, "K1ABE", decision, f.at.Add(time.Second)); allowed {
		t.Fatal("unsupported temporal identity allowed")
	}
	decision.Winner = "K1ABD"
	decision.Selection.generationWitnesses[2]++
	if _, allowed := SelectTemporalReleaseSnapshotWithAuthority(f.resolver, f.key, f.cfg, "K1ABE", decision, f.at.Add(time.Second)); allowed {
		t.Fatal("retired lifecycle allowed")
	}
	decision.Selection = saved
	if _, allowed := SelectTemporalReleaseSnapshotWithAuthority(f.resolver, f.key, f.cfg, "K1ABE", decision, f.at.Add(2*time.Minute)); allowed {
		t.Fatal("expired release allowed")
	}
}
