package peer

import (
	"testing"
	"time"
)

// The iterator is shared by admission/accounting/commit. Its contract is
// allocation-free traversal with D bounded by input rather than population.
func BenchmarkPC92GraphRemovalTraversal(b *testing.B) {
	for _, action := range []string{"A", "D", "C"} {
		b.Run(action, func(b *testing.B) {
			existing := newBoundedIndex[memberKey, PC92Entry](8192)
			for i := range 8192 {
				e := PC92Entry{Call: scratchFixtureCall("K0", i, 3), Flags: 1}
				existing.Set(membershipKey(e), e)
			}
			plan := &graphPlan{record: &PC92Record{Action: action}, desired: newBoundedIndex[memberKey, plannedMember](1)}
			plan.desired.Set(memberKey{"K0AAA", false}, plannedMember{entry: PC92Entry{Call: "K0AAA", Flags: 1}})
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				count := 0
				for range plan.removals(existing) {
					count++
				}
				want := 0
				if action == "D" {
					want = 1
				}
				if action == "C" {
					want = 8191
				}
				if count != want {
					b.Fatalf("removal count %d != %d", count, want)
				}
			}
		})
	}
}

func BenchmarkPC92GraphTypedDelta(b *testing.B) {
	now := time.Now()
	g := newProtocolGraph(now)
	entries := make([]PC92Entry, 8192)
	for i := range entries {
		entries[i] = PC92Entry{Call: scratchFixtureCall("K0", i, 3), Flags: 1}
	}
	r := &PC92Record{Origin: "N2AAA", Subject: PC92Entry{Call: "N2AAA", Flags: 5}, Action: "C", Members: entries}
	p, err := g.prepare(r, "N0LOCAL", "W1AA", nil)
	if err != nil {
		b.Fatal(err)
	}
	g.commit(p, now)
	r.Action = "A"
	r.Members = entries[:1]
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		p, err = g.prepare(r, "N0LOCAL", "W1AA", nil)
		if err != nil {
			b.Fatal(err)
		}
		g.commit(p, now)
	}
	if g.edges != 8192 || g.users.Len() != 8192 {
		b.Fatal("delta changed population")
	}
}
