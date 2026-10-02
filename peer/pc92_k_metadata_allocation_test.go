package peer

import (
	"strings"
	"testing"
	"time"
)

func TestPC92GraphKSubjectNewNodeCharge(t *testing.T) {
	for _, tc := range []struct {
		name, subject   string
		metadata, fixed int
	}{
		// Calls and one-byte zero strings each round to 8 bytes. External K
		// adds two nodes plus one raw edge whose numeric strings remain empty.
		{"origin", "5N2AAA", 24, 512},
		{"external", "6N3EXT", 40, 2*512 + 256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			g := newProtocolGraph(now)
			r := graphRecord(t, "PC92^N2AAA^43200^K^"+tc.subject+"^0^0^H1^")
			plan, err := g.prepare(r, "N0LOCAL", "N1PEER", nil)
			if err != nil || plan == nil {
				t.Fatalf("prepare: %v", err)
			}
			peak, final := g.metadataMutationCharge(plan)
			want := graphMutationScratchBytes + tc.fixed + tc.metadata
			if peak != tc.metadata || final != tc.metadata || g.projectedCharge(plan) != want {
				t.Fatalf("new K charge peak=%d final=%d projected=%d; want metadata=%d projected=%d", peak, final, g.projectedCharge(plan), tc.metadata, want)
			}
			g.commit(plan, now)
			if n := g.nodes.Value(r.Subject.Call); n.Entry.Version != "0" || n.Entry.Build != "0" || g.metadataBytes != tc.metadata || g.retainedCharge() != want {
				t.Fatalf("new node commitment/charge=%+v metadata=%d retained=%d", n.Entry, g.metadataBytes, g.retainedCharge())
			}
			// A separate synthetic-headroom fixture tests exact admission without
			// treating these counters as a measured aggregate allocation proof.
			g = newProtocolGraph(now)
			g.metadataBytes = (96 << 20) - want + 1
			if _, err = g.prepare(r, "N0LOCAL", "N1PEER", nil); err == nil || g.nodes.Len() != 0 {
				t.Fatal("one-byte-over new subject escaped effective numeric charge")
			}
			g.metadataBytes--
			plan, err = g.prepare(r, "N0LOCAL", "N1PEER", nil)
			if err != nil || plan == nil || g.projectedCharge(plan) != 96<<20 {
				t.Fatalf("exact new subject headroom refused: %v", err)
			}
		})
	}
}

func TestPC92GraphKSubjectReplacementCharge(t *testing.T) {
	version, build := strings.Repeat("1", 8193), strings.Repeat("2", 4097)
	for _, tc := range []struct {
		name, suffix, nextVersion, nextBuild string
		cloned, finalNumeric                 int
	}{
		// Independently derived from the qualified allocator classes:
		// 8193 -> 9472, 4097 -> 4864, and each newly cloned "0" -> 8 bytes.
		{"both_cleared", "", "0", "0", 16, 16},
		{"build_cleared", ":" + version, version, "0", 8, 9472 + 8},
		{"version_cleared", "::" + build, "0", build, 8, 8 + 4864},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			g := newProtocolGraph(now)
			applyGraphRecord(t, g, "PC92^N2AAA^43200^C^5N2AAA:"+version+":"+build+"^H1^", now)
			const oldNumeric = 9472 + 4864
			if g.metadataBytes != 8+oldNumeric {
				t.Fatalf("fixture did not establish independently charged metadata: %d", g.metadataBytes)
			}
			r := graphRecord(t, "PC92^N2AAA^43201^K^5N2AAA"+tc.suffix+"^0^0^H1^")
			// The replacement's final state is smaller, but commitment first
			// clones changed strings while the old node still owns its backing.
			g.metadataBytes = (96 << 20) - graphMutationScratchBytes - 512 - tc.cloned + 1
			before := g.metadataBytes
			if _, err := g.prepare(r, "N0LOCAL", "N1PEER", nil); err == nil {
				t.Fatal("smaller final metadata concealed replacement overlap")
			}
			if old := g.nodes.Value("N2AAA").Entry; old.Version != version || old.Build != build || g.metadataBytes != before {
				t.Fatal("refused K mutated metadata or its charge")
			}
			g.metadataBytes--
			before = g.metadataBytes
			plan, err := g.prepare(r, "N0LOCAL", "N1PEER", nil)
			if err != nil || plan == nil {
				t.Fatalf("exact overlap allowance refused: %v", err)
			}
			peak, final := g.metadataMutationCharge(plan)
			wantFinal := before - oldNumeric + tc.finalNumeric
			if peak != before+tc.cloned || final != wantFinal || g.projectedCharge(plan) != 96<<20 {
				t.Fatalf("peak=%d final=%d projected=%d; want %d/%d/%d", peak, final, g.projectedCharge(plan), before+tc.cloned, wantFinal, 96<<20)
			}
			g.commit(plan, now)
			if entry := g.nodes.Value("N2AAA").Entry; entry.Version != tc.nextVersion || entry.Build != tc.nextBuild || g.metadataBytes != wantFinal {
				t.Fatalf("commit disagrees with independently derived metadata charge: %+v bytes=%d want %d", entry, g.metadataBytes, wantFinal)
			}
		})
	}
	t.Run("already_zero", func(t *testing.T) {
		now := time.Now()
		g := newProtocolGraph(now)
		applyGraphRecord(t, g, "PC92^N2AAA^43200^K^5N2AAA^0^0^H1^", now)
		before := g.retainedCharge()
		plan, err := g.prepare(graphRecord(t, "PC92^N2AAA^43201^K^5N2AAA:0:0^0^0^H1^"), "N0LOCAL", "N1PEER", nil)
		if err != nil || plan == nil {
			t.Fatal(err)
		}
		peak, final := g.metadataMutationCharge(plan)
		if peak != 24 || final != 24 || g.projectedCharge(plan) != before {
			t.Fatal("unchanged zero strings invented an overlap reservation")
		}
		g.commit(plan, now)
		if g.metadataBytes != 24 || g.retainedCharge() != before {
			t.Fatal("repeated zero K changed retained metadata charge")
		}
	})
}
