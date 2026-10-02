package peer

import (
	"fmt"
	"math/bits"
	"testing"
	"time"
	"unsafe"
)

func TestPC92GraphIndexAllocationEnvelopes(t *testing.T) {
	// Each graph coefficient covers live entries plus BOTH bucket generations
	// under the retained high-water count, using the qualified allocation table.
	// This checks the coefficient derivation, not runtime allocation qualification.
	for _, tc := range []struct {
		name                                     string
		limit, coefficient, entryBytes, keyBytes int
	}{
		{"members", maxGraphEdges, 256, pointerAllocationBytes(int(unsafe.Sizeof(boundedEntry[memberKey, PC92Entry]{}))), 0},
		{"users", maxGraphUsers, 132, pointerAllocationBytes(int(unsafe.Sizeof(boundedEntry[string, int]{}))), 48},
		{"ingress", maxIngressObservations, 160, pointerAllocationBytes(int(unsafe.Sizeof(boundedEntry[ingressKey, ingressObservation]{}))), 0},
		{"freshness", maxFreshnessOrigins, 196, pointerAllocationBytes(int(unsafe.Sizeof(boundedEntry[string, originWatermark]{}))), 48},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for high := 1; high <= tc.limit; high++ {
				buckets := 1 << bits.Len(uint(high-1))
				backing := pointerAllocationBytes(buckets * 8)
				// Growth replaces a half-sized array; no entry generation copies.
				growth := backing + pointerAllocationBytes((buckets/2)*8)
				shrink := backing
				if high >= 32 {
					remaining := high / 4
					shrink += pointerAllocationBytes((1 << bits.Len(uint(remaining-1))) * 8)
				}
				peak := high*(tc.entryBytes+tc.keyBytes) + max(growth, shrink)
				if peak > high*tc.coefficient {
					t.Fatalf("high-water%d peak%d exceeds coefficient%d; old backing must remain charged through compaction", high, peak, high*tc.coefficient)
				}
			}
		})
	}
	// Each node independently owns its member-index header, node allocation,
	// root index entry, root key and bucket allowance. Empty nodes retain no
	// member buckets, but these headers remain explicitly covered.
	node := pointerAllocationBytes(int(unsafe.Sizeof(graphNode{}))) +
		pointerAllocationBytes(int(unsafe.Sizeof(boundedIndex[memberKey, PC92Entry]{}))) +
		pointerAllocationBytes(int(unsafe.Sizeof(boundedEntry[string, *graphNode]{}))) + 48 + 32
	if node > 512 {
		t.Fatalf("node's complete fixed ownership%d exceeds512", node)
	}
}

func TestPC92GraphTypedScratchEnvelope(t *testing.T) {
	// Deliberately combine independent maxima rather than assuming one input
	// can attain all of them. C owns no population-sized removal array; D scans
	// desired keys directly. Existing graph generations are charged separately.
	const members = 8191
	entry := pointerAllocationBytes(int(unsafe.Sizeof(boundedEntry[memberKey, plannedMember]{})))
	desired := members*entry + pointerAllocationBytes(8192*8) + pointerAllocationBytes(4096*8)
	nodes := 4096*pointerAllocationBytes(int(unsafe.Sizeof(boundedEntry[string, PC92Entry]{}))) + pointerAllocationBytes(4096*8) + pointerAllocationBytes(2048*8)
	frames := pointerAllocationBytes(8196 * 16)
	decodedAndAdded := 2 * pointerAllocationBytes(members*int(unsafe.Sizeof(PC92Entry{})))
	// The prior normalization proof bounds at most24577 strings partitioning
	// 64KiB input at253959 rounded bytes.16KiB covers fixed headers/direct index.
	const stringsAndFixed = 253959 + (16 << 10) + (64 << 10)
	total := desired + nodes + frames + decodedAndAdded + stringsAndFixed
	t.Logf("typed planned entry=%d desired=%d nodes=%d frame=%d arrays=%d other=%d total=%d", entry, desired, nodes, frames, decodedAndAdded, stringsAndFixed, total)
	if total > graphMutationScratchBytes {
		t.Fatalf("scratch envelope%d exceeds%d", total, graphMutationScratchBytes)
	}
}

func forceGraphIndexCollisions[K comparable, V any](index *boundedIndex[K, V]) {
	index.resize(1)
	index.fixed = true
}

func TestPC92GraphCollisionExpiryDeletesCurrentWithoutSkipping(t *testing.T) {
	now := time.Now()
	graph := newProtocolGraph(now)
	forceGraphIndexCollisions(graph.nodes)
	forceGraphIndexCollisions(graph.users)
	forceGraphIndexCollisions(graph.ingress)
	forceGraphIndexCollisions(graph.freshness)
	for i := range 20 {
		call := scratchFixtureCall("N0", i, 3)
		applyGraphRecord(t, graph, "PC92^"+call+"^43200^C^5"+call+"^1K1USER^1K2USER^H2^", now)
		forceGraphIndexCollisions(graph.nodes.Value(call).Members)
		graph.nodes.Value(call).Observations = 1
		graph.commitWatermark(call, 43200, now, false)
		graph.observe(call, "W1AA", 2, now, false)
		graph.observe(call, "W2AA", 2, now, false)
	}
	graph.loseIngress("W1AA")
	if graph.ingress.Len() != 20 {
		t.Fatal("collision-chain deletion skipped an ingress owner")
	}
	protected := scratchFixtureCall("N0", 7, 3)
	direct := newBoundedIndex[string, bool](1)
	direct.Set(protected, true)
	graph.expire(now.Add(time.Hour), false, direct)
	if graph.nodes.Len() != 1 || graph.nodes.Value(protected) == nil || graph.users.Len() != 2 || graph.edges != 2 || graph.ingress.Len() != 1 || graph.freshness.Len() != 20 {
		t.Fatal("collision expiry lost direct authority, skipped expired owners or erased unsafe-clock freshness")
	}
	if graph.users.Value("K1USER") != 1 || graph.users.Value("K2USER") != 1 {
		t.Fatal("collision expiry retained a historical parent reference")
	}
	graph.expire(now.Add(time.Hour), true, direct)
	if graph.freshness.Len() != 1 {
		t.Fatal("safe detached-watermark cleanup skipped collision-chain entries")
	}
}

func TestPC92GraphConstantOccupancyChurnHasFixedIndexBacking(t *testing.T) {
	now := time.Now()
	graph := newProtocolGraph(now)
	var backing int
	for i := range 1000 {
		call := scratchFixtureCall("K0", i, 3)
		applyGraphRecord(t, graph, fmt.Sprintf("PC92^N2AAA^43200^C^5N2AAA^1%s^H1^", call), now)
		node := graph.nodes.Value("N2AAA")
		current := graph.nodes.AllocationBytes() + graph.users.AllocationBytes() + node.Members.AllocationBytes()
		if i == 0 {
			backing = current
		}
		if current != backing || graph.edges != 1 || graph.users.Len() != 1 || graph.users.Value(call) != 1 {
			t.Fatalf("history%d changed fixed occupancy/backing: bytes%d want%d", i, current, backing)
		}
	}
}

func TestPC92GraphReplacementReleasesRemovedMetadataBeforeAdding(t *testing.T) {
	now := time.Now()
	graph := newProtocolGraph(now)
	applyGraphRecord(t, graph, "PC92^N2AAA^43200^C^5N2AAA^1K1OLD^H1^", now)
	plan, err := graph.prepare(graphRecord(t, "PC92^N2AAA^43201^C^5N2AAA^1K2NEW^H1^"), "N0LOCAL", "W1AA", nil)
	if err != nil {
		t.Fatal(err)
	}
	peak, final := graph.metadataMutationCharge(plan)
	if peak != final {
		t.Fatalf("two-pass replacement unexpectedly retained removed calls: peak%d final%d", peak, final)
	}
	graph.commit(plan, now)
	if graph.users.Value("K1OLD") != 0 || graph.users.Value("K2NEW") != 1 {
		t.Fatal("overlap reservation altered complete replacement semantics")
	}
}
