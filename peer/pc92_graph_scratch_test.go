package peer

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

func scratchFixtureCall(prefix string, index, letters int) string {
	var suffix [4]byte
	for i := letters - 1; i >= 0; i-- {
		suffix[i] = byte('A' + index%26)
		index /= 26
	}
	return prefix + string(suffix[:letters])
}

// This reachable shape maximizes remove backing while the sparse graph still
// permits4095 new node plans. A full4096-node fixture cannot exercise that pair.
func TestPC92GraphSparseOriginReplacementScratch(t *testing.T) {
	graph := newProtocolGraph(time.Now())
	for start := 0; start < maxGraphUsers; start += 4096 {
		var wire strings.Builder
		wire.WriteString("PC92^N1ROOT^43200^A^^")
		for i := start; i < start+4096; i++ {
			wire.WriteString("1" + scratchFixtureCall("K0", i, 4) + "^")
		}
		wire.WriteString("H1^")
		record := graphRecord(t, wire.String())
		plan, err := graph.prepare(record, "N0LOCAL", "W1AA", nil)
		if err != nil {
			t.Fatal(err)
		}
		graph.commit(plan, time.Now())
	}
	if graph.nodes.Len() != 1 || graph.users.Len() != maxGraphUsers {
		t.Fatal("sparse-origin setup did not reach its authority bounds")
	}
	var next strings.Builder
	next.WriteString("PC92^N1ROOT^43201^C^5N1ROOT^")
	for i := range 4095 {
		next.WriteString("5" + scratchFixtureCall("N0", i, 3))
		if i < 4000 {
			next.WriteString(":1")
		}
		next.WriteByte('^')
	}
	for i := range 4096 {
		next.WriteString("1" + scratchFixtureCall("U0", i, 3) + "^")
	}
	next.WriteString("H1^")
	wire := next.String()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	frame, err := ParseFrame(wire)
	if err != nil {
		t.Fatal(err)
	}
	record, err := DecodePC92(frame)
	if err != nil {
		t.Fatal(err)
	}
	key := pc92Key(frame)
	direct := newBoundedIndex[string, bool](64)
	for i := range 64 {
		direct.Set(scratchFixtureCall("P0", i, 3), true)
	}
	plan, err := graph.prepare(record, "N0LOCAL", "W1AA", direct)
	if err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&after)
	if plan.addNodes.Len() != 4095 || len(plan.remove) != 65536 || len(plan.add) != 8191 {
		t.Fatalf("replacement did not exercise simultaneous plan owners: nodes%d remove%d add%d", plan.addNodes.Len(), len(plan.remove), len(plan.add))
	}
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("wire%d bytes; decode+prepare total allocation%d bytes; reserved%d", len(wire), allocated, graphMutationScratchBytes)
	if graphScratchRaceInstrumented {
		// Go's sync.Pool.Put deliberately drops one quarter of objects under
		// race instrumentation. regexp's pooled backtracker then repeatedly
		// reallocates scratch during callsign validation. That cumulative total
		// is not comparable to production; retain this whole reachable fixture
		// and every semantic assertion, but measure its ceiling in normal builds.
		t.Log("production TotalAlloc ceiling is checked without -race; instrumented sync.Pool drops make cumulative regex allocation incomparable")
	} else if allocated > graphMutationScratchBytes {
		t.Fatalf("transaction scratch exceeds its conservative reservation: allocated%d reserved%d", allocated, graphMutationScratchBytes)
	}
	runtime.KeepAlive(frame)
	runtime.KeepAlive(record)
	runtime.KeepAlive(plan)
	runtime.KeepAlive(graph)
	runtime.KeepAlive(key)
}
