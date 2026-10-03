//go:build !race

package peer

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestV15TopologyFullProjection(t *testing.T) {
	// Full-population timing is qualified without race instrumentation, using
	// the unchanged production deadline. Race coverage uses a smaller fixture.
	store, err := openTopologyStore(filepath.Join(t.TempDir(), "full.db"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	snapshot := graphProjection{at: time.Now(), nodes: make([]projectionNode, 4096), edges: make([]projectionEdge, 131072)}
	for i := range snapshot.nodes {
		snapshot.nodes[i] = projectionNode{entry: PC92Entry{Call: fmt.Sprintf("N%04d", i), Flags: 5, Version: "5457", Build: "633"}, complete: true, seen: snapshot.at}
	}
	for i := range snapshot.edges {
		snapshot.edges[i] = projectionEdge{parent: snapshot.nodes[i/32].entry.Call, entry: PC92Entry{Call: fmt.Sprintf("K%05d", i%32768), Flags: 1}}
	}
	start := time.Now()
	err = store.replaceProjection(t.Context(), snapshot)
	t.Logf("4096 nodes / 131072 edges projection elapsed=%s error=%v", time.Since(start), err)
	if err != nil {
		t.Fatal(err)
	}
	observer := topologyTestDB(t, store)
	var nodes, edges, users int
	if err = observer.QueryRow("select (select count(*) from peer_pc92_nodes),(select count(*) from peer_pc92_typed_edges),(select count(distinct call) from peer_pc92_typed_edges)").Scan(&nodes, &edges, &users); err != nil {
		t.Fatal(err)
	}
	if nodes != 4096 || edges != 131072 || users != 32768 {
		t.Fatalf("committed population=%d/%d/%d", nodes, edges, users)
	}
	if usage := store.db.usage.Load(); usage.EngineBacking.Load() > topologyEngineBytes {
		t.Fatal("engine exceeded its backing allowance")
	}
}
