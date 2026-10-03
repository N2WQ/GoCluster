//go:build race

package peer

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestV15TopologyRaceProjectionAndObserver(t *testing.T) {
	store, err := openTopologyStore(filepath.Join(t.TempDir(), "race.db"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	snapshot := graphProjection{at: time.Now(), nodes: make([]projectionNode, 64), edges: make([]projectionEdge, 2048)}
	for i := range snapshot.nodes {
		snapshot.nodes[i] = projectionNode{entry: PC92Entry{Call: fmt.Sprintf("N%04d", i), Flags: 5}, complete: true, seen: snapshot.at}
	}
	for i := range snapshot.edges {
		snapshot.edges[i] = projectionEdge{parent: snapshot.nodes[i/32].entry.Call, entry: PC92Entry{Call: fmt.Sprintf("K%05d", i), Flags: 1}}
	}
	if err = store.replaceProjection(t.Context(), snapshot); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = topologyTestDB(t, store).QueryRow("select count(*) from peer_pc92_typed_edges").Scan(&count); err != nil || count != 2048 {
		t.Fatalf("committed projection=%d err=%v", count, err)
	}
}
