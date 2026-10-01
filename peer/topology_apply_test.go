package peer

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestTopologyProjectionAtomicReplacementAndFailure(t *testing.T) {
	store, err := openTopologyStore(filepath.Join(t.TempDir(), "topology.db"), 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Now()
	snapshot := graphProjection{nodes: []projectionNode{{PC92Entry{Call: "N1NODE", Flags: 5}, true, now}},
		edges: []projectionEdge{{"N1NODE", PC92Entry{Call: "K1USER", Flags: 1}}}, at: now}
	if err = store.replaceProjection(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	// A duplicate primary key forces failure after DELETE and a successful first
	// INSERT. The transaction must preserve the entire prior subject population.
	snapshot.edges = append(snapshot.edges, snapshot.edges[0])
	if err = store.replaceProjection(context.Background(), snapshot); err == nil {
		t.Fatal("fixture failed to trigger transaction rollback")
	}
	var count int
	if err = store.db.QueryRowContext(t.Context(), `select count(*) from peer_pc92_edges`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("rollback count=%d err=%v", count, err)
	}
	snapshot.edges = nil
	if err = store.replaceProjection(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if err = store.db.QueryRowContext(t.Context(), `select count(*) from peer_pc92_edges`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("empty replacement count=%d err=%v", count, err)
	}
}
func TestTopologyProjectionDoesNotRestoreAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topology.db")
	store, err := openTopologyStore(path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.replaceProjection(context.Background(), graphProjection{nodes: []projectionNode{{PC92Entry{Call: "N1NODE", Flags: 5}, true, time.Now()}}}); err != nil {
		t.Fatal(err)
	}
	store.Close()
	m := newProtocolTestManager(t)
	reopened, err := openTopologyStore(path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	m.topology = reopened
	if m.protocol.graph.nodes.Len() != 0 || m.protocol.graph.freshness.Len() != 0 {
		t.Fatal("stored diagnostic rows became live authority")
	}
}
func TestTopologySchemaMigrationPreservesDiagnosticRows(t *testing.T) {
	store, err := openTopologyStore(filepath.Join(t.TempDir(), "old.db"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.db.ExecContext(t.Context(), `drop table peer_nodes;create table peer_nodes(origin text,call text);insert into peer_nodes values('PC19','N1OLD')`); err != nil {
		t.Fatal(err)
	}
	if err = ensurePeerNodesSchema(store.db); err != nil {
		t.Fatal(err)
	}
	var call string
	if err = store.db.QueryRowContext(t.Context(), `select call from peer_nodes where origin='PC19'`).Scan(&call); err != nil || call != "N1OLD" {
		t.Fatalf("migration lost old rows: %q %v", call, err)
	}
}
