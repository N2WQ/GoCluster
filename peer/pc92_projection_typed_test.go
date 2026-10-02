package peer

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func projectionSQLRows(t *testing.T, store *topologyStore, query string) []string {
	t.Helper()
	rows, err := store.db.QueryContext(t.Context(), query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result []string
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(values))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err = rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		result = append(result, fmt.Sprint(values))
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestTopologyTypedSchemaUpgradePreservesHistoricalRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "upgrade.db")
	store, err := openTopologyStore(path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.db.ExecContext(t.Context(), `drop table peer_pc92_typed_edges;insert into peer_pc92_edges(parent,call,bitmap,version) values('N1OLD','K1OLD',1,'historic')`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = ensurePC92ProjectionSchema(store); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	snapshot := graphProjection{at: now, nodes: []projectionNode{{entry: PC92Entry{Call: "N1NEW", Flags: 5}, complete: true, seen: now}}, edges: []projectionEdge{
		{"N1NEW", PC92Entry{Call: "K1DUAL", Flags: 1}},
		{"N1NEW", PC92Entry{Call: "K1DUAL", Flags: 5}},
	}}
	if err = store.replaceProjection(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	const nodeRows = `select call,bitmap,version,build,ip,complete,updated_at from peer_pc92_nodes order by call`
	const edgeRows = `select parent,call,kind,bitmap,version,build,ip,updated_at from peer_pc92_typed_edges order by parent,call,kind`
	previousNodes, previousEdges := projectionSQLRows(t, store, nodeRows), projectionSQLRows(t, store, edgeRows)
	var count int
	if err = store.db.QueryRowContext(t.Context(), `select count(*) from peer_pc92_typed_edges where parent='N1NEW' and call='K1DUAL' and kind in (0,1)`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("typed rows=%d err=%v", count, err)
	}
	var version string
	if err = store.db.QueryRowContext(t.Context(), `select version from peer_pc92_edges where parent='N1OLD' and call='K1OLD'`).Scan(&version); err != nil || version != "historic" {
		t.Fatalf("historical row=%q err=%v", version, err)
	}
	// Distinct kinds fit; a duplicate of the same kind must fail after current
	// nodes and the first new edge have already changed within the transaction.
	snapshot.nodes[0].entry.Call = "N2FAIL"
	snapshot.edges = append(snapshot.edges, snapshot.edges[0])
	if err = store.replaceProjection(context.Background(), snapshot); err == nil {
		t.Fatal("same-kind duplicate did not fail")
	}
	if !reflect.DeepEqual(projectionSQLRows(t, store, nodeRows), previousNodes) || !reflect.DeepEqual(projectionSQLRows(t, store, edgeRows), previousEdges) {
		t.Fatal("failed replacement did not preserve both complete previous current sets")
	}
	if err = store.db.QueryRowContext(t.Context(), `select count(*) from peer_pc92_nodes where call='N1NEW' and complete=1`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("previous nodes lost: %d %v", count, err)
	}
	if err = store.db.QueryRowContext(t.Context(), `select count(*) from peer_pc92_nodes`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("failed node generation survived: %d %v", count, err)
	}
	if err = store.db.QueryRowContext(t.Context(), `select count(*) from peer_pc92_typed_edges where parent='N1NEW' and call='K1DUAL'`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("previous typed edges lost: %d %v", count, err)
	}
}

func TestPC92ProjectionRetainsTypedGenerationThroughGraphReplacement(t *testing.T) {
	p, _, _, now := controllerTestOwner(t)
	p.manager.topology = &topologyStore{}
	oldVersion, newVersion := strings.Repeat("1", 4097), strings.Repeat("2", 8193)
	applyGraphRecord(t, p.graph, "PC92^N2AAA^43200^C^5N2AAA:"+oldVersion+"^1K1DUAL^5K1DUAL^H1^", now)
	p.project(now)
	if len(p.projection) != 1 {
		t.Fatal("initial typed snapshot missing")
	}
	active := <-p.projection
	if len(active.edges) != 2 || active.charge <= 0 {
		t.Fatal("active snapshot collapsed typed edges")
	}
	applyGraphRecord(t, p.graph, "PC92^N2AAA^43201^C^5N2AAA:"+newVersion+"^H1^", now.Add(time.Second))
	p.project(now.Add(301 * time.Second))
	if len(p.projection) != 1 {
		t.Fatal("replacement snapshot missing")
	}
	queued := <-p.projection
	if p.projectionBytes.Load() != int64(active.charge+queued.charge) || active.charge+queued.charge > projectionAllocationLimit {
		t.Fatal("overlapping generations escaped combined reservation")
	}
	findVersion := func(snapshot graphProjection) string {
		for _, n := range snapshot.nodes {
			if n.entry.Call == "N2AAA" {
				return n.entry.Version
			}
		}
		return ""
	}
	if findVersion(active) != oldVersion || findVersion(queued) != newVersion || len(active.edges) != 2 || len(queued.edges) != 0 {
		t.Fatal("live replacement corrupted immutable diagnostic generation")
	}
	seen := [2]bool{}
	for _, edge := range active.edges {
		if edge.entry.IsNode() {
			seen[1] = true
		} else {
			seen[0] = true
		}
	}
	if !seen[0] || !seen[1] {
		t.Fatal("active projection lost one relationship kind")
	}
	p.releaseProjection(&active)
	if p.projectionBytes.Load() != int64(queued.charge) {
		t.Fatal("releasing active prematurely released queued reservation")
	}
	p.releaseProjection(&queued)
	if p.projectionBytes.Load() != 0 {
		t.Fatal("last projection release retained backing charge")
	}
}

func TestTopologyTypedSchemaFailureIsAtomic(t *testing.T) {
	store, err := openTopologyStore(filepath.Join(t.TempDir(), "schema.db"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// SQLite permits an index/table name collision to be staged before the
	// migration. Failure occurs at the last CREATE, after the nodes CREATE.
	_, err = store.db.ExecContext(t.Context(), `drop table peer_pc92_nodes;drop table peer_pc92_typed_edges;insert into peer_pc92_edges(parent,call) values('N1OLD','K1OLD');create index peer_pc92_typed_edges on peer_pc92_edges(call)`)
	if err != nil {
		t.Fatal(err)
	}
	if err = ensurePC92ProjectionSchema(store); err == nil {
		t.Fatal("fixture failed to interrupt schema creation")
	}
	var count int
	if err = store.db.QueryRowContext(t.Context(), `select count(*) from sqlite_master where type='table' and name='peer_pc92_nodes'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial schema retained: %d %v", count, err)
	}
	if err = store.db.QueryRowContext(t.Context(), `select count(*) from peer_pc92_edges where parent='N1OLD' and call='K1OLD'`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("historical row lost: %d %v", count, err)
	}
	if _, err = store.db.ExecContext(t.Context(), `drop index peer_pc92_typed_edges`); err != nil {
		t.Fatal(err)
	}
	if err = ensurePC92ProjectionSchema(store); err != nil {
		t.Fatal(err)
	}
}

func TestTopologyTypedProjectionRollbackUpgrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rollback.db")
	store, err := openTopologyStore(path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err = store.replaceProjection(t.Context(), graphProjection{at: now, nodes: []projectionNode{{entry: PC92Entry{Call: "N1NEW", Flags: 5}, seen: now}}}); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = openTopologyStore(path, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// Exercise the previous writer's exact table shape. This proves additive
	// SQL compatibility, not execution of a retained old Go binary.
	if _, err = store.db.ExecContext(t.Context(), `begin;delete from peer_pc92_edges;delete from peer_pc92_nodes;insert into peer_pc92_nodes(call,bitmap) values('N2OLD',5);insert into peer_pc92_edges(parent,call,bitmap,version,build,ip,updated_at) values('N2OLD','K2OLD',1,'','','',1);commit`); err != nil {
		t.Fatal(err)
	}
	if err = ensurePC92ProjectionSchema(store); err != nil {
		t.Fatal(err)
	}
	if err = store.replaceProjection(t.Context(), graphProjection{at: now, nodes: []projectionNode{{entry: PC92Entry{Call: "N3NOW", Flags: 5}, seen: now}}, edges: []projectionEdge{{"N3NOW", PC92Entry{Call: "K3NOW", Flags: 1}}}}); err != nil {
		t.Fatal(err)
	}
	var call string
	if err = store.db.QueryRowContext(t.Context(), `select call from peer_pc92_nodes`).Scan(&call); err != nil || call != "N3NOW" {
		t.Fatalf("current nodes=%q %v", call, err)
	}
	if err = store.db.QueryRowContext(t.Context(), `select call from peer_pc92_typed_edges`).Scan(&call); err != nil || call != "K3NOW" {
		t.Fatalf("current edges=%q %v", call, err)
	}
	if err = store.db.QueryRowContext(t.Context(), `select call from peer_pc92_edges`).Scan(&call); err != nil || call != "K2OLD" {
		t.Fatalf("historical edges=%q %v", call, err)
	}
}
