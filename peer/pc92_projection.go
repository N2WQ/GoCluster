package peer

import (
	"context"
	"fmt"
	"time"
	"unsafe"
)

type projectionNode struct {
	entry    PC92Entry
	complete bool
	seen     time.Time
}
type projectionEdge struct {
	parent string
	entry  PC92Entry
}
type graphProjection struct {
	charge int
	nodes  []projectionNode
	edges  []projectionEdge
	at     time.Time
}

const (
	// Keep a fixed local working reservation for current, published and pending
	// 1000-user/64-peer indexes, arrays and encoding work. Session metadata uses
	// compact oversized-value markers; publication never borrows those rejected
	// strings into its snapshots. Only fitting entries become current/published.
	localPublicationReservedBytes = 12 << 20
	projectionAllocationLimit     = (48 << 20) - localPublicationReservedBytes
)

// project keeps one queued generation plus the active database write. Reserve
// their combined backing storage before building, including immutable strings
// borrowed from the graph: those strings can outlive later graph replacement.
// An oversized diagnostic snapshot is refused whole without changing authority.
func (p *protocolController) project(now time.Time) {
	if p.manager.topology == nil {
		return
	}
	interval := time.Duration(p.manager.cfg.Topology.PersistIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = 300 * time.Second
	}
	if !p.lastProjection.IsZero() && now.Sub(p.lastProjection) < interval {
		return
	}
	p.lastProjection = now
	select {
	case previous := <-p.projection:
		p.releaseProjection(&previous)
	default:
	}
	charge := p.graph.projectionCharge()
	if !p.reserveProjection(charge) {
		p.diagnostic("PC92 diagnostic projection allocation capacity exhausted")
		return
	}
	snapshot := graphProjection{charge: charge, nodes: make([]projectionNode, 0, p.graph.nodes.Len()), edges: make([]projectionEdge, 0, p.graph.edges), at: now}
	for call, n := range p.graph.nodes.All() {
		snapshot.nodes = append(snapshot.nodes, projectionNode{n.Entry, n.Complete, n.Seen})
		for _, e := range n.Members.All() {
			snapshot.edges = append(snapshot.edges, projectionEdge{call, e})
		}
	}
	select {
	case p.projection <- snapshot:
	default:
		p.releaseProjection(&snapshot)
	}
}

func (g *protocolGraph) projectionCharge() int {
	charge := pointerAllocationBytes(int(unsafe.Sizeof(graphProjection{}))) +
		pointerAllocationBytes(g.nodes.Len()*int(unsafe.Sizeof(projectionNode{}))) +
		pointerAllocationBytes(g.edges*int(unsafe.Sizeof(projectionEdge{})))
	for parent, node := range g.nodes.All() {
		charge += entryBytes(node.Entry)
		for _, entry := range node.Members.All() {
			charge += allocationBytes(len(parent)) + entryBytes(entry)
		}
	}
	return charge
}

func (p *protocolController) reserveProjection(charge int) bool {
	for used := p.projectionBytes.Load(); ; used = p.projectionBytes.Load() {
		if charge < 0 || int64(charge) > projectionAllocationLimit-used {
			return false
		}
		if p.projectionBytes.CompareAndSwap(used, used+int64(charge)) {
			return true
		}
	}
}

func (p *protocolController) releaseProjection(snapshot *graphProjection) {
	charge := snapshot.charge
	*snapshot = graphProjection{}
	p.projectionBytes.Add(-int64(charge))
}

// drainQueuedProjections runs after all manager workers have joined. A canceled
// DB worker cannot otherwise exclude a final enqueue by a still-running owner.
func (p *protocolController) drainQueuedProjections() {
	for {
		select {
		case snapshot := <-p.projection:
			p.releaseProjection(&snapshot)
		default:
			return
		}
	}
}

func (m *Manager) projectionLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case snapshot := <-m.protocol.projection:
			if err := m.topology.replaceProjection(ctx, snapshot); err != nil && ctx.Err() == nil {
				m.reportDiagnostic("topology_projection_failed", "", "storage_error")
			}
			m.protocol.releaseProjection(&snapshot)
		}
	}
}
func (t *topologyStore) replaceProjection(parent context.Context, snapshot graphProjection) error {
	ctx, cancel := newTopologyDBContext(parent)
	defer cancel()
	err := t.db.transaction(ctx, func(tx *topologyTransaction) error {
		// Old diagnostic rows are never restored as live routing authority. Keep
		// their configured retention effective without tying receive work to SQLite.
		if err := tx.Exec(`delete from peer_nodes where updated_at < ?`, snapshot.at.Add(-t.retention).Unix()); err != nil {
			return err
		}
		if err := tx.Exec(`delete from peer_pc92_nodes`); err != nil {
			return err
		}
		if err := tx.Exec(`delete from peer_pc92_typed_edges`); err != nil {
			return err
		}
		for _, n := range snapshot.nodes {
			e := n.entry
			if err := tx.Exec(`insert into peer_pc92_nodes(call,bitmap,version,build,ip,complete,updated_at) values(?,?,?,?,?,?,?)`, e.Call, e.Flags, e.Version, e.Build, entryIP(e), n.complete, n.seen.Unix()); err != nil {
				return err
			}
		}
		for _, edge := range snapshot.edges {
			e := edge.entry
			if err := tx.Exec(`insert into peer_pc92_typed_edges(parent,call,kind,bitmap,version,build,ip,updated_at) values(?,?,?,?,?,?,?,?)`, edge.parent, e.Call, e.IsNode(), e.Flags, e.Version, e.Build, entryIP(e), snapshot.at.Unix()); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		t.recordProjectionCommit(snapshot.at)
	}
	return err
}
func entryIP(e PC92Entry) string {
	if e.IP.IsValid() {
		return e.IP.String()
	}
	return ""
}
func ensurePC92ProjectionSchema(t *topologyStore) error {
	ctx, cancel := newTopologyDBContext(context.Background())
	defer cancel()
	err := t.db.transaction(ctx, func(tx *topologyTransaction) error {
		// The old call-only table remains historical for rollback diagnostics.
		// Current nodes and typed edges are replaced together by replaceProjection;
		// old edges must not be joined to current nodes as a current generation.
		return tx.Exec(`create table if not exists peer_pc92_nodes (
 call text primary key,bitmap integer,version text,build text,ip text,complete integer,updated_at integer);
 create table if not exists peer_pc92_edges (
 parent text,call text,bitmap integer,version text,build text,ip text,updated_at integer,primary key(parent,call));
 create table if not exists peer_pc92_typed_edges (
 parent text not null,call text not null,kind integer not null check(kind in (0,1)),bitmap integer,version text,build text,ip text,updated_at integer,primary key(parent,call,kind));`)
	})
	if err != nil {
		return fmt.Errorf("PC92 projection schema: %w", err)
	}
	return nil
}
