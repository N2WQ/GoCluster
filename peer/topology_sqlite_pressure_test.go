//go:build sqlite3_qualification && !race

package peer

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
)

func topologyPressureCharge(s graphProjection) int {
	charge := pointerAllocationBytes(int(unsafe.Sizeof(s))) + pointerAllocationBytes(len(s.nodes)*int(unsafe.Sizeof(projectionNode{}))) + pointerAllocationBytes(len(s.edges)*int(unsafe.Sizeof(projectionEdge{})))
	for _, n := range s.nodes {
		charge += entryBytes(n.entry)
	}
	for _, e := range s.edges {
		charge += allocationBytes(len(e.parent)) + entryBytes(e.entry)
	}
	return charge
}

func topologyPressureShapes(t *testing.T) [2]graphProjection {
	t.Helper()
	var shapes [2]graphProjection
	for shape := range shapes {
		s := &shapes[shape]
		s.nodes = make([]projectionNode, maxGraphNodes)
		s.edges = make([]projectionEdge, maxGraphEdges)
		for i := range s.nodes {
			s.nodes[i] = projectionNode{entry: PC92Entry{Call: fmt.Sprintf("N%05dABCDEFGH", i), Flags: 5, Version: "5457", Build: "633", IP: netip.MustParseAddr("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff")}, complete: true}
		}
		for i := range s.edges {
			s.edges[i] = projectionEdge{parent: s.nodes[i/32].entry.Call, entry: PC92Entry{Call: fmt.Sprintf("K%05dABCDEFGH", i%maxGraphUsers), Flags: 1}}
		}
		s.charge = topologyPressureCharge(*s)
	}
	// Fill the remaining two-generation admission allowance with numeric node
	// metadata. Each field remains below the wire frame limit; the aggregate
	// reaches the actual allocation boundary, not an arbitrary short field.
	s := &shapes[1]
	for i := range s.nodes {
		old := allocationBytes(len(s.nodes[i].entry.Version))
		lo, hi := len(s.nodes[i].entry.Version), 64000
		for lo < hi {
			mid := (lo + hi + 1) / 2
			if 2*(s.charge-old+allocationBytes(mid)) <= projectionAllocationLimit {
				lo = mid
			} else {
				hi = mid - 1
			}
		}
		s.nodes[i].entry.Version = strings.Repeat("9", lo)
		s.charge += allocationBytes(lo) - old
	}
	for i, s := range shapes {
		if s.charge != topologyPressureCharge(s) || 2*s.charge > projectionAllocationLimit {
			t.Fatalf("shape %d exceeds actual admission: %d", i, s.charge)
		}
		t.Logf("shape=%d nodes=%d edges=%d distinctUsers=%d oneGenerationCharge=%d twoGenerationCharge=%d", i, len(s.nodes), len(s.edges), maxGraphUsers, s.charge, 2*s.charge)
		graph := newProtocolGraph(time.Now())
		maxWire := 0
		for nodeIndex, node := range s.nodes {
			record := &PC92Record{Origin: node.entry.Call, Timestamp: "43200", Action: "C", Subject: node.entry, Hop: 1, Members: make([]PC92Entry, 32)}
			for j := range record.Members {
				record.Members[j] = s.edges[nodeIndex*32+j].entry
			}
			wire, err := EncodePC92(record)
			if err != nil {
				t.Fatalf("shape %d wire admission: %v", i, err)
			}
			maxWire = max(maxWire, len(wire))
			frame, err := ParseFrame(wire)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodePC92(frame)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := graph.prepare(decoded, "N0LOCAL", "N1PEER", nil)
			if err != nil || plan == nil {
				t.Fatalf("shape %d graph admission node %d: %v", i, nodeIndex, err)
			}
			graph.commit(plan, time.Now())
		}
		if graph.nodes.Len() != maxGraphNodes || graph.edges != maxGraphEdges || graph.users.Len() != maxGraphUsers || graph.retainedCharge() > 96<<20 || graph.projectionCharge() != s.charge {
			t.Fatalf("shape %d actual graph differs: nodes=%d users=%d edges=%d retained=%d projection=%d", i, graph.nodes.Len(), graph.users.Len(), graph.edges, graph.retainedCharge(), graph.projectionCharge())
		}
		t.Logf("shape=%d actual decoded graph admitted bytes=%d maximumWireBytes=%d", i, graph.retainedCharge(), maxWire)
	}
	if projectionAllocationLimit-2*shapes[1].charge >= 32 {
		t.Fatal("maximum-metadata fixture did not reach admission boundary")
	}
	return shapes
}

func TestV15TopologyPressureShapesAdmission(t *testing.T) { topologyPressureShapes(t) }

func TestV15TopologyPersistenceDiagnostic(t *testing.T) {
	if os.Getenv("GOCLUSTER_SQLITE_DIAGNOSTIC") != "1" {
		t.Skip("explicit diagnostic workload is not enabled")
	}
	topologyPersistencePressure(t, 20*time.Second, false)
}

func TestV15TopologyPersistenceThirtyMinutes(t *testing.T) {
	if os.Getenv("GOCLUSTER_SQLITE_LONG") != "1" {
		t.Skip("explicit full 30-minute persistence qualification is not enabled")
	}
	topologyPersistencePressure(t, 10*time.Minute, true)
}

func topologyPersistencePressure(t *testing.T, phaseDuration time.Duration, authoritative bool) {
	t.Helper()
	if runtime.GOMAXPROCS(0) != 2 || os.Getenv("GOGC") != "50" || debug.SetMemoryLimit(-1) != 1536<<20 {
		t.Fatal("prescribed profile requires GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1536MiB")
	}
	shapes := topologyPressureShapes(t)
	m := newProtocolTestManager(t)
	store, err := openTopologyStore(filepath.Join(t.TempDir(), "pressure.db"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	m.topology = store
	m.legacyCh = make(chan legacyWork, defaultLegacyQueue)
	ctx, cancel := context.WithCancel(t.Context())
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); m.projectionLoop(ctx) }()
	go func() { defer workers.Done(); m.legacyWorker(ctx) }()
	defer func() { cancel(); workers.Wait(); m.protocol.drainQueuedProjections(); store.Close() }()
	start := time.Now()
	lastProjection, lastLegacy := start, start
	var offered int
	var latest time.Time
	var latestShape int
	var maxProjectionGap, maxLegacyGap time.Duration
	var maxLegacyQueued int
	offer := func(now time.Time) {
		phase := int(now.Sub(start) / phaseDuration)
		shape := 0
		if phase == 1 || (phase >= 2 && offered%2 != 0) {
			shape = 1
		}
		snapshot := shapes[shape]
		snapshot.at = now
		select {
		case previous := <-m.protocol.projection:
			m.protocol.releaseProjection(&previous)
		default:
		}
		if !m.protocol.reserveProjection(snapshot.charge) {
			t.Fatal("two-generation projection admission unexpectedly refused")
		}
		select {
		case m.protocol.projection <- snapshot:
		default:
			m.protocol.releaseProjection(&snapshot)
			t.Fatal("bounded projection offer failed")
		}
		select {
		case m.legacyCh <- legacyWork{frame: &Frame{Type: "PC16"}, ts: now}:
		default:
			t.Fatalf("legacy queue saturated elapsed=%s offered=%d commits=%d/%d", now.Sub(start), offered, store.db.projectionCommits.Load(), store.db.legacyCommits.Load())
		}
		maxLegacyQueued = max(maxLegacyQueued, len(m.legacyCh))
		latest = now
		latestShape = shape
		offered++
		if offered%10 == 0 {
			t.Logf("elapsed=%s offered=%d commits=%d/%d legacyQueued=%d", now.Sub(start), offered, store.db.projectionCommits.Load(), store.db.legacyCommits.Load(), len(m.legacyCh))
		}
	}
	offer(start)
	nextOffer := start.Add(time.Second)
	end := start.Add(3 * phaseDuration)
	monitor := time.NewTicker(100 * time.Millisecond)
	defer monitor.Stop()
	for now := range monitor.C {
		if !now.Before(end) {
			break
		}
		if !now.Before(nextOffer) {
			offer(now)
			nextOffer = nextOffer.Add(time.Second)
		}
		p := time.Unix(0, store.db.lastProjectionCommit.Load())
		l := time.Unix(0, store.db.lastLegacyCommit.Load())
		if p.After(lastProjection) {
			maxProjectionGap = max(maxProjectionGap, p.Sub(lastProjection))
			lastProjection = p
		}
		if l.After(lastLegacy) {
			maxLegacyGap = max(maxLegacyGap, l.Sub(lastLegacy))
			lastLegacy = l
		}
		if time.Since(lastProjection) > 5*time.Second || time.Since(lastLegacy) > 5*time.Second || maxProjectionGap > 5*time.Second || maxLegacyGap > 5*time.Second {
			t.Fatalf("five-second progress failed phase=%d projectionAge=%s legacyAge=%s commits=%d/%d", int(now.Sub(start)/phaseDuration), now.Sub(lastProjection), now.Sub(lastLegacy), store.db.projectionCommits.Load(), store.db.legacyCommits.Load())
		}
	}
	if offered != int(3*phaseDuration/time.Second) {
		t.Fatalf("prescribed offer count changed: %d", offered)
	}
	drainStart := time.Now()
	for store.db.lastProjectionSnapshot.Load() < latest.UnixNano() || len(m.legacyCh) != 0 || store.db.inOperation.Load() {
		if time.Since(drainStart) > 5*time.Second {
			t.Fatal("latest snapshot did not drain within five seconds")
		}
		time.Sleep(10 * time.Millisecond)
	}
	drainElapsed := time.Since(drainStart)
	observer := topologyTestDB(t, store)
	var nodes, edges int
	if err = observer.QueryRowContext(t.Context(), "select (select count(*) from peer_pc92_nodes),(select count(*) from peer_pc92_typed_edges)").Scan(&nodes, &edges); err != nil || nodes != maxGraphNodes || edges != maxGraphEdges {
		t.Fatalf("final independent contents=%d/%d err=%v", nodes, edges, err)
	}
	rows, err := observer.QueryContext(t.Context(), "select call,version,build,ip,complete from peer_pc92_nodes order by call")
	if err != nil {
		t.Fatal(err)
	}
	i := 0
	for rows.Next() {
		var call, version, build, ip string
		var complete bool
		if err = rows.Scan(&call, &version, &build, &ip, &complete); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		want := shapes[latestShape].nodes[i]
		if call != want.entry.Call || version != want.entry.Version || build != want.entry.Build || ip != entryIP(want.entry) || complete != want.complete {
			rows.Close()
			t.Fatalf("final node %d differs from latest offered generation", i)
		}
		i++
	}
	err = rows.Err()
	rows.Close()
	if err != nil || i != maxGraphNodes {
		t.Fatalf("final nodes observed=%d err=%v", i, err)
	}
	rows, err = observer.QueryContext(t.Context(), "select parent,call,kind,bitmap,version,build,ip,updated_at from peer_pc92_typed_edges order by parent,call")
	if err != nil {
		t.Fatal(err)
	}
	i = 0
	for rows.Next() {
		var parent, call, version, build, ip string
		var kind, bitmap int
		var updated int64
		if err = rows.Scan(&parent, &call, &kind, &bitmap, &version, &build, &ip, &updated); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		want := shapes[latestShape].edges[i]
		if parent != want.parent || call != want.entry.Call || kind != 0 || bitmap != int(want.entry.Flags) || version != want.entry.Version || build != want.entry.Build || ip != entryIP(want.entry) || updated != latest.Unix() {
			rows.Close()
			t.Fatalf("final edge %d differs from latest offered generation", i)
		}
		i++
	}
	err = rows.Err()
	rows.Close()
	if err != nil || i != maxGraphEdges {
		t.Fatalf("final edges observed=%d err=%v", i, err)
	}
	t.Logf("authoritative=%t phaseDuration=%s offered=%d commits=%d/%d maxCommitGap=%s/%s maxLegacyQueued=%d drain=%s latestSnapshot=%d; independent observer checked every node and typed edge", authoritative, phaseDuration, offered, store.db.projectionCommits.Load(), store.db.legacyCommits.Load(), maxProjectionGap, maxLegacyGap, maxLegacyQueued, drainElapsed, store.db.lastProjectionSnapshot.Load())
}
