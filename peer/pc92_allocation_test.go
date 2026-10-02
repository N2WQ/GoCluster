package peer

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestPC92GraphEmptyCReleasesHistoricalMemberBacking(t *testing.T) {
	graph := newProtocolGraph(time.Now())
	members := make([]PC92Entry, 4096)
	for i := range members {
		members[i] = PC92Entry{Call: fmt.Sprintf("K%dAA", i), Flags: 1}
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < 32; i++ {
		call := fmt.Sprintf("N%dAA", i)
		record := &PC92Record{Origin: call, Action: "C", Subject: PC92Entry{Call: call, Flags: 5}, Members: members}
		for _, population := range [][]PC92Entry{members, nil} {
			record.Members = population
			plan, err := graph.prepare(record, "N0LOCAL", "W1AA", nil)
			if err != nil {
				t.Fatal(err)
			}
			graph.commit(plan, time.Now())
		}
	}
	if graph.edges != 0 || graph.users.Len() != 0 || graph.memberHigh != 0 {
		t.Fatal("empty complete snapshots retained historical member reservations")
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	// Previous code retained a large Members map on each empty node, exceeding
	// this generous 8 MiB allowance despite reporting zero owned edges/users.
	if delta := int64(after.HeapAlloc) - int64(before.HeapAlloc); delta > 8<<20 {
		t.Fatalf("empty-node retained heap grew with historical memberships: %d bytes", delta)
	}
	runtime.KeepAlive(graph)
	runtime.KeepAlive(members)
}

func TestPC92GraphPartialShrinkKeepsSurvivorsAndReclaimsCapacity(t *testing.T) {
	graph := newProtocolGraph(time.Now())
	members := make([]PC92Entry, 128)
	for i := range members {
		members[i] = PC92Entry{Call: fmt.Sprintf("K%dAA", i), Flags: 1}
	}
	record := &PC92Record{Origin: "N2AAA", Action: "C", Subject: PC92Entry{Call: "N2AAA", Flags: 5}, Members: members}
	plan, err := graph.prepare(record, "N0LOCAL", "W1AA", nil)
	if err != nil {
		t.Fatal(err)
	}
	graph.commit(plan, time.Now())
	record.Action, record.SubjectImplicit, record.Members = "D", true, members[:96]
	plan, err = graph.prepare(record, "N0LOCAL", "W1AA", nil)
	if err != nil {
		t.Fatal(err)
	}
	graph.commit(plan, time.Now())
	if graph.edges != 32 || graph.memberHigh != 32 || graph.usersHigh != 32 {
		t.Fatalf("quarter-occupancy shrink retained capacity: edges=%d member_slots=%d user_slots=%d", graph.edges, graph.memberHigh, graph.usersHigh)
	}
	for _, entry := range members[96:] {
		if graph.nodes.Value("N2AAA").Members.Value(memberKey{entry.Call, false}) != entry || graph.users.Value(entry.Call) != 1 {
			t.Fatal("map reclamation changed a surviving membership")
		}
	}
}

func TestPC92GraphLargeMetadataReservesRoundedBackingAndOverlap(t *testing.T) {
	graph := newProtocolGraph(time.Now())
	version := strings.Repeat("1", 32769)
	record := graphRecord(t, "PC92^N2AAA^43200^C^5N2AAA:"+version+"^H1^")
	// The old length-only charge would admit this wire. Its version allocation
	// is 40,960 bytes, so 32,769 bytes of remaining metadata room is insufficient.
	graph.metadataBytes = (96 << 20) - graphMutationScratchBytes - 512 - 8 - len(version)
	if _, err := graph.prepare(record, "N0LOCAL", "W1AA", nil); err == nil {
		t.Fatal("large received metadata bypassed allocator rounding")
	}
	graph.metadataBytes = (96 << 20) - graphMutationScratchBytes - 512 - 8 - 40960
	plan, err := graph.prepare(record, "N0LOCAL", "W1AA", nil)
	if err != nil {
		t.Fatal(err)
	}
	graph.commit(plan, time.Now())
	if graph.retainedCharge() != 96<<20 {
		t.Fatal("exact rounded metadata admission did not consume its reservation")
	}
	record.Subject.Version = strings.Repeat("2", 32769)
	if _, err := graph.prepare(record, "N0LOCAL", "W1AA", nil); err == nil {
		t.Fatal("equal-size replacement omitted overlap with the previous version allocation")
	}
	if graph.nodes.Value("N2AAA").Entry.Version != version {
		t.Fatal("refused metadata replacement mutated authority")
	}
	graph.metadataBytes -= 40960
	plan, err = graph.prepare(record, "N0LOCAL", "W1AA", nil)
	if err != nil {
		t.Fatal(err)
	}
	graph.commit(plan, time.Now())
	if graph.nodes.Value("N2AAA").Entry.Version != record.Subject.Version {
		t.Fatal("metadata replacement did not succeed after overlap headroom returned")
	}
}

func TestPC92ProjectionBoundsActiveAndQueuedMetadata(t *testing.T) {
	p, _, _, now := controllerTestOwner(t)
	p.manager.topology = &topologyStore{}
	// Graph-owned metadata can substantially exceed the projection partition.
	// These fixtures test the conservative reservation, not measured heap use;
	// the equal version strings here deliberately share their fixture backing.
	version := strings.Repeat("1", 10000)
	for i := 0; i < 3000; i++ {
		call := fmt.Sprintf("N%dAA", i)
		p.graph.nodes.Set(call, &graphNode{Entry: PC92Entry{Call: call, Flags: 5, Version: version}})
	}
	p.project(now)
	if len(p.projection) != 1 {
		t.Fatal("fitting complete projection refused")
	}
	active := <-p.projection
	if active.charge <= projectionAllocationLimit/2 || active.charge >= projectionAllocationLimit {
		t.Fatalf("fixture did not straddle the two-generation limit: %d", active.charge)
	}
	p.project(now.Add(301 * time.Second))
	if len(p.projection) != 0 || p.projectionBytes.Load() != int64(active.charge) || p.graph.nodes.Len() != 3000 {
		t.Fatal("projection overlap exceeded its bound or changed live authority")
	}
	p.releaseProjection(&active)
	p.project(now.Add(602 * time.Second))
	if len(p.projection) != 1 {
		t.Fatal("projection did not recover after active generation release")
	}
	p.drainQueuedProjections()
	if p.projectionBytes.Load() != 0 || len(p.projection) != 0 {
		t.Fatal("terminal drain retained projection storage")
	}
	for _, n := range p.graph.nodes.All() {
		n.Entry.Version = strings.Repeat("2", 20000)
	}
	p.project(now.Add(903 * time.Second))
	if p.projectionBytes.Load() != 0 || len(p.projection) != 0 || p.graph.nodes.Len() != 3000 {
		t.Fatal("oversized projection was partially retained or changed authority")
	}
}

func TestPC92ProjectionLeavesFixedLocalPublicationReservation(t *testing.T) {
	p, _, _, _ := controllerTestOwner(t)
	if localPublicationReservedBytes != 12<<20 || projectionAllocationLimit+localPublicationReservedBytes != 48<<20 {
		t.Fatal("local publication and optional projection no longer share48 MiB")
	}
	if !p.reserveProjection(projectionAllocationLimit) || p.reserveProjection(1) {
		t.Fatal("projection allocation crossed the shared partition boundary")
	}
	snapshot := graphProjection{charge: projectionAllocationLimit}
	p.releaseProjection(&snapshot)
	if p.projectionBytes.Load() != 0 || !p.reserveProjection(1) {
		t.Fatal("projection reservation did not recover after release")
	}
}
