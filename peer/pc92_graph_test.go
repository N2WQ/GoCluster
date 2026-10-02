package peer

import (
	"fmt"
	"net/netip"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func graphRecord(t *testing.T, wire string) *PC92Record {
	t.Helper()
	frame, err := ParseFrame(wire)
	if err != nil {
		t.Fatal(err)
	}
	record, err := DecodePC92(frame)
	if err != nil {
		t.Fatalf("decode fixture %q: %v", wire, err)
	}
	return record
}

func applyGraphRecord(t *testing.T, graph *protocolGraph, wire string, now time.Time) {
	t.Helper()
	plan, err := graph.prepare(graphRecord(t, wire), "N0LOCAL", "N1PEER", nil)
	if err != nil || plan == nil {
		t.Fatalf("prepare fixture %q: plan=%v error=%v", wire, plan, err)
	}
	graph.commit(plan, now)
}

func TestPC92GraphMultipleParentsAndEmptyC(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	graph := newProtocolGraph(now)
	applyGraphRecord(t, graph, "PC92^N2AAA^43200^C^5N2AAA^1K1USER^H10^", now)
	applyGraphRecord(t, graph, "PC92^N3BBB^43200^C^5N3BBB^1K1USER^H10^", now)
	if graph.users.Value("K1USER") != 2 || graph.edges != 2 {
		t.Fatalf("multi-parent user state: users=%v edges=%d", graph.users, graph.edges)
	}
	applyGraphRecord(t, graph, "PC92^N2AAA^43201^D^^1K1USER^H10^", now.Add(time.Second))
	if graph.users.Value("K1USER") != 1 || graph.nodes.Value("N3BBB").Members.Value(memberKey{"K1USER", false}).Call != "K1USER" {
		t.Fatal("removing one parent lost the alternate user membership")
	}
	applyGraphRecord(t, graph, "PC92^N3BBB^43202^C^5N3BBB^H10^", now.Add(2*time.Second))
	if graph.edges != 0 || graph.users.Len() != 0 || graph.nodes.Value("N3BBB").Members.Len() != 0 || !graph.nodes.Value("N3BBB").Complete {
		t.Fatal("empty authoritative C did not clear exactly the subject membership")
	}
}

func TestPC92GraphExternalSubjectCreatesParentEdge(t *testing.T) {
	now := time.Now()
	graph := newProtocolGraph(now)
	applyGraphRecord(t, graph, "PC92^N2AAA^43200^C^7N3EXT:5401^1K1USER^H10^", now)
	// DXSpider pc92_handle_first_slot calls _add_thingy on the true parent
	// when an external subject is first encountered. Its users belong to that
	// external subject, not to the PC92 origin.
	if entry, ok := graph.nodes.Value("N2AAA").Members.Get(memberKey{"N3EXT", true}); !ok || !entry.IsExternal() || !entry.IsNode() {
		t.Fatal("external subject has no origin-to-subject route edge")
	}
	if graph.nodes.Value("N3EXT").Members.Value(memberKey{"K1USER", false}).Call != "K1USER" {
		t.Fatal("external user's membership was not attached to the external subject")
	}
	if _, exists := graph.nodes.Value("N2AAA").Members.Get(memberKey{"K1USER", false}); exists {
		t.Fatal("external user was incorrectly attached directly to origin")
	}
}

func TestPC92GraphMetadataWithoutIPDoesNotEraseKnownIP(t *testing.T) {
	now := time.Now()
	graph := newProtocolGraph(now)
	applyGraphRecord(t, graph, "PC92^N2AAA^43200^A^^1K1USER:192.0.2.1^5N3BBB:5457:633:192.0.2.2^H10^", now)
	applyGraphRecord(t, graph, "PC92^N2AAA^43201^A^^0K1USER^5N3BBB:5458:634^H10^", now)
	user := graph.nodes.Value("N2AAA").Members.Value(memberKey{"K1USER", false})
	node := graph.nodes.Value("N3BBB").Entry
	if user.IP != netip.MustParseAddr("192.0.2.1") || !user.Here() {
		t.Fatalf("member update erased known IP or changed receiver-retained Here: %+v", user)
	}
	if node.IP != netip.MustParseAddr("192.0.2.2") || node.Version != "5457" || node.Build != "" {
		t.Fatalf("member update lost IP or changed receiver-owned node version/build: %+v", node)
	}
	applyGraphRecord(t, graph, "PC92^N2AAA^43202^A^^1K1USER:192.0.2.9^H10^", now)
	if graph.nodes.Value("N2AAA").Members.Value(memberKey{"K1USER", false}).IP != netip.MustParseAddr("192.0.2.9") {
		t.Fatal("explicit replacement IP was not applied")
	}
}

func TestPC92GraphProtectsDirectSubjectAndLocalNode(t *testing.T) {
	now := time.Now()
	graph := newProtocolGraph(now)
	direct := newBoundedIndex[string, bool](1)
	direct.Set("N3DIR", true)
	for _, wire := range []string{
		"PC92^N2AAA^43200^C^7N3DIR^1K1USER^H10^",
		"PC92^N2AAA^43200^C^7N0LOCAL^1K1USER^H10^",
		"PC92^N0LOCAL^43200^C^5N0LOCAL^1K1USER^H10^",
	} {
		plan, err := graph.prepare(graphRecord(t, wire), "N0LOCAL", "N1PEER", direct)
		if err != nil || plan != nil || graph.nodes.Len() != 0 || graph.edges != 0 {
			t.Fatalf("protected authority changed for %q: plan=%v error=%v", wire, plan, err)
		}
	}
}

func TestPC92GraphFullUserCapacityAllowsReplacingLastParentUser(t *testing.T) {
	now := time.Now()
	graph := newProtocolGraph(now)
	// Populate the full unique-user budget with actual owner edges. The next
	// complete C exchanges one last-parent user for one new identity, so peak
	// committed occupancy remains exactly at the limit.
	for group := 0; group < 16; group++ {
		nodeCall := fmt.Sprintf("N%dAAA", group+2)
		node := &graphNode{Entry: PC92Entry{Call: nodeCall, Flags: 5}, Members: newBoundedIndex[memberKey, PC92Entry](maxGraphEdges), Observations: 3}
		graph.nodes.Set(nodeCall, node)
		graph.metadataBytes += entryBytes(node.Entry)
		for i := 0; i < 4096; i++ {
			call := fmt.Sprintf("K%dAA", group*4096+i)
			node.Members.Set(memberKey{call, false}, PC92Entry{Call: call, Flags: 1})
			graph.metadataBytes += entryBytes(node.Members.Value(memberKey{call, false}))
			graph.users.Set(call, 1)
		}
	}
	owner := graph.nodes.Value("N2AAA")
	graph.edges = maxGraphUsers
	members := make([]PC92Entry, 0, owner.Members.Len())
	for call, entry := range owner.Members.All() {
		if call.Call != "K0AA" {
			members = append(members, entry)
		}
	}
	members = append(members, PC92Entry{Call: "K9NEW", Flags: 1})
	record := &PC92Record{Origin: "N2AAA", Action: "C", Subject: owner.Entry, Members: members}
	plan, err := graph.prepare(record, "N0LOCAL", "N1PEER", nil)
	if err != nil {
		t.Fatalf("fitting authoritative replacement refused at exact capacity: %v", err)
	}
	graph.commit(plan, now)
	if graph.users.Len() != maxGraphUsers || graph.users.Value("K0AA") != 0 || graph.users.Value("K9NEW") != 1 || graph.edges != maxGraphUsers {
		t.Fatal("replacement did not preserve the exact user/edge bound")
	}
}

func TestPC92GraphCAndKRenewLivenessButOnlyCCompletes(t *testing.T) {
	now := time.Now()
	graph := newProtocolGraph(now)
	applyGraphRecord(t, graph, "PC92^N2AAA^43200^C^5N2AAA^1K1USER^H10^", now)
	graph.observe("N2AAA", "N1PEER", 10, now, false)
	graph.observe("N2AAA", "N4ALT", 9, now, false)
	graph.loseIngress("N1PEER")
	if graph.nodes.Value("N2AAA").Complete || graph.ingress.Len() != 1 {
		t.Fatal("lost ingress failed to invalidate completeness or preserve alternate")
	}
	graph.nodes.Value("N2AAA").Observations = 1
	applyGraphRecord(t, graph, "PC92^N2AAA^43201^K^5N2AAA^0^1^H10^", now.Add(time.Second))
	if graph.nodes.Value("N2AAA").Observations != 3 || graph.nodes.Value("N2AAA").Complete || graph.users.Value("K1USER") != 1 {
		t.Fatal("K altered membership/completeness or failed to renew liveness")
	}
	applyGraphRecord(t, graph, "PC92^N2AAA^43202^C^5N2AAA^H10^", now.Add(2*time.Second))
	if !graph.nodes.Value("N2AAA").Complete || graph.users.Len() != 0 {
		t.Fatal("authoritative empty C did not recover completeness")
	}
}

func TestPC9xFreshnessStrictWindowAndMidnight(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		now, value, previous float64
		exists, want         bool
	}{
		{"past inside", 43200, 42301, 0, false, true},
		{"past boundary", 43200, 42300, 0, false, false},
		{"future inside", 43200, 44099, 0, false, true},
		{"future boundary", 43200, 44100, 0, false, false},
		{"same id", 43200, 43200, 43200, true, false},
		{"backward id", 43200, 43199, 43200, true, false},
		{"new day", 84, 84, 86235, true, true},
		{"yesterday after new day", 84, 86235, 84, true, false},
		{"midnight window", 30, 86350, 0, false, true},
		{"fraction advances", 43200, 43200.99, 43200.98, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(tc.now) * time.Second)
			if got := freshTime(tc.value, now, originWatermark{Value: tc.previous}, tc.exists); got != tc.want {
				t.Fatalf("freshTime=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestPC92GraphClockUnsafeRetainsDetachedWatermarks(t *testing.T) {
	now := time.Now()
	graph := newProtocolGraph(now)
	graph.commitWatermark("N2AAA", 43200, now, true)
	graph.expire(now.Add(2*time.Hour), false, nil)
	if graph.freshness.Len() != 1 || graph.messageOrigins != 1 {
		t.Fatal("unsafe-clock elapsed time erased replay protection")
	}
	graph.expire(now.Add(2*time.Hour), true, nil)
	if graph.freshness.Len() != 0 || graph.messageOrigins != 0 {
		t.Fatal("safe expiry leaked watermark or message-only counter")
	}
}

func TestPC92GraphSimultaneousFullOccupancyAndReplacement(t *testing.T) {
	now := time.Now()
	graph := newProtocolGraph(now)
	// All graph cardinalities coexist. Each unique user has two parents and
	// each node has 32 users, keeping every input C well inside the wire cap.
	for i := 0; i < maxGraphNodes; i++ {
		call := fmt.Sprintf("N%dAA", i)
		members := make([]PC92Entry, 0, 32)
		for j := 0; j < 32; j++ {
			members = append(members, PC92Entry{Call: fmt.Sprintf("K%dAA", (i%2048)*32+j), Flags: 1})
		}
		record := &PC92Record{Origin: call, Action: "C", Subject: PC92Entry{Call: call, Flags: 5}, Members: members}
		plan, err := graph.prepare(record, "N0LOCAL", "W1AA", nil)
		if err != nil {
			t.Fatalf("cold population refused at node %d: %v", i, err)
		}
		graph.commit(plan, now)
		graph.commitWatermark(call, 43200, now, false)
		for peer := 1; peer <= 64; peer++ {
			graph.observe(call, fmt.Sprintf("W%dAA", peer), 10, now, false)
		}
	}
	// Detached topology watermarks and message-only origins retain separate
	// replay authority after their route objects cease to be authoritative.
	for i := 0; graph.freshness.Len() < maxFreshnessOrigins; i++ {
		graph.commitWatermark(fmt.Sprintf("X%dAA", i), 43200, now, i < maxMessageOrigins)
	}
	assertFull := func() {
		t.Helper()
		if graph.nodes.Len() != maxGraphNodes || graph.users.Len() != maxGraphUsers || graph.edges != maxGraphEdges || graph.ingress.Len() != maxIngressObservations || graph.freshness.Len() != maxFreshnessOrigins {
			t.Fatalf("simultaneous occupancy not established: nodes=%d users=%d edges=%d ingress=%d freshness=%d", graph.nodes.Len(), graph.users.Len(), graph.edges, graph.ingress.Len(), graph.freshness.Len())
		}
		if graph.retainedCharge() > 96<<20 {
			t.Fatalf("approved simultaneous ordinary-key population exceeds graph charge budget: %.2f MiB", float64(graph.retainedCharge())/(1<<20))
		}
	}
	assertFull()
	applyGraphRecord(t, graph, "PC92^N0AA^43201^A^^1K0AA:192.0.2.1^H10^", now)
	assertFull()
	// Remove one redundant parent, then replace the now-last-parent user by a
	// different user in a complete C. Finally restore the second parent.
	applyGraphRecord(t, graph, "PC92^N2048AA^43202^D^^1K0AA^H10^", now)
	members := make([]PC92Entry, 0, 32)
	for call, entry := range graph.nodes.Value("N0AA").Members.All() {
		if call.Call != "K0AA" {
			members = append(members, entry)
		}
	}
	members = append(members, PC92Entry{Call: "K9NEW", Flags: 1})
	replacement := &PC92Record{Origin: "N0AA", Action: "C", Subject: PC92Entry{Call: "N0AA", Flags: 5}, Members: members}
	plan, err := graph.prepare(replacement, "N0LOCAL", "W1AA", nil)
	if err != nil {
		t.Fatalf("fitting C replacement refused at concurrent population limits: %v", err)
	}
	graph.commit(plan, now)
	applyGraphRecord(t, graph, "PC92^N2048AA^43203^A^^1K9NEW^H10^", now)
	assertFull()
	if graph.users.Value("K0AA") != 0 || graph.users.Value("K9NEW") != 2 {
		t.Fatal("replacement left historical ownership or lost the restored alternate")
	}
	if graph.canObserve("N0AA", "W65AA") || !graph.canObserve("N0AA", "W1AA") {
		t.Fatal("full ingress budget did not distinguish new from existing observations")
	}
	if graph.canWatermark("Z1NEW", false) || !graph.canWatermark("N0AA", false) {
		t.Fatal("full freshness budget did not distinguish new from existing authority")
	}
}

func TestPC92GraphUserIndexCannotRetainWholeInputFrame(t *testing.T) {
	// Numeric normalization discards these leading zeroes. The resulting user
	// entry is tiny, but its callsign originally aliases a nearly maximal wire
	// frame. Every retained index must own that small key, not the input buffer.
	wire := "PC92^N2AAA^43200^A^^1K1USER:" + strings.Repeat("0", 60000) + "^H10^"
	graph := newProtocolGraph(time.Now())
	applyGraphRecord(t, graph, wire, time.Now())
	start := uintptr(unsafe.Pointer(unsafe.StringData(wire)))
	end := start + uintptr(len(wire))
	for call := range graph.users.All() {
		address := uintptr(unsafe.Pointer(unsafe.StringData(call)))
		if address >= start && address < end {
			t.Fatal("user reference index retains the entire wire frame through a callsign substring")
		}
	}
}
