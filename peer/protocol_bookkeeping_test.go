package peer

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// sessionTestIndex keeps fixture declarations readable. Production registries
// never use a runtime map; tests may use one as independent input/oracle data.
func sessionTestIndex(sessions map[string]*session) *boundedIndex[string, *session] {
	out := newFixedIndex[string, *session](64)
	for key, value := range sessions {
		if out.Set(key, value) == nil {
			panic("session fixture exceeds approved registry limit")
		}
	}
	return out
}

func TestPC92BookkeepingDiagnosticReasonsFitFixedBacking(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	reasons := make(map[string]bool)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".go") || strings.HasSuffix(file.Name(), "_test.go") {
			continue
		}
		source, err := parser.ParseFile(token.NewFileSet(), file.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(source, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !slices.Contains([]string{"diagnostic", "gate", "failAdmission"}, selector.Sel.Name) {
				return true
			}
			argument := call.Args[len(call.Args)-1]
			literal, ok := argument.(*ast.BasicLit)
			if !ok {
				// The two wrappers pass their already-enumerated reason onward.
				if id, ok := argument.(*ast.Ident); !ok || id.Name != "reason" || selector.Sel.Name != "diagnostic" {
					t.Errorf("diagnostic call in %s gained a non-literal reason", file.Name())
				}
				return true
			}
			reason, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatal(err)
			}
			reasons[reason] = true
			return true
		})
	}
	p := newProtocolController(&Manager{})
	if len(reasons) != 18 || p.diagnosticAt.limit != len(reasons) || len(p.diagnosticAt.buckets) != 32 {
		t.Fatalf("reason enumeration/backing changed: reasons=%d limit=%d buckets=%d", len(reasons), p.diagnosticAt.limit, len(p.diagnosticAt.buckets))
	}
	for reason := range reasons {
		p.diagnostic(reason)
	}
	for reason := range reasons {
		before := p.diagnosticAt.Value(reason)
		p.diagnostic(reason)
		if before.IsZero() || p.diagnosticAt.Value(reason) != before {
			t.Fatal("diagnostic was omitted at full capacity or throttle age changed")
		}
	}
}

func TestPC92BookkeepingCompletePublicationPopulation(t *testing.T) {
	m := newProtocolTestManager(t)
	users := make([]LocalUser, 1000)
	for i := range users {
		users[i] = LocalUser{SessionID: uint64(i + 1), Login: fmt.Sprintf("K%dUSER", i), IP: "2001:db8::1"}
	}
	m.SetMembershipProvider(func() LocalMembership {
		return LocalMembership{Complete: true, RawCount: len(users), Users: users}
	})
	for i := range 64 {
		call := fmt.Sprintf("W%dNODE", i)
		m.outboundPeers = append(m.outboundPeers, PeerEndpoint{remoteCall: call})
		m.sessions.Set(call, &session{id: call, remoteCall: call, pc9x: true, remoteVersion: "5457", remoteBuild: "633"})
	}
	// These fixtures do not own transports; release them before Manager.Stop.
	t.Cleanup(func() {
		for call := range m.sessions.All() {
			m.sessions.Delete(call)
		}
	})
	entries, complete := m.protocol.membershipEntries()
	if !complete || entries.Len() != 1064 || entries.limit != 1064 || len(entries.buckets) != 2048 {
		t.Fatalf("complete publication population lost records: complete=%t count=%d", complete, entries.Len())
	}
	reserved := m.protocol.reservedNodes()
	if reserved.Len() != 65 || len(reserved.buckets) != 128 {
		t.Fatal("configured identities did not fill the exact reserved-node bound")
	}
	values := entryValues(entries)
	for i, entry := range values {
		if i > 0 && values[i-1].Call >= entry.Call {
			t.Fatal("publication sorting or exact identity uniqueness changed")
		}
	}
	wire, err := m.protocol.encodeRecord("C", "43200", values)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := ParseFrame(wire)
	if err != nil {
		t.Fatal(err)
	}
	record, err := DecodePC92(frame)
	if err != nil || len(record.Members) != 1064 {
		t.Fatalf("complete wire population changed: decode=%v", err)
	}
	clone := newFixedIndex[string, PC92Entry](1064)
	for i := len(values) - 1; i >= 0; i-- {
		clone.Set(values[i].Call, values[i])
	}
	if !sameMembership(entries, clone) || !m.protocol.publicationFits(entries) {
		t.Fatal("complete membership changed with insertion order or failed reserved publication")
	}
	clone.Delete(users[0].Login)
	if sameMembership(entries, clone) {
		t.Fatal("membership removal was hidden by index replacement")
	}
}

// Include the pointer-bearing malloc header conservatively, even for small
// objects where the runtime does not need it. The independent runtime size
// class oracle lives in dedupe_allocation_test.go; no production accounting
// function supplies an expected value here.
func bookkeepingIndexAllowance[K comparable, V any](count int) (entry, buckets, total int) {
	bucketCount := 1
	for bucketCount < count {
		bucketCount *= 2
	}
	entry = dedupeOracleAllocation(int(unsafe.Sizeof(boundedEntry[K, V]{})) + 8)
	buckets = dedupeOracleAllocation(bucketCount*8 + 8)
	total = entry*count + buckets + dedupeOracleAllocation(int(unsafe.Sizeof(boundedIndex[K, V]{}))+8)
	return entry, buckets, total
}

func TestPC92BookkeepingFixedAllocationEnvelope(t *testing.T) {
	m := newProtocolTestManager(t)
	p := m.protocol
	for name, got := range map[string][2]int{
		"sessions": {m.sessions.limit, 64}, "candidates": {m.candidates.limit, 128},
		"owned runs": {m.ownedRuns.limit, 192}, "blocked peers": {m.blockedPeers.limit, 64},
		"admission failures": {m.admissionFailures.limit, 64}, "blocked records": {p.blockedRecords.limit, 64},
		"blocked input": {p.blockedInput.limit, 64}, "recovering": {p.recovering.limit, 64},
		"pending K": {p.pendingK.limit, 64}, "blocked": {p.blocked.limit, 64},
		"diagnostics": {p.diagnosticAt.limit, 18}, "published": {p.published.limit, 1064},
	} {
		if got[0] != got[1] {
			t.Fatalf("%s constructor limit=%d; allocation proof requires%d", name, got[0], got[1])
		}
	}
	type row struct {
		name                  string
		count, generations    int
		entry, buckets, total int
	}
	var rows []row
	add := func(name string, count, generations, entry, buckets, total int) {
		rows = append(rows, row{name, count, generations, entry, buckets, total})
	}
	entry, buckets, total := bookkeepingIndexAllowance[string, *session](64)
	add("sessions", 64, 1, entry, buckets, total)
	entry, buckets, total = bookkeepingIndexAllowance[*session, *candidateState](128)
	add("candidates", 128, 1, entry, buckets, total)
	entry, buckets, total = bookkeepingIndexAllowance[*session, bool](192)
	add("owned runs", 192, 1, entry, buckets, total)
	entry, buckets, total = bookkeepingIndexAllowance[string, bool](64)
	add("blocked peers / input / direct snapshot", 64, 3, entry, buckets, total)
	entry, buckets, total = bookkeepingIndexAllowance[string, admissionFailure](64)
	add("admission failure swap", 64, 2, entry, buckets, total)
	entry, buckets, total = bookkeepingIndexAllowance[string, string](64)
	add("blocked records", 64, 1, entry, buckets, total)
	entry, buckets, total = bookkeepingIndexAllowance[string, time.Time](64)
	add("blocked since", 64, 1, entry, buckets, total)
	entry, buckets, total = bookkeepingIndexAllowance[*session, recoveryState](64)
	add("recovering", 64, 1, entry, buckets, total)
	entry, buckets, total = bookkeepingIndexAllowance[*session, bool](64)
	add("pending K", 64, 1, entry, buckets, total)
	entry, buckets, total = bookkeepingIndexAllowance[string, time.Time](18)
	add("diagnostics", 18, 1, entry, buckets, total)
	entry, buckets, total = bookkeepingIndexAllowance[string, PC92Entry](1064)
	// Current, published, the new tick membership, and nested K's membership
	// are the four distinct possible publication generations on one actor stack.
	add("publication generations", 1064, 4, entry, buckets, total)
	entry, buckets, total = bookkeepingIndexAllowance[string, PC92Entry](1128)
	add("reserved publication union", 1128, 1, entry, buckets, total)
	entry, buckets, total = bookkeepingIndexAllowance[string, int](1000)
	add("membership counts", 1000, 1, entry, buckets, total)
	entry, buckets, total = bookkeepingIndexAllowance[string, bool](65)
	add("reserved identities", 65, 1, entry, buckets, total)
	indexBytes := 0
	for _, r := range rows {
		indexBytes += r.generations * r.total
		t.Logf("%s: count=%d generations=%d entry=%d bucket-array=%d one-generation=%d", r.name, r.count, r.generations, r.entry, r.buckets, r.total)
	}
	if indexBytes > 1<<20 {
		t.Fatalf("small index backing no longer fits proved sub-envelope: %d", indexBytes)
	}
	t.Logf("simultaneous index backing bound=%d; excludes separately owned wire/string/value payloads", indexBytes)
}
