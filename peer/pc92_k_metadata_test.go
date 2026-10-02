package peer

import (
	"fmt"
	"net/netip"
	"reflect"
	"testing"
	"time"
)

// These literals come from the pinned receiver's K handler, not the effective
// subject helper. Each consumer starts with fresh 5457/633 authority.
func kNumericReplacementCases() []struct{ name, suffix, version, build string } {
	return []struct{ name, suffix, version, build string }{
		{"build_omitted", ":5457", "5457", "0"},
		{"both_omitted", "", "0", "0"},
		{"explicit_zero", ":0:0", "0", "0"},
		{"version_omitted", "::634", "0", "634"},
	}
}

func TestPC92GraphKSubjectNumericReplacement(t *testing.T) {
	for _, tc := range kNumericReplacementCases() {
		t.Run(tc.name, func(t *testing.T) {
			p, source, _, now := controllerTestOwner(t)
			receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^5N2AAA:5457:633:192.0.2.1^1K1USER^5N3CHLD^H1^", now)
			node := p.graph.nodes.Value("N2AAA")
			if node == nil || node.Entry.Version != "5457" || node.Entry.Build != "633" || !node.Complete {
				t.Fatal("numeric seed did not establish populated complete authority")
			}
			node.Observations = 1
			at := now.Add(time.Second)
			receiveControllerWire(t, p, source, "PC92^N2AAA^43201^K^5N2AAA"+tc.suffix+"^0^0^H1^", at)
			if node.Entry.Version != tc.version || node.Entry.Build != tc.build || node.Entry.IP != netip.MustParseAddr("192.0.2.1") {
				t.Fatalf("K numeric replacement/absent IP=%+v; want %s/%s with retained IP", node.Entry, tc.version, tc.build)
			}
			if node.Members.Len() != 2 || p.graph.edges != 2 || p.graph.users.Value("K1USER") != 1 || !node.Complete || node.Observations != 3 || !node.Seen.Equal(at) {
				t.Fatal("zero K counts changed membership/completeness or failed liveness renewal")
			}
			receiveControllerWire(t, p, source, "PC92^N2AAA^43202^K^5N2AAA:5459:635^0^0^H1^", now.Add(2*time.Second))
			if node.Entry.Version != "5459" || node.Entry.Build != "635" {
				t.Fatal("zero subject metadata prevented later nonzero restoration")
			}
		})
	}
}

func TestPC92GraphSubjectNumericActionIsolation(t *testing.T) {
	for _, action := range []string{"A", "C", "D"} {
		for _, subject := range []string{"5N2AAA", "5N2AAA:0:0", ""} {
			t.Run(action+"/"+subject, func(t *testing.T) {
				now := time.Now()
				g := newProtocolGraph(now)
				applyGraphRecord(t, g, "PC92^N2AAA^43200^C^5N2AAA:5457:633:192.0.2.1^1K1USER^H1^", now)
				applyGraphRecord(t, g, fmt.Sprintf("PC92^N2AAA^43201^%s^%s^1K1USER^H1^", action, subject), now.Add(time.Second))
				entry := g.nodes.Value("N2AAA").Entry
				if entry.Version != "5457" || entry.Build != "633" || entry.IP.String() != "192.0.2.1" || !entry.Here() {
					t.Fatalf("non-K omission altered node authority: %+v", entry)
				}
			})
		}
	}
	// Explicitly retain the implicit-C Here contract with a previously clear bit.
	g := newProtocolGraph(time.Now())
	applyGraphRecord(t, g, "PC92^N2AAA^43200^C^4N2AAA:5457:633^H1^", time.Now())
	applyGraphRecord(t, g, "PC92^N2AAA^43201^C^^H1^", time.Now())
	if entry := g.nodes.Value("N2AAA").Entry; entry.Here() || entry.Version != "5457" || entry.Build != "633" {
		t.Fatalf("implicit C replaced absent subject metadata: %+v", entry)
	}
}

func TestPC92GraphKEffectiveSubjectIsolation(t *testing.T) {
	for _, knownOrigin := range []bool{false, true} {
		t.Run(fmt.Sprint(knownOrigin), func(t *testing.T) {
			now := time.Now()
			g := newProtocolGraph(now)
			origin := PC92Entry{Call: "N2AAA", Flags: 5}
			if knownOrigin {
				applyGraphRecord(t, g, "PC92^N2AAA^43200^C^5N2AAA:5457:633^H1^", now)
				origin = g.nodes.Value("N2AAA").Entry
			}
			const wire = "PC92^N2AAA^43201^K^6N3EXT^0^0^H10^"
			r := graphRecord(t, wire)
			before := *r
			plan, err := g.prepare(r, "N0LOCAL", "N1PEER", nil)
			if err != nil || plan == nil {
				t.Fatalf("prepare: %v", err)
			}
			_ = g.projectedCharge(plan)
			g.commit(plan, now)
			if !reflect.DeepEqual(*r, before) || r.Subject.Version != "" || r.Subject.Build != "" {
				t.Fatal("effective node view mutated the decoded record")
			}
			if g.nodes.Value("N2AAA").Entry != origin {
				t.Fatal("external K changed distinct-origin metadata")
			}
			if edge := g.nodes.Value("N2AAA").Members.Value(memberKey{"N3EXT", true}); edge != before.Subject {
				t.Fatalf("external relationship adopted effective node values: %+v", edge)
			}
			if node := g.nodes.Value("N3EXT").Entry; node.Version != "0" || node.Build != "0" || node.Flags != 6 {
				t.Fatalf("external subject failed K numeric replacement: %+v", node)
			}
		})
	}
	// Drive the real receive/relay consumer separately: only its transport hop
	// may change, even though authoritative numeric values are explicit zero.
	p, source, destination, now := controllerTestOwner(t)
	receiveControllerWire(t, p, source, "PC92^N2AAA^43200^K^5N2AAA^0^0^H10^", now)
	select {
	case wire := <-destination.priorityLineCh:
		if wire != "PC92^N2AAA^43200^K^5N2AAA^0^0^H9^" {
			t.Fatalf("effective graph metadata escaped into transit: %q", wire)
		}
	default:
		t.Fatal("valid K was not relayed")
	}
}
