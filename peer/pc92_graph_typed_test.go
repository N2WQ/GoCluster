package peer

import (
	"net/netip"
	"testing"
	"time"
)

func TestPC92GraphTypedMembershipAndDelete(t *testing.T) {
	for _, members := range []string{
		"1K1DUAL:192.0.2.1^5K1DUAL:5457:633:192.0.2.2^",
		"5K1DUAL:5457:633:192.0.2.2^1K1DUAL:192.0.2.1^",
	} {
		t.Run(members, func(t *testing.T) {
			now := time.Now()
			g := newProtocolGraph(now)
			applyGraphRecord(t, g, "PC92^N2AAA^43200^C^5N2AAA^"+members+"H1^", now)
			n := g.nodes.Value("N2AAA")
			if n.Members.Len() != 2 || g.edges != 2 || g.users.Value("K1DUAL") != 1 || g.nodes.Value("K1DUAL") == nil {
				t.Fatal("C collapsed independent user and node relationships")
			}
			if n.Members.Value(memberKey{"K1DUAL", false}).IP != netip.MustParseAddr("192.0.2.1") || n.Members.Value(memberKey{"K1DUAL", true}).IP != netip.MustParseAddr("192.0.2.2") {
				t.Fatal("metadata crossed the kind boundary")
			}
			applyGraphRecord(t, g, "PC92^N2AAA^43201^D^^5K1DUAL^H1^", now)
			applyGraphRecord(t, g, "PC92^N2AAA^43202^D^^5K1DUAL^H1^", now)
			if n.Members.Len() != 1 || n.Members.Value(memberKey{"K1DUAL", false}).Call != "K1DUAL" || g.users.Value("K1DUAL") != 1 {
				t.Fatal("node D removed same-call user or repeated D changed references")
			}
			applyGraphRecord(t, g, "PC92^N2AAA^43203^C^5N2AAA^5K1DUAL^H1^", now)
			if n.Members.Len() != 1 || g.users.Value("K1DUAL") != 0 || !n.Members.Value(memberKey{"K1DUAL", true}).IsNode() {
				t.Fatal("user-to-node C failed atomic typed replacement")
			}
			applyGraphRecord(t, g, "PC92^N2AAA^43204^C^5N2AAA^1K1DUAL^H1^", now)
			if n.Members.Len() != 1 || g.users.Value("K1DUAL") != 1 || n.Members.Value(memberKey{"K1DUAL", false}).IsNode() {
				t.Fatal("node-to-user C failed atomic typed replacement")
			}
			if n.Members.Value(memberKey{"K1DUAL", false}).IP.IsValid() {
				t.Fatal("new user relationship inherited removed node metadata")
			}
		})
	}
}

func TestPC92GraphCMetadataSelection(t *testing.T) {
	for _, tc := range []struct{ name, input, address string }{
		{"stable", "1K1DUAL:192.0.2.2^", "192.0.2.1"},
		{"repeated", "1K1DUAL:192.0.2.2^0K1DUAL^", "192.0.2.2"},
		{"second-kind", "1K1DUAL:192.0.2.2^5K1DUAL:5457:633:192.0.2.3^", "192.0.2.2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			g := newProtocolGraph(now)
			applyGraphRecord(t, g, "PC92^N2AAA^43200^A^^1K1DUAL:192.0.2.1^H1^", now)
			applyGraphRecord(t, g, "PC92^N2AAA^43201^C^5N2AAA^"+tc.input+"H1^", now)
			got := g.nodes.Value("N2AAA").Members.Value(memberKey{"K1DUAL", false})
			if got.IP.String() != tc.address {
				t.Fatalf("C metadata selection = %s, want receiver value %s", got.IP, tc.address)
			}
		})
	}
}

func TestPC92GraphOrderedMetadata(t *testing.T) {
	now := time.Now()
	g := newProtocolGraph(now)
	applyGraphRecord(t, g, "PC92^N2AAA^43200^A^^1K1USER:10:20:192.0.2.1^0K1USER:11:21^5N3NODE:5457:633:192.0.2.2^4N3NODE:5458^H1^", now)
	n := g.nodes.Value("N2AAA")
	user := n.Members.Value(memberKey{"K1USER", false})
	node := n.Members.Value(memberKey{"N3NODE", true})
	if !user.Here() || user.Version != "" || user.Build != "" || user.IP.String() != "192.0.2.1" || !node.Here() || node.Version != "5457" || node.Build != "" || node.IP.String() != "192.0.2.2" {
		t.Fatalf("repeated same-kind entries discarded prior explicit metadata: user=%+v node=%+v", user, node)
	}
}

func TestPC92GraphMemberNodeNumericAuthorityAndAlternateIP(t *testing.T) {
	now := time.Now()
	g := newProtocolGraph(now)
	applyGraphRecord(t, g, "PC92^N2AAA^43200^A^^4N3NODE::633:192.0.2.1^5N3NODE:5458^H1^", now)
	node := g.nodes.Value("N3NODE")
	if node.Entry.Version != "5401" || node.Entry.Build != "" {
		t.Fatalf("new node did not retain first/default numeric authority: %+v", node.Entry)
	}
	applyGraphRecord(t, g, "PC92^N3NODE^43201^K^4N3NODE:5459:635^0^0^H1^", now)
	applyGraphRecord(t, g, "PC92^N4BBB^43202^A^^5N3NODE:5460:636:192.0.2.2^H1^", now)
	applyGraphRecord(t, g, "PC92^N2AAA^43203^A^^5N3NODE:5461:637^H1^", now)
	if node.Entry.Version != "5459" || node.Entry.Build != "635" || node.Entry.Here() || node.Entry.IP.String() != "192.0.2.2" {
		t.Fatalf("member update overwrote node authority or restored stale edge IP: %+v", node.Entry)
	}
}

func TestPC92GraphMemberHereAndExplicitSubject(t *testing.T) {
	now := time.Now()
	g := newProtocolGraph(now)
	applyGraphRecord(t, g, "PC92^N2AAA^43200^A^^0K1USER^4N3NODE^H1^", now)
	n := g.nodes.Value("N2AAA")
	if !n.Members.Value(memberKey{"K1USER", false}).Here() || !g.nodes.Value("N3NODE").Entry.Here() {
		t.Fatal("new member did not acquire receiver constructor Here default")
	}
	applyGraphRecord(t, g, "PC92^N3NODE^43201^K^4N3NODE^0^0^H1^", now)
	if g.nodes.Value("N3NODE").Entry.Here() {
		t.Fatal("explicit subject did not clear node Here")
	}
	applyGraphRecord(t, g, "PC92^N2AAA^43202^A^^5N3NODE^H1^", now)
	if g.nodes.Value("N3NODE").Entry.Here() || n.Members.Value(memberKey{"N3NODE", true}).Here() {
		t.Fatal("member update overrode explicit subject Here authority")
	}
}

func TestPC92GraphExternalSubjectPreservesSameCallUser(t *testing.T) {
	now := time.Now()
	g := newProtocolGraph(now)
	applyGraphRecord(t, g, "PC92^N2AAA^43200^A^^1N3EXT^H1^", now)
	applyGraphRecord(t, g, "PC92^N2AAA^43201^C^7N3EXT^1K1USER^H1^", now)
	n := g.nodes.Value("N2AAA")
	if n.Members.Len() != 2 || !n.Members.Value(memberKey{"N3EXT", true}).IsExternal() || g.users.Value("N3EXT") != 1 || g.edges != 3 {
		t.Fatal("same-call user suppressed the external-subject node relationship")
	}
}

func TestPC92GraphTypedExpiryAndCollisionReplacement(t *testing.T) {
	now := time.Now()
	g := newProtocolGraph(now)
	applyGraphRecord(t, g, "PC92^N2AAA^43200^A^^1K1DUAL^5K1DUAL^1K2USER^H1^", now)
	applyGraphRecord(t, g, "PC92^N3BBB^43200^A^^1K1DUAL^H1^", now)
	g.nodes.Value("K1DUAL").Observations = 1
	forceGraphIndexCollisions(g.nodes.Value("N2AAA").Members)
	g.expire(now.Add(time.Hour), true, nil)
	if g.nodes.Value("K1DUAL") != nil || g.users.Value("K1DUAL") != 2 || g.nodes.Value("N2AAA").Members.Len() != 2 {
		t.Fatal("node expiry removed an independent user relationship")
	}
	applyGraphRecord(t, g, "PC92^N2AAA^43201^C^5N2AAA^5K1DUAL^1K3NEW^H1^", now)
	if g.users.Value("K1DUAL") != 1 || g.users.Value("K2USER") != 0 || g.users.Value("K3NEW") != 1 || g.nodes.Value("N2AAA").Members.Len() != 2 {
		t.Fatal("two-pass C skipped a colliding removal or damaged alternate ownership")
	}
}
