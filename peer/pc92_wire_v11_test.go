package peer

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"dxcluster/config"
)

func TestFrameAuthorityPayloadPositions(t *testing.T) {
	for _, tc := range []struct {
		wire   string
		fields []string
		hop    int
	}{
		{"PC92^N1NODE^123^K^5N1NODE^3^21^^H123/branch^H99^", []string{"N1NODE", "123", "K", "5N1NODE", "3", "21", "", "H123/branch"}, 99},
		{"PC92^N1NODE^123^K^5N1NODE^3^21^^H98^H99^", []string{"N1NODE", "123", "K", "5N1NODE", "3", "21", "", "H98"}, 99},
		{"PC93^N1NODE^123^LOGGER^K1ABC^*^H9x^H1ABC^^H99^", []string{"N1NODE", "123", "LOGGER", "K1ABC", "*", "H9x", "H1ABC", ""}, 99},
		{"PC93^N1NODE^123^LOGGER^K1ABC^*^H123^H98^H99^", []string{"N1NODE", "123", "LOGGER", "K1ABC", "*", "H123", "H98"}, 99},
	} {
		t.Run(tc.wire, func(t *testing.T) {
			frame, err := ParseFrame(tc.wire)
			if err != nil {
				t.Fatal(err)
			}
			if frame.Hop != tc.hop || !reflect.DeepEqual(frame.Fields, tc.fields) {
				t.Fatalf("frame=%+v want=%q", frame, tc.fields)
			}
			want := frame.Type + "^" + strings.Join(tc.fields, "^") + "^H0^"
			if got := frame.Encode(0); got != want {
				t.Fatalf("encoded %q want %q", got, want)
			}
			if frame.Type == "PC92" {
				record, err := DecodePC92(frame)
				if err != nil {
					t.Fatal(err)
				}
				wire, err := EncodePC92(record)
				if err != nil || wire != tc.wire {
					t.Fatalf("codec wire=%q err=%v", wire, err)
				}
			}
		})
	}
}

func TestPC92KeyPreservesCompletePayload(t *testing.T) {
	frames := []string{
		"PC92^N1NODE^123^K^5N1NODE^3^21^^H98^H99^",
		"PC92^N1NODE^123^K^5N1NODE^3^21^^H97^H99^",
		"PC92^N1NODE^123^K^5N1NODE^3^21^^^H98^H99^",
		"PC92^N1NODE^123^K^5N1NODE^3^21^^H98^^H99^",
	}
	seen := map[string]bool{}
	for _, wire := range frames {
		frame, err := ParseFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodePC92(frame); err != nil {
			t.Fatal(err)
		}
		key := pc92Key(frame)
		if seen[key] {
			t.Fatalf("distinct payload collapsed: %s", wire)
		}
		seen[key] = true
		if len(key) > 32 {
			t.Fatalf("unbounded key %q", key)
		}
		frame.Hop = 1
		if pc92Key(frame) != key {
			t.Fatal("hop changed payload key")
		}
	}
}

func TestPC92QueuedAndStagedPayloadPreserved(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	wire := "PC92^N2AAA^43200^K^5N2AAA^3^21^^H123/branch^H99^"
	frame, err := ParseFrame(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !p.enqueue(frame, source, now) {
		t.Fatal("queue refused fixture")
	}
	if input := <-p.input; input.wire != wire {
		t.Fatalf("queued payload=%q", input.wire)
	}
	// The isolated candidate fixture exercises retained wire encoding; full
	// establishment/expiry authority is covered by lifecycle integration tests.
	candidate := &candidateState{}
	p.manager.candidates.Set(source, candidate)
	record, err := DecodePC92(frame)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.manager.stagePC92Record(source, frame, record); err != nil {
		t.Fatal(err)
	}
	if len(candidate.staged) != 1 || candidate.staged[0] != wire {
		t.Fatalf("staged payload=%q", candidate.staged)
	}
	p.manager.releaseStaged(candidate)
}

func TestPC92MalformedHopPayloadNoAuthority(t *testing.T) {
	p, source, destination, now := controllerTestOwner(t)
	receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^5N2AAA^1K1OLD^H10^", now)
	keys, _, _ := p.pc92.occupancy()
	outputs := len(destination.priorityLineCh)
	frame, err := ParseFrame("PC92^N2AAA^43202^C^5N2AAA^1K1NEW^H9x^H99^")
	if err != nil {
		t.Fatal(err)
	}
	if frame.Fields[len(frame.Fields)-1] != "H9x" {
		t.Fatal("malformed member was silently stripped")
	}
	p.receive(frame, source, now)
	after, _, _ := p.pc92.occupancy()
	if p.graph.users.Value("K1OLD") != 1 || p.graph.users.Value("K1NEW") != 0 || p.graph.freshness.Value("N2AAA").Value != 43200 || keys != after || len(destination.priorityLineCh) != outputs {
		t.Fatal("malformed C changed authority/cache/relay")
	}
}

func TestPC92ImplicitCMetadataAndEmptySnapshot(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^5N2AAA:5457:633:192.0.2.1^1K1OLD^H10^", now)
	frame := receiveControllerWire(t, p, source, "PC92^N2AAA^43201^C^^1K1NEW^H10^", now)
	record, err := DecodePC92(frame)
	if err != nil || !record.SubjectImplicit {
		t.Fatalf("implicit C=%+v err=%v", record, err)
	}
	wire, err := EncodePC92(record)
	if err != nil || wire != "PC92^N2AAA^43201^C^^1K1NEW^H10^" {
		t.Fatalf("implicit encode=%s err=%v", wire, err)
	}
	if p.graph.users.Value("K1OLD") != 0 || p.graph.users.Value("K1NEW") != 1 {
		t.Fatal("implicit C did not replace membership")
	}
	receiveControllerWire(t, p, source, "PC92^N2AAA^43202^C^^H10^", now)
	node := p.graph.nodes.Value("N2AAA")
	if node == nil || node.Entry.Version != "5457" || node.Entry.Build != "633" || node.Entry.IP.String() != "192.0.2.1" || p.graph.users.Len() != 0 {
		t.Fatalf("empty implicit C metadata/state=%+v users=%d", node, p.graph.users.Len())
	}
}

func TestNewManagerWireContractBeforeStorage(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*config.PeeringConfig)
	}{
		{"flags", func(c *config.PeeringConfig) { c.PC92Bitmap = 7 }},
		{"local mismatch", func(c *config.PeeringConfig) { c.LocalCallsign = "N9OTHER" }},
		{"metadata", func(c *config.PeeringConfig) { c.LegacyVersion = "1.57" }},
		{"remote missing", func(c *config.PeeringConfig) { c.Peers = []config.PeeringPeer{{Enabled: true}} }},
		{"remote collision", func(c *config.PeeringConfig) {
			c.Peers = []config.PeeringPeer{{Enabled: true, RemoteCallsign: "K1PEER/P"}, {Enabled: true, RemoteCallsign: "K1PEER"}}
		}},
		{"backoff", func(c *config.PeeringConfig) { c.Backoff.MaxMS = 300001 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := completeProtocolTestConfig(config.PeeringConfig{}, "N0LOCAL")
			cfg.Topology.DBPath = filepath.Join(t.TempDir(), "must-not-open.sqlite")
			tc.edit(&cfg)
			m, err := NewManager(cfg, "N0LOCAL", nil, 0, nil)
			if err == nil {
				m.Stop()
				t.Fatal("invalid active config accepted")
			}
			if _, err := os.Stat(cfg.Topology.DBPath); !os.IsNotExist(err) {
				t.Fatalf("invalid constructor touched storage: %v", err)
			}
		})
	}
}

func TestFrameSharedProtocolRegression(t *testing.T) {
	for _, wire := range []string{
		"PC11^14074.0^K1ABC^23-Dec-2025^2001Z^TEST^W1XYZ^N1NODE^H3^",
		"PC61^14074.0^K1ABC^23-Dec-2025^2001Z^TEST^W1XYZ^N1NODE^192.0.2.1^H3^",
		"PC26^7074.0^K1ABC^24-Dec-2025^1501Z^TEST^W1XYZ^N1NODE^ ^H3^",
		"PC23^19-Apr-2026^1200Z^120^5^1^No storms^W1AW^N1NODE^H3^",
		"PC73^N1NODE^19-Apr-2026^1200Z^120^5^1^No storms^W1AW^H3^",
		"PC51^N1NODE^N2NODE^1^H3^",
		"PC18^DXSpider Version: 1.57 Build: 633 [pc9x 91]^5457^",
	} {
		frame, err := ParseFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		expected := strings.Split(strings.TrimSuffix(strings.TrimSuffix(wire, "^H3^"), "^"), "^")[1:]
		if frame.Type == "PC18" {
			expected = append(expected, "")
		}
		if !reflect.DeepEqual(frame.Fields, expected) {
			t.Fatalf("shared payload changed %s: %q want %q", wire, frame.Fields, expected)
		}
	}
}

func TestPC92TransitPreservesPayloadIdentity(t *testing.T) {
	p, source, destination, now := controllerTestOwner(t)
	wire := "PC92^EA8/N2AAA/P-00^43200^A^^1EA8/K1USER/P^H10^"
	receiveControllerWire(t, p, source, wire, now)
	select {
	case got := <-destination.priorityLineCh:
		if got != strings.Replace(wire, "^H10^", "^H9^", 1) {
			t.Fatalf("transit payload rewritten: %s", got)
		}
	default:
		t.Fatal("transit not forwarded")
	}
	if p.graph.users.Value("K1USER") != 1 {
		t.Fatal("internal identity was not canonicalized")
	}
}

func TestPC92PublicationCanonicalAliasLifecycle(t *testing.T) {
	p, _, _, _ := controllerTestOwner(t)
	snapshot := LocalMembership{Complete: true, Revision: 1, RawCount: 1, Users: []LocalUser{{SessionID: 1, Login: "EA8/K1USER/P-00", IP: "192.0.2.1"}}}
	p.manager.SetMembershipProvider(func() LocalMembership { return snapshot })
	entries, ok := p.membershipEntries()
	if !ok || entries.Value("K1USER").IP.String() != "192.0.2.1" {
		t.Fatal("canonical publication missing")
	}
	snapshot.Users = append(snapshot.Users, LocalUser{SessionID: 2, Login: "K1USER"})
	snapshot.RawCount = 2
	snapshot.Revision++
	entries, ok = p.membershipEntries()
	if !ok || entries.Value("K1USER").Call != "" {
		t.Fatal("ambiguous alias published")
	}
	snapshot.Users = snapshot.Users[:1]
	snapshot.RawCount = 1
	snapshot.Revision++
	snapshot.Users[0].IP = "2001:db8::1"
	entries, ok = p.membershipEntries()
	if !ok || entries.Value("K1USER").IP.String() != "2001:db8::1" {
		t.Fatal("unique alias/IP did not recover")
	}
	snapshot.Users[0].Login = "EA8/N0LOCAL/P"
	entries, ok = p.membershipEntries()
	if !ok || entries.Value("N0LOCAL").Call != "" {
		t.Fatal("reserved node identity published as user")
	}
	snapshot.Users[0].Login = "W1AW/P"
	entries, ok = p.membershipEntries()
	if !ok || entries.Value("W1AW").Call != "" {
		t.Fatal("unrepresentable short-base alias invented")
	}
}

func BenchmarkFrameAuthorityPayload(b *testing.B) {
	wire := "PC92^N1NODE^123^K^5N1NODE^3^21^^H123/branch^H99^"
	b.ReportAllocs()
	for b.Loop() {
		frame, err := ParseFrame(wire)
		if err != nil || len(frame.Fields) != 8 || frame.Fields[7] != "H123/branch" || frame.Hop != 99 {
			b.Fatalf("payload changed: %+v %v", frame, err)
		}
	}
}
