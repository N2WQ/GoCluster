package peer

import (
	"errors"
	"net/netip"
	"strings"
	"testing"
)

// Literal wire vectors follow DXProtout::_gen_pc92/pc92k and
// DXProtHandle::_decode_pc92_call at pinned 3e9b3621d94dd45c68702e4a0f896aac33f2a91d.
func TestDecodePC92ReferenceForms(t *testing.T) {
	for _, tc := range []struct {
		wire, action, subject, ip string
		members, nodes, users     int
	}{
		{"PC92^GB7DJK^43200^A^^1G1TLH:203.0.113.7^H99^", "A", "GB7DJK", "", 1, 0, 0},
		{"PC92^GB7DJK^43201^C^5GB7DJK:5457^1G1TLH:2001,db8,,1^7GB7TLH^H98^", "C", "GB7DJK", "", 2, 0, 0},
		{"PC92^GB7DJK^43202^D^^1G1TLH^H1^", "D", "GB7DJK", "", 1, 0, 0},
		{"PC92^GB7DJK^43203^C^7GB7EXT:5457^H99^", "C", "GB7EXT", "", 0, 0, 0},
		{"PC92^GB7DJK^43204^K^5GB7DJK:5457:633^0^0^H99^", "K", "GB7DJK", "", 0, 0, 0},
		{"PC92^GB7DJK^43205^K^5GB7DJK:5457:633^3^21^^mojo/3e9b362^H99^", "K", "GB7DJK", "", 0, 3, 21},
		{"PC92^GB7DJK^43206^K^5GB7DJK:5457:633^3^21^2001,db8,,1^mojo/3e9b362^H99^", "K", "GB7DJK", "2001:db8::1", 0, 3, 21},
	} {
		t.Run(tc.wire, func(t *testing.T) {
			f, err := ParseFrame(tc.wire)
			if err != nil {
				t.Fatal(err)
			}
			r, err := DecodePC92(f)
			if err != nil {
				t.Fatal(err)
			}
			if r.Action != tc.action || r.Subject.Call != tc.subject || len(r.Members) != tc.members || r.NodeCount != tc.nodes || r.UserCount != tc.users {
				t.Fatalf("unexpected record: %+v", r)
			}
			if tc.ip != "" && r.Subject.IP != netip.MustParseAddr(tc.ip) {
				t.Fatalf("IP=%v", r.Subject.IP)
			}
			encoded, err := EncodePC92(r)
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := ParseFrame(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = DecodePC92(parsed); err != nil {
				t.Fatalf("encoded invalid: %s: %v", encoded, err)
			}
		})
	}
}

func TestPC92EntryFlagsAndMetadata(t *testing.T) {
	for flag := byte('0'); flag <= '7'; flag++ {
		raw := string(flag) + "K1ABC:v5457:0.633:,,ffff,192.0.2.1"
		entry, err := DecodePC92Entry(raw)
		if err != nil {
			t.Fatal(err)
		}
		if entry.Flags != flag-'0' || entry.Version != "5457" || entry.Build != "633" || entry.IP.String() != "192.0.2.1" {
			t.Fatalf("%q => %+v", raw, entry)
		}
	}
	entry, err := DecodePC92Entry("1K1ABC:2001,db8,,1")
	if err != nil || entry.Version != "" || entry.IP.String() != "2001:db8::1" {
		t.Fatalf("short IPv6 %+v %v", entry, err)
	}
	entry, err = DecodePC92Entry("0K1ABC")
	if err != nil || entry.IP.IsValid() || entry.Here() {
		t.Fatalf("missing IP/here %+v %v", entry, err)
	}
}

func TestDecodePC92RejectsWholeMalformedRecords(t *testing.T) {
	for _, wire := range []string{
		"PC92^GB7DJK^1^C^5GB7DJK^1K1ABC^9K2BAD^H99^",
		"PC92^GB7DJK^1^C^5GB7DJK^1K1ABC^^H99^",
		"PC92^GB7DJK^1^C^5GB7DJK^1K1ABC^1NOTACALL^H99^",
		"PC92^GB7DJK^1^C^1GB7DJK^H99^",
		"PC92^GB7DJK^1^C^5GB7EXT^H99^",
		"PC92^GB7DJK^1^K^5GB7DJK^0^H99^",
		"PC92^GB7DJK^1^K^5GB7DJK^-1^0^H99^",
		"PC92^GB7DJK^1^K^5GB7DJK^0^0^bad-ip^H99^",
		"PC92^GB7DJK^NaN^C^5GB7DJK^H99^",
		"PC92^GB7DJK^86400^C^5GB7DJK^H99^",
		"PC92^GB7DJK^1^A^^H99^",
	} {
		f, err := ParseFrame(wire)
		if err != nil {
			t.Fatal(err)
		}
		if r, err := DecodePC92(f); err == nil || r != nil {
			t.Errorf("malformed record returned authority: %s: %+v %v", wire, r, err)
		}
	}
	for _, action := range []string{"F", "R", "Z"} {
		f, _ := ParseFrame("PC92^GB7DJK^1^" + action + "^GB7TLH^K1ABC^H99^")
		if r, err := DecodePC92(f); !errors.Is(err, ErrUnsupportedPC92) || r != nil {
			t.Errorf("action %s returned %+v %v", action, r, err)
		}
	}
}

func TestPC92EntryCountAndWireBounds(t *testing.T) {
	f := &Frame{Type: "PC92", Hop: 99, Fields: []string{"K1ABC", "1", "C", "5K1ABC"}}
	for i := 0; i < 8192; i++ {
		f.Fields = append(f.Fields, "1K2ABC")
	}
	if r, err := DecodePC92(f); err == nil || r != nil {
		t.Fatal("8193 entries accepted")
	}
	f.Fields = f.Fields[:len(f.Fields)-1]
	if _, err := DecodePC92(f); err != nil {
		t.Fatal(err)
	}
	f.Fields = []string{"K1ABC", "1", "K", "5K1ABC", "0", "0", "", strings.Repeat("x", MaxPeerFrameBytes)}
	if _, err := DecodePC92(f); err == nil {
		t.Fatal("oversized constructed frame accepted")
	}
}

func FuzzDecodePC92Atomic(f *testing.F) {
	f.Add("PC92^GB7DJK^43205^K^5GB7DJK:5457:633^3^21^^mojo/3e9b362^H99^")
	f.Add("PC92^GB7DJK^43200^C^5GB7DJK^1G1TLH:2001,db8,,1^H99^")
	f.Fuzz(func(t *testing.T, wire string) {
		frame, err := ParseFrame(wire)
		if err != nil {
			return
		}
		r, err := DecodePC92(frame)
		if err != nil {
			if r != nil {
				t.Fatal("partial record on error")
			}
			return
		}
		encoded, err := EncodePC92(r)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) > MaxPeerFrameBytes {
			t.Fatal("unbounded encoding")
		}
		frame, err = ParseFrame(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = DecodePC92(frame); err != nil {
			t.Fatal(err)
		}
	})
}
