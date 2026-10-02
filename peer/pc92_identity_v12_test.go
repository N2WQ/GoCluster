package peer

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func v12IdentityFrame(raw, role, action string) *Frame {
	fields := []string{"N2AAA", "43201", action, "5N2AAA"}
	switch role {
	case "origin":
		fields[0], fields[3] = raw, "7K2EXT"
	case "subject":
		fields[3] = "7" + raw
	}
	if action == "K" {
		fields = append(fields, "0", "0")
	} else {
		fields = append(fields, "1K2GOOD")
		if role == "member" {
			fields = append(fields, "1"+raw)
		}
	}
	return &Frame{Type: "PC92", Fields: fields, Hop: 10}
}

func TestPC92V12RawIdentityRoles(t *testing.T) {
	for _, raw := range []string{"K1ABC/", "/K1ABC", "K1ABC-000", "k1abc", " K1ABC", "EA8/K1ABC/P-00"} {
		for _, role := range []string{"origin", "subject", "member"} {
			for _, action := range []string{"A", "C", "D", "K"} {
				if role == "member" && action == "K" {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/%s", raw, role, action), func(t *testing.T) {
					if r, err := DecodePC92(v12IdentityFrame(raw, role, action)); err == nil || r != nil {
						t.Fatalf("raw-invalid identity gained authority: %+v %v", r, err)
					}
				})
			}
		}
	}
}

func TestPC92V12RolePadding(t *testing.T) {
	for _, suffix := range []string{" ", "  ", "\t", "\n", "\r", "\x00", "\u00a0"} {
		for _, role := range []string{"origin", "subject", "member"} {
			want := role != "origin" && strings.Trim(suffix, " ") == ""
			r, err := DecodePC92(v12IdentityFrame("K1ABC"+suffix, role, "C"))
			if (err == nil) != want || !want && r != nil {
				t.Errorf("role=%s padding=%q record=%+v err=%v", role, suffix, r, err)
			}
		}
		entry, err := DecodePC92Entry("1K1ABC" + suffix + ":5457:633:192.0.2.1")
		want := strings.Trim(suffix, " ") == ""
		if (err == nil) != want || want && (entry.Call != "K1ABC" || entry.Version != "5457" || entry.Build != "633" || entry.IP.String() != "192.0.2.1") {
			t.Errorf("entry padding=%q: %+v %v", suffix, entry, err)
		}
	}
}

func TestPC92V12StableIdentityBoundary(t *testing.T) {
	for raw, want := range map[string]string{"K1ABC": "K1ABC", "K1ABC/P": "K1ABC", "EA8/K1ABC": "K1ABC", "EA8/K1ABC/P": "K1ABC", "K1ABC-00": "K1ABC", "K1ABC-01": "K1ABC-1", "W1AW/P": "", "K1ABC-01/P": ""} {
		for _, role := range []string{"origin", "subject", "member"} {
			r, err := DecodePC92(v12IdentityFrame(raw, role, "C"))
			if want == "" {
				if err == nil || r != nil {
					t.Errorf("unrepresentable %s %q accepted", role, raw)
				}
				continue
			}
			if err != nil {
				t.Fatalf("valid %s %q: %v", role, raw, err)
			}
			got := r.Origin
			switch role {
			case "subject":
				got = r.Subject.Call
			case "member":
				got = r.Members[1].Call
			}
			if got != want {
				t.Errorf("%s %q mapped to %q, want %q", role, raw, got, want)
			}
		}
	}
}

func TestPC92V12WholeRecordAtomicity(t *testing.T) {
	for _, action := range []string{"A", "C", "D"} {
		f := v12IdentityFrame("K1ABC/", "member", action)
		if r, err := DecodePC92(f); err == nil || r != nil {
			t.Fatalf("%s accepted partial membership: %+v %v", action, r, err)
		}
		f.Fields[3] = ""
		f.Fields = f.Fields[:5]
		if r, err := DecodePC92(f); err != nil || !r.SubjectImplicit || r.Subject.Call != "N2AAA" {
			t.Fatalf("valid implicit %s: %+v %v", action, r, err)
		}
		f.Fields[0] = "N2AAA/"
		if r, err := DecodePC92(f); err == nil || r != nil {
			t.Fatalf("implicit subject bypassed invalid origin: %+v %v", r, err)
		}
	}
}

func TestPC92V12EncoderLocalOrigin(t *testing.T) {
	for _, origin := range []string{" k1abc-01 ", "K1ABC/", "EA8/K1ABC/MM-00", "K1ABC/P-01"} {
		for _, action := range []string{"A", "C", "D", "K"} {
			for _, implicit := range []bool{false, true} {
				if implicit && action == "K" {
					continue
				}
				r := &PC92Record{Origin: origin, Timestamp: "43200", Action: action, Subject: PC92Entry{Call: origin, Flags: 5, Version: "5457"}, SubjectImplicit: implicit, Hop: 99}
				if action == "K" {
					r.Extensions = []string{"", "mojo/reference"}
				} else {
					r.Members = []PC92Entry{{Call: "EA8/K2USER/P-00", Flags: 1}}
				}
				before := *r
				before.Members = append([]PC92Entry(nil), r.Members...)
				before.Extensions = append([]string(nil), r.Extensions...)
				wire, err := EncodePC92(r)
				if err != nil {
					t.Fatal(err)
				}
				want := "K1ABC"
				if strings.Contains(origin, "01") {
					want = "K1ABC-1"
				}
				if !strings.HasPrefix(wire, "PC92^"+want+"^") || !reflect.DeepEqual(*r, before) {
					t.Fatalf("local encoding changed input or failed canonical origin: %q", wire)
				}
				decoded, err := DecodePC92(mailboxFrame(t, wire))
				if err != nil || decoded.Origin != want || decoded.Subject.Call != want || decoded.SubjectImplicit != implicit {
					t.Fatalf("encoded authority %+v: %v", decoded, err)
				}
			}
		}
	}
	if wire, err := EncodePC92(&PC92Record{Origin: "NOTACALL", Subject: PC92Entry{Call: "K1ABC", Flags: 7}}); err == nil || wire != "" {
		t.Fatalf("invalid local origin emitted %q: %v", wire, err)
	}
}
