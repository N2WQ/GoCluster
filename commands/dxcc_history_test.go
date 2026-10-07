package commands

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/spot"
	"howett.net/plist"
)

func historyCanonicalCTY(t testing.TB) *cty.CTYDatabase {
	t.Helper()
	records := map[string]cty.PrefixInfo{
		"3D2": {Prefix: "3D2", ADIF: 176}, "R": {Prefix: "UA", ADIF: 54},
		"3D2RX": {Prefix: "3D2/R", ADIF: 460},
		"3Y":    {Prefix: "3Y/B", ADIF: 24}, "3Y0X": {Prefix: "3Y/P", ADIF: 199},
		"K": {Prefix: "K", ADIF: 291}, "W6": {Prefix: "K", ADIF: 291},
		"I": {Prefix: "I", ADIF: 248}, "IT9": {Prefix: "IT9", ADIF: 248}, "IG9": {Prefix: "IG9", ADIF: 248},
	}
	encoded, err := plist.Marshal(records, plist.XMLFormat)
	if err != nil {
		t.Fatal(err)
	}
	db, err := cty.LoadCTYDatabaseFromReader(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestHistoryCanonicalPrecedenceAndCompatibility(t *testing.T) {
	db := historyCanonicalCTY(t)
	var rows []*spot.Spot
	for i, code := range []int{54, 176, 460, 24, 199, 291, 248} {
		s := spot.NewSpot("ENTITY"+string(rune('A'+i)), "W1ABC", 14030, "CW")
		s.DXMetadata.ADIF = code
		s.Time = time.Now().Add(-time.Duration(i) * time.Second)
		rows = append(rows, s)
	}
	calls := 0
	p := NewProcessor(nil, &fakeArchive{spots: rows}, nil, func() *cty.CTYDatabase { calls++; return db }, nil, nil)
	match := func(s *spot.Spot) bool { return s != nil }
	for _, tc := range []struct{ selector, call string }{
		{"3D2/R", "ENTITYC"}, {"3y/p", "ENTITYE"}, {"IT9", "ENTITYG"}, {"K", "ENTITYF"},
	} {
		for _, command := range []string{"SHOW DX " + tc.selector + " 1", "SHOW MYDX 1 " + tc.selector} {
			before := calls
			response := p.ProcessCommandForClient(command, "N2WQ", "", match, "go")
			if !strings.Contains(response, tc.call) || strings.Count(response, "ENTITY") != 1 || calls != before+1 {
				t.Fatalf("canonical precedence/snapshot failed: %s -> %q, calls=%d", command, response, calls)
			}
		}
	}
	before := calls
	if response := p.ProcessCommandForClient("SHOW DX 1", "N2WQ", "", match, "go"); !strings.Contains(response, "ENTITYA") || calls != before {
		t.Fatalf("count-only changed: %q", response)
	}
	if response := p.ProcessCommandForClient("SHOW DX 291", "N2WQ", "", match, "go"); response != "Invalid count. Use 1-250.\n" {
		t.Fatalf("numeric entity syntax accidentally added: %q", response)
	}
	if response := p.ProcessCommandForClient("SHOW/DX 3D2/R 1", "N2WQ", "", match, "cc"); !strings.Contains(response, "ENTITYC") {
		t.Fatalf("CC alias: %q", response)
	}
	if response := p.ProcessCommandForClient("SHOW MYDX K 1", "N2WQ", "", func(*spot.Spot) bool { return false }, "go"); response != "No matching retained spots.\n" {
		t.Fatalf("client filter bypassed: %q", response)
	}
	// Detail lookup deliberately retains portable-call behavior, not canonical precedence.
	if response := p.ProcessCommandForClient("SHOW DXCC 3D2/R", "N2WQ", "", nil, "go"); !strings.Contains(response, "ADIF 54") {
		t.Fatalf("detail lookup changed: %q", response)
	}
}

func TestHistoryCanonicalConflictAndRefresh(t *testing.T) {
	db := historyCanonicalCTY(t)
	p := NewProcessor(nil, &fakeArchive{}, nil, func() *cty.CTYDatabase { return db }, nil, nil)
	if code, errText := p.resolveHistorySelector("K"); code.adif != 291 || errText != "" {
		t.Fatalf("initial snapshot: %+v %q", code, errText)
	}
	db = historyCanonicalCTY(t)
	db.Data["K"] = cty.PrefixInfo{Prefix: "K", ADIF: 999}
	db.Data["W6"] = cty.PrefixInfo{Prefix: "K", ADIF: 999}
	if code, errText := p.resolveHistorySelector("K"); code.adif != 999 || errText != "" {
		t.Fatalf("refresh not visible: %+v %q", code, errText)
	}
	db.Data["CONFLICT"] = cty.PrefixInfo{Prefix: "K", ADIF: 291}
	if _, errText := p.resolveHistorySelector("K"); errText != "Conflicting DXCC canonical prefix.\n" {
		t.Fatalf("conflict fell through: %q", errText)
	}
}
