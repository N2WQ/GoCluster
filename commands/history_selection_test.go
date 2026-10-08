package commands

import (
	"reflect"
	"strings"
	"testing"
	"time"
	"unsafe"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestHistoryBandModeParser(t *testing.T) {
	db := historyCanonicalCTY(t)
	p := NewProcessor(nil, &fakeArchive{}, nil, func() *cty.CTYDatabase { return db }, nil, nil)
	for _, prefix := range []string{"SHOW DX", "SH DX", "SHOW MYDX", "SH MYDX", "SHOW/DX", "SH/DX"} {
		for _, args := range []string{
			"k1abc 20 band 20,40m,20m mode cw ft8 CW comment POTA:  up 5!",
			"20 k1abc MODE CW,FT8 BAND 20m 40 COMMENT POTA:  up 5!",
		} {
			command, handled, text := p.ParseHistoryCommand(prefix+" "+args, "cc")
			if !handled || text != "" || command.Query.count != 20 || command.Query.selector.call != "K1ABC" ||
				!reflect.DeepEqual(command.Query.bands, []string{"20m", "40m"}) ||
				!reflect.DeepEqual(command.Query.modes, []string{"CW", "FT8"}) || command.Query.comment != "POTA:  up 5!" {
				t.Fatalf("%s %s: %+v handled=%v text=%q", prefix, args, command, handled, text)
			}
		}
	}
	for _, tc := range []struct {
		args  string
		bands []string
		modes []string
	}{
		{"BAND 1.25m,70cm", []string{"1.25m", "70cm"}, nil},
		{"MODE psk31,PSK63 unknown", nil, []string{"PSK", "UNKNOWN"}},
		{"BAND 20,,40,", []string{"20m", "40m"}, nil},
	} {
		command, handled, text := p.ParseHistoryCommand("SHOW DX "+tc.args, "go")
		if !handled || text != "" || command.Query.count != 50 || command.Query.selector.kind != historyNone ||
			!reflect.DeepEqual(command.Query.bands, tc.bands) || !reflect.DeepEqual(command.Query.modes, tc.modes) {
			t.Fatalf("%s: %+v %q", tc.args, command.Query, text)
		}
	}
	command, _, text := p.ParseHistoryCommand("SHOW DX BAND 20 COMMENT POTA BAND 40 MODE CW", "go")
	if text != "" || command.Query.comment != "POTA BAND 40 MODE CW" || len(command.Query.modes) != 0 {
		t.Fatalf("COMMENT remainder was interpreted as clauses: %+v %q", command.Query, text)
	}
}

func TestHistoryBandModeInvalidRequests(t *testing.T) {
	p := NewProcessor(nil, &fakeArchive{}, nil, nil, nil, nil)
	for _, args := range []string{
		"BAND", "MODE", "BAND , ,", "MODE ,", "BAND MODE CW", "MODE BAND 20",
		"BAND 20,BOGUS", "MODE CW,BOGUS", "BAND ALL", "BAND NONE", "MODE ALL", "MODE NONE",
		"BAND 20 BAND 40", "MODE CW MODE FT8", "BAND 20 MODE CW BAND 40", "MODE CW K1ABC",
		"BAND 20 COMMENT", "MODE CW COMMENT POTA\t",
	} {
		command, handled, text := p.ParseHistoryCommand("SHOW DX "+args, "go")
		if !handled || text == "" || !reflect.DeepEqual(command.Query, HistoryQuery{}) {
			t.Fatalf("invalid %q was accepted or retained partial state: %+v %v %q", args, command, handled, text)
		}
	}
	for _, prefix := range []string{"SHOW/DX", "SH/DX"} {
		if _, handled, text := p.ParseHistoryCommand(prefix+" BAND 20 MODE CW", "go"); !handled || text != "Use SHOW DX or SH DX for DX history.\n" {
			t.Fatalf("dialect restriction changed: %q", text)
		}
	}
	if _, _, text := p.ParseHistoryCommand("SHOW DX NEXT H1"+strings.Repeat("A", 32)+" BAND 20", "go"); !strings.Contains(text, "Invalid history continuation") {
		t.Fatalf("NEXT accepted new selections: %q", text)
	}
}

func TestHistoryBandModeMatchingBeforeCount(t *testing.T) {
	db := historyCanonicalCTY(t)
	var rows []*spot.Spot
	for _, row := range []struct {
		call, mode, comment string
		freq                float64
	}{
		{"K2ABC", "CW", "POTA", 14030},
		{"K1ABC", "CW", "POTA", 21030},
		{"K1ABC", "RTTY", "POTA", 14080},
		{"K1ABC", "CW", "SOTA", 14031},
		{"K1ABC", "CW", "POTA", 14032},
		{"K1ABC", "FT8", "POTA", 7074},
		{"K1ABC", "", "POTA", 14033},
		{"K1ABC", "PSK31", "POTA", 14070},
	} {
		s := spot.NewSpot(row.call, "W1AAA", row.freq, row.mode)
		s.Comment = row.comment
		rows = append(rows, s)
	}
	p := NewProcessor(nil, &fakeArchive{spots: rows}, nil, func() *cty.CTYDatabase { return db }, nil, nil)
	for _, tc := range []struct {
		args string
		want []int
	}{
		{"K1ABC 1 BAND 20,40 MODE CW FT8 COMMENT POTA", []int{4}},
		{"K1ABC 20 BAND 20,40 MODE CW FT8 COMMENT POTA", []int{4, 5}},
		{"K1ABC MODE UNKNOWN BAND 20", []int{6}},
		{"K1ABC MODE PSK63", []int{7}},
		{"K1ABC BAND 40", []int{5}},
		{"K1ABC BAND 6 MODE CW", nil},
	} {
		command, _, text := p.ParseHistoryCommand("SHOW DX "+tc.args, "go")
		if text != "" {
			t.Fatal(text)
		}
		page, err := p.ReadHistoryPage(command.Query, nil, func(*spot.Spot) bool { return true }, time.Now(), nil)
		if err != nil || len(page.Spots) != len(tc.want) {
			t.Fatalf("%s: got %+v %v want %v", tc.args, page, err, tc.want)
		}
		for i, index := range tc.want {
			if page.Spots[i] != rows[index] {
				t.Fatalf("%s: row %d got %s want %s", tc.args, i, page.Spots[i].FormatDXCluster(), rows[index].FormatDXCluster())
			}
		}
		response := p.ProcessCommandForClient("SHOW DX "+tc.args, "W1AAA", "", func(*spot.Spot) bool { return true }, "go")
		if strings.Count(response, "DX de") != len(tc.want) {
			t.Fatalf("generic processor lost selections: %s: %q", tc.args, response)
		}
	}
	command, _, _ := p.ParseHistoryCommand("SHOW DX K1ABC BAND 20 MODE CW COMMENT POTA", "go")
	f := filter.NewFilter()
	f.SetBand("20m", false)
	page, err := p.ReadHistoryPage(command.Query, nil, f.Matches, time.Now(), nil)
	if err != nil || len(page.Spots) != 0 {
		t.Fatalf("explicit selection overrode saved filter: %+v %v", page, err)
	}
}

func TestHistorySelectionsRetainOnlyUniqueSupportedValues(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	bands := spot.SupportedBandNames()
	modes := filter.SupportedModes()
	// Direct API callers can exceed the telnet byte budget. Repetition must not
	// grow retained state beyond the finite supported vocabulary.
	line := "SHOW DX BAND " + strings.Repeat(strings.Join(bands, ",")+",", 100) + " MODE " + strings.Repeat(strings.Join(modes, ",")+",", 100)
	command, _, text := p.ParseHistoryCommand(line, "go")
	if text != "" || len(command.Query.bands) != len(bands) || len(command.Query.modes) != len(modes) {
		t.Fatalf("unbounded or missing unique values: %+v %q", command.Query, text)
	}
	if !reflect.DeepEqual(command.Query.bands, bands) || !reflect.DeepEqual(command.Query.modes, modes) {
		t.Fatal("retained duplicate or noncanonical values")
	}
}

func TestHistorySelectionDetachesExactSelector(t *testing.T) {
	db := historyCanonicalCTY(t)
	p := NewProcessor(nil, nil, nil, func() *cty.CTYDatabase { return db }, nil, nil)
	line := "SHOW DX K9BOUND 20 BAND " + strings.Repeat("20m,40m,", 100)
	command, _, text := p.ParseHistoryCommand(line, "go")
	if text != "" || command.Query.selector.call != "K9BOUND" {
		t.Fatalf("exact selector fixture failed: %+v %q", command.Query, text)
	}
	// Compare addresses only, without dereferencing or pointer arithmetic. An
	// uppercase token otherwise survives normalization with the same backing
	// bytes, including when the callsign cache retains the first such input.
	borrowed := strings.Fields(line)[2]
	if unsafe.StringData(command.Query.selector.call) == unsafe.StringData(borrowed) {
		t.Fatal("short exact selector retains the entire large command")
	}
}

func FuzzHistoryBandModeSelectionResults(f *testing.F) {
	for _, mask := range []uint8{0, 1, 2, 4, 8, 16, 31} {
		f.Add(mask, false, false)
	}
	f.Fuzz(func(t *testing.T, mask uint8, reverse, comma bool) {
		separator := " "
		if comma {
			separator = ","
		}
		var bands, modes []string
		if mask&1 != 0 {
			bands = append(bands, "20")
		}
		if mask&2 != 0 {
			bands = append(bands, "40m")
		}
		for i, mode := range []string{"CW", "FT8", "UNKNOWN"} {
			if mask&(4<<i) != 0 {
				modes = append(modes, mode)
			}
		}
		bandClause, modeClause := "", ""
		if len(bands) != 0 {
			bandClause = " BAND " + strings.Join(bands, separator)
		}
		if len(modes) != 0 {
			modeClause = " MODE " + strings.Join(modes, separator)
		}
		if reverse {
			bandClause, modeClause = modeClause, bandClause
		}
		var rows, want []*spot.Spot
		for bandIndex, frequency := range []float64{14030, 7030} {
			for modeIndex, mode := range []string{"CW", "FT8", ""} {
				s := spot.NewSpot("K1ABC", "W1AAA", frequency+float64(modeIndex), mode)
				rows = append(rows, s)
				// This fixture oracle uses input bits and row positions, not the
				// production selection parser, normalizers or predicates.
				if (mask&3 == 0 || mask&(1<<bandIndex) != 0) && (mask&28 == 0 || mask&(4<<modeIndex) != 0) {
					want = append(want, s)
				}
			}
		}
		p := NewProcessor(nil, &fakeArchive{spots: rows}, nil, nil, nil, nil)
		command, handled, text := p.ParseHistoryCommand("SHOW DX"+bandClause+modeClause, "go")
		if !handled || text != "" {
			t.Fatalf("valid generated list rejected: %q", text)
		}
		page, err := p.ReadHistoryPage(command.Query, nil, func(*spot.Spot) bool { return true }, time.Now(), nil)
		if err != nil || !reflect.DeepEqual(page.Spots, want) {
			t.Fatalf("mask=%d reverse=%v comma=%v: got %+v %v want %+v", mask, reverse, comma, page.Spots, err, want)
		}
	})
}

func TestHistoryBandModeHelp(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		topics := []string{"SHOW MYDX"}
		if dialect == "cc" {
			topics = append(topics, "SHOW/DX")
		} else {
			topics = append(topics, "SHOW DX")
		}
		for _, topic := range topics {
			text := p.ProcessCommandForClient("HELP "+topic, "W1AAA", "", nil, dialect)
			for _, phrase := range []string{"BAND <list>", "MODE <list>", "OR within lists", "AND between selections", "UNKNOWN", "Put COMMENT", "NEXT retains all selections"} {
				if !strings.Contains(text, phrase) {
					t.Fatalf("%s HELP %s lacks %q: %q", dialect, topic, phrase, text)
				}
			}
		}
	}
}
