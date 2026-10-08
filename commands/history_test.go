package commands

import (
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
	"dxcluster/cty"
	"dxcluster/spot"
)

func TestHistoryExactIdentityAndAliases(t *testing.T) {
	db := historyCanonicalCTY(t)
	var rows []*spot.Spot
	for _, call := range []string{"K1ABC", "K1ABCD", "W6XYZ", "K1ABC/P", "W6/LZ5VV", "ZZ0ZZ"} {
		s := spot.NewSpot(call, "W1AAA", 14030, "CW")
		s.DXMetadata.ADIF = 291
		rows = append(rows, s)
	}
	p := NewProcessor(nil, &fakeArchive{spots: rows}, nil, func() *cty.CTYDatabase { return db }, nil, nil)
	for _, command := range []string{"SHOW DX", "SH DX", "SHOW MYDX", "SH MYDX", "SHOW/DX", "SH/DX"} {
		for _, args := range []string{"K1ABC 20", "20 K1ABC", "K1ABC-1 20", "K1ABC/P 20"} {
			parsed, handled, errText := p.ParseHistoryCommand(command+" "+args, "cc")
			if !handled || errText != "" {
				t.Fatalf("%s %s: %v %q", command, args, handled, errText)
			}
			page, err := p.ReadHistoryPage(parsed.Query, nil, func(*spot.Spot) bool { return true }, time.Now(), nil)
			if err != nil || len(page.Spots) != 2 {
				t.Fatalf("%s %s: %+v %v", command, args, page, err)
			}
			for _, row := range page.Spots {
				if row.DXCallNorm != "K1ABC" {
					t.Fatalf("widened match: %s", row.DXCallNorm)
				}
			}
		}
	}
	for _, call := range []string{"W6/LZ5VV/M", "ZZ0ZZ"} {
		parsed, _, text := p.ParseHistoryCommand("SHOW DX "+call, "go")
		page, err := p.ReadHistoryPage(parsed.Query, nil, func(*spot.Spot) bool { return true }, time.Now(), nil)
		if text != "" || err != nil || len(page.Spots) != 1 {
			t.Fatalf("%s: %q %+v %v", call, text, page, err)
		}
	}
	parsed, _, _ := p.ParseHistoryCommand("SHOW DX K", "go")
	page, err := p.ReadHistoryPage(parsed.Query, nil, func(*spot.Spot) bool { return true }, time.Now(), nil)
	if err != nil || len(page.Spots) != len(rows) {
		t.Fatalf("canonical entity: %+v %v", page, err)
	}
	parsed, _, _ = p.ParseHistoryCommand("SHOW DX K9NOPE", "go")
	page, err = p.ReadHistoryPage(parsed.Query, nil, func(*spot.Spot) bool { return true }, time.Now(), nil)
	if err != nil || len(page.Spots) != 0 {
		t.Fatalf("exact no-match widened: %+v %v", page, err)
	}
}

func TestHistoryFilterBeforeCountAndPresentation(t *testing.T) {
	db := historyCanonicalCTY(t)
	blocked := spot.NewSpot("K1ABC", "W1AAA", 10130, "CW")
	wanted := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	p := NewProcessor(nil, &fakeArchive{spots: []*spot.Spot{blocked, wanted}}, nil, func() *cty.CTYDatabase { return db }, nil, nil)
	parsed, _, _ := p.ParseHistoryCommand("SHOW DX K1ABC 1", "go")
	page, err := p.ReadHistoryPage(parsed.Query, nil, func(s *spot.Spot) bool { return s.Frequency == 14030 }, time.Now(), nil)
	if err != nil || len(page.Spots) != 1 || page.Spots[0] != wanted {
		t.Fatalf("filter after count: %+v %v", page, err)
	}
	newer := spot.NewSpot("K2NEW", "W1AAA", 14031, "CW")
	older := spot.NewSpot("K2OLD", "W1AAA", 14032, "CW")
	newer.Time, older.Time = time.Now(), time.Now().Add(-time.Minute)
	ordered := RenderHistoryPage(archive.HistoryPage{Spots: []*spot.Spot{newer, older}, End: archive.HistoryExhausted}, "", false, false, false)
	if strings.Index(ordered, "K2OLD") >= strings.Index(ordered, "K2NEW") || !strings.Contains(ordered, older.FormatDXCluster()) || !strings.Contains(ordered, newer.FormatDXCluster()) {
		t.Fatalf("page is not chronological or spot format changed: %q", ordered)
	}
	text := RenderHistoryPage(archive.HistoryPage{End: archive.HistoryBudgetReached}, "H1"+strings.Repeat("A", 32), false, false, false)
	if strings.Contains(text, "No matching") || !strings.Contains(text, "incomplete") || !strings.Contains(text, "NEXT") {
		t.Fatal(text)
	}
	text = RenderHistoryPage(archive.HistoryPage{End: archive.HistoryExhausted}, "", true, true, true)
	if strings.Contains(text, "No matching") || !strings.Contains(text, "Warning") || !strings.Contains(text, "Older") {
		t.Fatal(text)
	}
}

func FuzzHistoryCommand(f *testing.F) {
	for _, seed := range []string{"SHOW DX K1ABC 20", "SHOW MYDX 20 W6/LZ5VV", "SHOW DX NEXT H1" + strings.Repeat("A", 32), "SHOW DX NEXT", "SH/DX 251", "SHOW DX K$", "SHOW DX COMMENT POTA:  up 5!", "SHOW MYDX 1 K1ABC COMMENT ALL", "SHOW DX COMMENT " + strings.Repeat("a", 65), "SHOW DX BAND 20,40 MODE CW FT8 COMMENT POTA:  up 5!", "SHOW DX MODE UNKNOWN", "SHOW DX BAND 20 BAND 40"} {
		f.Add(seed)
	}
	db := historyCanonicalCTY(f)
	p := NewProcessor(nil, &fakeArchive{}, nil, func() *cty.CTYDatabase { return db }, nil, nil)
	f.Fuzz(func(t *testing.T, line string) {
		if len(line) > 128 {
			t.Skip()
		}
		command, handled, errText := p.ParseHistoryCommand(line, "cc")
		if !handled || errText != "" {
			return
		}
		if command.Token != "" {
			if !ValidHistoryToken(command.Token) {
				t.Fatal("accepted invalid token")
			}
			return
		}
		if command.Query.count < 1 || command.Query.count > 250 {
			t.Fatal("unbounded count")
		}
		if command.Query.selector.kind == historyExactCall && !spot.IsValidNormalizedCallsign(command.Query.selector.call) {
			t.Fatal("invalid exact identity")
		}
		if len(command.Query.bands) > len(spot.SupportedBandNames()) || len(command.Query.modes) > len(spot.SupportedFilterModes()) {
			t.Fatal("unbounded retained selection")
		}
	})
}
