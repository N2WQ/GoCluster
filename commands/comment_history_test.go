package commands

import (
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestHistoryCommentParserAndGenericProcessor(t *testing.T) {
	db := historyCanonicalCTY(t)
	wanted := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	wanted.Comment = `POTA:  up 5,! "*?"`
	wrongSpaces := spot.NewSpot("K1ABC", "W1AAA", 14031, "CW")
	wrongSpaces.Comment = `POTA: up 5,! "*?"`
	wrongCall := spot.NewSpot("K2ABC", "W1AAA", 14032, "CW")
	wrongCall.Comment = wanted.Comment
	p := NewProcessor(nil, &fakeArchive{spots: []*spot.Spot{wrongCall, wrongSpaces, wanted}}, nil, func() *cty.CTYDatabase { return db }, nil, nil)
	for _, prefix := range []string{"SHOW DX", "SH DX", "SHOW MYDX", "SH MYDX", "SHOW/DX", "SH/DX"} {
		for _, args := range []string{"k1abc 1", "1 k1abc"} {
			line := prefix + " " + args + ` cOmMeNt   pota:  up 5,! "*?"  `
			command, handled, text := p.ParseHistoryCommand(line, "cc")
			if !handled || text != "" || command.Query.comment != `pota:  up 5,! "*?"` || command.Query.count != 1 {
				t.Fatalf("%q: %+v handled=%v text=%q", line, command, handled, text)
			}
			page, err := p.ReadHistoryPage(command.Query, nil, func(*spot.Spot) bool { return true }, time.Now(), nil)
			if err != nil || len(page.Spots) != 1 || page.Spots[0] != wanted {
				t.Fatalf("phrase or selector/count mismatch: %+v %v", page, err)
			}
			response := p.ProcessCommandForClient(line, "W1AAA", "", func(*spot.Spot) bool { return true }, "cc")
			if !strings.Contains(response, wanted.FormatDXCluster()) || strings.Contains(response, wrongSpaces.FormatDXCluster()) {
				t.Fatalf("generic caller lost phrase: %q", response)
			}
		}
	}
	for _, command := range []string{"SHOW/DX", "SH/DX"} {
		if _, handled, text := p.ParseHistoryCommand(command+" COMMENT POTA", "go"); !handled || text != "Use SHOW DX or SH DX for DX history.\n" {
			t.Fatalf("dialect restriction changed: %v %q", handled, text)
		}
	}
	if _, handled, _ := p.ParseHistoryCommand("SHOW COMMENT POTA", "go"); handled {
		t.Fatal("unapproved SHOW COMMENT alias")
	}
}

func TestHistoryCommentLimitsAndSelection(t *testing.T) {
	p := NewProcessor(nil, &fakeArchive{}, nil, nil, nil, nil)
	for _, phrase := range []string{"POTA", "ALL", "NONE", "COMMENT", strings.Repeat("a", 64)} {
		command, handled, text := p.ParseHistoryCommand("SHOW DX COMMENT "+phrase, "go")
		if !handled || text != "" || command.Query.comment != phrase || command.Query.count != 50 {
			t.Fatalf("valid literal %q: %+v %v %q", phrase, command, handled, text)
		}
	}
	for _, phrase := range []string{"", "   ", "a\tb", "\tPOTA", "POTA\t", "\u00a0POTA", "POTA\u00a0", "PÖTA", "a\x7fb", strings.Repeat("a", 65)} {
		if _, handled, text := p.ParseHistoryCommand("SHOW DX COMMENT "+phrase, "go"); !handled || !strings.Contains(text, "Invalid COMMENT phrase") {
			t.Fatalf("invalid literal %q: %v %q", phrase, handled, text)
		}
	}
	token := "H1" + strings.Repeat("A", 32)
	if _, _, text := p.ParseHistoryCommand("SHOW DX NEXT "+token+" COMMENT POTA", "go"); !strings.Contains(text, "Invalid history continuation") {
		t.Fatalf("NEXT changed phrase: %q", text)
	}
	s := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	s.Comment = "POTA"
	p.archive = &fakeArchive{spots: []*spot.Spot{s}}
	query, _, _ := p.ParseHistoryCommand("SHOW DX COMMENT POTA", "go")
	page, err := p.ReadHistoryPage(query.Query, nil, func(*spot.Spot) bool { return false }, time.Now(), nil)
	if err != nil || len(page.Spots) != 0 {
		t.Fatalf("explicit query bypassed caller filters: %+v %v", page, err)
	}
}

func TestCommentCommandHelp(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, topic := range []string{"PASS COMMENT", "REJECT COMMENT", "REMOVE PASS COMMENT", "REMOVE REJECT COMMENT", "RESET FILTER COMMENT", "SHOW FILTER COMMENT"} {
			text := p.ProcessCommandForClient("HELP "+topic, "W1AAA", "", nil, dialect)
			if !strings.Contains(text, "Usage: "+topic) {
				t.Fatalf("%s %s: %q", dialect, topic, text)
			}
			for _, line := range strings.Split(text, "\n") {
				if len(line) > 78 {
					t.Fatalf("help exceeds width: %q", line)
				}
			}
		}
		text := p.ProcessCommandForClient("HELP SHOW DX", "W1AAA", "", nil, dialect)
		if !strings.Contains(text, "COMMENT <phrase>") || !strings.Contains(text, "even for self-spots") || !strings.Contains(text, "NEXT retains") {
			t.Fatalf("missing archive comment contract: %q", text)
		}
	}
}

func FuzzHistoryCommentRemainder(f *testing.F) {
	for _, seed := range []string{"POTA", "up  5", `:! ,"*?"`, "ALL", "a\tb", strings.Repeat("a", 65)} {
		f.Add(seed)
	}
	p := NewProcessor(nil, &fakeArchive{}, nil, nil, nil, nil)
	f.Fuzz(func(t *testing.T, phrase string) {
		if len(phrase) > 128 {
			t.Skip()
		}
		command, handled, text := p.ParseHistoryCommand("SHOW DX COMMENT "+phrase, "go")
		trimmed := strings.Trim(phrase, " ")
		valid := filter.ValidCommentPhrase(trimmed)
		if !handled || (text == "") != valid {
			t.Fatalf("phrase acceptance mismatch %q: %v %q", phrase, handled, text)
		}
		if valid && command.Query.comment != trimmed {
			t.Fatalf("literal changed: %q => %q", trimmed, command.Query.comment)
		}
	})
}
