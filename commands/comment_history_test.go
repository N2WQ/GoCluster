package commands

import (
	"strings"
	"testing"
	"time"

	"dxcluster/archive"
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

// These fixed rejection vectors intentionally do not consult the shared phrase
// validator: they catch both validator drift and generic pre-parser trimming.
func TestGenericHistoryCommentInvalidPhraseGoldens(t *testing.T) {
	const invalid = "Invalid COMMENT phrase. Use 1-64 printable ASCII bytes.\n"
	p := NewProcessor(nil, &fakeArchive{}, nil, nil, nil, nil)
	match := func(*spot.Spot) bool { return true }
	for _, dialect := range []string{"go", "cc"} {
		for _, prefix := range []string{"SHOW DX", "SH DX", "SHOW MYDX", "SH MYDX", "SHOW/DX", "SH/DX"} {
			if dialect == "go" && strings.Contains(prefix, "/") {
				continue
			}
			for _, phrase := range []string{"", "   ", "POTA\t", "POTA\u00a0", "\tPOTA", "\u00a0POTA", "POTA\n", "POTA\r", "POTA\v", "POTA\f", "POTA\x00", "POTA\x7f", "PÖTA", "PO\tTA", strings.Repeat("a", 65)} {
				line := prefix + " COMMENT " + phrase
				if text := p.ProcessCommandForClient(line, "W1AAA", "", match, dialect); text != invalid {
					t.Errorf("%s %q: want exact invalid phrase rejection, got %q", dialect, line, text)
				}
			}
			// A nil predicate retains the established logged-user guard even
			// when the history phrase is malformed.
			for _, phrase := range []string{"POTA", "POTA\t", "POTA\u00a0"} {
				if text := p.ProcessCommandForClient(prefix+" COMMENT "+phrase, "W1AAA", "", nil, dialect); text != noLoggedUserMsg {
					t.Errorf("nil-filter routing changed: %s %s %q => %q", dialect, prefix, phrase, text)
				}
			}
		}
		for _, ordinary := range []string{"SHOW BUILD", "SHOW OWN"} {
			for _, predicate := range []func(*spot.Spot) bool{nil, match} {
				want := p.ProcessCommandForClient(ordinary, "W1AAA", "", predicate, dialect)
				if text := p.ProcessCommandForClient("\t "+ordinary+"\t\u00a0\r\n", "W1AAA", "", predicate, dialect); text != want {
					t.Errorf("ordinary command whitespace routing changed: %s %s => %q, want %q", dialect, ordinary, text, want)
				}
			}
		}
	}
	for _, prefix := range []string{"SHOW/DX", "SH/DX"} {
		for _, predicate := range []func(*spot.Spot) bool{nil, match} {
			if text := p.ProcessCommandForClient(prefix+" COMMENT POTA\t", "W1AAA", "", predicate, "go"); text != "Use SHOW DX or SH DX for DX history.\n" {
				t.Errorf("slash alias restriction changed: %s => %q", prefix, text)
			}
		}
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
	for _, seed := range []string{"POTA", "up  5", `:! ,"*?"`, "ALL", "a\tb", "POTA\t", "POTA\u00a0", " POTA ", strings.Repeat("a", 65)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, phrase string) {
		if len(phrase) > 128 {
			t.Skip()
		}
		p := NewProcessor(nil, &fakeArchive{}, nil, nil, nil, nil)
		line := "SHOW DX 1 COMMENT " + phrase
		command, handled, text := p.ParseHistoryCommand(line, "go")
		trimmed := strings.Trim(phrase, " ")
		valid := filter.ValidCommentPhrase(trimmed)
		if !handled || (text == "") != valid {
			t.Fatalf("phrase acceptance mismatch %q: %v %q", phrase, handled, text)
		}
		if valid && command.Query.comment != trimmed {
			t.Fatalf("literal changed: %q => %q", trimmed, command.Query.comment)
		}
		// Put a nonmatching row first and a literal matching row second. A
		// count-one generic query must skip the first row, so ignoring COMMENT
		// or losing its literal remainder produces observably wrong output.
		wrong := spot.NewSpot("K2WRONG", "W1AAA", 14031, "CW")
		wanted := spot.NewSpot("K1RIGHT", "W1AAA", 14030, "CW")
		wanted.Comment = trimmed
		p.archive = &fakeArchive{spots: []*spot.Spot{wrong, wanted}}
		response := p.ProcessCommandForClient(line, "W1AAA", "", func(*spot.Spot) bool { return true }, "go")
		if !valid {
			if response != "Invalid COMMENT phrase. Use 1-64 printable ASCII bytes.\n" {
				t.Fatalf("generic caller accepted invalid phrase %q: %q", phrase, response)
			}
			return
		}
		if response != RenderHistoryPage(archive.HistoryPage{Spots: []*spot.Spot{wanted}, End: archive.HistoryExhausted}, "", false, false, false) {
			t.Fatalf("generic caller lost phrase-sensitive selection for %q: %q", phrase, response)
		}
	})
}
