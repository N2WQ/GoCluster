package commands

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"
)

func loadHistoryCommentTaxonomy(t testing.TB, kind string) {
	t.Helper()
	aliases := "BAND, MODE"
	if kind == "supported" {
		aliases += ", COMMENT"
	}
	definition := "modes:\n  - name: CW\n    filter_visible: true\n    default_filter_allowed: true\n    variants: [" + aliases + "]\n" +
		"  - name: FT8\n    filter_visible: true\n    default_filter_allowed: true\n"
	if kind == "invisible" {
		definition += "  - name: HIDDEN\n    filter_visible: false\n    variants: [COMMENT]\n"
	}
	definition += "events: []\n"
	path := filepath.Join(t.TempDir(), "taxonomy.yaml")
	if err := os.WriteFile(path, []byte(definition), 0600); err != nil {
		t.Fatal(err)
	}
	taxonomy, err := spot.LoadTaxonomyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	previous := spot.CurrentTaxonomy()
	spot.ConfigureTaxonomy(taxonomy)
	t.Cleanup(func() { spot.ConfigureTaxonomy(previous) })
}

func TestHistoryCommentModeAliasParser(t *testing.T) {
	loadHistoryCommentTaxonomy(t, "supported") // Immutable global snapshot; sequential.
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, prefix := range []string{"SHOW DX", "SH DX", "SHOW MYDX", "SH MYDX", "SHOW/DX", "SH/DX"} {
		for _, tc := range []struct {
			args, phrase string
			modes, bands []string
		}{
			{"MODE COMMENT", "", []string{"CW"}, nil},
			{"MODE CW, COMMENT", "", []string{"CW"}, nil},
			{"MODE ,COMMENT,,FT8,", "", []string{"CW", "FT8"}, nil},
			{"MODE COMMENT COMMENT POTA:  up 5!", "POTA:  up 5!", []string{"CW"}, nil},
			{"MODE CW,COMMENT COMMENT COMMENT  up:5!", "COMMENT  up:5!", []string{"CW"}, nil},
			{"MODE CW COMMENT POTA!", "POTA!", []string{"CW"}, nil},
			{"BAND 20, COMMENT POTA!", "POTA!", nil, []string{"20m"}},
			{"MODE BAND, COMMENT, MODE BAND 20 COMMENT a:  b!", "a:  b!", []string{"CW"}, []string{"20m"}},
			{"MODE COMMENT COMMENT " + strings.Repeat("x", 64), strings.Repeat("x", 64), []string{"CW"}, nil},
		} {
			line := prefix + " 1 " + tc.args
			command, handled, text := p.ParseHistoryCommand(line, "cc")
			if !handled || text != "" || command.Query.comment != tc.phrase || command.Query.count != 1 ||
				!reflect.DeepEqual(command.Query.modes, tc.modes) || !reflect.DeepEqual(command.Query.bands, tc.bands) {
				t.Fatalf("%q: query=%+v handled=%t error=%q", line, command.Query, handled, text)
			}
		}
	}
	for _, args := range []string{
		"MODE CW, COMMENT FT8", "MODE COMMENT COMMENT", "MODE COMMENT COMMENT   ",
		"MODE COMMENT MODE FT8", "MODE CW,COMMENT FT8", "MODE CW,COMMENT COMMENT " + strings.Repeat("x", 65),
		"MODE COMMENT COMMENT POTA\t", "MODE COMMENT COMMENT \tPOTA", "MODE COMMENT COMMENT POTA\u00a0",
	} {
		command, handled, text := p.ParseHistoryCommand("SHOW DX "+args, "go")
		if !handled || !strings.HasPrefix(text, "Invalid") || !reflect.DeepEqual(command.Query, HistoryQuery{}) {
			t.Fatalf("invalid request retained partial query: %q %+v %t %q", args, command.Query, handled, text)
		}
	}
	if _, handled, text := p.ParseHistoryCommand("SH/DX MODE COMMENT", "go"); !handled || text != "Use SHOW DX or SH DX for DX history.\n" {
		t.Fatalf("slash dialect restriction changed: %t %q", handled, text)
	}
}

func TestHistoryCommentUnsupportedAliasIsClause(t *testing.T) {
	for _, kind := range []string{"absent", "invisible"} {
		t.Run(kind, func(t *testing.T) {
			loadHistoryCommentTaxonomy(t, kind)
			p := NewProcessor(nil, nil, nil, nil, nil, nil)
			for _, args := range []string{"MODE CW, COMMENT POTA!", "BAND 20, COMMENT POTA!"} {
				command, handled, text := p.ParseHistoryCommand("SHOW DX "+args, "go")
				if !handled || text != "" || command.Query.comment != "POTA!" || !HistoryCommentClauseStarted([]byte(args+" ")) {
					t.Fatalf("unsupported alias consumed marker: %q %+v %t %q", args, command.Query, handled, text)
				}
			}
			for _, args := range []string{"MODE COMMENT POTA!", "MODE CW,COMMENT", "MODE ,COMMENT,"} {
				command, handled, text := p.ParseHistoryCommand("SHOW DX "+args, "go")
				if !handled || !strings.HasPrefix(text, "Invalid") || !reflect.DeepEqual(command.Query, HistoryQuery{}) {
					t.Fatalf("unsupported/empty selection accepted: %q %+v %t %q", args, command.Query, handled, text)
				}
			}
		})
	}
}

func TestHistoryCommentAliasGenericMatchingBeforeCount(t *testing.T) {
	loadHistoryCommentTaxonomy(t, "supported")
	wanted := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	wanted.Comment = "POTA:  UP 5!"
	wrongMode := spot.NewSpot("K1ABC", "W1AAA", 14074, "FT8")
	wrongMode.Comment = wanted.Comment
	wrongSpaces := spot.NewSpot("K1ABC", "W1AAA", 14031, "CW")
	wrongSpaces.Comment = "POTA: UP 5!"
	p := NewProcessor(nil, &fakeArchive{spots: []*spot.Spot{wrongMode, wrongSpaces, wanted}}, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		line := "SHOW DX 1 MODE COMMENT COMMENT pOtA:  up 5!"
		command, handled, text := p.ParseHistoryCommand(line, dialect)
		if !handled || text != "" || command.Query.comment != "pOtA:  up 5!" {
			t.Fatalf("raw phrase changed: %+v %t %q", command.Query, handled, text)
		}
		page, err := p.ReadHistoryPage(command.Query, nil, func(*spot.Spot) bool { return true }, time.Now(), nil)
		if err != nil || len(page.Spots) != 1 || page.Spots[0] != wanted {
			t.Fatalf("mode/spacing decoys consumed count-one search: %+v %v", page, err)
		}
		response := p.ProcessCommandForClient(line, "W1AAA", "", func(*spot.Spot) bool { return true }, dialect)
		if !strings.Contains(response, wanted.FormatDXCluster()) || strings.Contains(response, wrongMode.FormatDXCluster()) || strings.Contains(response, wrongSpaces.FormatDXCluster()) {
			t.Fatalf("generic path lost alias/phrase/count: %q", response)
		}
		if response := p.ProcessCommandForClient(line, "W1AAA", "", nil, dialect); response != noLoggedUserMsg {
			t.Fatalf("nil-filter guard changed: %q", response)
		}
	}
}

func TestHistoryCommentAliasBorrowedPrefixAllocations(t *testing.T) {
	loadHistoryCommentTaxonomy(t, "supported")
	for _, tc := range []struct {
		args string
		want bool
	}{
		{"MODE COMMENT ", false}, {"MODE CW, COMMENT ", false},
		{"MODE CW,COMMENT COMMENT ", true}, {"MODE COMMENT COMMENT ", true},
		{"MODE COMMENT COMMENT", false}, {"MODE COMMENT BAND 20 COMMENT ", true},
		{"NEXT COMMENT ", false}, {"MODE NEXT COMMENT ", false},
	} {
		args := []byte(tc.args)
		if got := HistoryCommentClauseStarted(args); got != tc.want {
			t.Fatalf("prefix %q started=%t want=%t", tc.args, got, tc.want)
		}
		if count := testing.AllocsPerRun(100, func() { HistoryCommentClauseStarted(args) }); count != 0 {
			t.Fatalf("borrowed per-byte prefix allocated: %q %v", tc.args, count)
		}
	}
}

func FuzzHistoryCommentAliasBoundary(f *testing.F) {
	loadHistoryCommentTaxonomy(f, "supported")
	for _, phrase := range []string{"POTA:  up 5!", "COMMENT", "", "  ", "POTA\t", "PÖTA", strings.Repeat("x", 64), strings.Repeat("x", 65)} {
		f.Add(false, true, phrase)
		f.Add(true, false, phrase)
	}
	f.Fuzz(func(t *testing.T, afterComma, literal bool, phrase string) {
		if len(phrase) > 128 {
			return
		}
		args := "MODE COMMENT"
		if afterComma {
			args = "MODE CW, COMMENT"
		}
		if HistoryCommentClauseStarted([]byte(args + " ")) {
			t.Fatal("alias value prematurely opened literal punctuation")
		}
		wantPhrase, valid := "", true
		if literal {
			args += " COMMENT " + phrase
			wantPhrase = strings.Trim(phrase, " ")
			valid = len(wantPhrase) >= 1 && len(wantPhrase) <= 64
			for i := 0; i < len(wantPhrase); i++ {
				valid = valid && wantPhrase[i] >= 32 && wantPhrase[i] <= 126
			}
			if !HistoryCommentClauseStarted([]byte(args + " ")) {
				t.Fatal("actual standalone marker did not open literal punctuation")
			}
		}
		p := NewProcessor(nil, nil, nil, nil, nil, nil)
		command, handled, text := p.ParseHistoryCommand("SHOW DX 1 "+args, "go")
		if valid {
			if !handled || text != "" || command.Query.count != 1 || command.Query.comment != wantPhrase || !reflect.DeepEqual(command.Query.modes, []string{"CW"}) {
				t.Fatalf("literal alias oracle disagrees: args=%q query=%+v handled=%t error=%q", args, command.Query, handled, text)
			}
		} else if !handled || !strings.HasPrefix(text, "Invalid COMMENT phrase") || !reflect.DeepEqual(command.Query, HistoryQuery{}) {
			t.Fatalf("invalid phrase retained partial query: args=%q query=%+v handled=%t error=%q", args, command.Query, handled, text)
		}
	})
}
