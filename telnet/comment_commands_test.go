package telnet

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/spot"
)

func TestCommentCommandGrammarPersistenceAndMatching(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			s := presetTestServer(t)
			c := configurationTestClient(s, "W1ABC-1")
			c.dialect = dialect
			e := newFilterCommandEngine()
			for _, line := range []string{"PASS NOFILTER", "PASS BAND 20,40", "PASS COMMENT POTA", "PASS COMMENT  up  5: please!  ", "PASS COMMENT pota", "REJECT COMMENT QRT"} {
				resp, ok := e.Handle(c, line)
				if !ok || strings.Contains(resp, "Invalid") || strings.Contains(resp, "Usage:") {
					t.Fatalf("%q: %q", line, resp)
				}
			}
			if !reflect.DeepEqual(c.filter.Comments, []string{"POTA", "up  5: please!"}) {
				t.Fatalf("not literal/idempotent: %q", c.filter.Comments)
			}
			for _, tc := range []struct {
				comment string
				freq    float64
				want    bool
			}{
				{"POTA-1234", 14025, true}, {"up  5: PLEASE!", 7025, true},
				{"POTA-1234 QRT", 14025, false}, {"up 5: please!", 14025, false},
				{"POTA-1234", 21025, false}, {"", 14025, false},
			} {
				sp := spot.NewSpot("K1ABC", "N2ABC", tc.freq, "CW")
				sp.Comment = tc.comment
				if got := c.filter.Matches(sp); got != tc.want {
					t.Fatalf("comment=%q freq=%v got=%v want=%v", tc.comment, tc.freq, got, tc.want)
				}
			}
			e.Handle(c, "REJECT COMMENT pOtA")
			if len(c.filter.Comments) != 1 || !reflect.DeepEqual(c.filter.BlockComments, []string{"QRT", "pOtA"}) {
				t.Fatal("opposite action did not move phrase")
			}
			e.Handle(c, "REMOVE REJECT COMMENT POTA")
			e.Handle(c, "RESET FILTER COMMENT PASS")
			stored, err := filter.LoadUserRecord(c.callsign)
			if err != nil || len(stored.Comments) != 0 || !reflect.DeepEqual(stored.BlockComments, []string{"QRT"}) {
				t.Fatalf("persisted rules: %+v %v", stored, err)
			}
			for _, reset := range []string{"RESET FILTER COMMENT", "RESET FILTER", "PASS NOFILTER"} {
				e.Handle(c, "PASS COMMENT ALL")
				e.Handle(c, "REJECT COMMENT NONE")
				e.Handle(c, reset)
				if len(c.filter.Comments)+len(c.filter.BlockComments) != 0 {
					t.Fatalf("%s retained comments", reset)
				}
			}
		})
	}
}

func TestCommentCommandLimitsAtomicityAndChurn(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	e := newFilterCommandEngine()
	e.Handle(c, "REJECT COMMENT moving")
	for i := range filter.MaxCommentPhrases {
		e.Handle(c, fmt.Sprintf("PASS COMMENT phrase%02d", i))
	}
	before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
	path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	disk, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	c.refreshConfigurationRevision()
	revision := c.configurationRevision
	for _, line := range []string{"PASS COMMENT moving", "PASS COMMENT ", "PASS COMMENT " + strings.Repeat("a", 65), "REJECT COMMENT café", "REMOVE PASS COMMENT", "RESET FILTER COMMENT BAD", "PASS COMMENT a\tword"} {
		resp, ok := e.Handle(c, line)
		if !ok || (!strings.Contains(resp, "Invalid") && !strings.Contains(resp, "Usage:")) {
			t.Fatalf("accepted %q: %q", line, resp)
		}
		got := filter.ConfigurationFromFilter(c.filter, c.configuredSettings)
		after, err := os.ReadFile(path)
		if err != nil || !got.Equal(before) || !bytes.Equal(after, disk) || c.configurationRevision != revision {
			t.Fatalf("failed operation changed configuration: %q %v", line, err)
		}
	}
	e.Handle(c, "RESET FILTER COMMENT")
	e.Handle(c, "PASS COMMENT "+strings.Repeat("a", 64))
	if len(c.filter.Comments) != 1 {
		t.Fatal("64-byte phrase rejected")
	}
	e.Handle(c, "RESET FILTER COMMENT")
	// Exercise unique historical input without retaining it through a backing slice.
	for i := range 200 {
		phrase := fmt.Sprintf("unique%d", i)
		if _, err := applyCommentFilterCommand(c.filter, commentFilterCommand{list: "PASS", phrase: phrase}); err != nil {
			t.Fatal(err)
		}
		applyCommentFilterCommand(c.filter, commentFilterCommand{list: "PASS", phrase: phrase, remove: true})
		if len(c.filter.Comments) != 0 || cap(c.filter.Comments) > 1 {
			t.Fatal("historical entries retained")
		}
	}
}

func TestCommentCommandExactYAMLOverlapRemoval(t *testing.T) {
	f := filter.NewFilter()
	f.Comments, f.BlockComments = []string{"POTA", "pota", "other"}, []string{"Pota", "POTA"}
	changed, err := applyCommentFilterCommand(f, commentFilterCommand{list: "PASS", phrase: "pota"})
	if err != nil || !changed || !reflect.DeepEqual(f.Comments, []string{"POTA", "pota", "other"}) || len(f.BlockComments) != 0 {
		t.Fatal("move did not clear every opposite equivalent")
	}
	applyCommentFilterCommand(f, commentFilterCommand{list: "PASS", phrase: "pota", remove: true})
	if !reflect.DeepEqual(f.Comments, []string{"other"}) {
		t.Fatal("remove retained equivalent duplicate")
	}
}

func FuzzCommentFilterCommand(f *testing.F) {
	for _, seed := range []string{"PASS COMMENT POTA", "REMOVE REJECT COMMENT a: b!", "RESET FILTER COMMENT PASS", "REJECT COMMENT *?,x", "PASS COMMENT a  b", "PASS BAND 20"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, line string) {
		if len(line) > 128 {
			return
		}
		cmd, handled, valid := parseCommentFilterCommand(line)
		if !handled || !valid {
			return
		}
		if !cmd.reset && !filter.ValidCommentPhrase(cmd.phrase) {
			t.Fatal("accepted invalid phrase")
		}
		cfg := filter.NewFilter()
		if _, err := applyCommentFilterCommand(cfg, cmd); err != nil {
			t.Fatal(err)
		}
		if err := filter.ConfigurationFromFilter(cfg, filter.SettingsConfiguration{}).ValidateCommentRules(); err != nil {
			t.Fatal(err)
		}
	})
}
