package telnet

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/spot"
)

func TestFilterSwitchOriginalArguments(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			s := presetTestServer(t)
			c := configurationTestClient(s, "W1ABC-1")
			c.dialect = dialect
			e := newFilterCommandEngine()
			for _, domain := range []string{"BEACON", "WWV", "WCY", "ANNOUNCE", "PC93", "SELF", "TOXIC"} {
				for _, verb := range []string{"PASS", "REJECT", "SET/FILTER", "UNSET/FILTER"} {
					if dialect == DialectGo && strings.Contains(verb, "/") {
						continue
					}
					command := verb + " " + domain
					// Exercise both initial states: an already-enabled switch must
					// not mask an unintended mutation in the rejected command.
					for _, initial := range []string{"PASS", "REJECT"} {
						requireFilterArgumentControl(t, e, c, initial+" "+domain)
						for _, tail := range []string{"ON", "OFF", "BAD", "ALL OFF", "ALL ALL", "NONE"} {
							assertFilterArgumentRefusal(t, s, c, e, command+" "+tail)
						}
					}
					requireFilterArgumentControl(t, e, c, command+" all")
				}
			}
			for _, command := range []string{"PASS NOFILTER", "SET/FILTER NOFILTER"} {
				if dialect == DialectGo && strings.Contains(command, "/") {
					continue
				}
				requireFilterArgumentControl(t, e, c, "REJECT BAND ALL")
				for _, tail := range []string{"ON", "OFF", "ALL ALL", "BAND"} {
					assertFilterArgumentRefusal(t, s, c, e, command+" "+tail)
				}
				requireFilterArgumentControl(t, e, c, command+" ALL")
				if !c.filter.AllBands || c.filter.BlockAllBands {
					t.Fatal("compatible NOFILTER ALL no longer resets the category")
				}
			}
		})
	}
}

func TestCCShortcutOriginalArguments(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.dialect = DialectCC
	e := newFilterCommandEngine()
	requireFilterArgumentControl(t, e, c, "PASS NOFILTER")
	commands := []string{"SET/ANN", "SET/NOANN", "SET/BEACON", "SET/NOBEACON",
		"SET/WWV", "SET/NOWWV", "SET/WCY", "SET/NOWCY", "SET/SELF", "SET/NOSELF",
		"SET/SKIMMER", "SET/NOSKIMMER", "SET/NOFILTER"}
	for _, mode := range spot.CCShortcutModes() {
		commands = append(commands, "SET/"+mode, "SET/NO"+mode)
	}
	for _, command := range commands {
		for _, tail := range []string{"ON", "OFF", "ALL", "FT8 CW"} {
			assertFilterArgumentRefusal(t, s, c, e, command+" "+tail)
		}
		requireFilterArgumentControl(t, e, c, command)
	}
	// Literal consumer controls prevent arity tests from accepting a parser
	// that simply refuses every shortcut or silently reverses its operation.
	requireFilterArgumentControl(t, e, c, "PASS NOFILTER")
	requireFilterArgumentControl(t, e, c, "SET/NOFT8")
	if c.filter.Matches(spot.NewSpot("W6ABC", "K1ABC", 7074, "FT8")) ||
		!c.filter.Matches(spot.NewSpot("W6ABC", "K1ABC", 14074, "CW")) {
		t.Fatal("mode shortcut changed the wrong visibility")
	}
	requireFilterArgumentControl(t, e, c, "SET/FT8")
	if !c.filter.Matches(spot.NewSpot("W6ABC", "K1ABC", 7074, "FT8")) {
		t.Fatal("valid mode shortcut did not restore visibility")
	}
	requireFilterArgumentControl(t, e, c, "SET/NOBEACON")
	if c.filter.BeaconsEnabled() {
		t.Fatal("valid beacon shortcut did not disable beacons")
	}
	requireFilterArgumentControl(t, e, c, "SET/BEACON")
	if !c.filter.BeaconsEnabled() {
		t.Fatal("valid beacon shortcut did not enable beacons")
	}
}

func TestCCInlineFlagOriginalArguments(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.dialect = DialectCC
	e := newFilterCommandEngine()
	requireFilterArgumentControl(t, e, c, "PASS NOFILTER")
	for _, domain := range []string{"BAND", "MODE", "SOURCE", "EVENT", "DXCALL", "DECALL",
		"BEACON", "WWV", "WCY", "ANNOUNCE", "PC93", "SELF", "TOXIC", "NOFILTER"} {
		for _, flag := range []string{"ON", "OFF"} {
			command := "SET/FILTER " + domain + "/" + flag
			for _, tail := range []string{"ON", "OFF", "ALL", "20 CW"} {
				assertFilterArgumentRefusal(t, s, c, e, command+" "+tail)
			}
			if domain == "NOFILTER" && flag == "OFF" {
				assertFilterArgumentRefusalFeedback(t, s, c, e, command, invalidFilterCommandMsg)
				continue // Existing unsupported operation; do not invent semantics.
			}
			requireFilterArgumentControl(t, e, c, command)
		}
	}
	requireFilterArgumentControl(t, e, c, "PASS NOFILTER")
	requireFilterArgumentControl(t, e, c, "SET/FILTER BAND/OFF")
	if !c.filter.BlockAllBands || c.filter.AllBands {
		t.Fatal("argument-free /OFF lost its block-all operation")
	}
	requireFilterArgumentControl(t, e, c, "SET/FILTER BAND/ON")
	if !c.filter.AllBands || c.filter.BlockAllBands {
		t.Fatal("argument-free /ON lost its allow-all operation")
	}
}

func requireFilterArgumentControl(t *testing.T, e *filterCommandEngine, c *Client, command string) {
	t.Helper()
	response, handled := e.Handle(c, command)
	if !handled || strings.Contains(response, "Usage:") || strings.Contains(response, "No changes applied") ||
		strings.Contains(response, "Invalid") || strings.Contains(response, "warning") {
		t.Fatalf("supported control refused: %q -> %q", command, response)
	}
}

func assertFilterArgumentRefusal(t *testing.T, s *Server, c *Client, e *filterCommandEngine, command string) {
	t.Helper()
	assertFilterArgumentRefusalFeedback(t, s, c, e, command, "")
}

func assertFilterArgumentRefusalFeedback(t *testing.T, s *Server, c *Client, e *filterCommandEngine, command, exactFeedback string) {
	t.Helper()
	before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
	revision := c.configurationRevision
	file := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
	saved, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	attempts := 0
	prior := s.commitConfigurationFn
	s.commitConfigurationFn = func(*filter.PreparedUserRecord) error { attempts++; return nil }
	defer func() { s.commitConfigurationFn = prior }()
	response, handled := e.Handle(c, command)
	after, err := os.ReadFile(file)
	feedbackMatches := strings.Contains(response, "No changes applied.")
	if exactFeedback != "" {
		feedbackMatches = response == exactFeedback
	}
	if !handled || !feedbackMatches || attempts != 0 || err != nil || !bytes.Equal(saved, after) ||
		!before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) || c.configurationRevision != revision {
		t.Fatalf("original argument lost or refused command had effects: %q -> %q, commits=%d, err=%v", command, response, attempts, err)
	}
}

func TestMalformedCCShortcutKeepsGoDialectHint(t *testing.T) {
	c := newTestClient()
	c.dialect = DialectGo
	before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
	outcome := newFilterCommandEngine().stageOwned(c, "SET/NOFT8 ON")
	if !outcome.handled || outcome.publish || !strings.Contains(outcome.response, "Command matches the cc dialect.") ||
		!before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) {
		t.Fatalf("malformed shortcut lost the existing GO hint or mutated: %+v", outcome)
	}
}

func FuzzCCShortcutArgumentRefusal(f *testing.F) {
	for _, tail := range []string{"ON", "OFF", "ALL", "20 CW", "BAD", "\tALL", ""} {
		f.Add(tail)
	}
	e := newFilterCommandEngine()
	f.Fuzz(func(t *testing.T, tail string) {
		if len(tail) > 256 || len(strings.Fields(tail)) == 0 {
			return
		}
		for _, command := range []string{"SET/BEACON", "SET/NOBEACON", "SET/NOFILTER",
			"SET/FT8", "SET/NOFT8", "SET/FILTER BAND/ON", "SET/FILTER BAND/OFF"} {
			c := newTestClient()
			c.dialect = DialectCC
			before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
			outcome := e.stageOwned(c, command+" "+tail)
			if !outcome.handled || outcome.publish || !strings.Contains(outcome.response, "No changes applied.") ||
				!before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) {
				t.Fatalf("shortcut/flag tail produced effects: %q -> %+v", tail, outcome)
			}
		}
	})
}
