package telnet

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/filter"
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
		})
	}
}

func TestCCShortcutOriginalArguments(t *testing.T) {
	s := presetTestServer(t)
	c := configurationTestClient(s, "W1ABC-1")
	c.dialect = DialectCC
	e := newFilterCommandEngine()
	requireFilterArgumentControl(t, e, c, "RESET FILTER")
	commands := []string{"SET/ANN", "SET/NOANN", "SET/BEACON", "SET/NOBEACON",
		"SET/WWV", "SET/NOWWV", "SET/WCY", "SET/NOWCY", "SET/SELF", "SET/NOSELF",
	}
	for _, command := range commands {
		for _, tail := range []string{"ON", "OFF", "ALL", "FT8 CW"} {
			assertFilterArgumentRefusal(t, s, c, e, command+" "+tail)
		}
		requireFilterArgumentControl(t, e, c, command)
	}
	// Literal consumer controls prevent arity tests from accepting a parser
	// that simply refuses every shortcut or silently reverses its operation.
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
	requireFilterArgumentControl(t, e, c, "RESET FILTER")
	for _, domain := range []string{"BEACON", "WWV", "WCY", "ANNOUNCE", "PC93", "SELF", "TOXIC"} {
		for _, flag := range []string{"ON", "OFF"} {
			command := "SET/FILTER " + domain + "/" + flag
			for _, tail := range []string{"ON", "OFF", "ALL", "20 CW"} {
				assertFilterArgumentRefusal(t, s, c, e, command+" "+tail)
			}
			requireFilterArgumentControl(t, e, c, command)
		}
	}
	requireFilterArgumentControl(t, e, c, "SET/FILTER BEACON/OFF")
	if c.filter.BeaconsEnabled() {
		t.Fatal("argument-free /OFF failed to disable beacon control")
	}
	requireFilterArgumentControl(t, e, c, "SET/FILTER BEACON/ON")
	if !c.filter.BeaconsEnabled() {
		t.Fatal("argument-free /ON failed to enable beacon control")
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
	before := c.captureV6Configuration()
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
		!before.Equal(c.captureV6Configuration()) || c.configurationRevision != revision {
		t.Fatalf("original argument lost or refused command had effects: %q -> %q, commits=%d, err=%v", command, response, attempts, err)
	}
}

func TestRetiredCCShortcutProvidesRuleGuidanceInGoDialect(t *testing.T) {
	c := newTestClient()
	c.dialect = DialectGo
	before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
	outcome := newFilterCommandEngine().stageOwned(c, "SET/NOFT8 ON")
	if !outcome.handled || outcome.publish || !strings.Contains(outcome.response, "HELP RULE") ||
		!before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) {
		t.Fatalf("retired shortcut lost canonical guidance or mutated: %+v", outcome)
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
