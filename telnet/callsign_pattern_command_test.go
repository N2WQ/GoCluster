package telnet

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"dxcluster/filter"
	"dxcluster/spot"
)

func TestCallsignPatternHumanAdmission(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, domain := range []string{"DXCALL", "DECALL"} {
			for _, block := range []bool{false, true} {
				verb := "PASS"
				if block {
					verb = "REJECT"
				}
				if dialect == DialectCC {
					verb = "SET/FILTER"
					if block {
						verb = "UNSET/FILTER"
					}
				}
				t.Run(string(dialect)+"/"+verb+"/"+domain, func(t *testing.T) {
					s := presetTestServer(t)
					engine := newFilterCommandEngine()
					c := configurationTestClient(s, "W1ABC-1")
					c.dialect = dialect
					command := verb + " " + domain + " "
					response, handled := engine.Handle(c, command+"k1*,*abc,ea8/w1xyz,w2abc-1,ALL")
					if !handled || strings.Contains(response, "Invalid") || strings.Contains(response, "warning") {
						t.Fatalf("supported list failed: %q", response)
					}
					want := []string{"K1*", "*ABC", "EA8/W1XYZ", "W2ABC-1", "ALL"}
					assertCallsignRules(t, s, c, domain, block, want)
					for _, invalid := range []string{"W*ABC", "*ABC*", "W1**", "W1?", "W1.ABC", "W1_ABC", "W1[ABC]", "W1é", "W1%v"} {
						// A valid token on either side must not leak into publication.
						for _, value := range []string{"N0XYZ," + invalid, invalid + " N0XYZ"} {
							assertCallsignRefusalUnchanged(t, s, c, engine, command+value)
						}
					}
					// A standalone star keeps list semantics; ALL remains a distinct
					// reset/block-all operation. In-list ALL above is literal.
					if response, _ := engine.Handle(c, command+"*"); strings.Contains(response, "Invalid") {
						t.Fatal(response)
					}
					assertCallsignRules(t, s, c, domain, block, append(want, "*"))
					if response, _ := engine.Handle(c, command+"ALL"); !strings.Contains(response, "All ") {
						t.Fatalf("ALL operation changed: %q", response)
					}
					want = nil
					if block {
						want = []string{"*"}
					}
					assertCallsignRules(t, s, c, domain, block, want)
				})
			}
		}
	}
}

func assertCallsignRules(t *testing.T, s *Server, c *Client, domain string, block bool, want []string) {
	t.Helper()
	record, err := filter.LoadUserRecord(c.callsign)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []*filter.Filter{c.filter, &record.Filter} {
		patterns := f.DXCallsigns
		if domain == "DECALL" {
			patterns = f.DECallsigns
		}
		if block {
			patterns = f.BlockDXCallsigns
			if domain == "DECALL" {
				patterns = f.BlockDECallsigns
			}
		}
		if !slices.Equal(patterns, want) {
			t.Fatalf("live/saved patterns disagree: got %v, want %v", patterns, want)
		}
	}
	if err := s.validateMachineConfiguration(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)); err != nil {
		t.Fatalf("human configuration is machine-invalid: %v", err)
	}
}

func assertCallsignRefusalUnchanged(t *testing.T, s *Server, c *Client, e *filterCommandEngine, command string) {
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
	if !handled || response != invalidCallsignPatternMsg || attempts != 0 || err != nil || !bytes.Equal(saved, after) ||
		!before.Equal(filter.ConfigurationFromFilter(c.filter, c.configuredSettings)) || c.configurationRevision != revision {
		t.Fatalf("invalid pattern changed live/revision/disk or lacked refusal: %q, commits=%d, err=%v", response, attempts, err)
	}
}

func TestCallsignPatternMatcherPreservation(t *testing.T) {
	for _, domain := range []string{"DXCALL", "DECALL"} {
		for _, tc := range []struct {
			pattern, matching, other string
		}{
			{"k1abc", "K1ABC", "K1ABCD"},
			{"k1*", "K1ABC", "W1ABC"},
			{"*abc", "W1ABC", "W1XYZ"},
			{"ea8/k1abc", "EA8/K1ABC", "K1ABC"},
		} {
			t.Run(domain+"/"+tc.pattern, func(t *testing.T) {
				s := presetTestServer(t)
				c := configurationTestClient(s, "W1ABC-1")
				e := newFilterCommandEngine()
				if response, _ := e.Handle(c, "PASS "+domain+" "+tc.pattern); strings.Contains(response, "Invalid") {
					t.Fatal(response)
				}
				matches := func(call string) bool {
					report := spot.NewSpot(call, call, 14074, "CW")
					if domain == "DECALL" {
						// Actual DE matching uses the baseline identity of an SSID.
						report = spot.NewSpot("N0XYZ", call+"-7", 14074, "CW")
						report.DECallStripped = spot.NormalizeOwnCallsign(report.DECall)
						if report.DECallStripped != call {
							t.Fatalf("baseline DE fixture differs from expected identity: %q", report.DECallStripped)
						}
					}
					return c.filter.Matches(report)
				}
				if !matches(tc.matching) || matches(tc.other) {
					t.Fatal("supported pattern does not preserve exact/prefix/suffix matching")
				}
				// A different, overlapping REJECT remains effective after PASS *.
				e.Handle(c, "REJECT "+domain+" "+tc.matching)
				e.Handle(c, "PASS "+domain+" *")
				if matches(tc.matching) || !matches(tc.other) {
					t.Fatal("star lost block precedence or unrestricted matching")
				}
				e.Handle(c, "PASS "+domain+" ALL")
				if !matches(tc.matching) || !matches(tc.other) {
					t.Fatal("ALL did not clear both pattern lists")
				}
				e.Handle(c, "REJECT "+domain+" ALL")
				if matches(tc.matching) || matches(tc.other) {
					t.Fatal("REJECT ALL did not block every call")
				}
				e.Handle(c, "PASS "+domain+" *")
				if !matches(tc.matching) || !matches(tc.other) {
					t.Fatal("PASS star did not remove the identical block star")
				}
			})
		}
	}
}

func FuzzHumanCallsignPatternAdmission(f *testing.F) {
	for _, seed := range []string{"K1ABC", "w1*,*abc", "*", "ALL", "ALL,K1ABC", "N0XYZ,W*ABC", "*ABC*,N0XYZ", "W1?", "W1é", "", ",,"} {
		f.Add(seed)
	}
	// This literal grammar is independent of the production predicate.
	grammar := regexp.MustCompile(`^(?:[A-Za-z0-9/-]+|\*|\*[A-Za-z0-9/-]+|[A-Za-z0-9/-]+\*)$`)
	e := newFilterCommandEngine()
	f.Fuzz(func(t *testing.T, value string) {
		if len(value) > 256 {
			return
		}
		tokens := strings.Fields(strings.ReplaceAll(value, ",", " "))
		valid := len(tokens) > 0
		for _, token := range tokens {
			valid = valid && grammar.MatchString(token)
		}
		for _, domain := range []string{"DXCALL", "DECALL"} {
			for _, verb := range []string{"PASS", "REJECT"} {
				c := newTestClient()
				c.filter.AddDXCallsignPattern("W1*")
				c.filter.AddBlockDECallsignPattern("N0XYZ")
				before := filter.ConfigurationFromFilter(c.filter, c.configuredSettings).Clone()
				outcome := e.stageOwned(c, verb+" "+domain+" "+value)
				after := filter.ConfigurationFromFilter(c.filter, c.configuredSettings)
				if !outcome.handled || outcome.publish != valid {
					t.Fatalf("admission disagrees with literal grammar: %q -> %+v", value, outcome)
				}
				if !valid && !before.Equal(after) {
					t.Fatal("refusal partially changed the staged pattern lists")
				}
				if valid {
					if err := (&Server{}).validateMachineConfiguration(after); err != nil {
						t.Fatalf("published patterns are machine-invalid: %v", err)
					}
				}
			}
		}
	})
}
