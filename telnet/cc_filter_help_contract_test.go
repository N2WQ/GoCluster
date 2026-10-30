package telnet

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/filter"
)

func TestCCFilterHelpSurvivingSwitchAcceptance(t *testing.T) {
	s, c := v7IndependentClient(t)
	engine := s.filterEngine
	original, err := c.captureV7Configuration()
	if err != nil {
		t.Fatal(err)
	}
	pointer, settings, references := c.filter, c.configuredSettings, c.v7PresetReferences
	for _, domain := range []string{"BEACON", "WWV", "WCY", "ANNOUNCE", "PC93", "SELF", "TOXIC"} {
		for _, verb := range []string{"UNSET/FILTER", "SET/FILTER"} {
			response, handled := engine.Handle(c, verb+" "+domain)
			if !handled || strings.Contains(response, "No changes applied.") ||
				strings.Contains(response, "warning") || c.filter != pointer ||
				c.configuredSettings != settings || c.v7PresetReferences != references {
				t.Fatalf("surviving CC switch changed rules/settings/baseline: %s %s => %q", verb, domain, response)
			}
			enabled := verb == "SET/FILTER"
			got := map[string]bool{
				"BEACON": c.filter.BeaconsEnabled(), "WWV": c.filter.WWVEnabled(), "WCY": c.filter.WCYEnabled(),
				"ANNOUNCE": c.filter.AnnounceEnabled(), "PC93": c.filter.AnnounceEnabled(),
				"SELF": c.filter.SelfEnabled(), "TOXIC": c.filter.ToxicEnabled(),
			}[domain]
			if got != enabled {
				t.Fatalf("CC switch reversed operation: %s %s", verb, domain)
			}
			live, err := c.captureV7Configuration()
			if err != nil {
				t.Fatal(err)
			}
			record, err := filter.LoadV7UserRecord(c.callsign)
			if err != nil || !record.Configuration.Equal(live) || !(filter.V6Configuration{Filters: original.Filters.GO}).Equal(filter.V6Configuration{Filters: live.Filters.GO}) {
				t.Fatalf("native switch acknowledgement lost durable/wrong-bank state: %v", err)
			}
		}
	}
}

func TestCCFilterHelpRetiredFormsHaveNoEffects(t *testing.T) {
	s, c := v7IndependentClient(t)
	if response, handled := s.filterEngine.Handle(c, "SET/FILTER BEACON/OFF"); !handled || strings.Contains(response, "No changes") {
		t.Fatal("native refusal fixture was not durably initialized")
	}
	for _, line := range []string{"SET/FILTER NEARBY ON", "UNSET/FILTER NEARBY OFF", "SET/FILTER BAND 20", "UNSET/FILTER MODE CW", "SET/FILTER DXBM 20 CW",
		"SET/FILTER NOFILTER", "PASS COMMENT CQ", "PASS MINSNR CW 10"} {
		before, err := c.captureV7Configuration()
		if err != nil {
			t.Fatal(err)
		}
		revision := c.configurationRevision
		path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
		disk := presetDiskBytes(t, path)
		response, handled := s.filterEngine.Handle(c, line)
		after, err := c.captureV7Configuration()
		if err != nil {
			t.Fatal(err)
		}
		if !handled || strings.Contains(response, "HELP RULE") || !strings.Contains(response, "No changes applied.") ||
			!before.Equal(after) || c.configurationRevision != revision ||
			!bytes.Equal(disk, presetDiskBytes(t, path)) {
			t.Fatalf("retired CC form executed, saved or lost recovery guidance: %q => %q", line, response)
		}
	}
}
