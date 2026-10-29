package telnet

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/filter"
)

func TestCCFilterHelpSurvivingSwitchAcceptance(t *testing.T) {
	x := newHelpOwnerSession(t, DialectCC)
	c, engine := x.client, x.server.filterEngine
	original, pointer, settings, baseline := c.filter.PreparedRules(), c.filter, c.configuredSettings, c.presetReference.Baseline
	for _, domain := range []string{"BEACON", "WWV", "WCY", "ANNOUNCE", "PC93", "SELF", "TOXIC"} {
		for _, verb := range []string{"UNSET/FILTER", "SET/FILTER"} {
			writes := x.writes
			response, handled := engine.Handle(c, verb+" "+domain)
			if !handled || x.writes != writes+1 || strings.Contains(response, "No changes applied.") ||
				strings.Contains(response, "warning") || c.filter != pointer || c.filter.PreparedRules() != original ||
				c.configuredSettings != settings || c.presetReference.Baseline != baseline {
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
			x.assertSaved(t)
		}
	}
}

func TestCCFilterHelpRetiredFormsHaveNoEffects(t *testing.T) {
	x := newHelpOwnerSession(t, DialectCC)
	c := x.client
	for _, line := range []string{"SET/FILTER NEARBY ON", "UNSET/FILTER NEARBY OFF", "SET/FILTER BAND 20", "UNSET/FILTER MODE CW", "SET/FILTER DXBM 20 CW",
		"SET/NOFILTER", "SET/FILTER NOFILTER", "PASS COMMENT CQ", "PASS MINSNR CW 10"} {
		before, writes, revision := c.captureV6Configuration(), x.writes, c.configurationRevision
		path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
		disk := presetDiskBytes(t, path)
		response, handled := x.server.filterEngine.Handle(c, line)
		if !handled || !strings.Contains(response, "HELP RULE") || !strings.Contains(response, "No changes applied.") ||
			!before.Equal(c.captureV6Configuration()) || x.writes != writes || c.configurationRevision != revision ||
			!bytes.Equal(disk, presetDiskBytes(t, path)) {
			t.Fatalf("retired CC form executed, saved or lost recovery guidance: %q => %q", line, response)
		}
	}
}
