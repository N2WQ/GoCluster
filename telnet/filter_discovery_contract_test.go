package telnet

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/filter"
)

func TestFilterDiscoveryBooleanOwnerFeedbackHasNoEffects(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		x := newHelpOwnerSession(t, dialect)
		c := x.client
		for _, line := range []string{"PASS", "REJECT", "PASS RULE", "REJECT RULE 10 MODE=CW", "PASS MODE CW", "REJECT BAND ALL"} {
			before, writes := c.captureV5Configuration(), x.writes
			revision, _ := c.configurationRevisionToken()
			path := filepath.Join(filter.UserDataDir, c.callsign+".yaml")
			disk := presetDiskBytes(t, path)
			response, handled := x.server.filterEngine.Handle(c, line)
			afterRevision, _ := c.configurationRevisionToken()
			if !handled || !strings.Contains(response, "HELP RULE") || !strings.Contains(response, "No changes applied.") ||
				len(response) > 1024 || x.writes != writes || before.Fingerprint() != c.captureV5Configuration().Fingerprint() ||
				afterRevision != revision || !bytes.Equal(disk, presetDiskBytes(t, path)) {
				t.Fatalf("discovery input had effects or lost bounded rule guidance: %q => %q", line, response)
			}
		}
	}
}

func TestFilterDiscoveryPC93AliasCommitsActualSwitch(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		x := newHelpOwnerSession(t, dialect)
		for _, action := range []string{"REJECT", "PASS"} {
			writes := x.writes
			response, handled := x.server.filterEngine.Handle(x.client, action+" PC93")
			if !handled || x.writes != writes+1 || strings.Contains(response, "No changes applied.") ||
				x.client.filter.AnnounceEnabled() != (action == "PASS") {
				t.Fatalf("PC93 did not commit independent ANNOUNCE switch: %q", response)
			}
			x.assertSaved(t)
		}
	}
}
