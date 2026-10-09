package telnet

import (
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	"dxcluster/spot"
)

// Verify the advertised recipe through executable commands and actual spot
// matching. Unrelated selections are deliberately active, so an accidental
// global reset cannot pass just by producing the requested bands and modes.
func TestHelpBandModeRecipe(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			server := presetTestServer(t)
			client := configurationTestClient(server, "W1ABC-1")
			client.dialect = dialect
			engine := newFilterCommandEngine()
			run := func(line string) {
				t.Helper()
				response, handled := engine.Handle(client, line)
				for _, bad := range []string{"Invalid", "Usage:", "Unknown", "failed"} {
					if !handled || strings.Contains(response, bad) {
						t.Fatalf("%q: handled=%v response=%q", line, handled, response)
					}
				}
			}
			run("PASS NOFILTER")
			run("PASS COMMENT CQ")
			allow, block, show := "PASS", "REJECT", "SHOW FILTER"
			if dialect == DialectCC {
				allow, block, show = "SET/FILTER", "UNSET/FILTER", "SHOW/FILTER"
			}
			run(allow + " SOURCE HUMAN")
			run(block + " DXCALL W1*")
			sources := maps.Clone(client.filter.Sources)
			comments := slices.Clone(client.filter.Comments)
			blockedCalls := slices.Clone(client.filter.BlockDXCallsigns)
			for _, line := range []string{block + " BAND ALL", allow + " BAND 20,40", block + " MODE ALL", allow + " MODE CW,FT8", show} {
				run(line)
			}
			if !maps.Equal(sources, client.filter.Sources) || !reflect.DeepEqual(comments, client.filter.Comments) || !reflect.DeepEqual(blockedCalls, client.filter.BlockDXCallsigns) {
				t.Fatal("recipe changed unrelated source, comment or callsign selections")
			}
			for _, tc := range []struct {
				call, mode, comment string
				frequency           float64
				human, want         bool
			}{
				{"K1ABC", "CW", "CQ", 14025, true, true},
				{"K1ABC", "FT8", "CQ", 7074, true, true},
				{"K1ABC", "CW", "CQ", 21025, true, false},
				{"K1ABC", "CW", "CQ", 3525, true, false},
				{"K1ABC", "USB", "CQ", 14200, true, false},
				{"K1ABC", "", "CQ", 14025, true, false},
				{"K1ABC", "CW", "QRT", 14025, true, false},
				{"W1XYZ", "CW", "CQ", 14025, true, false},
				{"K1ABC", "CW", "CQ", 14025, false, false},
			} {
				sp := spot.NewSpot(tc.call, "N2ABC", tc.frequency, tc.mode)
				sp.Comment, sp.IsHuman = tc.comment, tc.human
				if got := client.filter.Matches(sp); got != tc.want {
					t.Fatalf("%+v: matched=%v want=%v", tc, got, tc.want)
				}
			}
		})
	}
}
