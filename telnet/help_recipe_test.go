package telnet

import (
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
			run("PASS RULE 0 SOURCE=HUMAN AND COMMENT=CQ")
			run("REJECT RULE 9 DXCALL=W1*")
			show := "SHOW FILTER"
			if dialect == DialectCC {
				show = "SHOW/FILTER"
			}
			reject := client.filter.PreparedRules().Definitions()[1]
			for _, line := range []string{"PASS RULE 0 BAND=20M,40M AND MODE=CW,FT8 AND SOURCE=HUMAN AND COMMENT=CQ", show} {
				run(line)
			}
			if rules := client.filter.PreparedRules().Definitions(); len(rules) != 2 || rules[1] != reject {
				t.Fatal("recipe changed independent callsign rejection")
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
