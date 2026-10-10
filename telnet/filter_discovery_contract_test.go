package telnet

import (
	"bytes"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	"dxcluster/filter"
)

// The family list is a literal contract, independent of suggestions and HELP.
// COMMENT is intercepted before domain dispatch; PC93 is an alias, not a family.
// Reader/owner/storage evidence here is composed; the checkout fixture owns
// accept-loop, automatic response-pause and writer-delivery qualification.
func discoveryFamilies() []string {
	return strings.Fields("BAND MODE SOURCE EVENT COMMENT DXCALL DECALL CONFIDENCE PATH DXGRID2 DEGRID2 DXCONT DECONT DXZONE DEZONE DXDXCC DEDXCC DXSTATE DESTATE MINSNR BEACON WWV WCY ANNOUNCE SELF TOXIC NEARBY DXBM NOFILTER")
}

const discoveryTypes = "Types: BAND, MODE, MINSNR, SOURCE, EVENT, COMMENT, DXCALL, DECALL,\n" +
	"       CONFIDENCE, PATH, DXGRID2, DEGRID2, DXCONT, DECONT, DXZONE, DEZONE,\n" +
	"       DXDXCC, DEDXCC, DXSTATE, DESTATE, DXBM, NEARBY,\n" +
	"       BEACON, WWV, WCY, ANNOUNCE, SELF, TOXIC\n"

func discoveryExpected(verb string, unknown bool) string {
	clear, extra := "ALL", "PASS also supports NOFILTER; PC93 aliases ANNOUNCE.\n"
	ending := "PASS supports NOFILTER. Type HELP PASS for complete syntax and examples.\n"
	if verb == "REJECT" {
		clear, extra = "NONE", "PC93 aliases ANNOUNCE.\n"
		ending = "Type HELP REJECT for complete syntax and examples.\n"
	}
	if unknown {
		return "Unknown filter type.\n" + discoveryTypes + "COMMENT uses PASS/REJECT COMMENT in either dialect.\n" +
			extra + "Type HELP " + verb + " for usage.\n"
	}
	return "Usage: " + verb + " <type> ...\n" + discoveryTypes +
		"COMMENT uses PASS/REJECT COMMENT <literal phrase>.\n" +
		"MINSNR takes modes and <integer dB|" + clear + ">.\n" +
		"NEARBY takes ON|OFF with either verb; PC93 aliases ANNOUNCE.\n" + ending
}

func TestFilterDiscoveryLiteralSuggestionsMatchPerActionInventory(t *testing.T) {
	e, c := newFilterCommandEngine(), newTestClient()
	for _, verb := range []string{"PASS", "REJECT"} {
		for _, unknown := range []bool{false, true} {
			command := verb
			if unknown {
				command += " INVENTED"
			}
			response, handled := e.Handle(c, command)
			if !handled || response != discoveryExpected(verb, unknown) {
				t.Fatalf("literal discovery feedback differs for %q: %q", command, response)
			}
			// Decode only the explicit type rows, so explanatory prose cannot
			// accidentally satisfy a missing category assertion.
			lines := strings.Split(response, "\n")
			types := strings.FieldsFunc(strings.TrimPrefix(strings.Join(lines[1:5], " "), "Types:"), func(r rune) bool {
				return r == ',' || r == ' '
			})
			want := slices.Clone(discoveryFamilies())
			if verb == "PASS" {
				types = append(types, "NOFILTER")
			} else {
				want = slices.DeleteFunc(want, func(name string) bool { return name == "NOFILTER" })
				if strings.Contains(response, "NOFILTER") {
					t.Fatal("REJECT suggestions advertise unsupported NOFILTER")
				}
			}
			slices.Sort(types)
			slices.Sort(want)
			if !slices.Equal(types, want) {
				t.Fatalf("%s literal discovery inventory differs: got %v want %v", verb, types, want)
			}
			assertDiscoveryBound(t, response)
		}
	}
}

func TestFilterDiscoveryOwnerInventory(t *testing.T) {
	e := newFilterCommandEngine()
	want := discoveryFamilies()
	if len(want) != 29 || len(e.domains) != 29 {
		t.Fatalf("want 29 families and 28 domain owners plus PC93 alias; families=%d entries=%d", len(want), len(e.domains))
	}
	seen := make(map[string]bool)
	for name, handler := range e.domains {
		if name == "PC93" {
			if handler != e.domains["ANNOUNCE"] {
				t.Fatal("PC93 must alias the same ANNOUNCE owner")
			}
			continue
		}
		if handler == nil || handler.name != name || name == "COMMENT" || !slices.Contains(want, name) {
			t.Fatalf("domain %q has no literal canonical-family contract", name)
		}
		seen[name] = true
	}
	for _, name := range want {
		if name == "COMMENT" {
			for _, verb := range []string{"PASS", "REJECT"} {
				_, handled, valid := parseCommentFilterCommand(verb + " COMMENT CQ DX")
				if !handled || !valid {
					t.Fatalf("%s COMMENT lost its separate literal owner", verb)
				}
			}
		} else if !seen[name] {
			t.Fatalf("literal family %s has no domain owner", name)
		}
	}
}

func assertDiscoveryBound(t *testing.T, response string) {
	t.Helper()
	rows := strings.Split(strings.TrimSuffix(response, "\n"), "\n")
	if len(rows) > 9 {
		t.Fatalf("recoverable discovery reply has %d rows, want at most 9: %q", len(rows), response)
	}
	for _, row := range rows {
		if len(row) > 78 {
			t.Fatalf("discovery reply exceeds 78 bytes: %q", row)
		}
	}
}

func assertDiscoveryRefusal(t *testing.T, x *helpOwnerSession, command, expected string, ziutek bool) {
	t.Helper()
	c := x.client
	before := captureHistoryCommentAliasState(t, c)
	files, path, next := helpOwnerFiles(t), c.pathSnapshot(), c.solarNextSummaryAt
	diag, dedupe, cadence := c.getDiagMode(), c.getDedupePolicy(), c.getSolarSummaryMinutes()
	fine, coarse, snapshot := c.filter.NearbyUserFine, c.filter.NearbyUserCoarse, c.filter.NearbySnapshot
	snapshotContents := fmt.Sprintf("%#v", snapshot)
	pauseEpoch, pausePending := c.readPauseEpoch, c.readPausePending.Load()
	pauseClosed, spotQueue := c.readPauseClosed, len(c.spotChan)
	pauseUntil, discarded, suppressed := c.readPauseUntilUnixNano.Load(), c.readPauseDiscardBefore.Load(), c.readPauseSuppressed.Load()
	writes, reads := x.writes, x.reads
	response, handled := x.server.filterEngine.Handle(c, helpOwnerInput(t, command, ziutek))
	if !handled || response != expected {
		t.Fatalf("%s %q discovery response differs: handled=%t\ngot  %q\nwant %q", c.dialect, command, handled, response, expected)
	}
	assertDiscoveryBound(t, response)
	before.assertOwners(t, c, x.writes-writes)
	after := captureHistoryCommentAliasState(t, c)
	if x.reads != reads || !reflect.DeepEqual(files, helpOwnerFiles(t)) || c.pathSnapshot() != path || !c.solarNextSummaryAt.Equal(next) ||
		c.getDiagMode() != diag || c.getDedupePolicy() != dedupe || c.getSolarSummaryMinutes() != cadence ||
		c.filter.NearbyUserFine != fine || c.filter.NearbyUserCoarse != coarse || c.filter.NearbySnapshot != snapshot || fmt.Sprintf("%#v", c.filter.NearbySnapshot) != snapshotContents ||
		c.readPauseEpoch != pauseEpoch || c.readPausePending.Load() != pausePending || c.readPauseClosed != pauseClosed || len(c.spotChan) != spotQueue || c.readPauseUntilUnixNano.Load() != pauseUntil ||
		c.readPauseDiscardBefore.Load() != discarded || c.readPauseSuppressed.Load() != suppressed ||
		before.search != after.search || before.query != after.query || before.token != after.token || !bytes.Equal(before.position, after.position) ||
		before.unreadable != after.unreadable || before.hadMatches != after.hadMatches || before.generation != after.generation || before.closed != after.closed {
		t.Fatalf("%q changed runtime/cursor/queue ownership or exact library bytes", command)
	}
}

func TestFilterDiscoveryActualOwnerFeedbackHasNoEffects(t *testing.T) {
	const missingDXBM = "Usage: PASS DXBM <band-codes> or REJECT DXBM <band-codes>\nType HELP PASS or HELP REJECT for usage.\n"
	const badDXBM = "Unknown DXBM band: ALL\nSupported: 160, 80, 40, 30, 20, 17, 15, 12, 10, 6, 2 (and 1 if enabled).\nModes ignored; use REJECT MODE <mode>.\n"
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		for _, ziutek := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ziutek=%t", dialect, ziutek), func(t *testing.T) {
				x := newHelpOwnerSession(t, dialect)
				if !strings.Contains(runHistory(t, x.server, x.client, "SH MYDX 1"), "K1ABC") || x.client.historySearch == nil {
					t.Fatal("refusal controls require a populated real history cursor")
				}
				x.client.readPauseEpoch = 23
				x.client.readPausePending.Store(true)
				x.client.readPauseUntilUnixNano.Store(71)
				x.client.readPauseDiscardBefore.Store(37)
				x.client.readPauseSuppressed.Store(11)
				cases := []struct{ command, expected string }{
					{"PASS", discoveryExpected("PASS", false)}, {"REJECT", discoveryExpected("REJECT", false)},
					{"PASS INVENTED", discoveryExpected("PASS", true)}, {"REJECT INVENTED", discoveryExpected("REJECT", true)},
					{"PASS DXBM", missingDXBM}, {"REJECT DXBM", missingDXBM},
					{"PASS DXBM ALL", badDXBM}, {"REJECT DXBM ALL", badDXBM},
					{"RESET MODE", "Usage: RESET FILTER\nType HELP for usage.\n"},
				}
				for _, verb := range []string{"PASS", "REJECT"} {
					for _, family := range []string{"DXSTATE", "DESTATE"} {
						cases = append(cases, struct{ command, expected string }{verb + " " + family,
							"Usage: " + verb + " " + family + " <state>[,<state>...] or ALL\nUS/Canadian mailing codes include provinces and territories.\nType HELP " + verb + " for usage.\n"})
					}
				}
				if dialect == DialectCC {
					cases = append(cases, struct{ command, expected string }{"SET/FILTER", discoveryExpected("PASS", false)},
						struct{ command, expected string }{"UNSET/FILTER", discoveryExpected("REJECT", false)},
						struct{ command, expected string }{"SET/FILTER INVENTED", discoveryExpected("PASS", true)},
						struct{ command, expected string }{"UNSET/FILTER INVENTED", discoveryExpected("REJECT", true)},
						struct{ command, expected string }{"SET/FILTER DXBM", missingDXBM},
						struct{ command, expected string }{"UNSET/FILTER DXBM", missingDXBM},
						struct{ command, expected string }{"SET/FILTER DXBM ALL", badDXBM},
						struct{ command, expected string }{"UNSET/FILTER DXBM ALL", badDXBM})
					for _, tc := range []struct{ head, verb string }{{"SET/FILTER", "PASS"}, {"UNSET/FILTER", "REJECT"}} {
						cases = append(cases, struct{ command, expected string }{tc.head + " DXSTATE",
							"Usage: " + tc.verb + " DXSTATE <state>[,<state>...] or ALL\nUS/Canadian mailing codes include provinces and territories.\nType HELP " + tc.verb + " for usage.\n"})
					}
				}
				for _, tc := range cases {
					assertDiscoveryRefusal(t, x, tc.command, tc.expected, ziutek)
				}
			})
		}
	}
}

func TestFilterDiscoveryPC93AliasCommitsActualSwitch(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			x := newHelpOwnerSession(t, dialect)
			c := x.client
			for _, tc := range []struct {
				command string
				enabled bool
			}{{"REJECT PC93", false}, {"PASS PC93", true}} {
				writes := x.writes
				response, handled := x.server.filterEngine.Handle(c, tc.command)
				if !handled || response == "" || x.writes != writes+1 || c.filter.AnnounceEnabled() != tc.enabled {
					t.Fatalf("alias failed to commit literal ANNOUNCE state: %q -> %q", tc.command, response)
				}
				x.assertSaved(t)
				record, err := filter.LoadUserRecord(c.callsign)
			if err != nil || record.AnnounceEnabled() != tc.enabled {
					t.Fatalf("PC93 acknowledgement hid unsaved ANNOUNCE state: %v", err)
				}
			}
		})
	}
}

func TestFilterDiscoveryNoFilterActionBoundary(t *testing.T) {
	for _, dialect := range []DialectName{DialectGo, DialectCC} {
		t.Run(string(dialect), func(t *testing.T) {
			x := newHelpOwnerSession(t, dialect)
			c := x.client
			assertDiscoveryRefusal(t, x, "REJECT NOFILTER", "Invalid filter command. Type HELP for usage.\n", false)
			writes := x.writes
			response, handled := x.server.filterEngine.Handle(c, "PASS NOFILTER")
			if !handled || response != "All filters disabled\n" || x.writes != writes+1 || !c.filter.AllBands || !c.filter.AllModes ||
				len(c.filter.Comments) != 0 || !maps.Equal(c.filter.MinSNR, map[string]int{}) {
				t.Fatalf("PASS-only NOFILTER did not clear the literal controls: %q", response)
			}
			x.assertSaved(t)
		})
	}
}
