package uls

import (
	"context"
	"strings"
	"testing"
	"time"
)

const isedLookupMain = `VE3AAA;A;B;;;ON;;;;;;;;;;;;
VE3BBB;A;B;;;BC;;;;;;;;;;;;
VA7CCC;A;B;;;BC;;;;;;;;;;;;
VO2DDD;A;B;;;NL;;;;;;;;;;;;
VY0EEE;A;B;;;NU;;;;;;;;;;;;
`

func TestISEDLookupInclusiveUTCDates(t *testing.T) {
	db := newISEDTestDB(t, isedLookupMain, "CG3;2026-10-07;2026-10-08;event;;;VE3\n")
	for _, tc := range []struct {
		date  string
		found bool
	}{{"2026-10-06T23:59:59Z", false}, {"2026-10-07T00:00:00Z", true}, {"2026-10-08T23:59:59Z", true}, {"2026-10-09T00:00:00Z", false}, {"2026-10-06T20:00:00-04:00", true}} {
		now, err := time.Parse(time.RFC3339, tc.date)
		if err != nil {
			t.Fatal(err)
		}
		result, err := queryCanadianLicense(context.Background(), db, "CG3AAA", now)
		if err != nil || !result.Available || result.Found != tc.found {
			t.Errorf("%s lookup=%+v err=%v", tc.date, result, err)
		}
		if tc.found && result.State != "ON" {
			t.Errorf("%s state=%q", tc.date, result.State)
		}
	}
}

func TestISEDLookupPrefixContracts(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, event, call, state string
		available, found         bool
	}{
		{"assigned substitution", "CG3;2026-10-01;2026-10-31;event;;;VE3\n", "CG3AAA", "ON", true, true},
		{"unassigned base", "CG3;2026-10-01;2026-10-31;event;;;VE3\n", "CG3ZZZ", "", true, false},
		{"blank national mapping", "CK;2026-10-01;2026-10-31;event;;;\n", "CK3AAA", "ON", true, true},
		{"VA mapping", "XL7;2026-10-01;2026-10-31;event;;;VA7\n", "XL7CCC", "BC", true, true},
		{"VO mapping", "XJ2;2026-10-01;2026-10-31;event;;;VO2\n", "XJ2DDD", "NL", true, true},
		{"VY zero mapping", "XK0;2026-10-01;2026-10-31;event;;;VY\n", "XK0EEE", "NU", true, true},
		{"declared slash sources", "CG3;2026-10-01;2026-10-31;event;;;VE3/VA3\n", "CG3AAA", "ON", true, true},
		{"declared exact source", "CG3;2026-10-01;2026-10-31;event;;;VE3AAA\n", "CG3AAA", "ON", true, true},
		{"declared source excludes base", "CG3;2026-10-01;2026-10-31;event;;;VE3BBB\n", "CG3AAA", "", true, false},
		{"typo source uncertainty", "XJ2;2026-10-01;2026-10-31;event;;;V02\n", "XJ2DDD", "", false, false},
		{"unsupported VO source uncertainty", "XJ2;2026-10-01;2026-10-31;event;;;VO3\n", "XJ2DDD", "", false, false},
		{"unsupported VA source uncertainty", "XL7;2026-10-01;2026-10-31;event;;;VA9\n", "XL7CCC", "", false, false},
		{"unsupported prefix uncertainty", "QQ3;2026-10-01;2026-10-31;event;;;VE3\n", "QQ3AAA", "", false, false},
		{"VE0 is not mapped", "CG0;2026-10-01;2026-10-31;event;;;VE0\n", "CG0AAA", "", false, false},
		{"bad date uncertainty", "CG3;not-a-date;2026-10-31;event;;;VE3\n", "CG3AAA", "", false, false},
		{"reversed date uncertainty", "CG3;2009-12-01;2009-11-01;event;;;VE3\n", "CG3AAA", "", false, false},
		{"invalid expired mapping inactive", "CG0;2020-10-01;2020-10-31;event;;;VE0\n", "CG0AAA", "", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newISEDTestDB(t, isedLookupMain, tc.event)
			result, err := queryCanadianLicense(context.Background(), db, tc.call, now)
			if err != nil || result.Available != tc.available || result.Found != tc.found || result.State != tc.state {
				t.Fatalf("lookup=%+v err=%v want available=%t found=%t state=%q", result, err, tc.available, tc.found, tc.state)
			}
		})
	}
}

func TestISEDExactSpecialMembershipAndConflicts(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	first := "CF3EVENT;2026-10-01;2026-10-31;event;;VE3AAA;\n"
	second := "CF3EVENT;2026-10-01;2026-10-31;event;;VE3BBB;\n"
	for _, tc := range []struct {
		name, events, call, state string
		available, found          bool
	}{
		{"trustee province", first, "CF3EVENT", "ON", true, true},
		{"missing trustee", "CF3EVENT;2026-10-01;2026-10-31;event;;VE3ZZZ;\n", "CF3EVENT", "", true, true},
		{"foreign trustee not inferred", "CF3EVENT;2026-10-01;2026-10-31;event;;K1ABC;\n", "CF3EVENT", "", true, true},
		{"conflicts forward", first + second, "CF3EVENT", "", true, true},
		{"conflicts reverse", second + first, "CF3EVENT", "", true, true},
		{"inactive disagreement", first + "CF3EVENT;2020-10-01;2020-10-31;event;;VE3BBB;\n", "CF3EVENT", "ON", true, true},
		{"ordinary conflicting active assignment", "VE3AAA;2026-10-01;2026-10-31;event;;VE3BBB;\n", "VE3AAA", "", true, true},
		{"ordinary remains member with uncertain event", "VE3AAA;bad;2026-10-31;event;;VE3BBB;\n", "VE3AAA", "", true, true},
		{"reversed exact event unknown", "CG7GMT;2009-12-01;2009-11-01;event;;VE3AAA;\n", "CG7GMT", "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newISEDTestDB(t, isedLookupMain, tc.events)
			result, err := queryCanadianLicense(context.Background(), db, tc.call, now)
			if err != nil || result.Available != tc.available || result.Found != tc.found || result.State != tc.state {
				t.Fatalf("lookup=%+v err=%v", result, err)
			}
		})
	}
}

func TestISEDLookupIndexes(t *testing.T) {
	db := newISEDTestDB(t, isedLookupMain, "CG3;2026-10-01;2026-10-31;event;;;VE3\n")
	rows, err := db.QueryContext(t.Context(), `EXPLAIN QUERY PLAN SELECT e.special,trustee.state,base.state FROM Events e LEFT JOIN CA trustee ON trustee.call_sign=e.trustee LEFT JOIN CA base ON base.call_sign=? WHERE e.special IN(?,?,?);`, "VE3AAA", "CG3AAA", "CG", "CG3")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(detail + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	text := plan.String()
	if !strings.Contains(text, "idx_Events_special") || strings.Contains(text, "SCAN e") || strings.Contains(text, "SCAN trustee") || strings.Contains(text, "SCAN base") {
		t.Fatalf("unindexed lookup plan:\n%s", text)
	}
}

func TestISEDRIC9CompleteLiteralPrefixOracle(t *testing.T) {
	// These literal rows transcribe RIC-9 Table I, independently of the mapping
	// implementation. Array positions are the observed ASCII area digits 0..9.
	golden := []struct {
		special  string
		ordinary [10]string
	}{
		{"CG", [10]string{"", "VE1", "VE2", "VE3", "VE4", "VE5", "VE6", "VE7", "VE8", "VE9"}},
		{"CK", [10]string{"", "VE1", "VE2", "VE3", "VE4", "VE5", "VE6", "VE7", "VE8", "VE9"}},
		{"VX", [10]string{"", "VE1", "VE2", "VE3", "VE4", "VE5", "VE6", "VE7", "VE8", "VE9"}},
		{"XM", [10]string{"", "VE1", "VE2", "VE3", "VE4", "VE5", "VE6", "VE7", "VE8", "VE9"}},
		{"VC", [10]string{"", "VE1", "VE2", "VE3", "VE4", "VE5", "VE6", "VE7", "VE8", "VE9"}},
		{"CF", [10]string{"", "VA1", "VA2", "VA3", "VA4", "VA5", "VA6", "VA7", "", ""}},
		{"CJ", [10]string{"", "VA1", "VA2", "VA3", "VA4", "VA5", "VA6", "VA7", "", ""}},
		{"VG", [10]string{"", "VA1", "VA2", "VA3", "VA4", "VA5", "VA6", "VA7", "", ""}},
		{"XL", [10]string{"", "VA1", "VA2", "VA3", "VA4", "VA5", "VA6", "VA7", "", ""}},
		{"VB", [10]string{"", "VA1", "VA2", "VA3", "VA4", "VA5", "VA6", "VA7", "", ""}},
		{"CH", [10]string{"", "VO1", "VO2", "", "", "", "", "", "", ""}},
		{"CY", [10]string{"", "VO1", "VO2", "", "", "", "", "", "", ""}},
		{"XJ", [10]string{"", "VO1", "VO2", "", "", "", "", "", "", ""}},
		{"XN", [10]string{"", "VO1", "VO2", "", "", "", "", "", "", ""}},
		{"VD", [10]string{"", "VO1", "VO2", "", "", "", "", "", "", ""}},
		{"CI", [10]string{"VY0", "VY1", "VY2", "", "", "", "", "", "", ""}},
		{"CZ", [10]string{"VY0", "VY1", "VY2", "", "", "", "", "", "", ""}},
		{"XK", [10]string{"VY0", "VY1", "VY2", "", "", "", "", "", "", ""}},
		{"XO", [10]string{"VY0", "VY1", "VY2", "", "", "", "", "", "", ""}},
		{"VF", [10]string{"VY0", "VY1", "VY2", "", "", "", "", "", "", ""}},
	}
	// All base assignments use ON deliberately: province comes from the source
	// address, so an implementation inferring province from a prefix fails too.
	var assigned strings.Builder
	for _, prefix := range []string{"VE0", "VE1", "VE2", "VE3", "VE4", "VE5", "VE6", "VE7", "VE8", "VE9", "VA1", "VA2", "VA3", "VA4", "VA5", "VA6", "VA7", "VO1", "VO2", "VY0", "VY1", "VY2"} {
		assigned.WriteString(prefix + "AAA;A;B;;;ON;;;;;;;;;;;;\n")
	}
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	for _, mode := range []string{"two-character event", "three-character event"} {
		t.Run(mode, func(t *testing.T) {
			var events strings.Builder
			for _, row := range golden {
				if mode == "two-character event" {
					events.WriteString(row.special + ";2026-10-01;2026-10-31;event;;;\n")
				} else {
					for digit := byte('0'); digit <= '9'; digit++ {
						events.WriteString(row.special + string(digit) + ";2026-10-01;2026-10-31;event;;;\n")
					}
				}
			}
			db := newISEDTestDB(t, assigned.String(), events.String())
			for _, row := range golden {
				for digit, prefix := range row.ordinary {
					call := row.special + string(byte('0'+digit)) + "AAA"
					wantBase := ""
					if prefix != "" {
						wantBase = prefix + "AAA"
					}
					if got := canadianSubstitutionBase(call); got != wantBase {
						t.Errorf("%s base=%q want literal %q", call, got, wantBase)
					}
					result, err := queryCanadianLicense(context.Background(), db, call, now)
					if err != nil {
						t.Fatalf("%s indexed lookup: %v", call, err)
					}
					if prefix != "" {
						if !result.Available || !result.Found || result.State != "ON" {
							t.Errorf("%s indexed lookup=%+v want assigned %s province ON", call, result, wantBase)
						}
					} else if result.Available || result.Found || result.State != "" {
						t.Errorf("%s unsupported mapping lookup=%+v want unknown", call, result)
					}
				}
			}
		})
	}
	for _, prefix := range []string{"AA", "QQ", "QZ", "VE", "VA", "VO", "VY"} {
		for digit := byte('0'); digit <= '9'; digit++ {
			if base := canadianSubstitutionBase(prefix + string(digit) + "AAA"); base != "" {
				t.Errorf("unsupported prefix %s%c inferred base %q", prefix, digit, base)
			}
		}
	}
}

func TestISEDRIC9IslandEventsUseExactEvidence(t *testing.T) {
	db := newISEDTestDB(t, isedLookupMain, "CY0S;2026-10-01;2026-10-31;event;;VE3AAA;\nCY9S;2026-10-01;2026-10-31;event;;;\n")
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct{ call, province string }{{"CY0S", "ON"}, {"CY9S", ""}} {
		if base := canadianSubstitutionBase(tc.call); base != "" {
			t.Errorf("island event %s inferred base %q", tc.call, base)
		}
		result, err := queryCanadianLicense(context.Background(), db, tc.call, now)
		if err != nil || !result.Available || !result.Found || result.State != tc.province {
			t.Errorf("exact island %s=%+v err=%v want province %q", tc.call, result, err, tc.province)
		}
	}
	for _, call := range []string{"CY0AAA", "CY9AAA"} {
		result, err := queryCanadianLicense(context.Background(), db, call, now)
		if err != nil || !result.Available || result.Found || result.State != "" {
			t.Errorf("unlisted island %s=%+v err=%v", call, result, err)
		}
	}
}
