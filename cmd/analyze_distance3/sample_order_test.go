package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// The original score/subject/winner/outcome keys tie for each family. Support
// groups oppose lexical reason order; the final reason tie crosses each cap.
// Reverse insertion must change neither chosen rows nor their printed order.
func TestDistance3RealSQLiteSampleTiesAreReproducible(t *testing.T) {
	var values []distance3FixtureRow
	for i := range 11 {
		value := distance3FixtureValue("APPLIED_TIE", 65, "applied", fmt.Sprintf("z%02d", i), 3)
		if i >= 9 {
			value.support = 4
			value.reporters = 5
			value.reason = fmt.Sprintf("a%02d", i)
		}
		values = append(values, value)
	}
	for i := range 6 {
		value := distance3FixtureValue("REJECTED_TIE", 65, "rejected", fmt.Sprintf("z%02d", i), 3)
		if i >= 4 {
			value.support = 4
			value.reporters = 5
			value.reason = fmt.Sprintf("a%02d", i)
		}
		values = append(values, value)
	}
	forward := distance3FixtureDB(t, values)
	slices.Reverse(values)
	reverse := distance3FixtureDB(t, values)
	var reports [2]string
	for i, path := range []string{forward, reverse} {
		cases := distance3OrderCases(t, path)
		s := summarizeDistance3(cases)
		if s.applied != 11 || s.rejected != 6 || s.unknown != 0 || len(s.appliedSamples) != 10 || len(s.rejectedSamples) != 5 {
			t.Fatalf("family/cap counts: %+v", s)
		}
		for j, c := range s.appliedSamples {
			wantReason, wantSupport := fmt.Sprintf("z%02d", j), 3
			if j == 9 {
				wantReason, wantSupport = "a09", 4
			}
			if c.subject != "APPLIED_TIE" || c.reason != wantReason || c.winnerSupport != wantSupport {
				t.Errorf("applied sample%d = %+v, want literal support%d reason%s", j, c, wantSupport, wantReason)
			}
		}
		for j, c := range s.rejectedSamples {
			wantReason, wantSupport := fmt.Sprintf("z%02d", j), 3
			if j == 4 {
				wantReason, wantSupport = "a04", 4
			}
			if c.subject != "REJECTED_TIE" || c.reason != wantReason || c.winnerSupport != wantSupport {
				t.Errorf("rejected sample%d = %+v, want literal support%d reason%s", j, c, wantSupport, wantReason)
			}
		}
		var out bytes.Buffer
		if err := reportDistance3(&out, "fixed historical fixture", cases); err != nil {
			t.Fatal(err)
		}
		reports[i] = out.String()
		appliedSection := distance3OrderSection(t, reports[i], "Applied samples (at most 10):", "Rejected samples (at most 5):")
		rejectedSection := distance3OrderSection(t, reports[i], "Rejected samples (at most 5):", "HYPOTHETICAL SCALAR GATE COMPARISONS")
		if strings.Count(appliedSection, " -> ") != 10 || strings.Count(rejectedSection, " -> ") != 5 || strings.Contains(appliedSection, "reason=\"a10\"") || strings.Contains(rejectedSection, "reason=\"a05\"") {
			t.Fatalf("wrong sampled members/caps:\n%s\n%s", appliedSection, rejectedSection)
		}
	}
	if reports[0] != reports[1] {
		t.Fatalf("insertion order changed report:\nforward:\n%s\nreverse:\n%s", reports[0], reports[1])
	}
}

func distance3OrderCases(t *testing.T, path string) []distance3Case {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("order fixture close: %v", err)
		}
	})
	cases, err := loadDistance3Cases(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return cases
}

func distance3OrderSection(t *testing.T, report, start, end string) string {
	t.Helper()
	a := strings.Index(report, start)
	b := strings.Index(report, end)
	if a < 0 || b <= a {
		t.Fatalf("missing/incorrect sample section bounds %q..%q:\n%s", start, end, report)
	}
	return report[a:b]
}
