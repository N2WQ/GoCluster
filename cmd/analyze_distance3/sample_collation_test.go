package main

import (
	"bytes"
	"slices"
	"testing"
)

func TestDistance3SampleOrderIgnoresLegacyTextCollation(t *testing.T) {
	// Literal ASCII order. The legacy NOCASE column considers all sixteen equal;
	// display keeps their case, so insertion order must not choose the ten samples.
	subjects := []string{"W1ABCD", "W1ABCd", "W1ABcD", "W1ABcd", "W1AbCD", "W1AbCd", "W1AbcD", "W1Abcd",
		"W1aBCD", "W1aBCd", "W1aBcD", "W1aBcd", "W1abCD", "W1abCd", "W1abcD", "W1abcd"}
	var values []distance3FixtureRow
	for _, subject := range subjects {
		values = append(values, distance3FixtureValue(subject, 65, "applied", "", 3))
	}
	var reports [2]string
	for i := range reports {
		if i == 1 {
			slices.Reverse(values)
		}
		path := distance3FixtureDB(t, values,
			"ALTER TABLE decisions RENAME COLUMN subject TO original_subject",
			"ALTER TABLE decisions ADD COLUMN subject TEXT COLLATE NOCASE",
			"UPDATE decisions SET subject=original_subject")
		cases := distance3OrderCases(t, path)
		s := summarizeDistance3(cases)
		if s.applied != 16 || len(s.appliedSamples) != 10 {
			t.Fatalf("unexpected selection/sample counts: %+v", s)
		}
		for j, c := range s.appliedSamples {
			if c.subject != subjects[j] {
				t.Errorf("case-preserving sample%d=%q, want %q", j, c.subject, subjects[j])
			}
		}
		var out bytes.Buffer
		if err := reportDistance3(&out, "fixed collation fixture", cases); err != nil {
			t.Fatal(err)
		}
		reports[i] = out.String()
	}
	if reports[0] != reports[1] {
		t.Fatal("legacy NOCASE ties allowed insertion order to change displayed samples")
	}
}
