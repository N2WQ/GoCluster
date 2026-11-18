package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func diagnosticWireReplace(t *testing.T, data []byte, old, replacement string) []byte {
	t.Helper()
	if !bytes.Contains(data, []byte(old)) {
		t.Fatal("wire mutation target absent:", old)
	}
	return bytes.Replace(data, []byte(old), []byte(replacement), 1)
}

func TestDiagnosticSelectionBeforeTypedGrowth(t *testing.T) {
	selection := diagnosticSelectionFixture()
	dayBytes, err := json.Marshal(selection.Days[0])
	if err != nil {
		t.Fatal(err)
	}
	recordBytes, err := json.Marshal(selection.Days[0].Selected[0])
	if err != nil {
		t.Fatal(err)
	}
	root := `{"schema_version":2,"exposed":true,"max_selected_per_date":512,"days":[`
	day := `{"all_focal_rows":513,"date_utc":"20261107","selected_count":513,"source_decisions":{"path":"old","sha256":"` + strings.Repeat("f", 64) + `"},"selected":[`
	record := `{"raw_id":"` + fmt.Sprintf("%064x:1", 1) + `","ordinal":1,"raw_sha256":"` + strings.Repeat("b", 64) + `","categories":[`
	for _, fixture := range []struct {
		name string
		data string
	}{
		{"days", root + strings.Repeat(string(dayBytes)+",", 4) + `{`},
		{"selected", root + day + strings.Repeat(string(recordBytes)+",", 512) + `{`},
		{"categories", root + day + record + strings.Repeat(`"x",`, 64) + `{`},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			// The over-limit element is deliberately malformed. A typed-first
			// decoder cannot reach postdecode validation or this bound sentinel.
			parsed, err := parseStudyDiagnosticSelection([]byte(fixture.data))
			if !errors.Is(err, errStudyDiagnosticCollectionBound) || parsed.Days != nil || parsed.SchemaVersion != 0 {
				t.Fatalf("typed population was entered before bound refusal: %+v %v", parsed, err)
			}
		})
	}
	selection.Days[0].Selected[0].Categories = nil
	for i := range 64 {
		selection.Days[0].Selected[0].Categories = append(selection.Days[0].Selected[0].Categories, fmt.Sprintf("category%d", i))
	}
	if _, err := parseStudyDiagnosticSelection(diagnosticSelectionBytes(t, selection)); err != nil {
		t.Fatal("valid 4-day/64-category boundary refused:", err)
	}
	// The separate strict-admission test supplies 512 distinct valid records.
}

func TestDiagnosticSelectionRequiredFactsAndRawUTF8(t *testing.T) {
	selection := diagnosticSelectionFixture()
	selection.Days[0].Selected = []studyDiagnosticSelectedRecord{}
	selection.Days[0].SelectedCount = 0
	data := diagnosticSelectionBytes(t, selection)
	if _, err := parseStudyDiagnosticSelection(data); err != nil {
		t.Fatal("explicit zero selected_count was refused:", err)
	}
	source, err := json.Marshal(selection.Days[0].SourceDecisions)
	if err != nil {
		t.Fatal(err)
	}
	for _, fixture := range []struct{ name, old, replacement string }{
		{"missing_zero_count", `"selected_count":0,`, ``},
		{"null_zero_count", `"selected_count":0`, `"selected_count":null`},
		{"missing_schema", `"schema_version":2,`, ``},
		{"null_schema", `"schema_version":2`, `"schema_version":null`},
		{"case_alias", `"schema_version":2`, `"schema_version":2,"Schema_Version":2`},
		{"day_case_alias", `"selected_count":0`, `"selected_count":0,"Selected_Count":0`},
		{"null_source", `"source_decisions":` + string(source), `"source_decisions":null`},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			if _, err := parseStudyDiagnosticSelection(diagnosticWireReplace(t, data, fixture.old, fixture.replacement)); err == nil {
				t.Fatal("absent/null/case-alias operative fact accepted")
			}
		})
	}
	invalid := diagnosticWireReplace(t, data, `"authority":null`, `"authority":"`+string([]byte{0xff})+`"`)
	if _, err := parseStudyDiagnosticSelection(invalid); err == nil {
		t.Fatal("invalid original UTF-8 repaired into accepted metadata")
	}
}

func TestDiagnosticJournalRequiredZeroFacts(t *testing.T) {
	day, attempts, _ := diagnosticAttemptFixture()
	attempt := attempts[0]
	attempt.Accepted = false
	attempt.Reason = ""
	attempt.Key.Bucket = 0
	attempt.Key.ToleranceHz = 0
	data := diagnosticJournalBytes(t, []studyDiagnosticAttempt{attempt})
	if _, err := studyDiagnosticReadJournal(bytes.NewReader(data), day, studyDiagnosticWanted{}, 7, 8, 1); err != nil {
		t.Fatal("explicit false/empty/zero journal facts refused:", err)
	}
	for _, fixture := range []struct{ name, old, replacement string }{
		{"missing_false", `"accepted":false,`, ``}, {"null_false", `"accepted":false`, `"accepted":null`},
		{"missing_empty", `,"reason":""`, ``}, {"null_empty", `"reason":""`, `"reason":null`},
		{"missing_bucket", `"Bucket":0,`, ``}, {"null_bucket", `"Bucket":0`, `"Bucket":null`},
		{"missing_tolerance", `,"ToleranceHz":0`, ``}, {"null_tolerance", `"ToleranceHz":0`, `"ToleranceHz":null`},
		{"case_alias", `"accepted":false`, `"accepted":false,"Accepted":false`},
		{"key_case_alias", `"Bucket":0`, `"Bucket":0,"bucket":0`},
		{"duplicate", `"accepted":false`, `"accepted":false,"accepted":false`},
		{"raw_utf8", `"reason":""`, `"reason":"` + string([]byte{0xff}) + `"`},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			mutated := diagnosticWireReplace(t, data, fixture.old, fixture.replacement)
			if joined, err := studyDiagnosticReadJournal(bytes.NewReader(mutated), day, studyDiagnosticWanted{}, 7, 8, 1); err == nil || joined != nil {
				t.Fatal("missing/null/aliased/invalid-byte zero fact was restored from omission")
			}
		})
	}
}

func TestDiagnosticOpaqueMetadataTokenBounds(t *testing.T) {
	for _, count := range []int{128, 129} {
		selection := diagnosticSelectionFixture()
		parts := make([]string, count)
		for i := range parts {
			parts[i] = fmt.Sprintf(`"key%d":0`, i)
		}
		selection.Authority = json.RawMessage("{" + strings.Join(parts, ",") + "}")
		_, err := parseStudyDiagnosticSelection(diagnosticSelectionBytes(t, selection))
		if (err == nil) != (count == 128) {
			t.Fatalf("bounded opaque key set count=%d err=%v", count, err)
		}
	}
	for _, depth := range []int{31, 32} {
		selection := diagnosticSelectionFixture()
		selection.Authority = json.RawMessage(strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth))
		_, err := parseStudyDiagnosticSelection(diagnosticSelectionBytes(t, selection))
		if (err == nil) != (depth == 31) {
			t.Fatalf("opaque token depth=%d err=%v", depth, err)
		}
	}
	data := diagnosticSelectionBytes(t, diagnosticSelectionFixture())
	for _, raw := range []string{`{"same":1,"same":2}`, `{"` + strings.Repeat("x", 1025) + `":1}`} {
		if _, err := parseStudyDiagnosticSelection(diagnosticWireReplace(t, data, `"authority":null`, `"authority":`+raw)); err == nil {
			t.Fatal("unbounded/duplicate opaque metadata keys accepted")
		}
	}
	if _, err := parseStudyDiagnosticSelection(diagnosticWireReplace(t, data, `"authority":null`, `"authority":1e9999`)); err != nil {
		t.Fatal("opaque research number was interpreted as a runtime float:", err)
	}
}
