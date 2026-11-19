package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func diagnosticSelectionFixture() studyDiagnosticSelection {
	selection := studyDiagnosticSelection{SchemaVersion: 2, Exposed: true, MaxSelectedPerDate: 512}
	for i, date := range []string{"20261107", "20261114", "20261115", "20261116"} {
		selection.Days = append(selection.Days, studyDiagnosticSelectionDay{DateUTC: date, AllFocalRows: 1, SelectedCount: 1,
			SourceDecisions: studyFile{Path: "old-" + date + ".jsonl", SHA256: strings.Repeat("f", 64)},
			Selected:        []studyDiagnosticSelectedRecord{{RawID: fmt.Sprintf("%064x:1", i+1), Ordinal: 1, RawSHA256: strings.Repeat("b", 64), Categories: []string{"synthetic_control"}}}})
	}
	return selection
}

func diagnosticSelectionBytes(t *testing.T, selection studyDiagnosticSelection) []byte {
	t.Helper()
	data, err := json.Marshal(selection)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func diagnosticFoundationDay() (*studyRawDay, []studyDiagnosticRawBinding) {
	at := time.Date(2026, 11, 15, 0, 10, 0, 123456789, time.UTC)
	day := studyTestReferenceDay([]studyRaw{studyTestRaw(at, "N0RX", "N0RX", "K1ABC", true), studyTestRaw(at, "N0RX", "N0RX", "K1ABC", true), studyTestRaw(at.Add(time.Second), "N1RX", "N1RX", "K1ABD", true)})
	bindings := make([]studyDiagnosticRawBinding, len(day.Rows))
	for i := range day.Rows {
		bindings[i] = studyDiagnosticRawBinding{DateUTC: "20261115", RawID: fmt.Sprintf("%s:%d", day.InputSHA256, i+1), Ordinal: int64(i + 1), RawSHA256: hashAnyStable(day.Rows[i].Row)}
	}
	return day, bindings
}

func TestDiagnosticSelectionStrictAdmission(t *testing.T) {
	valid := diagnosticSelectionBytes(t, diagnosticSelectionFixture())
	for _, fixture := range []struct {
		name string
		data []byte
	}{
		{"null", []byte("null")}, {"unknown", append([]byte(`{"unknown":0,`), valid[1:]...)},
		{"duplicate", append([]byte(`{"exposed":true,`), valid[1:]...)}, {"trailing", append(append([]byte(nil), valid...), []byte(" null")...)},
		{"oversized", []byte(strings.Repeat(" ", (4<<20)+1))},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			if _, err := parseStudyDiagnosticSelection(fixture.data); err == nil {
				t.Fatal("malformed selection accepted")
			}
		})
	}
	for _, variant := range []string{"schema", "exposure", "limit", "duplicate_day", "missing_day", "count", "null_records", "duplicate_id", "duplicate_ordinal", "raw_sha", "id_prefix", "ordinal", "category", "duplicate_category", "source"} {
		t.Run(variant, func(t *testing.T) {
			selection := diagnosticSelectionFixture()
			day := &selection.Days[0]
			switch variant {
			case "schema":
				selection.SchemaVersion = 1
			case "exposure":
				selection.Exposed = false
			case "limit":
				selection.MaxSelectedPerDate = 513
			case "duplicate_day":
				selection.Days[1].DateUTC = day.DateUTC
			case "missing_day":
				selection.Days = selection.Days[:3]
			case "count":
				day.SelectedCount++
			case "null_records":
				day.Selected = nil
				day.SelectedCount = 0
			case "duplicate_id":
				day.Selected = append(day.Selected, day.Selected[0])
				day.SelectedCount++
			case "duplicate_ordinal":
				record := day.Selected[0]
				record.RawID = strings.Repeat("d", 64) + ":1"
				day.Selected = append(day.Selected, record)
				day.SelectedCount++
			case "raw_sha":
				day.Selected[0].RawSHA256 = "bad"
			case "id_prefix":
				day.Selected[0].RawID = "bad:1"
			case "ordinal":
				day.Selected[0].Ordinal = 2
			case "category":
				day.Selected[0].Categories = []string{strings.Repeat("é", 513)}
			case "duplicate_category":
				day.Selected[0].Categories = []string{"same", "same"}
			case "source":
				day.SourceDecisions.Path = "https://example.invalid/old"
			}
			if _, err := parseStudyDiagnosticSelection(diagnosticSelectionBytes(t, selection)); err == nil {
				t.Fatal("invalid selection accepted")
			}
		})
	}
	for _, count := range []int{512, 513} {
		selection := diagnosticSelectionFixture()
		day := &selection.Days[0]
		day.Selected = nil
		for i := 1; i <= count; i++ {
			day.Selected = append(day.Selected, studyDiagnosticSelectedRecord{RawID: fmt.Sprintf("%064x:%d", 1, i), Ordinal: int64(i), RawSHA256: strings.Repeat("b", 64), Categories: []string{"synthetic"}})
		}
		day.SelectedCount = count
		day.AllFocalRows = count
		_, err := parseStudyDiagnosticSelection(diagnosticSelectionBytes(t, selection))
		if (err == nil) != (count == 512) {
			t.Fatalf("literal 512/date limit: count=%d err=%v", count, err)
		}
	}
}

func TestDiagnosticSelectionFrozenFilesAndArchiveBinding(t *testing.T) {
	selection := diagnosticSelectionFixture()
	for i := range selection.Days {
		selection.Days[i].SourceDecisions = studyAuditTestJSON(t, filepath.Join(t.TempDir(), "old.jsonl"), struct{ Old bool }{true})
	}
	selection.Authority = json.RawMessage(`{"opaque_instruction":"never fed to predictor"}`)
	selection.ParentSelection = json.RawMessage(`{"all_parent_ids_preserved":true,"future_research_field":42}`)
	file := studyAuditTestJSON(t, filepath.Join(t.TempDir(), "selection.json"), selection)
	loaded, digest, err := loadStudyDiagnosticSelection(file.Path, file.SHA256)
	if err != nil || digest != file.SHA256 || len(loaded.Days) != 4 || !reflect.DeepEqual(loaded.Days[0].Selected, selection.Days[0].Selected) || string(loaded.Authority) != string(selection.Authority) || string(loaded.ParentSelection) != string(selection.ParentSelection) {
		t.Fatal("valid frozen selection changed:", err)
	}
	if _, _, err := loadStudyDiagnosticSelection(file.Path, strings.Repeat("0", 64)); err == nil {
		t.Fatal("wrong external freeze accepted")
	}
	if err := os.WriteFile(selection.Days[0].SourceDecisions.Path, []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadStudyDiagnosticSelection(file.Path, file.SHA256); err == nil {
		t.Fatal("wrong old source hash accepted")
	}
	day, bindings := diagnosticFoundationDay()
	input := studyInput{DateUTC: "2026-11-15", Archive: studyFile{SHA256: day.InputSHA256}}
	selected := &selection.Days[2]
	selected.AllFocalRows, selected.SelectedCount, selected.Selected = 3, 1, []studyDiagnosticSelectedRecord{{RawID: bindings[0].RawID, RawSHA256: bindings[0].RawSHA256, Ordinal: 1, Categories: []string{"toy"}}}
	got, err := selection.bindDay(input, day)
	if err != nil || len(got) != 1 || got[0] != bindings[0] {
		t.Fatal("literal selected binding changed:", err)
	}
	for _, variant := range []string{"date", "archive", "ordinal", "row_sha", "focal", "focal_count"} {
		t.Run(variant, func(t *testing.T) {
			copyDay := *day
			copyDay.Rows = append([]studyRaw(nil), day.Rows...)
			copySelection := selection
			copySelection.Days = append([]studyDiagnosticSelectionDay(nil), selection.Days...)
			copySelection.Days[2].Selected = append([]studyDiagnosticSelectedRecord(nil), selected.Selected...)
			copyInput := input
			switch variant {
			case "date":
				copyInput.DateUTC = "20261115"
			case "archive":
				copyInput.Archive.SHA256 = strings.Repeat("c", 64)
			case "ordinal":
				copySelection.Days[2].Selected[0].Ordinal = 2
			case "row_sha":
				copyDay.Rows[0].Row.ReportDB++
			case "focal":
				copyDay.Rows[0].A = false
				copySelection.Days[2].AllFocalRows = 2
			case "focal_count":
				copySelection.Days[2].AllFocalRows++
			}
			if _, err := copySelection.bindDay(copyInput, &copyDay); err == nil {
				t.Fatal("forged archive binding accepted")
			}
		})
	}
}

func TestDiagnosticSelectionEntryDates(t *testing.T) {
	for _, date := range []string{"2026-11-07", "2026-11-14", "2026-11-15", "2026-11-16"} {
		if err := studyDiagnosticValidateEntry("selection.json", strings.Repeat("a", 64), date); err != nil {
			t.Fatal(err)
		}
	}
	for _, fixture := range [][3]string{{"", strings.Repeat("a", 64), "2026-11-15"}, {"selection.json", "", "2026-11-15"}, {"selection.json", "bad", "2026-11-15"}, {"selection.json", strings.Repeat("a", 64), "20261115"}, {"selection.json", strings.Repeat("a", 64), "2026-11-17"}} {
		if err := studyDiagnosticValidateEntry(fixture[0], fixture[1], fixture[2]); err == nil {
			t.Fatal("invalid entry date/pair accepted")
		}
	}
}

func FuzzStudyDiagnosticSelection(f *testing.F) {
	valid, err := json.Marshal(diagnosticSelectionFixture())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte(`null`))
	f.Add([]byte(`{"schema_version":2,"schema_version":1}`))
	f.Add([]byte("{\"schema_version\":2,\"authority\":\"\xff\"}"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 16384 {
			return
		}
		selection, err := parseStudyDiagnosticSelection(data)
		if err != nil {
			return
		}
		if selection.SchemaVersion != 2 || !selection.Exposed || selection.MaxSelectedPerDate != 512 || len(selection.Days) != 4 {
			t.Fatal("accepted selection violates fixed contract")
		}
		encoded, err := json.Marshal(selection)
		if err != nil {
			t.Fatal(err)
		}
		round, err := parseStudyDiagnosticSelection(encoded)
		if err != nil {
			t.Fatal("accepted selection failed strict roundtrip:", err)
		}
		for i := range round.Days {
			if round.Days[i].SelectedCount != len(round.Days[i].Selected) || len(round.Days[i].Selected) > 512 || !reflect.DeepEqual(round.Days[i].Selected, selection.Days[i].Selected) {
				t.Fatal("accepted selected records changed")
			}
		}
	})
}
