package feed

import (
	"math"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func readTestdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read testdata/%s: %v", name, err)
	}
	return b
}

func TestBuildGFZURLOmitsStatusAndFormatsUTC(t *testing.T) {
	start := time.Date(2026, 11, 26, 5, 16, 0, 0, time.FixedZone("EST", -5*3600))
	end := start.Add(36 * time.Hour)
	raw, err := BuildGFZURL("https://kp.gfz.de/app/json/?status=def", "Hp30", start, end)
	if err != nil {
		t.Fatalf("BuildGFZURL: %v", err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse built URL: %v", err)
	}
	q := u.Query()
	if q.Get("index") != "Hp30" {
		t.Fatalf("index=%q, want Hp30", q.Get("index"))
	}
	if q.Get("status") != "" {
		t.Fatalf("status=%q, want omitted for nowcast inclusion", q.Get("status"))
	}
	if q.Get("start") != "2026-11-26T10:16:00Z" || q.Get("end") != "2026-11-27T22:16:00Z" {
		t.Fatalf("unexpected UTC window: start=%q end=%q", q.Get("start"), q.Get("end"))
	}
}

func TestParseGFZHp30ColumnarNullAndCompletedGating(t *testing.T) {
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)
	samples, err := ParseGFZHp30(readTestdata(t, "gfz_hp30.json"), now)
	if err != nil {
		t.Fatalf("ParseGFZHp30: %v", err)
	}
	// Fixture has:
	// 14:30 -> 0.0 (valid zero!)
	// 15:00 -> null (must be dropped, NOT decoded as 0.0)
	// 15:30 -> 0.333
	// 16:00 -> 1.333
	// 16:30 -> 0.667 (valid), duplicate 16:30 -> 16.0 (out of bounds, skipped)
	// 17:00 -> 2.333 (in-progress at 17:16Z, must be dropped)
	if len(samples) != 4 {
		t.Fatalf("got %d samples (%+v), want 4", len(samples), samples)
	}
	wantTimes := []string{
		"2026-11-27T14:30:00Z",
		"2026-11-27T15:30:00Z",
		"2026-11-27T16:00:00Z",
		"2026-11-27T16:30:00Z",
	}
	wantVals := []float64{0.0, 0.333, 1.333, 0.667}
	for i := range wantTimes {
		if got := samples[i].BinStartUTC.Format(time.RFC3339); got != wantTimes[i] {
			t.Fatalf("sample[%d] time=%s, want %s", i, got, wantTimes[i])
		}
		if math.Abs(samples[i].Value-wantVals[i]) > 1e-12 {
			t.Fatalf("sample[%d] val=%v, want %v", i, samples[i].Value, wantVals[i])
		}
	}
}

func TestParseGFZHp30RejectsMalformedAndLengthMismatch(t *testing.T) {
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)
	cases := []string{
		`not json`,
		`{"datetime":["2026-11-27T15:00:00Z"]}`,
		`{"Hp30":[1.0]}`,
		`{"datetime":["2026-11-27T15:00:00Z"],"Hp30":[1.0,2.0]}`,
	}
	for _, body := range cases {
		if _, err := ParseGFZHp30([]byte(body), now); err == nil {
			t.Fatalf("expected error for payload %s", body)
		}
	}
}

func TestParseGFZFobsAllowsTodayAndRejectsFuture(t *testing.T) {
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)
	samples, err := ParseGFZFobs(readTestdata(t, "gfz_fobs.json"), now)
	if err != nil {
		t.Fatalf("ParseGFZFobs: %v", err)
	}
	// 2026-11-24 (123.6), 25 (null -> skipped), 26 (87.0), 27 (87.4, today allowed), 28 (future -> rejected).
	if len(samples) != 3 {
		t.Fatalf("got %d samples (%+v), want 3", len(samples), samples)
	}
	if samples[0].Value != 123.6 || samples[1].Value != 87.0 || samples[2].Value != 87.4 {
		t.Fatalf("unexpected Fobs values: %+v", samples)
	}
}

func TestParseNOAAKpDualSchemaAndSubBinExpansion(t *testing.T) {
	now := time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)

	objSamples, err := ParseNOAAKp(readTestdata(t, "noaa_kp_objects.json"), now)
	if err != nil {
		t.Fatalf("ParseNOAAKp objects: %v", err)
	}
	// 09:00 -> 6 bins (09:00..11:30)
	// 12:00 -> 6 bins (12:00..14:30)
	// 15:00 -> 4 completed bins before 17:00 (15:00, 15:30, 16:00, 16:30); 17:00 & 17:30 dropped!
	if len(objSamples) != 16 {
		t.Fatalf("object schema got %d expanded 30m bins, want 16", len(objSamples))
	}
	last := objSamples[len(objSamples)-1]
	if last.BinStartUTC.Format(time.RFC3339) != "2026-11-27T16:30:00Z" || last.Value != 2.33 {
		t.Fatalf("unexpected last Kp sub-bin: %+v", last)
	}

	legacySamples, err := ParseNOAAKp(readTestdata(t, "noaa_kp_legacy_2d.json"), now)
	if err != nil {
		t.Fatalf("ParseNOAAKp legacy 2D: %v", err)
	}
	// 09:00 (6 bins) + 12:00 (6 bins); 15:00 has "bad" and is skipped.
	if len(legacySamples) != 12 {
		t.Fatalf("legacy 2D schema got %d bins, want 12", len(legacySamples))
	}
	if legacySamples[0].Value != 0.33 || legacySamples[11].Value != 1.67 {
		t.Fatalf("unexpected legacy 2D values: first=%+v last=%+v", legacySamples[0], legacySamples[11])
	}

	// Empty array must not error.
	empty, err := ParseNOAAKp([]byte(`[]`), now)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty Kp array: samples=%v err=%v", empty, err)
	}
}
