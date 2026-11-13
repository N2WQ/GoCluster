package voacap

import (
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func countRow(at, station, obsid, spots, status string) string {
	return fmt.Sprintf(`{"time_tag":%q,"Station":%s,"Observatory":"HOL","Obsid":%s,"Numspot":%s,"Report_Status":%s,"Type":"spt","Region":null}`, at, station, obsid, spots, status)
}
func TestRuntimeSunspotReducerLiteralAndOrder(t *testing.T) {
	now := mustTime(t, "2026-06-10T21:00:00Z")
	a := countRow("2026-06-08T20:00:00", "1", "307", "3", "3")
	b := countRow("2026-06-08T20:00:00", "1", "309", "4", "2")
	for _, body := range []string{"[" + a + "," + b + "]", "[" + b + "," + a + "," + a + "]", strings.ReplaceAll("["+a+","+b+"]", `"Region":null`, `"Region":4572`)} {
		got, err := parseRuntimeSunspotReport([]byte(body), now)
		if err != nil || len(got) != 1 || got[0].raw != 27 {
			t.Fatalf("literal reported groups =>27: %+v %v", got, err)
		}
	}
}
func TestRuntimeSunspotReducerRejectsAmbiguity(t *testing.T) {
	now := mustTime(t, "2026-06-10T21:00:00Z")
	good := countRow("2026-06-08T20:00:00", "1", "307", "3", "2")
	for name, bad := range map[string]string{
		"conflicting_count":          strings.Replace(good, `"Numspot":3`, `"Numspot":4`, 1),
		"conflicting_status":         strings.Replace(good, `"Report_Status":2`, `"Report_Status":3`, 1),
		"competing_station":          countRow("2026-06-08T20:00:00", "2", "307", "3", "2"),
		"competing_observatory":      strings.Replace(good, `"Observatory":"HOL"`, `"Observatory":"RAM"`, 1),
		"negative":                   strings.Replace(good, `"Numspot":3`, `"Numspot":-1`, 1),
		"fractional":                 strings.Replace(good, `"Numspot":3`, `"Numspot":3.5`, 1),
		"integral_float_unsupported": strings.Replace(good, `"Numspot":3`, `"Numspot":3.0`, 1),
		"overflow":                   strings.Replace(good, `"Numspot":3`, `"Numspot":9007199254740992`, 1),
		"aggregate_overflow":         strings.Replace(good, `"Numspot":3`, `"Numspot":9007199254740991`, 1),
		"missing_id":                 strings.Replace(good, `"Obsid":307,`, "", 1),
		"deleted":                    strings.Replace(good, `"Report_Status":2`, `"Report_Status":4`, 1),
		"unsupported_type":           strings.Replace(good, `"Type":"spt"`, `"Type":"other"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			for _, body := range []string{"[" + good + "," + bad + "]", "[" + bad + "," + good + "]"} {
				if got, err := parseRuntimeSunspotReport([]byte(body), now); err == nil || len(got) != 0 {
					t.Fatalf("ambiguous report accepted: %+v %v", got, err)
				}
			}
		})
	}
	for _, body := range []string{"[]", "null", "{}", "[null]", "[{}]", "[" + strings.Replace(good, `"Numspot":3`, `"Numspot":3,"Numspot":3`, 1) + "]", "[" + strings.Replace(good, "2026-06-08", "2026-06-11", 1) + "]"} {
		if _, err := parseRuntimeSunspotReport([]byte(body), now); err == nil {
			t.Fatalf("unsupported/missing representation accepted: %s", body)
		}
	}
	// An assignable malformed count invalidates its whole aggregate, while a good
	// independent timestamp survives. Unassignable identity invalidates the body.
	bad := strings.Replace(good, `"Numspot":3`, `"Numspot":null`, 1)
	newer := countRow("2026-06-10T20:00:00", "1", "308", "9", "2")
	got, err := parseRuntimeSunspotReport([]byte("["+bad+","+good+","+newer+"]"), now)
	if err != nil || len(got) != 1 || got[0].raw != 19 {
		t.Fatalf("unrelated valid aggregate lost: %+v %v", got, err)
	}
	bad = strings.Replace(bad, `"Station":1,`, "", 1)
	if _, err := parseRuntimeSunspotReport([]byte("["+bad+","+newer+"]"), now); err == nil {
		t.Fatal("unassignable group did not reject body")
	}
}
func TestCountEWMALiteralOracles(t *testing.T) {
	for _, tc := range []struct {
		previous, next float64
		hours          int
		want           float64
	}{{100, 200, 24, 129.28932188134524}, {100, 200, 48, 150}, {100, 102, 48, 101}, {100, 200, 96, 175}} {
		got := advanceCountEWMA(tc.previous, tc.next, time.Duration(tc.hours)*time.Hour, 48*time.Hour)
		if math.Abs(got-tc.want) > 1e-12 {
			t.Fatalf("EWMA got %.17g want %.17g", got, tc.want)
		}
	}
	for _, tc := range []struct {
		value float64
		want  int
	}{{100.499, 100}, {100.500, 101}, {100.501, 101}} {
		got, err := roundedCountSSN(tc.value)
		if err != nil || got != tc.want {
			t.Fatalf("round %g=%d %v", tc.value, got, err)
		}
	}
	for _, v := range []float64{-1, math.NaN(), math.Inf(1), math.Ldexp(1, 63)} {
		if _, err := roundedCountSSN(v); err == nil {
			t.Fatalf("unsafe SSN accepted: %g", v)
		}
	}
}
func FuzzParseRuntimeSunspotReport(f *testing.F) {
	f.Add("[" + countRow("2026-06-08T20:00:00", "1", "307", "3", "3") + "]")
	f.Add("[]")
	f.Add(`[{"Numspot":-1}]`)
	now := time.Date(2026, 6, 10, 21, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, body string) {
		if len(body) > defaultMaxSunspotReportBytes {
			t.Skip()
		}
		got, err := parseRuntimeSunspotReport([]byte(body), now)
		if err != nil {
			return
		}
		for i, row := range got {
			if row.at.After(now) || !finiteNonnegativeCount(row.raw) || row.raw < 10 || i > 0 && !row.at.After(got[i-1].at) {
				t.Fatalf("invalid accepted series: %+v", got)
			}
		}
	})
}

func TestRuntimeSunspotDuplicateMembersIsolateAggregate(t *testing.T) {
	now := mustTime(t, "2026-06-10T21:00:00Z")
	first := countRow("2026-06-08T20:00:00", "1", "307", "3", "2")
	later := countRow("2026-06-10T20:00:00", "1", "308", "9", "2")
	for _, key := range []string{"Numspot", "Report_Status", "Type", "Region"} {
		bad := strings.Replace(first, `"`+key+`":`, `"`+key+`":null,"`+key+`":`, 1)
		for _, body := range []string{"[" + bad + "," + first + "," + later + "]", "[" + later + "," + first + "," + bad + "]"} {
			got, err := parseRuntimeSunspotReport([]byte(body), now)
			if err != nil || len(got) != 1 || got[0].raw != 19 {
				t.Fatalf("duplicated %s lost good aggregate: %+v %v", key, got, err)
			}
		}
	}
	for _, key := range []string{"time_tag", "Station", "Observatory"} {
		bad := strings.Replace(first, `"`+key+`":`, `"`+key+`":null,"`+key+`":`, 1)
		if _, err := parseRuntimeSunspotReport([]byte("["+bad+","+later+"]"), now); err == nil {
			t.Fatalf("ambiguous identity %s assigned", key)
		}
	}
}

func TestCountEWMAConstantMinimumRemainsValid(t *testing.T) {
	if got := advanceCountEWMA(10, 10, 2*time.Hour, 48*time.Hour); got != 10 {
		t.Fatalf("constant minimum changed %.17g", got)
	}
	m := newTestSunspotMonitor(t, nil)
	at := mustTime(t, "2026-06-08T20:00:00Z")
	if err := m.applyCountSeries([]countReportSample{{at, 10}, {at.Add(2 * time.Hour), 10}}, at.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := saveSunspotMonitorState(filepath.Join(t.TempDir(), "state.json"), m.stateSnapshot()); err != nil {
		t.Fatalf("valid minimum continuity rejected %v", err)
	}
}
