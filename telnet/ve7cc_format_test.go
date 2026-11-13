package telnet

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/cty"
	"dxcluster/spot"
)

func ve7ccTestSpot() *spot.Spot {
	return &spot.Spot{
		DXCall: "K1ABC", DECall: "VE7XYZ-12", Frequency: 14000.04,
		Time: time.Unix(946684800, 0), Mode: "CW", Comment: "hello",
		SourceType: spot.SourceManual,
		DXMetadata: spot.CallMetadata{ADIF: 291, ITUZone: 8, CQZone: 5, State: "MA", Country: "United States", Grid: "fn42ab"},
		DEMetadata: spot.CallMetadata{ADIF: 1, ITUZone: 2, CQZone: 3, State: "BC", Country: "Canada", Grid: "CN89", GridDerived: true},
	}
}

// Snapshot public serializer inputs and current display/dedupe evidence, never
// Spot's formatting locks. Clone the slice so in-place mutations cannot hide.
type ve7ccTestSnapshot struct {
	dxCall, deCall, dxCallNorm, deCallNorm string
	mode, modeNorm, comment, beaconComment string
	sourceNode, confidence                 string
	sourceType                             spot.SourceType
	frequency                              float64
	when                                   time.Time
	report                                 int
	hasReport, isBeacon, beaconSourceClass bool
	dxMetadata, deMetadata                 spot.CallMetadata
	cwDisplayAvailable                     bool
	cwDisplayComment                       string
	cwSpeeds                               []string
	secondaryDedupe                        [3]spot.SecondaryDedupeEvidence
}

func ve7ccSnapshot(s *spot.Spot) ve7ccTestSnapshot {
	return ve7ccTestSnapshot{
		dxCall: s.DXCall, deCall: s.DECall,
		dxCallNorm: s.DXCallNorm, deCallNorm: s.DECallNorm,
		mode: s.Mode, modeNorm: s.ModeNorm, comment: s.Comment,
		beaconComment: s.BeaconComment, sourceNode: s.SourceNode,
		sourceType: s.SourceType, frequency: s.Frequency, when: s.Time,
		report: s.Report, hasReport: s.HasReport, isBeacon: s.IsBeacon,
		beaconSourceClass: s.BeaconSourceClass, confidence: s.Confidence,
		dxMetadata: s.DXMetadata, deMetadata: s.DEMetadata,
		cwDisplayAvailable: s.CWDisplayAvailable, cwDisplayComment: s.CWDisplayComment,
		cwSpeeds: slices.Clone(s.CWSpeeds), secondaryDedupe: s.SecondaryDedupe,
	}
}

func ve7ccTestFields(t *testing.T, s *spot.Spot, context ve7ccContext) ([]string, ve7ccFormatNotes) {
	t.Helper()
	record, notes, err := appendVE7CCRecord(nil, s, context, maxVE7CCRecordBytes)
	if err != nil {
		t.Fatal(err)
	}
	fields := strings.Split(string(record), "^")
	if len(fields) != 20 {
		t.Fatalf("expected exactly 20 fields: %q", record)
	}
	for _, ch := range record {
		if ch < 32 || ch > 126 {
			t.Fatalf("non-ASCII byte %d", ch)
		}
	}
	return fields, notes
}

func TestVE7CCFormatGolden(t *testing.T) {
	s := ve7ccTestSpot()
	before := ve7ccSnapshot(s)
	got, notes, err := appendVE7CCRecord(nil, s, ve7ccContext{node: "N0NODE"}, maxVE7CCRecordBytes)
	want := "CC11^14000.0^K1ABC^ 1-Jan-2000^0000Z^CW hello^VE7XYZ-12^291^1^N0NODE^8^5^2^3^MA^BC^United States^Canada^FN42^cn89"
	if err != nil || string(got) != want || notes != (ve7ccFormatNotes{}) {
		t.Fatalf("got %q, %+v, %v; want %q", got, notes, err, want)
	}
	if !reflect.DeepEqual(before, ve7ccSnapshot(s)) {
		t.Fatal("spot mutated")
	}
	s.Frequency = 14000.06
	s.Time = time.Unix(946684800, 0).In(time.FixedZone("offset", 7*3600))
	s.DXCall = "IT9ABC"
	s.DXMetadata = spot.CallMetadata{ADIF: 248, ITUZone: 28, CQZone: 15, Country: "Italy"}
	s.DEMetadata.Grid = ""
	got, _, err = appendVE7CCRecord(nil, s, ve7ccContext{node: "N0NODE"}, maxVE7CCRecordBytes)
	want = "CC11^14000.1^IT9ABC^ 1-Jan-2000^0000Z^CW hello^VE7XYZ-12^248^1^N0NODE^28^15^2^3^^BC^Italy^Canada^^"
	if err != nil || string(got) != want {
		t.Fatalf("got %q, %v; want %q", got, err, want)
	}
}

func TestVE7CCFormatRawCommentWithCWMetadata(t *testing.T) {
	s := cwSpeedFixture()
	complete := ve7ccTestSpot()
	s.DXMetadata, s.DEMetadata = complete.DXMetadata, complete.DEMetadata
	s.HasReport, s.Report, s.Confidence = true, 0, "V"
	s.SecondaryDedupe[0] = spot.SecondaryDedupeEvidence{AnchorHz: 14024900, ModeID: 2, HasAnchor: true, Enabled: true}
	// Exercise all text projections first, including its shared format cache.
	for _, text := range []string{s.FormatDXClusterCW(false), s.FormatDXClusterCW(true), s.FormatDXClusterWithComment("D:diagnostic")} {
		if text == "" {
			t.Fatal("empty text-display fixture")
		}
	}
	before := ve7ccSnapshot(s)
	got, notes, err := appendVE7CCRecord(nil, s, ve7ccContext{node: "LOCAL"}, maxVE7CCRecordBytes)
	const want = "CC11^14025.0^K1ABC^ 2-Jan-2026^0304Z^CW 0 dB 22 WPM 30 WPM POTA^W1AW^291^1^^8^5^2^3^MA^BC^United States^Canada^FN42^cn89"
	if err != nil || string(got) != want || notes != (ve7ccFormatNotes{}) {
		t.Fatalf("got %q, %+v, %v; want raw canonical %q", got, notes, err, want)
	}
	if !reflect.DeepEqual(before, ve7ccSnapshot(s)) {
		t.Fatal("canonical spot, CW projection, or secondary evidence mutated")
	}
}

func TestVE7CCFormatComment(t *testing.T) {
	for _, tc := range []struct {
		mode, comment, fallback string
		report                  int
		hasReport, beacon       bool
		want                    string
	}{
		{"FT8", "heard", "", 0, true, false, "FT8 +0 dB heard"},
		{"CW", "heard", "", 0, true, false, "CW 0 dB heard"},
		{"FT8", "", "", -12, true, false, "FT8 -12 dB"},
		{"CW", "", "", 12, true, false, "CW 12 dB"},
		{"RTTY", "", "", 12, true, false, "RTTY 12 dB"},
		{"FT8", "", "", 99, false, false, "FT8"},
		{"", "  interior  spaces  ", "", 0, false, false, "  interior  spaces  "},
		{"CW", "", "", 0, false, true, "CW BEACON"},
		{"CW", " \t", spot.BeaconCommentNCDXF, 0, true, true, "CW 0 dB NCDXF BEACON"},
		{"CW", "\x00\u00e9", spot.BeaconCommentNCDXF, 0, false, true, "CW NCDXF BEACON"},
		{"CW", "real", spot.BeaconCommentNCDXF, 0, false, true, "CW real"},
		{"", "heard", "", 0, true, false, "+0 dB heard"},
	} {
		t.Run(tc.want, func(t *testing.T) {
			s := ve7ccTestSpot()
			s.Mode, s.Comment, s.BeaconComment = tc.mode, tc.comment, tc.fallback
			s.Report, s.HasReport, s.IsBeacon = tc.report, tc.hasReport, tc.beacon
			fields, _ := ve7ccTestFields(t, s, ve7ccContext{})
			if fields[5] != tc.want {
				t.Fatalf("got %q, want %q", fields[5], tc.want)
			}
		})
	}
}

func TestVE7CCFormatOrigin(t *testing.T) {
	for _, source := range []spot.SourceType{spot.SourcePeer, spot.SourceManual, spot.SourceUpstream, spot.SourceRBN, spot.SourceFT8, spot.SourceFT4, spot.SourcePSKReporter, ""} {
		for _, historical := range []bool{false, true} {
			s := ve7ccTestSpot()
			s.SourceType, s.SourceNode = source, "ORIGINAL"
			fields, _ := ve7ccTestFields(t, s, ve7ccContext{node: "LOCAL", historical: historical})
			want := ""
			if source == spot.SourcePeer {
				want = "ORIGINAL"
			} else if source == spot.SourceManual && !historical {
				want = "LOCAL"
			}
			if fields[9] != want {
				t.Fatalf("source=%s historical=%v: got %q want %q", source, historical, fields[9], want)
			}
		}
	}
}

func ve7ccTestCTY(t testing.TB) *cty.CTYDatabase {
	t.Helper()
	db, err := cty.LoadCTYDatabaseFromReader(strings.NewReader(`<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
<key>K</key><dict><key>Country</key><string>Current USA</string><key>ADIF</key><integer>291</integer><key>ITUZone</key><integer>8</integer><key>CQZone</key><integer>5</integer></dict>
<key>VE</key><dict><key>Country</key><string>Current Canada</string><key>ADIF</key><integer>1</integer><key>ITUZone</key><integer>2</integer></dict>
</dict></plist>`))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestVE7CCFormatMetadata(t *testing.T) {
	db := ve7ccTestCTY(t)
	for _, tc := range []struct {
		name         string
		adif         int
		country      string
		itu          int
		wantADIF     string
		wantCountry  string
		wantITU      string
		wantConflict bool
		wantLookups  uint64
	}{
		{"complete", 291, "Stored USA", 99, "291", "Stored USA", "99", false, 0},
		{"fill both", 291, "", 0, "291", "Current USA", "8", false, 1},
		{"fill country", 291, "", 99, "291", "Current USA", "99", false, 1},
		{"fill ITU", 291, "Stored USA", 0, "291", "Stored USA", "8", false, 1},
		{"missing ADIF", 0, "", 0, "", "", "", false, 0},
		{"negative ADIF", -1, "", 0, "", "", "", false, 0},
		{"conflicting CTY", 248, "", 0, "248", "", "", true, 1},
		{"preserve on conflict", 248, "Stored Italy", 0, "248", "Stored Italy", "", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := ve7ccTestSpot()
			s.DXMetadata.ADIF, s.DXMetadata.Country, s.DXMetadata.ITUZone = tc.adif, tc.country, tc.itu
			before := ve7ccSnapshot(s)
			start := db.Metrics()
			fields, notes := ve7ccTestFields(t, s, ve7ccContext{cty: db})
			if fields[7] != tc.wantADIF || fields[16] != tc.wantCountry || fields[10] != tc.wantITU || notes.conflict != tc.wantConflict {
				t.Fatalf("fields=%q notes=%+v", fields, notes)
			}
			if fields[11] != "5" || fields[14] != "MA" || fields[18] != "FN42" || !reflect.DeepEqual(before, ve7ccSnapshot(s)) {
				t.Fatal("stored metadata changed")
			}
			end := db.Metrics()
			if end.TotalLookups-start.TotalLookups != tc.wantLookups {
				t.Fatalf("lookup count: before=%+v after=%+v", start, end)
			}
			if (tc.wantCountry == "" || tc.wantITU == "" || tc.wantADIF == "") && !notes.unavailable {
				t.Fatal("missing unavailable note")
			}
		})
	}
	s := ve7ccTestSpot()
	s.DXCall = "ZZ0UNKNOWN"
	s.DXMetadata.Country, s.DXMetadata.ITUZone = "", 0
	s.DEMetadata.Country, s.DEMetadata.ITUZone = "", 0
	start := db.Metrics()
	fields, notes := ve7ccTestFields(t, s, ve7ccContext{cty: db})
	if fields[16] != "" || fields[17] != "Current Canada" || !notes.unavailable || notes.conflict ||
		db.Metrics().TotalLookups-start.TotalLookups != 2 {
		t.Fatalf("two-lookup/missing CTY contract: %q %+v", fields, notes)
	}
	s = ve7ccTestSpot()
	s.DXMetadata = spot.CallMetadata{ADIF: 291}
	before := ve7ccSnapshot(s)
	fields, notes = ve7ccTestFields(t, s, ve7ccContext{cty: db})
	if fields[7] != "291" || fields[10] != "8" || fields[16] != "Current USA" ||
		fields[11] != "" || fields[14] != "" || fields[18] != "" ||
		!notes.unavailable || !reflect.DeepEqual(before, ve7ccSnapshot(s)) {
		t.Fatalf("CTY supplemented fields other than missing country/ITU: %q %+v", fields, notes)
	}
}

func TestVE7CCFormatGrids(t *testing.T) {
	for _, tc := range []struct {
		grid    string
		derived bool
		want    string
	}{
		{"aa00AA", false, "AA00"}, {"RR99xx", true, "rr99"},
		{"fN42", false, "FN42"}, {"FN42", true, "fn42"},
		{"", false, ""}, {"FN4", false, ""}, {"SS00", false, ""},
		{"FN4A", false, ""}, {"^N42", false, ""}, {"\u00e942", false, ""},
	} {
		s := ve7ccTestSpot()
		s.DXMetadata.Grid, s.DXMetadata.GridDerived = tc.grid, tc.derived
		fields, notes := ve7ccTestFields(t, s, ve7ccContext{})
		if fields[18] != tc.want || notes.unavailable != (tc.want == "") {
			t.Fatalf("grid=%q derived=%v: %q %+v", tc.grid, tc.derived, fields[18], notes)
		}
	}
}

func TestVE7CCFormatSanitization(t *testing.T) {
	s := ve7ccTestSpot()
	bad := "A^\r\n\t\x00\x7f\u00e9\U0001f600\xffZ"
	want := "A~        Z"
	s.DXCall, s.DECall, s.Mode, s.Comment = bad, bad, bad, bad
	s.DXMetadata.State, s.DEMetadata.State = bad, bad
	s.DXMetadata.Country, s.DEMetadata.Country = bad, bad
	s.SourceType, s.SourceNode = spot.SourcePeer, bad
	before := ve7ccSnapshot(s)
	fields, notes := ve7ccTestFields(t, s, ve7ccContext{})
	for _, index := range []int{2, 6, 9, 14, 15, 16, 17} {
		if fields[index] != want {
			t.Fatalf("field %d=%q want %q", index, fields[index], want)
		}
	}
	if fields[5] != want+" "+want || !notes.sanitized || !reflect.DeepEqual(before, ve7ccSnapshot(s)) {
		t.Fatalf("comment=%q notes=%+v mutation=%v", fields[5], notes, !reflect.DeepEqual(before, ve7ccSnapshot(s)))
	}
	s = ve7ccTestSpot()
	fields, notes = ve7ccTestFields(t, s, ve7ccContext{node: bad})
	if fields[9] != want || !notes.sanitized {
		t.Fatalf("local node not sanitized: %q %+v", fields[9], notes)
	}
}

func TestVE7CCFormatBudget(t *testing.T) {
	s := ve7ccTestSpot()
	const base = "CC11^14000.0^K1ABC^ 1-Jan-2000^0000Z^CW hello^VE7XYZ-12^291^1^^8^5^2^3^MA^BC^United States^Canada^FN42^cn89"
	for _, limit := range []int{len(base), 65_536 - 2} {
		s.Comment = "hello" + strings.Repeat("x", limit-len(base))
		for _, budget := range []int{limit, limit + 2, math.MaxInt} {
			backing := bytes.Repeat([]byte{0xa5}, maxVE7CCRecordBytes+32)
			copy(backing, "prefix")
			got, _, err := appendVE7CCRecord(backing[:6], s, ve7ccContext{}, budget)
			if err != nil || len(got) != 6+limit || string(got[:6]) != "prefix" {
				t.Fatalf("limit=%d budget=%d: len=%d err=%v", limit, budget, len(got), err)
			}
		}
		for _, budget := range []int{-1, 0, 18, limit - 1} {
			backing := bytes.Repeat([]byte{0xa5}, maxVE7CCRecordBytes+32)
			before := bytes.Clone(backing)
			got, _, err := appendVE7CCRecord(backing[:6], s, ve7ccContext{}, budget)
			if !errors.Is(err, errVE7CCRecordTooLarge) || len(got) != 6 || !bytes.Equal(backing, before) {
				t.Fatalf("failed append wrote backing array: limit=%d budget=%d err=%v", limit, budget, err)
			}
		}
	}
	s.Comment += "x"
	backing := bytes.Repeat([]byte{0xa5}, maxVE7CCRecordBytes+32)
	before := bytes.Clone(backing)
	got, _, err := appendVE7CCRecord(backing[:6], s, ve7ccContext{}, math.MaxInt)
	if !errors.Is(err, errVE7CCRecordTooLarge) || len(got) != 6 || !bytes.Equal(backing, before) {
		t.Fatalf("hard cap overrun: len=%d err=%v", len(got), err)
	}
}

func TestVE7CCFormatBoundsHelperInputs(t *testing.T) {
	db := ve7ccTestCTY(t)
	for _, field := range []string{"call", "mode"} {
		t.Run(field, func(t *testing.T) {
			s := ve7ccTestSpot()
			s.DXMetadata.Country = ""
			s.HasReport = true
			if field == "call" {
				s.DXCall = strings.Repeat("k/", maxVE7CCRecordBytes)
			} else {
				s.Mode = strings.Repeat("cw", maxVE7CCRecordBytes)
			}
			backing := []byte("prefix unused")
			before := bytes.Clone(backing)
			start := db.Metrics().TotalLookups
			allocs := testing.AllocsPerRun(10, func() {
				got, _, err := appendVE7CCRecord(backing[:6], s, ve7ccContext{cty: db}, math.MaxInt)
				if len(got) != 6 || !errors.Is(err, errVE7CCRecordTooLarge) {
					panic("hostile helper input accepted")
				}
			})
			if allocs != 0 || db.Metrics().TotalLookups != start || !bytes.Equal(backing, before) {
				t.Fatalf("refusal allocated/looked up/wrote: allocs=%g lookups=%d", allocs, db.Metrics().TotalLookups-start)
			}
		})
	}
}

func TestVE7CCFormatInvalid(t *testing.T) {
	cases := []*spot.Spot{nil}
	for _, frequency := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		s := ve7ccTestSpot()
		s.Frequency = frequency
		cases = append(cases, s)
	}
	s := ve7ccTestSpot()
	s.DXCall = ""
	cases = append(cases, s)
	s = ve7ccTestSpot()
	s.DECall = ""
	cases = append(cases, s)
	for _, s := range cases {
		backing := []byte("prefix unused")
		got, _, err := appendVE7CCRecord(backing[:6], s, ve7ccContext{}, 1024)
		if !errors.Is(err, errVE7CCInvalidSpot) || string(got) != "prefix" || string(backing) != "prefix unused" {
			t.Fatalf("invalid spot: %q %v backing=%q", got, err, backing)
		}
	}
}

func TestVE7CCFormatAllocs(t *testing.T) {
	s := ve7ccTestSpot()
	s.HasReport = true
	context := ve7ccContext{node: "LOCAL", cty: ve7ccTestCTY(t)}
	buffer := make([]byte, 0, 1024)
	allocs := testing.AllocsPerRun(100, func() {
		got, _, err := appendVE7CCRecord(buffer[:0], s, context, 1024)
		if err != nil || len(got) == 0 {
			panic("append failed")
		}
	})
	if allocs != 0 || context.cty.Metrics().TotalLookups != 0 {
		t.Fatalf("pre-enriched warm-buffer append: allocs=%g lookups=%d", allocs, context.cty.Metrics().TotalLookups)
	}
}

func TestVE7CCFormatNumericExtremes(t *testing.T) {
	s := ve7ccTestSpot()
	s.Frequency, s.Report, s.HasReport = math.MaxFloat64, math.MinInt, true
	s.DXMetadata.ADIF, s.DXMetadata.ITUZone, s.DXMetadata.CQZone = math.MaxInt, math.MaxInt, math.MaxInt
	fields, _ := ve7ccTestFields(t, s, ve7ccContext{})
	wantReport := "CW -9223372036854775808 dB hello"
	if strconv.IntSize == 32 {
		wantReport = "CW -2147483648 dB hello"
	}
	if len(fields[1]) != 311 || !strings.HasSuffix(fields[1], ".0") || fields[5] != wantReport {
		t.Fatalf("extreme numbers malformed: %q", fields)
	}
}

func BenchmarkVE7CCFormat(b *testing.B) {
	s := ve7ccTestSpot()
	s.Mode, s.HasReport = "FT8", true
	buffer := make([]byte, 0, 1024)
	const want = "CC11^14000.0^K1ABC^ 1-Jan-2000^0000Z^FT8 +0 dB hello^VE7XYZ-12^291^1^LOCAL^8^5^2^3^MA^BC^United States^Canada^FN42^cn89"
	b.ReportAllocs()
	b.SetBytes(int64(len(want)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, notes, err := appendVE7CCRecord(buffer[:0], s, ve7ccContext{node: "LOCAL"}, 1024)
		if err != nil || notes != (ve7ccFormatNotes{}) || string(got) != want {
			b.Fatalf("incorrect repeated append: %q %+v %v", got, notes, err)
		}
	}
}

func BenchmarkVE7CCMetadata(b *testing.B) {
	const base = "CC11^14000.0^K1ABC^ 1-Jan-2000^0000Z^CW hello^VE7XYZ-12^291^1^LOCAL^8^5^2^3^MA^BC^United States^Canada^FN42^cn89"
	for _, name := range []string{"missing", "missing-both", "conflicting", "sanitized"} {
		b.Run(name, func(b *testing.B) {
			s := ve7ccTestSpot()
			context := ve7ccContext{node: "LOCAL", cty: ve7ccTestCTY(b)}
			want, wantNotes := base, ve7ccFormatNotes{}
			switch name {
			case "missing", "missing-both":
				s.DXMetadata.Country, s.DXMetadata.ITUZone = "", 0
				want = strings.Replace(want, "^United States^", "^Current USA^", 1)
				if name == "missing-both" {
					s.DEMetadata.Country, s.DEMetadata.ITUZone = "", 0
					want = strings.Replace(want, "^Canada^", "^Current Canada^", 1)
				}
			case "conflicting":
				s.DXMetadata.ADIF, s.DXMetadata.Country, s.DXMetadata.ITUZone = 248, "", 0
				want = "CC11^14000.0^K1ABC^ 1-Jan-2000^0000Z^CW hello^VE7XYZ-12^248^1^LOCAL^^5^2^3^MA^BC^^Canada^FN42^cn89"
				wantNotes.unavailable, wantNotes.conflict = true, true
			case "sanitized":
				s.Comment = "h^\t"
				want = strings.Replace(want, "CW hello", "CW h~ ", 1)
				wantNotes.sanitized = true
			}
			buffer := make([]byte, 0, 1024)
			if got, notes, err := appendVE7CCRecord(buffer[:0], s, context, 1024); err != nil || string(got) != want || notes != wantNotes {
				b.Fatalf("metadata benchmark setup: %q %+v %v", got, notes, err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				got, notes, err := appendVE7CCRecord(buffer[:0], s, context, 1024)
				if err != nil || string(got) != want || notes != wantNotes {
					b.Fatalf("metadata output changed: %q %+v %v", got, notes, err)
				}
			}
		})
	}
}

func FuzzVE7CCFormat(f *testing.F) {
	f.Add("clean", "K1ABC", 4096)
	f.Add("^\r\n\x00\u00e9\xff", "VE7XYZ", 8)
	f.Add(strings.Repeat("x", 65536), "IT9ABC", 65536)
	f.Fuzz(func(t *testing.T, text, call string, budget int) {
		s := ve7ccTestSpot()
		s.Comment, s.DXCall, s.Mode, s.SourceNode = text, call, text, text
		s.DXMetadata.Country, s.DEMetadata.State = text, text
		s.SourceType = spot.SourcePeer
		before := ve7ccSnapshot(s)
		backing := bytes.Repeat([]byte{0xa5}, maxVE7CCRecordBytes+16)
		copy(backing, "prefix")
		snapshot := bytes.Clone(backing)
		got, _, err := appendVE7CCRecord(backing[:6], s, ve7ccContext{}, budget)
		if !reflect.DeepEqual(before, ve7ccSnapshot(s)) || !bytes.Equal(got[:6], snapshot[:6]) {
			t.Fatal("input/prefix mutated")
		}
		if err != nil {
			if !bytes.Equal(backing, snapshot) || len(got) != 6 ||
				(!errors.Is(err, errVE7CCRecordTooLarge) && !errors.Is(err, errVE7CCInvalidSpot)) {
				t.Fatal("failure mutated buffer or returned unsafe error")
			}
			return
		}
		record := got[6:]
		if len(record)+2 > maxVE7CCRecordBytes || len(record) > budget || bytes.Count(record, []byte("^")) != 19 {
			t.Fatal("record violates independent size/shape oracle")
		}
		for _, ch := range record {
			if ch < 32 || ch > 126 {
				t.Fatal("non-printable ASCII output")
			}
		}
		fields := strings.Split(string(record), "^")
		if fields[0] != "CC11" || fields[1] != "14000.0" || fields[3] != " 1-Jan-2000" || fields[4] != "0000Z" || fields[7] != "291" {
			t.Fatal("fixed structural identity changed")
		}
	})
}
