// File role: Locks CSV report conversion to finite, representable values while
// preserving Atoi-first admission and the historical int(v + 0.5) rounding.
package main

import (
	"encoding/csv"
	"errors"
	"io"
	"math"
	"math/big"
	"strconv"
	"strings"
	"testing"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
)

func TestB22CSVReportRepresentability(t *testing.T) {
	for _, tc := range b22SNRConversionCases() {
		t.Run(tc.name, func(t *testing.T) {
			parser := newTestRBNHistoryCSV(t, b22SNRCSV(tc.raw))
			row, ok, err := parser.Read()
			if err != nil {
				t.Fatalf("Read(%q): %v", tc.raw, err)
			}
			if ok != tc.accept {
				t.Fatalf("Read(%q) accepted=%v, want %v; row=%+v", tc.raw, ok, tc.accept, row)
			}
			if !tc.accept {
				if row != (rbnHistoryRow{}) {
					t.Fatalf("rejected report retained a partial row: %+v", row)
				}
				return
			}
			if int64(row.ReportDB) != tc.want {
				t.Fatalf("Read(%q) report=%d, want %d", tc.raw, row.ReportDB, tc.want)
			}
			if row.DXCall != "OZ4ADX" || row.Spotter != "K1ABC" || row.Mode != "CW" || row.Band != "80m" {
				t.Fatalf("accepted report changed row fields: %+v", row)
			}
		})
	}
}

func TestB22CSVReportRejectAndContinue(t *testing.T) {
	parser := newTestRBNHistoryCSV(t, b22SNRCSV("NaN", "0", "", "-12"))
	var accepted, rejected int
	for _, tc := range []struct {
		accept bool
		want   int
	}{{false, 0}, {true, 0}, {false, 0}, {true, -12}} {
		row, ok, err := parser.Read()
		if err != nil {
			t.Fatal(err)
		}
		if ok != tc.accept || row.ReportDB != tc.want {
			t.Fatalf("accepted=%v report=%d, want %v/%d", ok, row.ReportDB, tc.accept, tc.want)
		}
		if ok {
			accepted++
		} else {
			rejected++
			if row != (rbnHistoryRow{}) {
				t.Fatalf("rejected row was not empty: %+v", row)
			}
		}
	}
	if accepted != 2 || rejected != 2 {
		t.Fatalf("accepted/rejected=%d/%d, want 2/2", accepted, rejected)
	}
	if _, ok, err := parser.Read(); ok || !errors.Is(err, io.EOF) {
		t.Fatalf("final Read accepted=%v err=%v, want false/EOF", ok, err)
	}
}

func TestB22CSVReportPresenceThroughReplayBuilder(t *testing.T) {
	parser := newTestRBNHistoryCSV(t, b22SNRCSV("0"))
	row, ok, err := parser.Read()
	if err != nil || !ok {
		t.Fatalf("reported-zero row accepted=%v err=%v", ok, err)
	}
	entry := newReplaySpot(row)
	if entry.Report != 0 || !entry.HasReport {
		t.Fatalf("replay Spot lost reported zero: report=%d present=%v", entry.Report, entry.HasReport)
	}
	cfg := config.CallCorrectionConfig{Enabled: true, RecencySeconds: 30, FrequencyToleranceHz: 100}
	present, ok := correctionflow.BuildResolverEvidenceSnapshot(entry, cfg, nil, row.Time)
	if !ok || present.Report != 0 || !present.HasReport {
		t.Fatalf("shared builder lost reported zero: accepted=%v evidence=%+v", ok, present)
	}
	entry.HasReport = false
	missing, ok := correctionflow.BuildResolverEvidenceSnapshot(entry, cfg, nil, row.Time)
	if !ok || missing.Report != 0 || missing.HasReport {
		t.Fatalf("shared builder invented a missing report: accepted=%v evidence=%+v", ok, missing)
	}
	if missing.Key != present.Key || missing.DXCall != present.DXCall || missing.Spotter != present.Spotter {
		t.Fatalf("presence changed receiver evidence identity: present=%+v missing=%+v", present, missing)
	}
}

func FuzzB22CSVReportConversion(f *testing.F) {
	for _, tc := range b22SNRConversionCases() {
		f.Add(tc.raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 512 {
			t.Skip("report exceeds bounded CSV fixture")
		}
		// Quoting keeps arbitrary report bytes in one db field rather than
		// allowing a mutation to change the fixture's other columns or rows.
		var body strings.Builder
		writer := csv.NewWriter(&body)
		for _, record := range [][]string{
			{"callsign", "freq", "band", "dx", "db", "date", "mode", "tx_mode"},
			{"K1ABC", "3547.0", "80m", "OZ4ADX", raw, "2026-04-26 03:18:00", "CQ", "CW"},
		} {
			if err := writer.Write(record); err != nil {
				t.Fatal(err)
			}
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			t.Fatal(err)
		}
		parser := newTestRBNHistoryCSV(t, body.String())
		row, ok, err := parser.Read()
		if err != nil {
			t.Fatal(err)
		}
		want, accept := b22SNRReference(raw)
		if ok != accept || (ok && int64(row.ReportDB) != want) {
			t.Fatalf("report=%q accepted/value=%v/%d, want %v/%d", raw, ok, row.ReportDB, accept, want)
		}
		if !ok && row != (rbnHistoryRow{}) {
			t.Fatalf("rejected report retained a partial row: %+v", row)
		}
	})
}

// b22SNRReference uses exact rationals and integer division to check the parsed
// float's truncated result and native-int range, without a float-to-int cast.
func b22SNRReference(raw string) (int64, bool) {
	raw = strings.TrimSpace(raw)
	if value, err := strconv.Atoi(raw); err == nil {
		return int64(value), true
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	// Preserve the actual float64 addition before exact rational truncation.
	transformed := new(big.Rat).SetFloat64(value + 0.5)
	if transformed == nil {
		return 0, false
	}
	upper := new(big.Int).Lsh(big.NewInt(1), uint(strconv.IntSize-1))
	lower := new(big.Int).Neg(upper)
	truncated := new(big.Int).Quo(transformed.Num(), transformed.Denom())
	if truncated.Cmp(lower) < 0 || truncated.Cmp(upper) >= 0 {
		return 0, false
	}
	return truncated.Int64(), true
}

func b22SNRCSV(reports ...string) string {
	var body strings.Builder
	body.WriteString("callsign,freq,band,dx,db,date,mode,tx_mode\n")
	for _, report := range reports {
		body.WriteString("K1ABC,3547.0,80m,OZ4ADX,")
		body.WriteString(report)
		body.WriteString(",2026-04-26 03:18:00,CQ,CW\n")
	}
	return body.String()
}

type b22SNRConversionCase struct {
	name   string
	raw    string
	accept bool
	want   int64
}

func b22SNRConversionCases() []b22SNRConversionCase {
	cases := []b22SNRConversionCase{
		{"integer_zero", "0", true, 0},
		{"trimmed_zero", " 0 ", true, 0},
		{"negative_integer", "-12", true, -12},
		{"positive_integer", "12", true, 12},
		{"positive_below_half", "12.49", true, 12},
		{"positive_half", "12.5", true, 13},
		{"positive_above_half", "12.75", true, 13},
		{"negative_below_half", "-12.49", true, -11},
		{"negative_half", "-12.5", true, -12},
		{"negative_above_half", "-12.75", true, -12},
		{"positive_zero_half", "0.5", true, 1},
		{"negative_zero_half", "-0.5", true, 0},
		{"negative_zero_fraction", "-0.75", true, 0},
		{"negative_one_half", "-1.5", true, -1},
		{"scientific_notation", "1e1", true, 10},
		{"blank", "", false, 0},
		{"whitespace", "  ", false, 0},
		{"malformed", "bad", false, 0},
		{"unit_suffix", "12dB", false, 0},
		{"nan", "NaN", false, 0},
		{"positive_inf", "+Inf", false, 0},
		{"negative_inf", "-Inf", false, 0},
		{"positive_infinity", "Infinity", false, 0},
		{"negative_infinity", "-Infinity", false, 0},
		{"positive_float_overflow", "1e999", false, 0},
		{"negative_float_overflow", "-1e999", false, 0},
		{"positive_finite_outside_int", "1e100", false, 0},
		{"negative_finite_outside_int", "-1e100", false, 0},
	}
	// These expected values are literal boundary vectors, not outputs from a
	// float-to-int reference cast. Atoi must preserve exact integer endpoints.
	if strconv.IntSize == 32 {
		return append(cases,
			b22SNRConversionCase{"integer_max", "2147483647", true, 2147483647},
			b22SNRConversionCase{"integer_min", "-2147483648", true, -2147483648},
			b22SNRConversionCase{"float_upper_below", "2147483647.25", true, 2147483647},
			b22SNRConversionCase{"float_upper_equal", "2147483647.5", false, 0},
			b22SNRConversionCase{"float_upper_above", "2147483648.0", false, 0},
			b22SNRConversionCase{"float_lower_above", "-2147483648.25", true, -2147483647},
			b22SNRConversionCase{"float_lower_equal", "-2147483648.5", true, -2147483648},
			b22SNRConversionCase{"float_lower_fraction_below", "-2147483648.75", true, -2147483648},
			b22SNRConversionCase{"float_lower_truncation_valid", "-2147483649.25", true, -2147483648},
			b22SNRConversionCase{"float_lower_truncation_invalid", "-2147483649.5", false, 0},
		)
	}
	// At 2^63 the float64 spacing exceeds 0.5. The original text can round
	// onto the lower endpoint; admission applies to the float's truncated result.
	return append(cases,
		b22SNRConversionCase{"integer_max", "9223372036854775807", true, 9223372036854775807},
		b22SNRConversionCase{"integer_min", "-9223372036854775808", true, -9223372036854775808},
		b22SNRConversionCase{"float_upper_below", "9223372036854774784.0", true, 9223372036854774784},
		b22SNRConversionCase{"float_upper_equal", "9223372036854775808.0", false, 0},
		b22SNRConversionCase{"float_upper_above", "9223372036854777856.0", false, 0},
		b22SNRConversionCase{"integer_max_as_float", "9223372036854775807.0", false, 0},
		b22SNRConversionCase{"float_lower_above", "-9223372036854774784.0", true, -9223372036854774784},
		b22SNRConversionCase{"float_lower_equal", "-9223372036854775808.0", true, -9223372036854775808},
		b22SNRConversionCase{"float_lower_below", "-9223372036854777856.0", false, 0},
		b22SNRConversionCase{"integer_text_rounds_to_lower", "-9223372036854775809", true, -9223372036854775808},
	)
}
