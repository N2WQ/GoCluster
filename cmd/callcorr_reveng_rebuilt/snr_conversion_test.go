// File role: Locks CSV report conversion to finite, representable values while
// preserving Atoi-first admission and the historical int(v + 0.5) rounding.
package main

import (
	"math"
	"math/big"
	"strconv"
	"strings"
	"testing"
)

func TestB22CSVReportRepresentability(t *testing.T) {
	header := mustParseTestRBNHeader(t)
	for _, tc := range b22SNRConversionCases() {
		t.Run(tc.name, func(t *testing.T) {
			row, status := parseRBNRecord(b22SNRRecord(tc.raw), header)
			wantStatus := rbnRecordInvalid
			if tc.accept {
				wantStatus = rbnRecordAccepted
			}
			if status != wantStatus {
				t.Fatalf("parseRBNRecord(%q) status=%d, want %d; row=%+v", tc.raw, status, wantStatus, row)
			}
			if !tc.accept {
				if row != (rbnRow{}) {
					t.Fatalf("rejected report retained a partial row: %+v", row)
				}
				return
			}
			if int64(row.Report) != tc.want {
				t.Fatalf("parseRBNRecord(%q) report=%d, want %d", tc.raw, row.Report, tc.want)
			}
			if row.DXCall != "OZ4ADX" || row.Spotter != "K1ABC" || row.Mode != "CW" || row.Band != "80m" {
				t.Fatalf("accepted report changed row fields: %+v", row)
			}
		})
	}
}

func TestB22CSVReportPreservesRowClassification(t *testing.T) {
	header := mustParseTestRBNHeader(t)
	var accepted, invalid, skipped int
	for _, tc := range []struct {
		raw   string
		class string
		mode  string
		want  rbnRecordStatus
	}{
		{"NaN", "CQ", "CW", rbnRecordInvalid},
		{"0", "CQ", "CW", rbnRecordAccepted},
		{"", "CQ", "CW", rbnRecordInvalid},
		{"-12", "CQ", "CW", rbnRecordAccepted},
		{"NaN", "DX", "CW", rbnRecordSkipped},
		{"NaN", "CQ", "SSB", rbnRecordSkipped},
	} {
		record := b22SNRRecord(tc.raw)
		record[6], record[7] = tc.class, tc.mode
		row, status := parseRBNRecord(record, header)
		if status != tc.want {
			t.Fatalf("report/class/mode=%q/%q/%q: status=%d, want %d", tc.raw, tc.class, tc.mode, status, tc.want)
		}
		switch status {
		case rbnRecordAccepted:
			accepted++
		case rbnRecordInvalid:
			invalid++
		case rbnRecordSkipped:
			skipped++
		}
		if status != rbnRecordAccepted && row != (rbnRow{}) {
			t.Fatalf("nonaccepted row was not empty: %+v", row)
		}
	}
	if accepted != 2 || invalid != 2 || skipped != 2 {
		t.Fatalf("accepted/invalid/skipped=%d/%d/%d, want 2/2/2", accepted, invalid, skipped)
	}
}

func FuzzB22CSVReportConversion(f *testing.F) {
	for _, tc := range b22SNRConversionCases() {
		f.Add(tc.raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 512 {
			t.Skip("report exceeds bounded record fixture")
		}
		header := mustParseTestRBNHeader(t)
		row, status := parseRBNRecord(b22SNRRecord(raw), header)
		want, accept := b22SNRReference(raw)
		wantStatus := rbnRecordInvalid
		if accept {
			wantStatus = rbnRecordAccepted
		}
		if status != wantStatus || (accept && int64(row.Report) != want) {
			t.Fatalf("report=%q status/value=%d/%d, want %d/%d", raw, status, row.Report, wantStatus, want)
		}
		if !accept && row != (rbnRow{}) {
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

func b22SNRRecord(report string) []string {
	return []string{"K1ABC", "3547.0", "80m", "OZ4ADX", report, "2026-04-26 03:18:00", "CQ", "CW"}
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
