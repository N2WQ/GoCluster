package spot

import (
	"strconv"
	"testing"
)

func TestCCV2LegacySNRGrammar(t *testing.T) {
	for _, tc := range []struct {
		text, mode, timestamp, comment string
		report                         int
		present                        bool
	}{
		{"CW -1250dB CQ", "CW", "", "-1250dB CQ", 0, false},
		{"CW 1.5dB CQ", "CW", "", "1.5dB CQ", 0, false},
		{"CW SNR:12dB CQ", "CW", "", "SNR:12dB CQ", 0, false},
		{"CW -201dB CQ", "CW", "", "-201dB CQ", 0, false},
		{"CW 201 dB CQ", "CW", "", "201 CQ", 0, false},
		{"CW -200dB CQ", "CW", "", "CQ", -200, true},
		{"CW +200 dB CQ", "CW", "", "CQ", 200, true},
		{"CW 0 dB CQ", "CW", "", "CQ", 0, true},
		{"1200Z-14dB CW 22 WPM CQ", "CW", "1200Z", "22 WPM CQ", -14, true},
		{"RTTY +9dB, 45 BPS CQ", "RTTY", "", "45 BPS CQ", 9, true},
		{"FT8 -14 CQ", "FT8", "", "CQ", -14, true},
		{"CW 73 CQ", "CW", "", "73 CQ", 0, false},
		{"CW 73 dB CQ", "CW", "", "CQ", 73, true},
	} {
		t.Run(tc.text, func(t *testing.T) {
			got, trace := ParseSpotCommentWithDisplayTrace(tc.text, 14020)
			want := CommentParseResult{Mode: tc.mode, TimeToken: tc.timestamp, Comment: tc.comment, Report: tc.report, HasReport: tc.present}
			if got != want || ParseSpotComment(tc.text, 14020) != want {
				t.Fatalf("parse=%+v, want %+v", got, want)
			}
			if len(trace) == 0 {
				t.Fatal("display trace disappeared")
			}
		})
	}
}

// This oracle constructs only whole integer tokens or deliberately malformed
// tokens. It never calls the production numeric/regex/token recognition helpers.
func FuzzCCV2LegacySNRGrammar(f *testing.F) {
	for _, n := range []int64{-1250, -201, -200, -1, 0, 1, 200, 201, 1250} {
		f.Add(n)
	}
	f.Fuzz(func(t *testing.T, n int64) {
		text := strconv.FormatInt(n, 10)
		for _, separator := range []string{"", " "} {
			got := ParseSpotComment("CW "+text+separator+"dB CQ", 14020)
			present := n >= -200 && n <= 200
			if got.HasReport != present || present && int64(got.Report) != n {
				t.Fatalf("integer %s separator %q: %+v", text, separator, got)
			}
		}
		for _, malformed := range []string{"X" + text + "dB", text + ".5dB", text + "dBX"} {
			if got := ParseSpotComment("CW "+malformed+" CQ", 14020); got.HasReport {
				t.Fatalf("malformed token %q fabricated %+v", malformed, got)
			}
		}
	})
}
