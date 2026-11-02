package spot

import "testing"

func TestCommentDisplayTraceRetainsCanonicalDecisions(t *testing.T) {
	tests := []struct {
		input   string
		removed map[string]bool
		peeled  map[string]string
	}{
		{
			input:   "CW +22 WPM 5 dB 0318Z",
			removed: map[string]bool{"CW": true, "+22": false, "WPM": false, "5": true, "dB": true, "0318Z": true},
			peeled:  map[string]string{"0318Z": ""},
		},
		{
			input:   "CW 20 WPM 30 WPM 5 dB",
			removed: map[string]bool{"CW": true, "20": false, "WPM": false, "30": true, "5": false, "dB": true},
		},
		{
			input:   "CW 0318Z22 WPM 5 dB",
			removed: map[string]bool{"CW": true, "0318Z22": false, "WPM": false, "5": true, "dB": true},
			peeled:  map[string]string{"0318Z22": "22"},
		},
		{
			input:   "CW é +22 WPM 5 dB 0318Z",
			removed: map[string]bool{"CW": true, "é": false, "+22": false, "WPM": false, "5": true, "dB": true, "0318Z": true},
			peeled:  map[string]string{"0318Z": ""},
		},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, trace := ParseSpotCommentWithDisplayTrace(tc.input, 3547)
			if want := legacyCommentParse(tc.input, 3547); got != want {
				t.Fatalf("canonical result changed: got %+v, want %+v", got, want)
			}
			seen := make(map[string]bool)
			for _, token := range trace {
				if token.Start < 0 || token.End > len(tc.input) || token.Start >= token.End {
					t.Fatalf("invalid token offsets: %+v", token)
				}
				original := tc.input[token.Start:token.End]
				want, ok := tc.removed[original]
				if !ok {
					t.Fatalf("unexpected trace token %q", original)
				}
				if token.Removed != want {
					t.Errorf("%q removed = %v, want %v", original, token.Removed, want)
				}
				if remainder, ok := tc.peeled[original]; token.TimePeeled != ok || ok && token.TimeRemainder != remainder {
					t.Errorf("%q time peel = %v/%q, want %v/%q", original, token.TimePeeled, token.TimeRemainder, ok, remainder)
				}
				seen[original] = true
			}
			for token := range tc.removed {
				if !seen[token] {
					t.Errorf("missing trace token %q", token)
				}
			}
		})
	}
}
