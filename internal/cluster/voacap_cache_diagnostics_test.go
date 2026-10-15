package cluster

import (
	"testing"

	"dxcluster/pathreliability"
)

func TestFormatVOACAPCacheIndexDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name  string
		index pathreliability.VOACAPForecastCacheIndexSnapshot
		want  string
	}{
		{"unattached", pathreliability.VOACAPForecastCacheIndexSnapshot{}, "n/a"},
		{"uninitialized", pathreliability.VOACAPForecastCacheIndexSnapshot{Attached: true}, "0/0/0 (M/R/E) ready=false dirty=false"},
		{"ready", pathreliability.VOACAPForecastCacheIndexSnapshot{Attached: true, Ready: true, Entries: 1234, RetentionEntries: 1234, EligibilityEntries: 1234}, "1,234/1,234/1,234 (M/R/E) ready=true dirty=false"},
		{"dirty", pathreliability.VOACAPForecastCacheIndexSnapshot{Attached: true, Dirty: true}, "0/0/0 (M/R/E) ready=false dirty=true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := fixedOverviewVOACAPFallbackProvider{snapshot: pathreliability.VOACAPClosedFallbackSnapshot{ForecastCacheIndex: tc.index}}
			want := "[yellow]VOACAP disk index[-]: " + tc.want
			if got := formatVOACAPCacheIndexLine(provider); got != want {
				t.Fatalf("cache diagnostics = %q, want %q", got, want)
			}
		})
	}
}
