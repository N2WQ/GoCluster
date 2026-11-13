package telnet

import (
	"dxcluster/pathreliability"
	"strings"
	"testing"
)

func TestShowPropReportsActualZeroAndMixedSolarInputs(t *testing.T) {
	cfg := pathreliability.DefaultConfig()
	zero := testShowPropForecast(cfg, 12, -8, -8, 0)
	zero.SSN = 0
	target := showPropTarget{grid: "FN32", input: "FN32", source: "grid"}
	rows := []showPropBandRows{{band: "20m", mode: "FT8", records: []pathreliability.VOACAPCachedForecast{zero}, status: pathreliability.VOACAPForecastWindowReady}}
	text := formatShowPropResponse("FN31", target, showPropCommand{band: "20m"}, "FT8", "QUIET", 24, rows, cfg)
	if !strings.Contains(text, "ssn=0 hours=24") || strings.Contains(text, "ssn=mixed") {
		t.Fatalf("zero input missing: %s", text)
	}
	older, newer := zero, zero
	older.SSN, newer.SSN = 85, 95
	rows = []showPropBandRows{
		{band: "20m", mode: "FT8", records: []pathreliability.VOACAPCachedForecast{older}, status: pathreliability.VOACAPForecastWindowRefreshing},
		{band: "40m", mode: "FT8", records: []pathreliability.VOACAPCachedForecast{newer}, status: pathreliability.VOACAPForecastWindowReady},
	}
	text = formatShowPropResponse("FN31", target, showPropCommand{}, "FT8", "QUIET", 24, rows, cfg)
	for _, want := range []string{"ssn=mixed hours=24", "20m ssn=85", "40m ssn=95"} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in %s", want, text)
		}
	}
}
