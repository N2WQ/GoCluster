package spot

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveIARURegionUsesContinentDefaultsAndOverrides(t *testing.T) {
	tests := []struct {
		name      string
		adif      int
		continent string
		want      IARURegion
	}{
		{name: "eu default", adif: 54, continent: "EU", want: IARURegion1},
		{name: "na default", adif: 291, continent: "NA", want: IARURegion2},
		{name: "asia default", adif: 324, continent: "AS", want: IARURegion3},
		{name: "asiatic russia override", adif: 15, continent: "AS", want: IARURegion1},
		{name: "chagos override", adif: 33, continent: "AF", want: IARURegion3},
		{name: "palestine override", adif: 510, continent: "AS", want: IARURegion1},
		{name: "unknown when metadata missing", adif: 0, continent: "", want: IARURegionUnknown},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveIARURegion(tc.adif, tc.continent); got != tc.want {
				t.Fatalf("ResolveIARURegion(%d, %q) = %q, want %q", tc.adif, tc.continent, got, tc.want)
			}
		})
	}
}

func TestLoadIARURegionsFileWarnsUnknownKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iaru_regions.yaml")
	data := []byte(`defaults_by_continent:
  EU: R1
adif_overrides:
  291: R2
unexpected: true
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	warnings, err := LoadIARURegionsFileWithWarnings(path)
	if err != nil {
		t.Fatalf("LoadIARURegionsFileWithWarnings() error: %v", err)
	}
	if !containsWarning(warnings, "unexpected") {
		t.Fatalf("expected warning to mention unknown key, got %#v", warnings)
	}
}

func containsWarning(messages []string, needle string) bool {
	for _, message := range messages {
		if strings.Contains(message, needle) {
			return true
		}
	}
	return false
}
