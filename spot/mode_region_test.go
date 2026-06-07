package spot

import (
	"os"
	"path/filepath"
	"testing"
)

func TestClassifyRegionalMode(t *testing.T) {
	tests := []struct {
		name     string
		freq     float64
		region   IARURegion
		wantMode string
		wantProv ModeProvenance
	}{
		{name: "region1 cw safe", freq: 7011.0, region: IARURegion1, wantMode: "CW", wantProv: ModeProvenanceRegionalCW},
		{name: "region1 voice default", freq: 7090.0, region: IARURegion1, wantMode: "LSB", wantProv: ModeProvenanceRegionalVoiceDefault},
		{name: "region3 mixed segment", freq: 7035.0, region: IARURegion3, wantMode: "", wantProv: ModeProvenanceRegionalMixedBlank},
		{name: "unknown region conservative cw", freq: 14020.0, region: IARURegionUnknown, wantMode: "CW", wantProv: ModeProvenanceRegionalCW},
		{name: "unknown region blank outside cw", freq: 14150.0, region: IARURegionUnknown, wantMode: "", wantProv: ModeProvenanceRegionalUnknownBlank},
		{name: "60m remains mixed", freq: 5357.0, region: IARURegion2, wantMode: "", wantProv: ModeProvenanceRegionalMixedBlank},
		{name: "6m remains mixed", freq: 50110.0, region: IARURegion1, wantMode: "", wantProv: ModeProvenanceRegionalMixedBlank},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sp := NewSpot("K1ABC", "W1AAA", tc.freq, "")
			sp.DXMetadata.IARURegion = tc.region
			got := ClassifyRegionalMode(sp)
			if got.Mode != tc.wantMode || got.Provenance != tc.wantProv {
				t.Fatalf("ClassifyRegionalMode(%0.1f, %q) = (%q, %q), want (%q, %q)", tc.freq, tc.region, got.Mode, got.Provenance, tc.wantMode, tc.wantProv)
			}
		})
	}
}

func TestLoadIARUModeInferenceFileWarnsUnknownKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "iaru_mode_inference.yaml")
	data := []byte(`voice_mode_by_band:
  40m: LSB
unknown_region_cw_safe:
  40m:
    - from_khz: 7000
      to_khz: 7040
      class: cw_safe
regions:
  R1:
    40m:
      - from_khz: 7000
        to_khz: 7040
        class: cw_safe
unexpected: true
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	warnings, err := LoadIARUModeInferenceFileWithWarnings(path)
	if err != nil {
		t.Fatalf("LoadIARUModeInferenceFileWithWarnings() error: %v", err)
	}
	if !containsWarning(warnings, "unexpected") {
		t.Fatalf("expected warning to mention unknown key, got %#v", warnings)
	}
}
