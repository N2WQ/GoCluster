package spot

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadTaxonomyFileShippedConfig(t *testing.T) {
	taxonomy, err := LoadTaxonomyFile(filepath.Join("..", "data", "config", "spot_taxonomy.yaml"))
	if err != nil {
		t.Fatalf("LoadTaxonomyFile() error: %v", err)
	}
	if !taxonomy.IsSupportedFilterMode("FT8") {
		t.Fatalf("expected FT8 to be filter-visible")
	}
	if taxonomy.EventMaskForName("POTA") == 0 {
		t.Fatalf("expected POTA event mask")
	}
	if !taxonomy.ModeWantsBareReport("FT8") {
		t.Fatalf("expected shipped FT8 to accept bare numeric reports")
	}
	if taxonomy.ModeWantsBareReport("FT2") {
		t.Fatalf("expected shipped FT2 to preserve current no-bare-report behavior")
	}
}

func TestLoadTaxonomyFileWarnsUnknownKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spot_taxonomy.yaml")
	body := []byte(`
modes:
  - name: CW
    display: CW
    filter_visible: true
    comment_tokens: [CW]
    unexpected: true
events: []
`)
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatalf("write taxonomy: %v", err)
	}
	_, warnings, err := LoadTaxonomyFileWithWarnings(path)
	if err != nil {
		t.Fatalf("LoadTaxonomyFileWithWarnings() error: %v", err)
	}
	if !containsWarning(warnings, "unexpected") {
		t.Fatalf("expected warning to mention unknown key, got %#v", warnings)
	}
}

func TestBuildTaxonomyRejectsDuplicateModeNames(t *testing.T) {
	file := taxonomyFile{Modes: []ModeDefinition{
		{Name: "FT8", Display: "FT8"},
		{Name: "ft8", Display: "FT8"},
	}}
	if _, err := buildTaxonomy(file); err == nil {
		t.Fatalf("expected duplicate mode names to fail")
	}
}

func TestBuildTaxonomyRejectsDuplicateCommentTokensAcrossModes(t *testing.T) {
	file := taxonomyFile{Modes: []ModeDefinition{
		{Name: "FT8", Display: "FT8", CommentTokens: []string{"DATA"}},
		{Name: "FT4", Display: "FT4", CommentTokens: []string{"data"}},
	}}
	if _, err := buildTaxonomy(file); err == nil {
		t.Fatalf("expected duplicate comment token to fail")
	}
}

func TestBuildTaxonomyRejectsTooManyEvents(t *testing.T) {
	file := taxonomyFile{
		Modes:  []ModeDefinition{{Name: "UNKNOWN", Display: "UNKNOWN", Synthetic: true}},
		Events: make([]EventDefinition, maxTaxonomyEvents+1),
	}
	for i := range file.Events {
		file.Events[i] = EventDefinition{Name: "EV" + string(rune('A'+i%26)) + string(rune('A'+i/26))}
	}
	if _, err := buildTaxonomy(file); err == nil {
		t.Fatalf("expected more than 64 events to fail")
	}
}

func TestTaxonomyOnlyModeParsesWithoutCodeChange(t *testing.T) {
	old := CurrentTaxonomy()
	file := defaultTaxonomyFile()
	file.Modes = append(file.Modes, ModeDefinition{
		Name:          "HELL",
		Display:       "HELL",
		FilterVisible: true,
		CommentTokens: []string{"HELL"},
	})
	taxonomy, err := buildTaxonomy(file)
	if err != nil {
		t.Fatalf("build taxonomy: %v", err)
	}
	ConfigureTaxonomy(taxonomy)
	t.Cleanup(func() { ConfigureTaxonomy(old) })

	result := ParseSpotComment("CQ HELL TEST", 14070)
	if result.Mode != "HELL" {
		t.Fatalf("expected YAML-only HELL mode, got %q", result.Mode)
	}
	if !taxonomy.IsSupportedFilterMode("HELL") {
		t.Fatalf("expected YAML-only HELL mode to be filter-visible")
	}
}
