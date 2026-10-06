package telnet

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/filter"
	"dxcluster/spot"
)

func TestHumanNearbyRestoredGridLossless(t *testing.T) {
	requireH3Mappings(t)
	for _, tc := range []struct {
		name, grid, firstLine string
	}{
		{"ordinary", "FN31PR", "Nearby        On; grid FN31PR\r\n"},
		{"unicode", "FN31PRé", "Nearby        On; grid \"FN31PR\\u00e9\"\r\n"},
		{"control", "FN31PR\tX", "Nearby        On; grid \"FN31PR\\tX\"\r\n"},
		{"long ASCII", "FN31PR" + strings.Repeat("X", 100), "Nearby        On; grid \""},
		{"long escaped", "FN31PR" + strings.Repeat("é\t \"\\", 30) + "  ", "Nearby        On; grid \""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := presetTestServer(t)
			f := filter.NewFilter()
			f.NearbyEnabled = true
			record := &filter.UserRecord{
				ConfigurationVersion: filter.CurrentConfigurationVersion,
				Filter:               *f, Dialect: "go", Grid: tc.grid,
			}
			if err := filter.SaveUserRecord("W1ABC-1", record); err != nil {
				t.Fatal(err)
			}
			c := configurationTestClient(s, "W1ABC-1")
			restored, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(time.Minute))
			if err != nil || restored.loadError != nil {
				t.Fatalf("restore failed: %v; record: %v", err, restored.loadError)
			}
			if c.configuredSettings.Grid != tc.grid || c.pathSnapshot().grid != tc.grid || !c.nearbyFilterInEffect() {
				t.Fatal("retained grid lost bytes or did not prepare usable NEARBY cells")
			}
			path := filepath.Join(filter.UserDataDir, "W1ABC-1.yaml")
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, command := range []string{"SHOW FILTER", "SHOW FILTER FULL", "SHOW FILTER NEARBY"} {
				t.Run(command, func(t *testing.T) {
					if !s.handleHumanReadback(c, command) {
						t.Fatal("readback not handled")
					}
					got := string((<-c.controlChan).raw)
					assertHumanWire(t, got)
					if !strings.Contains(got, tc.firstLine) {
						t.Fatalf("retained grid not rendered losslessly:\n%s", got)
					}
					if tc.name == "ordinary" {
						return
					}
					// Isolate the grid row and its continuation pieces from other
					// exact values, then use the standard-library unquote oracle.
					lines := strings.Split(got, "\r\n")
					for i, line := range lines {
						if !strings.HasPrefix(line, "Nearby        On; grid ") {
							continue
						}
						row := line + "\r\n"
						for _, piece := range lines[i+1:] {
							if !strings.HasPrefix(piece, "                       + ") {
								break
							}
							row += piece + "\r\n"
						}
						if decoded := unquoteHumanPieces(t, row); decoded != tc.grid {
							t.Fatalf("grid bytes changed: %q != %q", decoded, tc.grid)
						}
					}
				})
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) || c.configuredSettings.Grid != tc.grid || c.pathSnapshot().grid != tc.grid {
				t.Fatal("readback changed retained grid preferences")
			}
		})
	}
}

func TestHumanNearbyEscapedExpansionBudget(t *testing.T) {
	s, c := readbackTestClient()
	c.grid = "FN31PR" + strings.Repeat("é", 12000)
	c.filter.NearbyEnabled = true
	c.filter.NearbyUserFine, c.filter.NearbyUserCoarse = 1, 2
	for _, category := range []string{"", "FULL", "NEARBY"} {
		got, err := s.renderHumanReadback(c, "FILTER", category, time.Second)
		if got != "" || !errors.Is(err, errReadbackTooLarge) {
			t.Fatalf("%q escaped grid exceeded the budget without a size error: %v", category, err)
		}
	}
}

func TestHumanSummaryDenialsWin(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		set        func(*filter.Filter)
	}{
		{"band overlap", "Bands         None\r\n", func(f *filter.Filter) {
			f.AllBands = true
			f.Bands, f.BlockBands = map[string]bool{"20m": true}, map[string]bool{"20m": true}
		}},
		{"DX calls wildcard", "DX calls      None\r\n", func(f *filter.Filter) {
			f.DXCallsigns, f.BlockDXCallsigns = []string{"K1*"}, []string{"*"}
		}},
		{"DE calls wildcard", "DE calls      None\r\n", func(f *filter.Filter) {
			f.DECallsigns, f.BlockDECallsigns = []string{"W1*"}, []string{"*"}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c := readbackTestClient()
			c.filter = filter.NewFilter()
			candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
			if !c.filter.Matches(candidate) {
				t.Fatal("unrestricted fixture unexpectedly rejects the spot")
			}
			tc.set(c.filter)
			got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
			if err != nil || !strings.Contains(got, tc.want) {
				t.Fatalf("deny-wins summary missing: %v\n%s", err, got)
			}
			assertHumanWire(t, got)
			if c.filter.Matches(candidate) {
				t.Fatal("summary disagrees with matcher denial")
			}
		})
	}
}

func TestHumanQuotedPiecesDocumentationExample(t *testing.T) {
	value := "  W1ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ café\t\"Q\"\\end  "
	var h humanResponse
	if err := writeHumanPatterns(&h, "allow", []string{value}); err != nil {
		t.Fatal(err)
	}
	want := "  allow:\r\n    [1] \"  W1ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ c\"\r\n        + \"af\\u00e9\\t\\\"Q\\\"\\\\end  \"\r\n"
	if string(h.data) != want || unquoteHumanPieces(t, want) != value {
		t.Fatalf("documented pieces differ or lose stored bytes: %q", h.data)
	}
	assertHumanWire(t, string(h.data))
}

func TestHumanRestoredUnreachableSelections(t *testing.T) {
	for _, tc := range []struct {
		category, key, want string
		set                 func(*filter.Filter, string)
	}{
		{"SOURCE", "human", "Sources       None\r\n", func(f *filter.Filter, key string) { f.Sources = map[string]bool{key: true} }},
		{"MODE", "cw", "Modes         None; unknown modes hidden\r\n", func(f *filter.Filter, key string) { f.Modes = map[string]bool{key: true} }},
		{"BAND", "20M", "Bands         None\r\n", func(f *filter.Filter, key string) { f.Bands = map[string]bool{key: true} }},
		{"BAND", "20meters", "Bands         None\r\n", func(f *filter.Filter, key string) { f.Bands = map[string]bool{key: true} }},
		{"DXCONT", "na", "Continents: None", func(f *filter.Filter, key string) { f.DXContinents = map[string]bool{key: true} }},
		{"DECONT", "eu", "DE geography  Continents: None", func(f *filter.Filter, key string) { f.DEContinents = map[string]bool{key: true} }},
		{"DXGRID2", "fn", "Grids: None", func(f *filter.Filter, key string) { f.DXGrid2Prefixes = map[string]bool{key: true} }},
		{"DEGRID2", "JO31", "Grids: None", func(f *filter.Filter, key string) { f.DEGrid2Prefixes = map[string]bool{key: true} }},
		{"CONFIDENCE", "p", "Confidence    None; exempt modes still pass\r\n", func(f *filter.Filter, key string) { f.Confidence = map[string]bool{key: true} }},
	} {
		t.Run(tc.category+"/"+tc.key, func(t *testing.T) {
			s := presetTestServer(t)
			f := filter.NewFilter()
			f.ResetModes()
			tc.set(f, tc.key)
			cfg := filter.ConfigurationFromFilter(f, filter.SettingsConfiguration{Dialect: "go"})
			if err := filter.SaveConfiguration("W1ABC-1", cfg, nil, nil); err != nil {
				t.Fatal(err)
			}
			c := configurationTestClient(s, "W1ABC-1")
			if _, err := s.restoreAndRegisterClient(c, time.Now().UTC(), time.Now().Add(time.Minute)); err != nil {
				t.Fatal(err)
			}
			overview, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
			if err != nil || !strings.Contains(overview, tc.want) {
				t.Fatalf("unreachable restored selection misdescribed: %v\n%s", err, overview)
			}
			assertHumanWire(t, overview)
			detail, err := s.renderHumanReadback(c, "FILTER", tc.category, time.Second)
			if err != nil || !strings.Contains(detail, "    "+strconv.Quote(tc.key)+": true\r\n") {
				t.Fatal("exact view lost unreachable saved key")
			}
			candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, "FT8")
			candidate.IsHuman, candidate.Confidence = true, "P"
			candidate.DXMetadata.Continent, candidate.DEMetadata.Continent = "NA", "EU"
			candidate.DXMetadata.Grid, candidate.DEMetadata.Grid = "FN31", "JO31"
			candidate.EnsureNormalized()
			if tc.category == "CONFIDENCE" && spot.IsConfidenceFilterExemptMode(candidate.Mode) {
				t.Fatal("confidence oracle candidate is exempt")
			}
			if c.filter.Matches(candidate) {
				t.Fatal("matcher unexpectedly accepts unreachable allow key")
			}
		})
	}
}

func TestHumanRestoredUnreachableDenials(t *testing.T) {
	s, c := readbackTestClient()
	c.filter = filter.NewFilter()
	c.filter.ResetModes()
	c.filter.BlockBands = map[string]bool{"20M": true}
	c.filter.BlockModes = map[string]bool{"cw": true}
	c.filter.BlockSources = map[string]bool{"human": true, "UNSUPPORTED": true}
	c.filter.BlockDXContinents = map[string]bool{"na": true}
	c.filter.BlockDEGrid2 = map[string]bool{"fn": true, "FN31": true}
	c.filter.BlockConfidence = map[string]bool{"p": true}
	got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
	if err != nil || strings.Contains(got, "except") || strings.Contains(got, "blocked") {
		t.Fatalf("unreachable denial shown as active: %v\n%s", err, got)
	}
	assertHumanWire(t, got)
	candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
	candidate.IsHuman, candidate.Confidence = true, "P"
	candidate.DXMetadata.Continent, candidate.DEMetadata.Grid = "NA", "FN31"
	candidate.EnsureNormalized()
	if !c.filter.Matches(candidate) {
		t.Fatal("unreachable denial unexpectedly restricts matcher")
	}
	c.filter.BlockSources = map[string]bool{"HUMAN": true, "SKIMMER": true}
	got, err = s.renderHumanReadback(c, "FILTER", "", time.Second)
	if err != nil || !strings.Contains(got, "Sources       None\r\n") {
		t.Fatal("blocking both source classes failed to show None")
	}
}

func TestHumanEmptyBlockRejectsUnknownTokens(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		set        func(*filter.Filter, *spot.Spot)
	}{
		{"grid", "Grids: All except \"\"", func(f *filter.Filter, _ *spot.Spot) { f.BlockDXGrid2 = map[string]bool{"": true} }},
		{"band", "Bands         All except \"\"\r\n", func(f *filter.Filter, s *spot.Spot) {
			f.BlockBands = map[string]bool{"": true}
			s.Band, s.BandNorm = "", ""
		}},
		{"confidence", "Confidence    All except \"\"; exempt modes still pass\r\n", func(f *filter.Filter, s *spot.Spot) {
			f.BlockConfidence = map[string]bool{"": true}
			s.Confidence = "invalid"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c := readbackTestClient()
			c.filter = filter.NewFilter()
			c.filter.ResetModes()
			candidate := spot.NewSpot("K1ABC", "W1XYZ", 14074, "FT8")
			tc.set(c.filter, candidate)
			got, err := s.renderHumanReadback(c, "FILTER", "", time.Second)
			if err != nil || !strings.Contains(got, tc.want) {
				t.Fatalf("active unknown-token block hidden: %v\n%s", err, got)
			}
			assertHumanWire(t, got)
			if c.filter.Matches(candidate) {
				t.Fatal("empty block unexpectedly admits unknown token")
			}
		})
	}
}

func BenchmarkHumanMapPreviewBoundedPreparation(b *testing.B) {
	for _, count := range []int{100, 10000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			rules := filter.StringRules{AllowAll: true, Allow: make(map[string]bool, count)}
			for i := range count {
				rules.Allow[fmt.Sprintf("N%05d", i)] = true
			}
			want := fmt.Sprintf("Only %d entries", count)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if got := humanRuleSummary(rules, "entries", 64); got != want {
					b.Fatal("incorrect bounded map preview")
				}
			}
		})
	}
}
