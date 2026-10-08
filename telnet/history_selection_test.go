package telnet

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dxcluster/archive"
	"dxcluster/filter"
	"dxcluster/spot"
)

func TestHistoryConfiguredModeAliasesPreserveNext(t *testing.T) {
	// Taxonomy installation is process-wide; keep this test sequential and
	// restore the startup snapshot before the remaining session tests run.
	path := filepath.Join(t.TempDir(), "taxonomy.yaml")
	definition := "modes:\n  - name: CW\n    filter_visible: true\n    default_filter_allowed: true\n    variants: [BAND, MODE]\n  - name: FT8\n    filter_visible: true\n    default_filter_allowed: true\nevents: []\n"
	if err := os.WriteFile(path, []byte(definition), 0600); err != nil {
		t.Fatal(err)
	}
	taxonomy, err := spot.LoadTaxonomyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	previous := spot.CurrentTaxonomy()
	spot.ConfigureTaxonomy(taxonomy)
	t.Cleanup(func() { spot.ConfigureTaxonomy(previous) })
	first := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	second := spot.NewSpot("K1ABC", "W1AAA", 7030, "CW")
	excluded := spot.NewSpot("K1ABC", "W1AAA", 14074, "FT8")
	position := []byte("configured-alias-position")
	pages := 0
	s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		pages++
		if req.Limit != 1 || !req.Match(first) || !req.Match(second) || req.Match(excluded) {
			t.Fatal("configured alias selected the wrong modes or was lost by NEXT")
		}
		if pages == 1 {
			return archive.HistoryPage{Spots: []*spot.Spot{first}, Before: position, End: archive.HistoryCountReached}, nil
		}
		if !bytes.Equal(req.Before, position) {
			t.Fatalf("configured NEXT changed position: %q", req.Before)
		}
		return archive.HistoryPage{Spots: []*spot.Spot{second}, End: archive.HistoryExhausted}, nil
	})
	digest := c.historyFilterDigest()
	text := runHistory(t, s, c, "SHOW DX 1 MODE MODE, BAND BAND 20,40")
	if !strings.Contains(text, first.FormatDXCluster()) || c.historySearch == nil {
		t.Fatalf("configured alias search failed: %q", text)
	}
	token := c.historySearch.token
	for _, line := range []string{
		"SHOW DX MODE MODE FT8", "SHOW DX MODE MODE MODE FT8",
		"SHOW DX MODE CW,MODE FT8", "SHOW DX MODE MODE BAND 20 40",
	} {
		if text := runHistory(t, s, c, line); !strings.HasPrefix(text, "Invalid") || c.historySearch == nil || c.historySearch.token != token || pages != 1 || c.historyFilterDigest() != digest {
			t.Fatalf("invalid configured list changed cursor, scanned or mutated filters: %s: %q", line, text)
		}
	}
	if text := runHistory(t, s, c, "SHOW DX NEXT "+token); !strings.Contains(text, second.FormatDXCluster()) || pages != 2 || c.historySearch != nil || c.historyFilterDigest() != digest {
		t.Fatalf("configured NEXT lost selections or changed filters: %q", text)
	}
}

func TestHistoryBandModeSelectionsRetainedByNextAndSelf(t *testing.T) {
	first := spot.NewSpot("N2WQ", "W1AAA", 14030, "CW")
	second := spot.NewSpot("N2WQ", "W1AAA", 7074, "FT8")
	first.Comment, second.Comment = "POTA up:5!", "POTA up:5!"
	wrongBand := spot.NewSpot("N2WQ", "W1AAA", 21030, "CW")
	wrongMode := spot.NewSpot("N2WQ", "W1AAA", 14080, "RTTY")
	wrongComment := spot.NewSpot("N2WQ", "W1AAA", 14031, "CW")
	wrongBand.Comment, wrongMode.Comment, wrongComment.Comment = first.Comment, first.Comment, "SOTA"
	position := []byte("selected-position")
	pages := 0
	s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		pages++
		if req.Limit != 1 || !req.Match(first) || !req.Match(second) || req.Match(wrongBand) || req.Match(wrongMode) || req.Match(wrongComment) {
			t.Fatal("explicit selections were lost or bypassed by the self exception")
		}
		if pages == 1 {
			return archive.HistoryPage{Spots: []*spot.Spot{first}, Before: position, End: archive.HistoryCountReached}, nil
		}
		if !bytes.Equal(req.Before, position) {
			t.Fatalf("NEXT changed position: %q", req.Before)
		}
		return archive.HistoryPage{Spots: []*spot.Spot{second}, End: archive.HistoryExhausted}, nil
	})
	c.filter.BlockAllBands, c.filter.BlockAllModes = true, true
	digest := c.historyFilterDigest()
	text := runHistory(t, s, c, "SHOW DX 1 BAND 20,40 MODE CW,FT8 COMMENT pota up:5!")
	if !strings.Contains(text, first.FormatDXCluster()) || c.historySearch == nil || c.historyFilterDigest() != digest {
		t.Fatalf("first selected page failed or mutated preferences: %q", text)
	}
	token := c.historySearch.token
	for _, invalid := range []string{
		"SHOW DX BAND 20,BOGUS", "SHOW DX MODE CW,BOGUS", "SHOW DX BAND 20 BAND 40",
		"SHOW DX MODE CW MODE FT8", "SHOW DX BAND", "SHOW DX MODE NONE",
		"SHOW DX BAND 20 40", "SHOW DX MODE CW FT8", "SHOW DX BAND 20,40 MODE CW FT8",
		"SHOW DX MODE CW,FT8 BAND 20 40",
		"SHOW DX NEXT " + token + " MODE CW",
	} {
		if text := runHistory(t, s, c, invalid); !strings.HasPrefix(text, "Invalid") || c.historySearch == nil || c.historySearch.token != token || pages != 1 || c.historyFilterDigest() != digest {
			t.Fatalf("invalid command replaced query, scanned or mutated filters: %s: %q", invalid, text)
		}
	}
	if text := runHistory(t, s, c, "SHOW MYDX NEXT "+token); !strings.Contains(text, second.FormatDXCluster()) || pages != 2 || c.historySearch != nil || c.historyFilterDigest() != digest {
		t.Fatalf("NEXT changed selection or preferences: %q", text)
	}
}

func TestHistoryBandModeNarrowSavedFilters(t *testing.T) {
	for _, domain := range []string{"BAND", "MODE"} {
		t.Run(domain, func(t *testing.T) {
			wanted := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
			s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
				if req.Match(wanted) {
					t.Fatal("explicit query overrode the saved category block")
				}
				return archive.HistoryPage{End: archive.HistoryExhausted}, nil
			})
			if domain == "BAND" {
				c.filter.SetBand("20m", false)
			} else {
				c.filter.SetMode("CW", false)
			}
			digest := c.historyFilterDigest()
			if text := runHistory(t, s, c, "SHOW DX K1ABC BAND 20 MODE CW"); text != "No matching retained spots.\n" || c.historyFilterDigest() != digest {
				t.Fatalf("saved filters or response changed: %q", text)
			}
		})
	}
}

func TestHistoryBandModePendingPageInvalidatedAfterRestore(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	wanted := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		if !req.Match(wanted) {
			t.Error("initial selected fixture rejected")
		}
		close(entered)
		<-release
		if !req.Match(wanted) {
			t.Error("pending page borrowed mutable filters or query values")
		}
		return archive.HistoryPage{Spots: []*spot.Spot{wanted}, Before: []byte("p"), End: archive.HistoryCountReached}, nil
	})
	done := make(chan bool, 1)
	go func() { _, proceed := s.handleHistoryCommand(c, "SHOW DX 1 BAND 20 MODE CW"); done <- proceed }()
	<-entered
	for _, enabled := range []bool{false, true} {
		s.runPreferenceCommand(c, "fixture", func(c *Client, _ string) (string, bool) {
			c.updateFilter(func(f *filter.Filter) { f.SetMode("CW", enabled) })
			return "", true
		})
	}
	close(release)
	if !<-done {
		t.Fatal("restart response could not be published")
	}
	if response := <-c.controlChan; response.line != historyRestart || c.historySearch != nil {
		t.Fatalf("stale selected page published after restoration: %q", response.line)
	}
}

func TestHistoryBandModeCursorReplacementAndClose(t *testing.T) {
	wanted := spot.NewSpot("K1ABC", "W1AAA", 14030, "CW")
	other := spot.NewSpot("K1ABC", "W1AAA", 7074, "FT8")
	s, c := historyTestSession(func(req archive.HistoryRequest) (archive.HistoryPage, error) {
		if !req.Match(wanted) || req.Match(other) {
			t.Fatal("new cursor accumulated an earlier selection")
		}
		return archive.HistoryPage{Before: []byte("p"), End: archive.HistoryBudgetReached}, nil
	})
	var retired string
	c.filter.ResetModes() // Both CW and FT8 must be visible during replacement.
	for i := range 100 {
		command := "SHOW DX BAND 20 MODE CW"
		if i%2 != 0 {
			command = "SHOW DX BAND 40 MODE FT8"
		}
		runHistory(t, s, c, command)
		if c.historySearch == nil || c.historySearch.token == retired {
			t.Fatal("fresh search failed to replace the cursor")
		}
		if retired != "" {
			if text := runHistory(t, s, c, "SHOW DX NEXT "+retired); !strings.Contains(text, "Invalid history continuation") {
				t.Fatalf("retired search remained reachable: %q", text)
			}
		}
		retired = c.historySearch.token
		wanted, other = other, wanted
	}
	c.invalidateHistory()
	if c.historySearch != nil || !c.historyClosed {
		t.Fatal("close retained selected query")
	}
}
