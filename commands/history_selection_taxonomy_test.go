package commands

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"
)

func TestHistoryBandModeConfiguredAliases(t *testing.T) {
	// Use the real loader and restore its process-wide snapshot before other
	// tests run. This test must not run in parallel with taxonomy consumers.
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
	rows := []*spot.Spot{
		spot.NewSpot("K1ABC", "W1AAA", 14030, "CW"),
		spot.NewSpot("K1ABC", "W1AAA", 14074, "FT8"),
		spot.NewSpot("K1ABC", "W1AAA", 7030, "CW"),
	}
	p := NewProcessor(nil, &fakeArchive{spots: rows}, nil, nil, nil, nil)
	for _, prefix := range []string{"SHOW DX", "SH DX", "SHOW MYDX", "SH MYDX", "SHOW/DX", "SH/DX"} {
		for _, tc := range []struct {
			args  string
			modes []string
			want  []int
		}{
			{"MODE BAND", []string{"CW"}, []int{0, 2}},
			{"MODE MODE", []string{"CW"}, []int{0, 2}},
			{"MODE band,", []string{"CW"}, []int{0, 2}},
			{"MODE mode,", []string{"CW"}, []int{0, 2}},
			{"MODE CW, BAND, MODE, FT8", []string{"CW", "FT8"}, []int{0, 1, 2}},
			{"MODE FT8 , MODE , BAND", []string{"FT8", "CW"}, []int{0, 1, 2}},
			{"MODE BAND BAND 20", []string{"CW"}, []int{0}},
			{"MODE MODE BAND 20,40", []string{"CW"}, []int{0, 2}},
			{"BAND 20 MODE MODE,FT8", []string{"CW", "FT8"}, []int{0, 1}},
			{"MODE BAND,FT8 BAND 40", []string{"CW", "FT8"}, []int{2}},
		} {
			line := prefix + " " + tc.args
			command, handled, text := p.ParseHistoryCommand(line, "cc")
			if !handled || text != "" || !reflect.DeepEqual(command.Query.modes, tc.modes) {
				t.Fatalf("%s: %+v handled=%v text=%q", line, command, handled, text)
			}
			page, err := p.ReadHistoryPage(command.Query, nil, func(*spot.Spot) bool { return true }, time.Now(), nil)
			if err != nil || len(page.Spots) != len(tc.want) {
				t.Fatalf("%s: got %+v %v want rows %v", line, page, err, tc.want)
			}
			for i, index := range tc.want {
				if page.Spots[i] != rows[index] {
					t.Fatalf("%s: row %d got %p want %p", line, i, page.Spots[i], rows[index])
				}
			}
		}
	}
	for _, args := range []string{
		"MODE CW MODE FT8", "MODE MODE MODE FT8", "MODE CW BAND 20 MODE FT8",
		"MODE CW FT8", "MODE BAND FT8", "MODE CW, MODE FT8", "MODE CW MODE,FT8",
		"BAND 20 40 MODE MODE", "MODE MODE BAND 20 40", "MODE CW,BOGUS",
	} {
		command, handled, text := p.ParseHistoryCommand("SHOW DX "+args, "go")
		if !handled || !strings.HasPrefix(text, "Invalid ") || !reflect.DeepEqual(command.Query, HistoryQuery{}) {
			t.Fatalf("invalid configured request %q: %+v handled=%v text=%q", args, command, handled, text)
		}
	}
}
