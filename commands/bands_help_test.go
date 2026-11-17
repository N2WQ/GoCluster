package commands

import (
	"slices"
	"strings"
	"testing"

	"dxcluster/spot"
)

func TestMetaBandHelpDiscoveryAndMembership(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	var previous string
	for _, dialect := range []string{"go", "cc"} {
		text := p.ProcessCommandForClient("help bands", "N0CALL", "", nil, dialect)
		assertHelpWidth(t, text)
		if previous != "" && text != previous {
			t.Fatal("the two styles must discover the same shared band catalog")
		}
		previous = text
		for _, band := range spot.SupportedBandNames() {
			if !strings.Contains(text, band) {
				t.Fatalf("%s band HELP omitted physical band %q", dialect, band)
			}
		}
		for _, group := range spot.SupportedMetaBands() {
			start := strings.Index(text, "\n  "+group.Name+" ")
			if start < 0 {
				t.Fatalf("%s band HELP omitted meta band %q", dialect, group.Name)
			}
			rows := strings.Split(text[start+1:], "\n")
			members := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(rows[0]), group.Name))
			for _, row := range rows[1:] {
				if !strings.HasPrefix(row, "             ") {
					break
				}
				members += ", " + strings.TrimSpace(row)
			}
			got := strings.FieldsFunc(members, func(r rune) bool { return r == ',' || r == ' ' })
			if !slices.Equal(got, group.Members) {
				t.Fatalf("%s HELP %s members = %v, want %v", dialect, group.Name, got, group.Members)
			}
		}
		for _, topic := range []string{"HELP COMMANDS", "HELP FILTERS", "HELP SPOTS"} {
			navigation := p.ProcessCommandForClient(topic, "N0CALL", "", nil, dialect)
			if !strings.Contains(navigation, "BANDS") {
				t.Fatalf("%s %s does not discover band HELP", dialect, topic)
			}
		}
	}
}

func TestMetaBandHelpExamplesAndReplacementBoundaries(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	text := p.ProcessCommandForClient("HELP BANDS", "N0CALL", "", nil, "go")
	for _, required := range []string{
		"HF includes 160m and 60m.", "HFCONTEST selects bands, not contest activity.",
		"VHFUP selects supported bands, not every frequency above HF.",
		"4m covers 70.000-70.650 MHz.",
		"Names ignore letter case.", "WARC,6m", "Overlapping selections do not add anything twice.",
		"For native CC, use its individual selectors: WARC,6.",
		"Other PASS rules can allow spots on other bands.",
		"  SET/FILTER DXBM/OFF\n  SET/FILTER DXBM/PASS WARC\n",
		"Mode, source and geography selections still apply.",
		"GO: SHOW DX 20 BAND WARC", "CC: SHOW/DX 20 BAND WARC",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("band HELP omitted contract %q", required)
		}
	}
	warning := strings.Index(text, "This replaces rule 0, including its previous conditions.")
	command := strings.Index(text, "  PASS RULE 0 BAND=WARC AND MODE=CW\n")
	if warning < 0 || command < 0 || warning >= command {
		t.Fatal("GO rule replacement must be explained before the example")
	}
	if strings.Contains(text, "SHOW DX 20 BAND=WARC") || strings.Contains(text, "SHOW/DX 20 BAND=WARC") {
		t.Fatal("history uses BAND followed by a list, not an equals comparison")
	}
}
