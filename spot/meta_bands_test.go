package spot

import (
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestMetaBandMembers(t *testing.T) {
	want := []MetaBandDefinition{
		{Name: "WARC", Members: []string{"30m", "17m", "12m"}},
		{Name: "HFCONTEST", Members: []string{"160m", "80m", "40m", "20m", "15m", "10m"}},
		{Name: "HF", Members: []string{"160m", "80m", "60m", "40m", "30m", "20m", "17m", "15m", "12m", "10m"}},
		{Name: "VHF", Members: []string{"6m", "4m", "2m", "1.25m"}},
		{Name: "UHF", Members: []string{"70cm", "33cm", "23cm", "13cm"}},
		{Name: "VHFUP", Members: []string{"6m", "4m", "2m", "1.25m", "70cm", "33cm", "23cm", "13cm"}},
	}
	if got := SupportedMetaBands(); !reflect.DeepEqual(got, want) {
		t.Fatalf("meta catalog = %+v, want %+v", got, want)
	}
	for _, definition := range want {
		members, ok := MetaBandMembers(strings.ToLower(definition.Name))
		if !ok || !reflect.DeepEqual(members, definition.Members) {
			t.Fatalf("%s members = %v, %v", definition.Name, members, ok)
		}
		members[0] = "CORRUPTED"
		if again, _ := MetaBandMembers(definition.Name); !reflect.DeepEqual(again, definition.Members) {
			t.Fatal("member snapshot modified catalog")
		}
		if IsValidBand(definition.Name) || BandContainsFrequency(definition.Name, 50313) {
			t.Fatalf("meta %s became a physical band", definition.Name)
		}
	}
	snapshot := SupportedMetaBands()
	snapshot[0].Name, snapshot[0].Members[0] = "CORRUPTED", "CORRUPTED"
	if got := SupportedMetaBands(); !reflect.DeepEqual(got, want) {
		t.Fatal("discovery snapshot modified catalog")
	}
	for _, invalid := range []string{"", "NONWARC", "QUIETHF", "6m", " WARC", "VHF-CW"} {
		if members, ok := MetaBandMembers(invalid); ok || members != nil {
			t.Fatalf("unsupported meta %q accepted", invalid)
		}
	}
}

func TestMetaBandPhysicalBoundaries(t *testing.T) {
	// Literal approved kHz bounds are independent of production discovery.
	for _, test := range []struct {
		band      string
		low, high float64
	}{
		{"2200m", 135.7, 137.8}, {"630m", 472, 479}, {"160m", 1800, 2000},
		{"80m", 3500, 4000}, {"60m", 5260, 5405}, {"40m", 7000, 7300},
		{"30m", 10100, 10150}, {"20m", 14000, 14350}, {"17m", 18068, 18168},
		{"15m", 21000, 21450}, {"12m", 24890, 24990}, {"10m", 28000, 29700},
		{"6m", 50000, 54000}, {"4m", 70000, 70650}, {"2m", 144000, 148000},
		{"1.25m", 220000, 225000}, {"70cm", 420000, 450000},
		{"33cm", 902000, 928000}, {"23cm", 1240000, 1300000},
		{"13cm", 2300000, 2310000}, {"13cm", 2390000, 2450000},
	} {
		for _, probe := range []struct {
			frequency float64
			inside    bool
		}{
			{math.Nextafter(test.low, math.Inf(-1)), false}, {test.low, true},
			{math.Nextafter(test.low, math.Inf(1)), true},
			{math.Nextafter(test.high, math.Inf(-1)), true}, {test.high, true},
			{math.Nextafter(test.high, math.Inf(1)), false},
		} {
			if got := BandContainsFrequency(test.band, probe.frequency); got != probe.inside {
				t.Fatalf("%s contains %.17g = %v, want %v", test.band, probe.frequency, got, probe.inside)
			}
			want := "???"
			if probe.inside {
				want = test.band
			}
			if got := FreqToBand(probe.frequency); got != want {
				t.Fatalf("FreqToBand(%.17g) = %s, want %s", probe.frequency, got, want)
			}
		}
	}
	for _, frequency := range []float64{0, -1, math.NaN(), math.Inf(-1), math.Inf(1), 2350000} {
		if BandContainsFrequency("13cm", frequency) {
			t.Fatalf("invalid/gap frequency %v accepted", frequency)
		}
	}
	segments, ok := BandFrequencySegments("13CM")
	if !ok || !reflect.DeepEqual(segments, []BandSegment{{Min: 2300000, Max: 2310000}, {Min: 2390000, Max: 2450000}}) {
		t.Fatalf("split physical segments = %v, %v", segments, ok)
	}
	segments[0].Min = 1
	if again, _ := BandFrequencySegments("13cm"); again[0].Min != 2300000 {
		t.Fatal("segment snapshot modified catalog")
	}
	if segments, ok := BandFrequencySegments("UHF"); ok || segments != nil {
		t.Fatal("meta accepted as physical range")
	}
}
