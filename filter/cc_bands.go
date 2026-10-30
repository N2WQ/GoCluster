// File role: Fixed native CC frequency bands/slots. These are a separate
// protocol vocabulary from spot.SupportedBandNames, including native 4/1/MW.
package filter

import (
	"math"
	"slices"
	"sort"
)

// CCSlotDefinition describes a native frequency segment in kHz. At a shared
// endpoint the higher segment owns the frequency; each band's final high is in.
type CCSlotDefinition struct {
	Name, Band      string
	LowKHz, HighKHz float64
}

var ccNativeBands = [...]string{"160", "80", "60", "40", "30", "20", "17", "15", "12", "10", "6", "4", "2", "1", "MW"}

// Ascending lower endpoints are part of ccFrequencySlot's binary-search
// invariant. Whole-band commands expand only these documented slot names.
var ccNativeSlots = [...]CCSlotDefinition{
	{"160-CW", "160", 1800, 1850}, {"160-SSB", "160", 1850, 2000},
	{"80-CW", "80", 3500, 3580}, {"80-RTTY", "80", 3580, 3700}, {"80-SSB", "80", 3700, 4000},
	{"60-SSB", "60", 5260, 5405},
	{"40-CW", "40", 7000, 7040}, {"40-RTTY", "40", 7040, 7100}, {"40-SSB", "40", 7100, 7300},
	{"30-CW", "30", 10100, 10130}, {"30-RTTY", "30", 10130, 10150},
	{"20-CW", "20", 14000, 14070}, {"20-RTTY", "20", 14070, 14150}, {"20-SSB", "20", 14150, 14350},
	{"17-CW", "17", 18068, 18100}, {"17-RTTY", "17", 18100, 18110}, {"17-SSB", "17", 18110, 18168},
	{"15-CW", "15", 21000, 21070}, {"15-RTTY", "15", 21070, 21200}, {"15-SSB", "15", 21200, 21450},
	{"12-CW", "12", 24890, 24920}, {"12-RTTY", "12", 24920, 24930}, {"12-SSB", "12", 24930, 24990},
	{"10-CW", "10", 28000, 28070}, {"10-RTTY", "10", 28070, 28300}, {"10-SSB", "10", 28300, 29700},
	{"6-CW", "6", 50000, 50080}, {"6-SSB", "6", 50080, 50500}, {"6-FM", "6", 50500, 54000},
	{"4-MTR", "4", 70000, 70650},
	{"2-CW", "2", 144000, 144100}, {"2-SSB", "2", 144100, 144500}, {"2-FM", "2", 144500, 148000},
	{"1-CW", "1", 220000, 221000}, {"1-FM", "1", 221000, 222000}, {"1-SSB", "1", 222000, 224000},
	{"MW-MW", "MW", 500000, 47000000},
}

// CCSupportedBands and CCSupportedSlots return owned discovery snapshots.
func CCSupportedBands() []string { return slices.Clone(ccNativeBands[:]) }

func CCSupportedSlots() []CCSlotDefinition { return slices.Clone(ccNativeSlots[:]) }

func ccBandIndex(name string) int {
	for i, candidate := range ccNativeBands {
		if name == candidate {
			return i
		}
	}
	return -1
}

func ccSlotIndex(name string) int {
	for i := range ccNativeSlots {
		if name == ccNativeSlots[i].Name {
			return i
		}
	}
	return -1
}

func ccFrequencySlot(frequency float64) int {
	if frequency <= 0 || math.IsNaN(frequency) || math.IsInf(frequency, 0) {
		return -1
	}
	index := sort.Search(len(ccNativeSlots), func(i int) bool { return ccNativeSlots[i].LowKHz > frequency }) - 1
	if index < 0 || frequency > ccNativeSlots[index].HighKHz {
		return -1
	}
	return index
}
