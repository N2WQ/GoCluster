// Catalog-derived frequency cells retain precise band selections alongside
// legacy native slots. The finite partition owns endpoints explicitly; no edit
// history, runtime registry or caller-owned ranges survive preparation.
package filter

import (
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	"dxcluster/spot"
)

// CCFrequencyOverrides contains only known catalog-derived cell identities.
// The distinct map type gives storage decoding its own pre-allocation bound.
type CCFrequencyOverrides map[string]bool

// CCFrequencyCell describes either an exact endpoint or the open interval
// between adjacent endpoints, in kHz. IDs are stable numeric endpoint names.
type CCFrequencyCell struct {
	ID      string  `yaml:"id"`
	LowKHz  float64 `yaml:"low_khz"`
	HighKHz float64 `yaml:"high_khz"`
	Point   bool    `yaml:"point"`
}

var ccFrequencyEndpoints, ccFrequencyCells = buildCCFrequencyCells()

func buildCCFrequencyCells() ([]float64, []CCFrequencyCell) {
	var endpoints []float64
	for _, slot := range ccNativeSlots {
		endpoints = append(endpoints, slot.LowKHz, slot.HighKHz)
	}
	for _, band := range spot.SupportedBandNames() {
		segments, ok := spot.BandFrequencySegments(band)
		if !ok {
			panic("physical catalog contains an invalid band")
		}
		for _, segment := range segments {
			endpoints = append(endpoints, segment.Min, segment.Max)
		}
	}
	slices.Sort(endpoints)
	endpoints = slices.Compact(endpoints)
	cells := make([]CCFrequencyCell, 0, 2*len(endpoints)-1)
	for index, endpoint := range endpoints {
		if index > 0 {
			low := endpoints[index-1]
			cells = append(cells, CCFrequencyCell{ID: "I:" + ccFrequencyText(low) + ":" + ccFrequencyText(endpoint), LowKHz: low, HighKHz: endpoint})
		}
		cells = append(cells, CCFrequencyCell{ID: "P:" + ccFrequencyText(endpoint), LowKHz: endpoint, HighKHz: endpoint, Point: true})
	}
	return endpoints, cells
}

func ccFrequencyText(frequency float64) string {
	return strconv.FormatFloat(frequency, 'f', -1, 64)
}

// CCSupportedFrequencyCells returns detached discovery data, not a mutable
// registry. Two endpoints per segment bound the partition to at most 2E-1 cells.
func CCSupportedFrequencyCells() []CCFrequencyCell { return slices.Clone(ccFrequencyCells) }

func ccFrequencyCellIndex(frequency float64) int {
	if frequency <= 0 || math.IsNaN(frequency) || math.IsInf(frequency, 0) {
		return -1
	}
	index := sort.SearchFloat64s(ccFrequencyEndpoints, frequency)
	if index < len(ccFrequencyEndpoints) && ccFrequencyEndpoints[index] == frequency {
		return index * 2
	}
	if index == 0 || index == len(ccFrequencyEndpoints) {
		return -1
	}
	return index*2 - 1
}

func ccFrequencyCellForID(raw string) (int, bool) {
	// Bound scalar work before folding an untrusted key.
	if len(raw) > 64 {
		return 0, false
	}
	name := strings.ToUpper(raw)
	for index, cell := range ccFrequencyCells {
		if name == cell.ID {
			return index, true
		}
	}
	return 0, false
}

func (c CCFrequencyCell) probe() float64 {
	if c.Point {
		return c.LowKHz
	}
	return c.LowKHz + (c.HighKHz-c.LowKHz)/2
}

func ccNativeFrequencyDecision(dxbm CCDXBM, frequency float64) bool {
	if slot := ccFrequencySlot(frequency); slot >= 0 {
		if enabled, exists := dxbm.Slots[ccNativeSlots[slot].Name]; exists {
			return enabled
		}
	}
	return dxbm.BaseAllow
}

func prepareCCFrequencyOverrides(dxbm CCDXBM, input CCFrequencyOverrides) (CCFrequencyOverrides, error) {
	if len(input) > len(ccFrequencyCells) {
		return nil, storageAdmissionf("native CC frequency overrides exceed cell limit")
	}
	if !dxbm.Active && len(input) != 0 {
		return nil, storageAdmissionf("inactive DXBM cannot contain frequency overrides")
	}
	var result CCFrequencyOverrides
	seen := make(map[int]bool, len(input))
	for raw, enabled := range input {
		index, ok := ccFrequencyCellForID(raw)
		if !ok || seen[index] {
			return nil, storageAdmissionf("unknown or duplicate native CC frequency cell")
		}
		seen[index] = true
		cell := ccFrequencyCells[index]
		// A false override may equal BaseAllow yet veto an enabled native slot.
		if enabled != ccNativeFrequencyDecision(dxbm, cell.probe()) {
			if result == nil {
				result = make(CCFrequencyOverrides)
			}
			result[cell.ID] = enabled
		}
	}
	return result, nil
}

func editCCPhysicalBands(dxbm *CCDXBM, members []string, enabled bool) {
	if dxbm.FrequencyOverrides == nil {
		dxbm.FrequencyOverrides = make(CCFrequencyOverrides)
	}
	for _, cell := range ccFrequencyCells {
		for _, member := range members {
			if spot.BandContainsFrequency(member, cell.probe()) {
				dxbm.FrequencyOverrides[cell.ID] = enabled
				break
			}
		}
	}
}

func clearCCSlotFrequencyOverrides(dxbm *CCDXBM, slot int) {
	if len(dxbm.FrequencyOverrides) == 0 {
		return
	}
	for _, cell := range ccFrequencyCells {
		if ccFrequencySlot(cell.probe()) == slot {
			delete(dxbm.FrequencyOverrides, cell.ID)
		}
	}
}
