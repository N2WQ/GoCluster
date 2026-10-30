// File role: Allocation-free evaluation of immutable native CC generations,
// using fixed frequency slots and bounded sorted geography operands.
package filter

import (
	"slices"

	"dxcluster/spot"
)

type ccPreparedGeography struct {
	global   CCCategory
	bands    [len(ccNativeBands)]CCCategory
	bandMask uint16
}

// PreparedCCFilter owns one immutable, bounded generation. Fixed arrays select
// the relevant native band; sorted typed operands have no ADIF-sized side table.
// Old generations are released with their last client/request snapshot owner.
type PreparedCCFilter struct {
	configuration  CCConfiguration
	geography      [len(ccGeographyNames)]ccPreparedGeography
	slots          [len(ccNativeSlots)]bool
	human, skimmer bool
	needsFrequency bool
	counts         CCCounts
}

func buildPreparedCCFilter(owned CCConfiguration, bytes int) *PreparedCCFilter {
	prepared := &PreparedCCFilter{configuration: owned, counts: CCCounts{
		Modes: len(owned.Modes), Sources: len(owned.Sources), CanonicalBytes: bytes,
		DXBMSlots: len(owned.DXBM.Slots),
	}, needsFrequency: owned.DXBM.Active}
	for _, source := range owned.Sources {
		prepared.human = prepared.human || source == "HUMAN"
		prepared.skimmer = prepared.skimmer || source == "SKIMMER"
	}
	for index, name := range ccGeographyNames {
		geography := owned.Geography[name]
		prepared.geography[index].global = geography.Global
		prepared.countCategory(geography.Global)
		for band, category := range geography.Bands {
			bandIndex := ccBandIndex(band)
			prepared.geography[index].bands[bandIndex] = category
			prepared.geography[index].bandMask |= 1 << bandIndex
			prepared.needsFrequency = true
			prepared.countCategory(category)
		}
	}
	for index, slot := range ccNativeSlots {
		enabled, exists := owned.DXBM.Slots[slot.Name]
		if !exists {
			enabled = owned.DXBM.BaseAllow
		}
		prepared.slots[index] = enabled
	}
	return prepared
}

func (p *PreparedCCFilter) countCategory(category CCCategory) {
	p.counts.GeographyRows++
	p.counts.Countries += len(category.Countries)
	p.counts.States += len(category.States)
}

// CanonicalConfig returns an owned DTO; editing it cannot change this program.
func (p *PreparedCCFilter) CanonicalConfig() CCConfiguration {
	if p == nil {
		return CCConfiguration{}
	}
	return p.configuration.Clone()
}

func (p *PreparedCCFilter) Counts() CCCounts {
	if p == nil {
		return CCCounts{}
	}
	return p.counts
}

// Matches applies only personal CC categories; the caller owns six delivery
// controls and self/history exceptions. One lazy ruleView shares current mode
// and metadata semantics without parsing, cloning or holding delivery locks.
func (p *PreparedCCFilter) Matches(value *spot.Spot, _ EvaluationContext) bool {
	if p == nil || value == nil || value.IsHuman && !p.human || !value.IsHuman && !p.skimmer {
		return false
	}
	view := newRuleView(value, "")
	if _, exists := slices.BinarySearch(p.configuration.Modes, view.textValue(ruleFieldMode)); !exists {
		return false
	}
	band := -1
	if p.needsFrequency {
		slot := ccFrequencySlot(value.Frequency)
		if slot >= 0 {
			band = ccBandIndex(ccNativeSlots[slot].Band)
		}
		if p.configuration.DXBM.Active {
			allowed := p.configuration.DXBM.BaseAllow
			if slot >= 0 {
				allowed = p.slots[slot]
			}
			if !allowed {
				return false
			}
		}
	}
	for index := range p.geography {
		geography := &p.geography[index]
		category := &geography.global
		if band >= 0 && geography.bandMask&(1<<band) != 0 {
			category = &geography.bands[band]
		}
		if !matchesCCCategory(index, category, &view) {
			return false
		}
	}
	return true
}

func matchesCCCategory(index int, category *CCCategory, view *ruleView) bool {
	if category.Action == CCOff {
		return true
	}
	matched := false
	switch index {
	case 0, 2:
		field := ruleFieldDEDXCC
		if index == 2 {
			field = ruleFieldDXDXCC
		}
		number, known := view.integerValue(field)
		if known {
			_, matched = slices.BinarySearch(category.Countries, number)
		}
	case 1, 3:
		field := ruleFieldDEState
		if index == 3 {
			field = ruleFieldDXState
		}
		state := view.textValue(field)
		if state != "" {
			_, matched = slices.BinarySearch(category.States, state)
		}
	}
	if category.Action == CCPass {
		return matched
	}
	return !matched
}
