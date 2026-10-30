// File role: Detached native CC category edits; global replacement, band OFF
// and DXBM base/slot ordering retain distinct operator-visible state.
package filter

import (
	"fmt"
	"slices"
	"strings"
)

// Edits prepare the source before copying and the complete result before
// returning. Refusals cannot mutate either the caller DTO or an active program.
func (c CCConfiguration) WithMode(mode string, enabled bool) (CCConfiguration, error) {
	if len(mode) > MaxCCCanonicalBytes {
		return CCConfiguration{}, storageAdmissionf("native CC mode bytes exceed configuration limit")
	}
	canonical, _, err := PrepareCCConfiguration(c)
	if err != nil {
		return CCConfiguration{}, err
	}
	values, err := prepareCCModes([]string{mode})
	if err != nil {
		return CCConfiguration{}, err
	}
	canonical.Modes = editCCSelection(canonical.Modes, values[0], enabled)
	result, _, err := PrepareCCConfiguration(canonical)
	return result, err
}

func (c CCConfiguration) WithSource(source string, enabled bool) (CCConfiguration, error) {
	if len(source) > MaxCCCanonicalBytes {
		return CCConfiguration{}, storageAdmissionf("native CC source bytes exceed configuration limit")
	}
	canonical, _, err := PrepareCCConfiguration(c)
	if err != nil {
		return CCConfiguration{}, err
	}
	values, err := prepareCCSources([]string{source})
	if err != nil {
		return CCConfiguration{}, err
	}
	canonical.Sources = editCCSelection(canonical.Sources, values[0], enabled)
	result, _, err := PrepareCCConfiguration(canonical)
	return result, err
}

func editCCSelection(values []string, value string, enabled bool) []string {
	index, exists := slices.BinarySearch(values, value)
	if enabled && !exists {
		return slices.Insert(values, index, value)
	}
	if !enabled && exists {
		return slices.Delete(values, index, index+1)
	}
	return values
}

// Empty band selects the whole category. Global replacement clears band
// overrides before equality is considered, including an otherwise equal list.
func (c CCConfiguration) WithGeography(category, band string, selection CCCategory) (CCConfiguration, error) {
	if len(category) > len("DXSTATE") || len(band) > 3 {
		return CCConfiguration{}, fmt.Errorf("unsupported native CC geography or band")
	}
	canonical, _, err := PrepareCCConfiguration(c)
	if err != nil {
		return CCConfiguration{}, err
	}
	name := strings.ToUpper(category)
	if ccGeographyIndex(name) < 0 {
		return CCConfiguration{}, fmt.Errorf("unsupported native CC geography %q", category)
	}
	remaining := MaxCCCanonicalBytes
	if err := preflightCCCategory(selection, func(value string) bool {
		if len(value) > remaining {
			return false
		}
		remaining -= len(value)
		return true
	}); err != nil {
		return CCConfiguration{}, err
	}
	prepared, err := prepareCCCategory(name, selection)
	if err != nil {
		return CCConfiguration{}, err
	}
	geography := canonical.Geography[name]
	if band == "" {
		geography.Global, geography.Bands = prepared, make(map[string]CCCategory)
	} else {
		band = strings.ToUpper(band)
		if ccBandIndex(band) < 0 {
			return CCConfiguration{}, fmt.Errorf("unsupported native CC band %q", band)
		}
		geography.Bands[band] = prepared
	}
	canonical.Geography[name] = geography
	result, _, err := PrepareCCConfiguration(canonical)
	return result, err
}

// WithDXBM applies one incremental edit. Only the first edit after OFF seeds
// whitelist/blacklist base state; later PASS/REJECT edits preserve that base.
func (c CCConfiguration) WithDXBM(action CCAction, selectors []string) (CCConfiguration, error) {
	if len(action) > len("reject") {
		return CCConfiguration{}, fmt.Errorf("unsupported native CC DXBM action")
	}
	if len(selectors) > len(ccNativeSlots)+len(ccNativeBands) {
		return CCConfiguration{}, storageAdmissionf("native CC DXBM selector count exceeds limit")
	}
	canonical, _, err := PrepareCCConfiguration(c)
	if err != nil {
		return CCConfiguration{}, err
	}
	action = CCAction(strings.ToLower(string(action)))
	if action == CCOff {
		if len(selectors) != 0 {
			return CCConfiguration{}, fmt.Errorf("native CC DXBM OFF takes no selectors")
		}
		canonical.DXBM = CCDXBM{BaseAllow: true, Slots: make(map[string]bool)}
	} else {
		if action != CCPass && action != CCReject || len(selectors) == 0 {
			return CCConfiguration{}, fmt.Errorf("native CC DXBM requires pass/reject and at least one selector")
		}
		if !canonical.DXBM.Active {
			canonical.DXBM.Active = true
			canonical.DXBM.BaseAllow = action == CCReject
		}
		for _, selector := range selectors {
			if err := editCCDXBMSelector(&canonical.DXBM, selector, action == CCPass); err != nil {
				return CCConfiguration{}, err
			}
		}
	}
	result, _, err := PrepareCCConfiguration(canonical)
	return result, err
}

func editCCDXBMSelector(dxbm *CCDXBM, raw string, enabled bool) error {
	if len(raw) > 16 {
		return fmt.Errorf("unsupported native CC DXBM selector")
	}
	name := strings.ToUpper(raw)
	if index := ccSlotIndex(name); index >= 0 {
		dxbm.Slots[name] = enabled
		return nil
	}
	if ccBandIndex(name) < 0 {
		return fmt.Errorf("unsupported native CC DXBM selector %q", raw)
	}
	for _, slot := range ccNativeSlots {
		if slot.Band == name {
			dxbm.Slots[slot.Name] = enabled
		}
	}
	return nil
}
