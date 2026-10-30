// File role: Complete native CC inspection, using the existing counted ASCII
// response and reading-pause footer. The caller owns the configuration lease.
package telnet

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"dxcluster/filter"
)

func renderNativeCCBody(limit int, write func(*humanResponse) error) (string, error) {
	for _, countOnly := range []bool{true, false} {
		h := humanResponse{boundedResponse: boundedResponse{limit: limit}, countOnly: countOnly}
		if err := write(&h); err != nil {
			return "", err
		}
		if !countOnly {
			return string(h.data), nil
		}
	}
	return "", fmt.Errorf("native CC rendering did not complete")
}

func (s *Server) nativeCCReadback(c *Client, resource, category string, duration time.Duration) (string, error) {
	if c == nil || c.dialect != DialectCC {
		return "", fmt.Errorf("native CC readback requires the active CC bank")
	}
	category = strings.ToUpper(category)
	if category == "FULL" {
		category = ""
	}
	if !nativeCCReadbackCategory(category) {
		return "", fmt.Errorf("use SHOW/FILTER [DOC|DOS|DXCTY|DXSTATE|DXBM|MODE|SOURCE|BEACON|WWV|WCY|ANNOUNCE|SELF|TOXIC|FULL]")
	}
	status, runtime := s.captureReadbackStatus(c, nil), s.captureHumanRuntime(c)
	c.filterMu.RLock()
	configuration, available := c.filter.NativeCCConfiguration()
	c.filterMu.RUnlock()
	if !available {
		return "", fmt.Errorf("native CC filters are unavailable")
	}
	status.Configured = c.configuredSettings
	if c.filterBanks != nil {
		full, err := c.captureV7Configuration()
		if err != nil {
			return "", err
		}
		status.Preset = c.v7PresetStatuses(full)[string(c.dialect)]
	}
	return renderNativeCCBody(maxV7YAMLBytes, func(h *humanResponse) error {
		if err := writeHumanHeader(h, c, status); err != nil {
			return err
		}
		if err := h.line("Active filter bank: CC (native categories)"); err != nil {
			return err
		}
		if status.Session.TemporaryDefaults {
			if err := h.prose("", "", "Saving is unavailable; this session's temporary preferences are not saved."); err != nil {
				return err
			}
		}
		if c.configurationUnsaved {
			if err := h.prose("", "", "Active preferences include unsaved changes. They may be lost when you reconnect."); err != nil {
				return err
			}
		}
		if strings.EqualFold(resource, "CONFIG") {
			if err := writeHumanSettings(h, status.Configured, status, runtime, duration); err != nil {
				return err
			}
		}
		if err := writeNativeCCRows(h, configuration, category); err != nil {
			return err
		}
		if h.joined {
			if err := h.line("Quoted '+' pieces join without added spaces."); err != nil {
				return err
			}
		}
		if err := h.prose("", "", "The inactive GO bank is preserved. GET YAML CONFIG SCHEMA 7 exports both banks; schema 6 is not a complete backup."); err != nil {
			return err
		}
		return writeHumanFooter(h, duration)
	})
}

func nativeCCReadbackCategory(category string) bool {
	switch category {
	case "", "DOC", "DOS", "DXCTY", "DXSTATE", "DXBM", "MODE", "SOURCE", "BEACON", "WWV", "WCY", "ANNOUNCE", "PC93", "SELF", "TOXIC":
		return true
	default:
		return false
	}
}

func writeNativeCCRows(h *humanResponse, configuration filter.CCConfiguration, category string) error {
	if category == "" || category == "MODE" {
		if err := writeNativeCCValues(h, "Modes:", configuration.Modes); err != nil {
			return err
		}
		if err := h.line("  UNKNOWN selects a blank decoded mode. Empty selection admits no modes."); err != nil {
			return err
		}
	}
	if category == "" || category == "SOURCE" {
		if err := writeNativeCCValues(h, "Sources:", configuration.Sources); err != nil {
			return err
		}
		if err := h.line("  Source selection does not activate a feed or alter the mode selection."); err != nil {
			return err
		}
	}
	for _, name := range []string{"DOC", "DOS", "DXCTY", "DXSTATE"} {
		if category == "" || category == name {
			if err := writeNativeCCGeography(h, name, configuration.Geography[name]); err != nil {
				return err
			}
		}
	}
	if category == "" || category == "DXBM" {
		if err := writeNativeCCDXBM(h, configuration.DXBM); err != nil {
			return err
		}
	}
	return writeNativeCCControls(h, configuration, category)
}

func writeNativeCCValues(h *humanResponse, label string, values []string) error {
	if len(values) == 0 {
		return h.row(label, "(none)")
	}
	for _, value := range values {
		if len(value) > humanValueWidth {
			return h.valueRow(label, strings.Join(values, ", "), "")
		}
	}
	return h.row(label, strings.Join(values, ", "))
}

func writeNativeCCGeography(h *humanResponse, name string, geography filter.CCGeography) error {
	meaning := map[string]string{"DOC": "spotter country", "DOS": "spotter state/province", "DXCTY": "DX country", "DXSTATE": "DX state/province"}[name]
	if err := h.line(""); err != nil {
		return err
	}
	if err := h.line(name + " - " + meaning); err != nil {
		return err
	}
	country := name == "DOC" || name == "DXCTY"
	if err := writeNativeCCCategory(h, "Global:", geography.Global, country); err != nil {
		return err
	}
	if len(geography.Bands) == 0 {
		return h.line("  All native bands: INHERIT global")
	}
	for _, band := range filter.CCSupportedBands() {
		row, exists := geography.Bands[band]
		if !exists {
			if err := h.row("Band "+band+":", "INHERIT global"); err != nil {
				return err
			}
		} else if err := writeNativeCCCategory(h, "Band "+band+":", row, country); err != nil {
			return err
		}
	}
	return nil
}

func writeNativeCCCategory(h *humanResponse, label string, category filter.CCCategory, country bool) error {
	if err := h.row(label, strings.ToUpper(string(category.Action))); err != nil {
		return err
	}
	if category.Action == filter.CCOff {
		return nil
	}
	values, kind := category.States, "State values:"
	if country {
		kind, values = "ADIF values:", make([]string, len(category.Countries))
		for i, country := range category.Countries {
			values[i] = strconv.Itoa(country)
		}
	}
	if err := writeNativeCCValues(h, kind, values); err != nil {
		return err
	}
	if len(values) == 0 {
		text := "  Empty PASS matches no metadata."
		if category.Action == filter.CCReject {
			text = "  Empty REJECT excludes no metadata."
		}
		return h.line(text)
	}
	return nil
}

func writeNativeCCDXBM(h *humanResponse, dxbm filter.CCDXBM) error {
	if err := h.line(""); err != nil {
		return err
	}
	if !dxbm.Active {
		return h.row("DXBM:", "OFF (unrestricted frequency selection)")
	}
	base := "BLOCK"
	if dxbm.BaseAllow {
		base = "ALLOW"
	}
	if err := h.row("DXBM:", "ACTIVE; base "+base+". Frequencies outside native slots follow the base."); err != nil {
		return err
	}
	for _, slot := range filter.CCSupportedSlots() {
		value, overridden := dxbm.Slots[slot.Name]
		if !overridden {
			value = dxbm.BaseAllow
		}
		text := "BLOCK"
		if value {
			text = "ALLOW"
		}
		if overridden {
			text += " (override)"
		} else {
			text += " (base)"
		}
		if err := h.row(slot.Name+":", text); err != nil {
			return err
		}
	}
	return h.prose("", "", "DXBM slot names select frequency ranges, regardless of decoded mode. The separate mode selection also applies.")
}

func writeNativeCCControls(h *humanResponse, c filter.CCConfiguration, category string) error {
	for _, row := range []struct {
		name  string
		value filter.DefaultBool
	}{
		{"BEACON", c.IncludeBeacons}, {"WWV", c.AllowWWV}, {"WCY", c.AllowWCY},
		{"ANNOUNCE", c.AllowAnnounce}, {"SELF", c.AllowSelf}, {"TOXIC", c.AllowToxic},
	} {
		if category != "" && category != row.name && (category != "PC93" || row.name != "ANNOUNCE") {
			continue
		}
		value := "DEFAULT (ON)"
		switch row.value {
		case filter.DefaultBoolFalse:
			value = "OFF"
		case filter.DefaultBoolTrue:
			value = "ON"
		}
		if err := h.row(row.name+":", value); err != nil {
			return err
		}
	}
	if category == "" || category == "SELF" {
		return h.prose("", "", "GoCluster SELF controls spots of your own call. When enabled, they bypass personal categories; TOXIC still applies. History retains its existing own-call exception and explicit query selections.")
	}
	return nil
}
