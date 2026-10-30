// File role: Admit the actual and widest complete v7 export before acceptance.
// The caller holds its configuration lease and a temporary output reservation;
// human edits and machine transactions use the same bounded admission owner.
package telnet

import (
	"math"
	"strings"

	"dxcluster/filter"
)

func (s *Server) v7ConfigurationReadbackFits(c *Client, cfg filter.V7Configuration, prepared *Client, refs filter.V7PresetReferences) error {
	canonical, goRuntime, _, err := filter.PrepareV7Configuration(cfg)
	if err != nil {
		return err
	}
	status := s.captureReadbackStatus(c, prepared)
	presets := v7ReadbackPresetStatuses(canonical, refs)
	if _, err := renderV7YAMLReadback(canonical, goRuntime.PreparedRules(), status, presets, "CONFIG", "", ""); err != nil {
		return err
	}
	grid := status.Effective.Grid
	if len(grid) < 6 {
		grid = "AA00AA"
	}
	status.Effective = readbackEffectiveStatus{
		Dialect: status.Effective.Dialect, Grid: grid, NoiseClass: "INDUSTRIAL", DedupePolicy: "SLOW",
		PathMinObservationCount: math.MaxInt, SolarSummaryMinutes: math.MaxInt,
	}
	status.Session = readbackSessionStatus{
		Callsign: strings.Repeat("1", 32), DiagnosticComments: "DEDUPE", PauseRemaining: math.MaxInt,
		SuppressedSpots: math.MaxUint64, PauseActive: false, PausePendingDelivery: false, TemporaryDefaults: false,
		UnsavedChanges: true,
	}
	status.Server = readbackServerStatus{
		DefaultDialect: "go", DefaultDedupePolicy: "SLOW", DefaultNoiseClass: "INDUSTRIAL",
		PathMinObservationCount: math.MaxInt, AutoReadPauseMinRows: math.MaxInt, AutoReadPauseSeconds: math.MaxInt,
	}
	for dialect := range presets {
		presets[dialect] = readbackPresetStatus{Name: strings.Repeat("1", 32)}
	}
	if _, err := renderV7YAMLReadback(canonical, goRuntime.PreparedRules(), status, presets, "CONFIG", strings.Repeat("1", 32), strings.Repeat("\"", 128)); err != nil {
		return err
	}
	return s.v7HumanReadbackFits(c, canonical, prepared, refs)
}
