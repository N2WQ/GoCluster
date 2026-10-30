// File role: Exact v6 record/readback admission before any machine commit.
// Lease-owned destination metadata is authoritative; response capture never
// recursively borrows live path/filter locks while queued writers may exist.
package telnet

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strings"

	"dxcluster/filter"
)

func (s *Server) prepareV6ConfigurationRecord(c *Client, cfg filter.V6Configuration, ref *filter.V6PresetReference) (*filter.PreparedUserRecord, error) {
	if c.recordProtected {
		return nil, errProtectedRecord
	}
	record, err := filter.LoadV6UserRecord(c.callsign)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %w", filter.ErrCurrentUserRecord, err)
		}
		record = &filter.V6UserRecord{LastLoginUTC: c.lastLoginUTC}
	}
	record.ConfigurationVersion = filter.V6ConfigurationVersion
	record.Configuration, record.Preset = cfg, ref
	record.RecentIPs = filter.MergeRecentIPs(c.recentIPs, record.RecentIPs)
	return filter.PrepareV6UserRecord(c.callsign, record)
}

func attachV6ReadbackConfiguration(status configurationReadbackStatus, cfg filter.V6Configuration, ref *filter.V6PresetReference) configurationReadbackStatus {
	status.Configured = cfg.Settings
	if ref != nil {
		status.Preset = readbackPresetStatus{
			Associated: true, Name: ref.Name, Modified: !cfg.Equal(ref.Baseline.Configuration),
		}
	}
	return status
}

// First admit the actual complete envelope, including preset/session metadata;
// then reserve the widest ordinary status and envelope scalars.
// Both passes include the canonical program and ordinary captured status.
func (s *Server) v6ConfigurationReadbackFits(c *Client, cfg filter.V6Configuration, prepared *Client, ref *filter.V6PresetReference) error {
	status := attachV6ReadbackConfiguration(s.captureReadbackStatus(c, prepared), cfg, ref)
	set := prepared.filter.PreparedRules()
	warning := ""
	if c.filterBanks != nil {
		warning = legacyV6BankWarning
	}
	if _, err := renderV6YAMLReadback(cfg, set, status, "CONFIG", "", "", warning); err != nil {
		return err
	}
	grid := status.Effective.Grid
	if len(grid) < 6 {
		grid = "AA00AA"
	}
	status.Effective = readbackEffectiveStatus{
		Dialect: "go", Grid: grid, NoiseClass: "INDUSTRIAL", DedupePolicy: "SLOW",
		PathMinObservationCount: math.MaxInt, SolarSummaryMinutes: math.MaxInt,
	}
	status.Session = readbackSessionStatus{
		Callsign: strings.Repeat("1", 32), DiagnosticComments: "DEDUPE",
		PauseRemaining: math.MaxInt, SuppressedSpots: math.MaxUint64,
		UnsavedChanges: true,
	}
	status.Server = readbackServerStatus{
		DefaultDialect: "go", DefaultDedupePolicy: "SLOW", DefaultNoiseClass: "INDUSTRIAL",
		PathMinObservationCount: math.MaxInt, AutoReadPauseMinRows: math.MaxInt, AutoReadPauseSeconds: math.MaxInt,
	}
	status.Preset = readbackPresetStatus{Name: strings.Repeat("1", 32)}
	_, err := renderV6YAMLReadback(cfg, set, status, "CONFIG", strings.Repeat("1", 32), strings.Repeat("\"", 128), warning)
	return err
}
