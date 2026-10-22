// File role: Exact v5 record/readback admission before any machine commit.
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

func (s *Server) prepareV5ConfigurationRecord(c *Client, cfg filter.V5Configuration, ref *filter.V5PresetReference) (*filter.PreparedUserRecord, error) {
	if c.recordProtected {
		return nil, errProtectedRecord
	}
	record, err := filter.LoadV5UserRecord(c.callsign)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %w", filter.ErrCurrentUserRecord, err)
		}
		record = &filter.V5UserRecord{LastLoginUTC: c.lastLoginUTC}
	}
	record.ConfigurationVersion = filter.MVPConfigurationVersion
	record.Configuration, record.Preset = cfg, ref
	record.RecentIPs = filter.MergeRecentIPs(c.recentIPs, record.RecentIPs)
	return filter.PrepareV5UserRecord(c.callsign, record)
}

func attachV5ReadbackConfiguration(status configurationReadbackStatus, cfg filter.V5Configuration, ref *filter.V5PresetReference) configurationReadbackStatus {
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
// Both passes expand the effective expressions, never an old-category proxy.
func (s *Server) v5ConfigurationReadbackFits(c *Client, cfg filter.V5Configuration, prepared *Client, ref *filter.V5PresetReference) error {
	status := attachV5ReadbackConfiguration(s.captureReadbackStatus(c, prepared), cfg, ref)
	set := prepared.filter.PreparedRules()
	if _, err := renderV5YAMLReadback(cfg, set, status, "CONFIG", "", ""); err != nil {
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
	}
	status.Server = readbackServerStatus{
		DefaultDialect: "go", DefaultDedupePolicy: "SLOW", DefaultNoiseClass: "INDUSTRIAL",
		PathMinObservationCount: math.MaxInt, AutoReadPauseMinRows: math.MaxInt, AutoReadPauseSeconds: math.MaxInt,
	}
	status.Preset = readbackPresetStatus{Name: strings.Repeat("1", 32)}
	_, err := renderV5YAMLReadback(cfg, set, status, "CONFIG", strings.Repeat("1", 32), strings.Repeat("\"", 128))
	return err
}
