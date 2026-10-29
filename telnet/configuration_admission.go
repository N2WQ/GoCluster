// File role: Admits exact preference replacements before publication or writing.
// The enclosing full-SSID lease owns metadata and destination reads. Temporary
// admission uses bounded session metadata but never grants overwrite authority.
package telnet

import (
	"slices"

	"dxcluster/filter"
)

func (s *Server) prepareConfigurationRecord(c *Client, cfg filter.Configuration, ref *filter.PresetReference) (*filter.PreparedUserRecord, error) {
	reference, err := filter.V6PresetReferenceFromRuntime(ref)
	if err != nil {
		return nil, err
	}
	return s.prepareV6ConfigurationRecord(c, cfg.V6(), reference)
}

func (s *Server) commitConfigurationRecord(prepared *filter.PreparedUserRecord) error {
	if s != nil && s.commitConfigurationFn != nil {
		return s.commitConfigurationFn(prepared)
	}
	return prepared.Commit()
}

// Independently check the entire prospective temporary record. A refused saved
// file cannot exempt its replacement settings or applied baseline from quotas.
func (c *Client) admitTemporaryConfiguration(cfg filter.Configuration) error {
	record := &filter.UserRecord{
		Filter: cfg.FilterValue(), Dialect: cfg.Settings.Dialect,
		Grid: cfg.Settings.Grid, NoiseClass: cfg.Settings.NoiseClass,
		DedupePolicy:            cfg.Settings.DedupePolicy,
		PathMinObservationCount: cfg.Settings.PathMinObservationCount,
		SolarSummaryMinutes:     cfg.Settings.SolarSummaryMinutes,
		LastLoginUTC:            c.lastLoginUTC, RecentIPs: boundedSessionIPs(c.recentIPs),
		Preset: c.presetReference,
	}
	return filter.AdmitUserRecord(record)
}

func boundedSessionIPs(ips []string) []string {
	if len(ips) > 5 {
		ips = ips[:5]
	}
	return slices.Clone(ips)
}
