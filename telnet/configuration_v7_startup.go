// File role: Refuses activation before accepting connections if operator defaults
// cannot survive the complete profile and promised native/wire readback bounds.
package telnet

import (
	"dxcluster/filter"
)

func (s *Server) validateV7StartingConfiguration() error {
	c := &Client{server: s, callsign: "N0CALL", done: make(chan struct{})}
	reservation, err := c.reserveConfigurationOutput()
	if err != nil {
		return err
	}
	defer reservation.Release()
	cfg, err := filter.DefaultV7Configuration(filter.SettingsConfiguration{
		Dialect: string(s.configurationDefaultDialect()), DedupePolicy: s.effectiveDefaultDedupePolicy().label(),
	})
	if err != nil {
		return err
	}
	_, goFilter, ccFilter, err := filter.PrepareV7Configuration(cfg)
	if err != nil {
		return err
	}
	value := goFilter
	if s.effectiveV7Dialect(cfg.Settings) == DialectCC {
		value = ccFilter
	}
	c.filter, c.filterBanks = &value, &cfg.Filters
	c.configuredSettings, c.configurationInitialized = cfg.Settings, true
	c.dialect = s.effectiveV7Dialect(cfg.Settings)
	prepared, _ := s.prepareConfiguration(c, filter.ConfigurationFromFilter(&value, cfg.Settings), s.now())
	if err := filter.AdmitV7UserRecord(&filter.V7UserRecord{ConfigurationVersion: filter.V7ConfigurationVersion, Configuration: cfg}); err != nil {
		return err
	}
	return s.v7ConfigurationReadbackFits(c, cfg, prepared, filter.V7PresetReferences{})
}
