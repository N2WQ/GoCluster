// File role: Complete, immutable-preparation generation-seven preferences.
// This module grants no login, path or persistence replacement authority.
package filter

import (
	"crypto/sha256"
	"errors"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	V7ConfigurationVersion  = 7
	MaxV7ConfigurationBytes = 192 << 10
)

type V7FilterBanks struct {
	GO V6FilterConfiguration `yaml:"go"`
	CC CCConfiguration       `yaml:"cc"`
}

type V7Configuration struct {
	Filters  V7FilterBanks         `yaml:"filters"`
	Settings SettingsConfiguration `yaml:"settings"`
}

// PrepareV7Configuration owns both banks, even when only one will be activated.
// It preserves exact configured shared values; runtime fallback is caller-owned.
func PrepareV7Configuration(c V7Configuration) (V7Configuration, Filter, Filter, error) {
	if !v7SettingsMinimumFits(c.Settings, MaxV7ConfigurationBytes) {
		return V7Configuration{}, Filter{}, Filter{}, storageAdmissionf("v7 configuration exceeds byte limit")
	}
	goConfig, goFilter, err := PrepareV6Configuration(V6Configuration{Filters: c.Filters.GO, Settings: c.Settings})
	if err != nil {
		return V7Configuration{}, Filter{}, Filter{}, err
	}
	ccConfig, ccFilter, err := NewCCFilter(c.Filters.CC)
	if err != nil {
		return V7Configuration{}, Filter{}, Filter{}, err
	}
	c = V7Configuration{Filters: V7FilterBanks{GO: goConfig.Filters, CC: ccConfig}, Settings: cloneV7Settings(c.Settings)}
	data, err := yaml.Marshal(c)
	if err != nil {
		return V7Configuration{}, Filter{}, Filter{}, err
	}
	if len(data) > MaxV7ConfigurationBytes {
		return V7Configuration{}, Filter{}, Filter{}, storageAdmissionf("v7 configuration exceeds %d bytes", MaxV7ConfigurationBytes)
	}
	return c, goFilter, ccFilter, nil
}

func cloneV7Settings(s SettingsConfiguration) SettingsConfiguration {
	s.Dialect, s.Grid = strings.Clone(s.Dialect), strings.Clone(s.Grid)
	s.NoiseClass, s.DedupePolicy = strings.Clone(s.NoiseClass), strings.Clone(s.DedupePolicy)
	return s
}

func v7SettingsMinimumFits(s SettingsConfiguration, remaining int) bool {
	for _, value := range []string{s.Dialect, s.Grid, s.NoiseClass, s.DedupePolicy} {
		if len(value) > remaining {
			return false
		}
		remaining -= len(value)
	}
	return true
}

// Clone accepts already admitted state, matching the existing compact DTO APIs.
func (c V7Configuration) Clone() V7Configuration {
	c.Filters.GO.Rules = slices.Clone(c.Filters.GO.Rules)
	c.Filters.CC = c.Filters.CC.Clone()
	c.Settings = cloneV7Settings(c.Settings)
	return c
}

func (c V7Configuration) Equal(other V7Configuration) bool {
	return (V6Configuration{Filters: c.Filters.GO, Settings: c.Settings}).Equal(V6Configuration{Filters: other.Filters.GO, Settings: other.Settings}) && c.Filters.CC.Equal(other.Filters.CC)
}

// Fingerprint includes both banks and selection, never login/notice/baseline data.
func (c V7Configuration) Fingerprint() [32]byte {
	goHash := (V6Configuration{Filters: c.Filters.GO, Settings: c.Settings}).Fingerprint()
	ccHash := c.Filters.CC.Fingerprint()
	var payload [65]byte
	payload[0] = V7ConfigurationVersion
	copy(payload[1:33], goHash[:])
	copy(payload[33:], ccHash[:])
	return sha256.Sum256(payload[:])
}

// DefaultV7Configuration installs current operator modes/sources in both banks.
// All six controls remain DEFAULT. It never changes process-wide defaults.
func DefaultV7Configuration(settings SettingsConfiguration) (V7Configuration, error) {
	// Pin one admitted operator generation for both banks, even if another
	// startup owner publishes defaults while this detached state is prepared.
	defaults := newFilterDefaultRules()
	cc, err := ccConfigurationFromDefaultRules(defaults)
	if err != nil {
		return V7Configuration{}, err
	}
	goBank := V6FilterConfiguration{}.WithRuleSet(defaults)
	canonical, _, _, err := PrepareV7Configuration(V7Configuration{Filters: V7FilterBanks{GO: goBank, CC: cc}, Settings: settings})
	return canonical, err
}

func validateV7EncodingVersion(version int) error {
	if version != 0 && version != V7ConfigurationVersion {
		return ErrUnsupportedConfigurationVersion
	}
	return nil
}

func validateV7Generation(generation int) error {
	if generation != 0 && generation != V7ConfigurationVersion {
		return errors.New("invalid v7 reset generation")
	}
	return nil
}
