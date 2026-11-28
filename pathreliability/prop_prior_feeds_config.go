// File role: Optional-when-omitted YAML configuration and strict validation for
// internal/propprior live space-weather feed polling (prop_prior_feeds).
// Crawler notes: Keeps pathreliability free of net/http dependencies; cluster
// runtime translates PropPriorFeedsConfig into internal/propprior/feed.Config.
package pathreliability

import (
	"fmt"
	"net/url"
	"strings"

	"dxcluster/internal/yamlconfig"

	"gopkg.in/yaml.v3"
)

const (
	propPriorFeedsMaxBodyBytes int64 = 32 << 20 // 32 MiB defensive ceiling
	propPriorFeedsMaxUABytes         = 256
)

// PropPriorFeedsConfig configures opt-in live space-weather HTTP polling for
// internal/propprior's SpaceWeatherStore. Omitting the section in YAML keeps
// polling disabled for backward compatibility with existing private configs.
type PropPriorFeedsConfig struct {
	Enabled                   bool   `yaml:"enabled"`
	Hp30PrimaryURL            string `yaml:"hp30_primary_url"`
	Hp30FallbackURL           string `yaml:"hp30_fallback_url"`
	SFIPrimaryURL             string `yaml:"sfi_primary_url"`
	SFIFallbackURL            string `yaml:"sfi_fallback_url"`
	RTSWMagURL                string `yaml:"rtsw_mag_url"`
	RTSWWindURL               string `yaml:"rtsw_wind_url"`
	DstPrimaryURL             string `yaml:"dst_primary_url"`
	DstFallbackURL            string `yaml:"dst_fallback_url"`
	Hp30PollIntervalSeconds   int    `yaml:"hp30_poll_interval_seconds"`
	SFIPollIntervalSeconds    int    `yaml:"sfi_poll_interval_seconds"`
	RTSWPollIntervalSeconds   int    `yaml:"rtsw_poll_interval_seconds"`
	DstPollIntervalSeconds    int    `yaml:"dst_poll_interval_seconds"`
	RequestTimeoutSeconds     int    `yaml:"request_timeout_seconds"`
	UserAgent                 string `yaml:"user_agent"`
	MaxBodyBytesStandard      int64  `yaml:"max_body_bytes_standard"`
	MaxBodyBytesRTSW          int64  `yaml:"max_body_bytes_rtsw"`
	MaxBodyBytesGeospace      int64  `yaml:"max_body_bytes_geospace"`
	Hp30StaleAfterSeconds     int    `yaml:"hp30_stale_after_seconds"`
	SFIStaleAfterSeconds      int    `yaml:"sfi_stale_after_seconds"`
	DstStaleAfterSeconds      int    `yaml:"dst_stale_after_seconds"`
	FailoverConsecutiveErrors int    `yaml:"failover_consecutive_errors"`
}

var propPriorFeedsRequiredPaths = []yamlconfig.Path{
	{"prop_prior_feeds", "enabled"},
	{"prop_prior_feeds", "hp30_primary_url"},
	{"prop_prior_feeds", "hp30_fallback_url"},
	{"prop_prior_feeds", "sfi_primary_url"},
	{"prop_prior_feeds", "sfi_fallback_url"},
	{"prop_prior_feeds", "rtsw_mag_url"},
	{"prop_prior_feeds", "rtsw_wind_url"},
	{"prop_prior_feeds", "dst_primary_url"},
	{"prop_prior_feeds", "dst_fallback_url"},
	{"prop_prior_feeds", "hp30_poll_interval_seconds"},
	{"prop_prior_feeds", "sfi_poll_interval_seconds"},
	{"prop_prior_feeds", "rtsw_poll_interval_seconds"},
	{"prop_prior_feeds", "dst_poll_interval_seconds"},
	{"prop_prior_feeds", "request_timeout_seconds"},
	{"prop_prior_feeds", "user_agent"},
	{"prop_prior_feeds", "max_body_bytes_standard"},
	{"prop_prior_feeds", "max_body_bytes_rtsw"},
	{"prop_prior_feeds", "max_body_bytes_geospace"},
	{"prop_prior_feeds", "hp30_stale_after_seconds"},
	{"prop_prior_feeds", "sfi_stale_after_seconds"},
	{"prop_prior_feeds", "dst_stale_after_seconds"},
	{"prop_prior_feeds", "failover_consecutive_errors"},
}

func propPriorFeedsPresenceDiagnostics(source string, bs []byte) ([]yamlconfig.Diagnostic, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(bs, &root); err != nil {
		return nil, err
	}
	if len(root.Content) == 0 || root.Content[0].Kind != yaml.MappingNode {
		return nil, nil
	}
	doc := root.Content[0]
	var section *yaml.Node
	for i := 0; i+1 < len(doc.Content); i += 2 {
		if strings.TrimSpace(doc.Content[i].Value) == "prop_prior_feeds" {
			section = doc.Content[i+1]
			break
		}
	}
	if section == nil {
		// Section omitted: backward-compatible disabled state.
		return nil, nil
	}
	if section.Kind == yaml.ScalarNode && section.Tag == "!!null" {
		return []yamlconfig.Diagnostic{{
			Severity: yamlconfig.DiagnosticError,
			Source:   source,
			Path:     "prop_prior_feeds",
			Message:  `required YAML setting "prop_prior_feeds" must not be null`,
		}}, nil
	}
	if section.Kind != yaml.MappingNode {
		return []yamlconfig.Diagnostic{{
			Severity: yamlconfig.DiagnosticError,
			Source:   source,
			Path:     "prop_prior_feeds",
			Message:  `invalid YAML setting "prop_prior_feeds": must be a mapping`,
		}}, nil
	}
	return yamlconfig.RequiredPathDiagnostics(source, bs, propPriorFeedsRequiredPaths)
}

func (c *PropPriorFeedsConfig) finalize() error {
	if c == nil {
		return nil
	}
	urlFields := []struct {
		name string
		val  string
	}{
		{"hp30_primary_url", c.Hp30PrimaryURL},
		{"hp30_fallback_url", c.Hp30FallbackURL},
		{"sfi_primary_url", c.SFIPrimaryURL},
		{"sfi_fallback_url", c.SFIFallbackURL},
		{"rtsw_mag_url", c.RTSWMagURL},
		{"rtsw_wind_url", c.RTSWWindURL},
		{"dst_primary_url", c.DstPrimaryURL},
		{"dst_fallback_url", c.DstFallbackURL},
	}
	for _, f := range urlFields {
		if err := validatePropPriorFeedURL(f.val); err != nil {
			return fmt.Errorf("prop_prior_feeds.%s: %w", f.name, err)
		}
	}

	posInts := []struct {
		name string
		val  int
	}{
		{"hp30_poll_interval_seconds", c.Hp30PollIntervalSeconds},
		{"sfi_poll_interval_seconds", c.SFIPollIntervalSeconds},
		{"rtsw_poll_interval_seconds", c.RTSWPollIntervalSeconds},
		{"dst_poll_interval_seconds", c.DstPollIntervalSeconds},
		{"request_timeout_seconds", c.RequestTimeoutSeconds},
		{"hp30_stale_after_seconds", c.Hp30StaleAfterSeconds},
		{"sfi_stale_after_seconds", c.SFIStaleAfterSeconds},
		{"dst_stale_after_seconds", c.DstStaleAfterSeconds},
		{"failover_consecutive_errors", c.FailoverConsecutiveErrors},
	}
	for _, f := range posInts {
		if f.val <= 0 {
			return fmt.Errorf("prop_prior_feeds.%s must be > 0", f.name)
		}
	}

	c.UserAgent = strings.TrimSpace(c.UserAgent)
	if c.UserAgent == "" {
		return fmt.Errorf("prop_prior_feeds.user_agent must not be empty")
	}
	if len(c.UserAgent) > propPriorFeedsMaxUABytes {
		return fmt.Errorf("prop_prior_feeds.user_agent exceeds %d bytes", propPriorFeedsMaxUABytes)
	}

	caps := []struct {
		name string
		val  int64
	}{
		{"max_body_bytes_standard", c.MaxBodyBytesStandard},
		{"max_body_bytes_rtsw", c.MaxBodyBytesRTSW},
		{"max_body_bytes_geospace", c.MaxBodyBytesGeospace},
	}
	for _, f := range caps {
		if f.val <= 0 || f.val > propPriorFeedsMaxBodyBytes {
			return fmt.Errorf("prop_prior_feeds.%s must be between 1 and %d", f.name, propPriorFeedsMaxBodyBytes)
		}
	}
	return nil
}

func validatePropPriorFeedURL(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return fmt.Errorf("must not be empty")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("unsupported scheme %q (want http or https)", parsed.Scheme)
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return fmt.Errorf("host must not be empty")
	}
	return nil
}
