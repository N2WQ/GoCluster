// Package feed implements bounded, fault-tolerant live HTTP pollers and
// parsers for the four space-weather streams consumed by internal/propprior's
// SpaceWeatherStore.
//
// File role: Configuration contract, defaults, and strict validation for live
// propagation-prior space-weather polling.
// Crawler notes: Pure configuration validation; runtime callers load values
// from path_reliability.yaml (prop_prior_feeds) without silent re-defaulting.
package feed

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	// Default upstream endpoints verified against live GFZ and NOAA SWPC feeds.
	DefaultHp30PrimaryURL  = "https://kp.gfz.de/app/json/"
	DefaultHp30FallbackURL = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json"
	DefaultSFIPrimaryURL   = "https://services.swpc.noaa.gov/json/f107_cm_flux.json"
	DefaultSFIFallbackURL  = "https://kp.gfz.de/app/json/"
	DefaultRTSWMagURL      = "https://services.swpc.noaa.gov/json/rtsw/rtsw_mag_1m.json"
	DefaultRTSWWindURL     = "https://services.swpc.noaa.gov/json/rtsw/rtsw_wind_1m.json"
	DefaultDstPrimaryURL   = "https://services.swpc.noaa.gov/products/kyoto-dst.json"
	DefaultDstFallbackURL  = "https://services.swpc.noaa.gov/json/geospace/geospace_dst_7_day.json"

	DefaultHp30PollInterval = 10 * time.Minute
	DefaultSFIPollInterval  = 30 * time.Minute
	DefaultRTSWPollInterval = 5 * time.Minute
	DefaultDstPollInterval  = 15 * time.Minute
	DefaultRequestTimeout   = 15 * time.Second
	DefaultUserAgent        = "GoCluster-PropPrior/1.0"

	DefaultMaxBodyBytesStandard int64 = 1 << 20 // 1 MiB
	DefaultMaxBodyBytesRTSW     int64 = 8 << 20 // 8 MiB
	DefaultMaxBodyBytesGeospace int64 = 4 << 20 // 4 MiB

	DefaultHp30StaleAfter            = 90 * time.Minute
	DefaultSFIStaleAfter             = 36 * time.Hour
	DefaultDstStaleAfter             = 3 * time.Hour
	DefaultFailoverConsecutiveErrors = 3

	maxAllowedBodyBytes int64 = 32 << 20 // 32 MiB defensive ceiling
	maxUserAgentBytes         = 256
)

// Config governs the live space-weather HTTP pollers for internal/propprior.
// Callers must populate all fields explicitly; Validate rejects zero/missing
// values rather than substituting runtime defaults.
type Config struct {
	Enabled bool

	Hp30PrimaryURL  string
	Hp30FallbackURL string
	SFIPrimaryURL   string
	SFIFallbackURL  string
	RTSWMagURL      string
	RTSWWindURL     string
	DstPrimaryURL   string
	DstFallbackURL  string

	Hp30PollInterval time.Duration
	SFIPollInterval  time.Duration
	RTSWPollInterval time.Duration
	DstPollInterval  time.Duration
	RequestTimeout   time.Duration
	UserAgent        string

	MaxBodyBytesStandard int64
	MaxBodyBytesRTSW     int64
	MaxBodyBytesGeospace int64

	Hp30StaleAfter            time.Duration
	SFIStaleAfter             time.Duration
	DstStaleAfter             time.Duration
	FailoverConsecutiveErrors int
}

// DefaultConfig returns a complete, disabled-by-default Config for tests and
// callers constructing synthetic configurations. Runtime YAML loaders must
// supply explicit YAML values instead of silently falling back to this helper.
func DefaultConfig() Config {
	return Config{
		Enabled:                   false,
		Hp30PrimaryURL:            DefaultHp30PrimaryURL,
		Hp30FallbackURL:           DefaultHp30FallbackURL,
		SFIPrimaryURL:             DefaultSFIPrimaryURL,
		SFIFallbackURL:            DefaultSFIFallbackURL,
		RTSWMagURL:                DefaultRTSWMagURL,
		RTSWWindURL:               DefaultRTSWWindURL,
		DstPrimaryURL:             DefaultDstPrimaryURL,
		DstFallbackURL:            DefaultDstFallbackURL,
		Hp30PollInterval:          DefaultHp30PollInterval,
		SFIPollInterval:           DefaultSFIPollInterval,
		RTSWPollInterval:          DefaultRTSWPollInterval,
		DstPollInterval:           DefaultDstPollInterval,
		RequestTimeout:            DefaultRequestTimeout,
		UserAgent:                 DefaultUserAgent,
		MaxBodyBytesStandard:      DefaultMaxBodyBytesStandard,
		MaxBodyBytesRTSW:          DefaultMaxBodyBytesRTSW,
		MaxBodyBytesGeospace:      DefaultMaxBodyBytesGeospace,
		Hp30StaleAfter:            DefaultHp30StaleAfter,
		SFIStaleAfter:             DefaultSFIStaleAfter,
		DstStaleAfter:             DefaultDstStaleAfter,
		FailoverConsecutiveErrors: DefaultFailoverConsecutiveErrors,
	}
}

// Validate checks every endpoint URL, timing bound, body cap, and failover
// threshold, including when Enabled is false so misconfigurations fail fast.
func (c Config) Validate() error {
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
		if err := validateHTTPURL(f.val); err != nil {
			return fmt.Errorf("propprior/feed: invalid %s: %w", f.name, err)
		}
	}

	durFields := []struct {
		name string
		val  time.Duration
	}{
		{"hp30_poll_interval", c.Hp30PollInterval},
		{"sfi_poll_interval", c.SFIPollInterval},
		{"rtsw_poll_interval", c.RTSWPollInterval},
		{"dst_poll_interval", c.DstPollInterval},
		{"request_timeout", c.RequestTimeout},
		{"hp30_stale_after", c.Hp30StaleAfter},
		{"sfi_stale_after", c.SFIStaleAfter},
		{"dst_stale_after", c.DstStaleAfter},
	}
	for _, f := range durFields {
		if f.val <= 0 {
			return fmt.Errorf("propprior/feed: %s must be > 0", f.name)
		}
	}

	ua := strings.TrimSpace(c.UserAgent)
	if ua == "" {
		return errors.New("propprior/feed: user_agent must not be empty")
	}
	if len(ua) > maxUserAgentBytes {
		return fmt.Errorf("propprior/feed: user_agent exceeds %d bytes", maxUserAgentBytes)
	}

	capFields := []struct {
		name string
		val  int64
	}{
		{"max_body_bytes_standard", c.MaxBodyBytesStandard},
		{"max_body_bytes_rtsw", c.MaxBodyBytesRTSW},
		{"max_body_bytes_geospace", c.MaxBodyBytesGeospace},
	}
	for _, f := range capFields {
		if f.val <= 0 || f.val > maxAllowedBodyBytes {
			return fmt.Errorf("propprior/feed: %s must be between 1 and %d", f.name, maxAllowedBodyBytes)
		}
	}

	if c.FailoverConsecutiveErrors <= 0 {
		return errors.New("propprior/feed: failover_consecutive_errors must be >= 1")
	}
	return nil
}

func validateHTTPURL(raw string) error {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return errors.New("must not be empty")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("unsupported scheme %q (want http or https)", parsed.Scheme)
	}
	if strings.TrimSpace(parsed.Host) == "" {
		return errors.New("host must not be empty")
	}
	return nil
}
