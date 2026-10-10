// Package toxicity owns the optional human-comment classifier boundary.
// It keeps Worker credentials out of the main merged runtime config and makes
// the fail-open, bounded-cache behavior testable without telnet fanout state.
package toxicity

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"strings"
	"time"

	"dxcluster/internal/yamlconfig"
)

const (
	defaultTimeoutMS       = 1200
	defaultWorkers         = 2
	defaultQueueSize       = 128
	defaultCacheMaxEntries = 4096
	defaultCacheTTLSeconds = 86400
	defaultMaxCommentBytes = 512
)

// Config is loaded from optional toxicity.yaml at startup. Six numeric zero
// sentinels request documented defaults; required presence is still enforced.
type Config struct {
	Enabled         bool   `yaml:"enabled"`
	Endpoint        string `yaml:"endpoint"`
	BearerTokenEnv  string `yaml:"bearer_token_env"`
	TimeoutMS       int    `yaml:"timeout_ms"`
	Workers         int    `yaml:"workers"`
	QueueSize       int    `yaml:"queue_size"`
	CacheMaxEntries int    `yaml:"cache_max_entries"`
	CacheTTLSeconds int    `yaml:"cache_ttl_seconds"`
	MaxCommentBytes int    `yaml:"max_comment_bytes"`
	SafeGateFile    string `yaml:"safe_gate_file"`
}

var requiredConfigPaths = []yamlconfig.Path{
	{"enabled"},
	{"endpoint"},
	{"bearer_token_env"},
	{"timeout_ms"},
	{"workers"},
	{"queue_size"},
	{"cache_max_entries"},
	{"cache_ttl_seconds"},
	{"max_comment_bytes"},
	{"safe_gate_file"},
}

// LoadConfig reads optional Worker classifier config. Missing files disable the
// feature; present files are strict so operator typos do not silently change
// safety behavior.
func LoadConfig(path string) (Config, bool, error) {
	var cfg Config
	if strings.TrimSpace(path) == "" {
		return cfg, false, nil
	}
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cfg, false, nil
		}
		return cfg, false, err
	}
	if err := yamlconfig.DecodeFile(path, &cfg, requiredConfigPaths); err != nil {
		return Config{}, false, err
	}
	if err := normalizeConfig(&cfg); err != nil {
		return Config{}, false, err
	}
	return cfg, true, nil
}

func normalizeConfig(cfg *Config) error {
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)
	cfg.BearerTokenEnv = strings.TrimSpace(cfg.BearerTokenEnv)
	cfg.SafeGateFile = strings.TrimSpace(cfg.SafeGateFile)
	if cfg.TimeoutMS == 0 {
		cfg.TimeoutMS = defaultTimeoutMS
	}
	if cfg.Workers == 0 {
		cfg.Workers = defaultWorkers
	}
	if cfg.QueueSize == 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.CacheMaxEntries == 0 {
		cfg.CacheMaxEntries = defaultCacheMaxEntries
	}
	if cfg.CacheTTLSeconds == 0 {
		cfg.CacheTTLSeconds = defaultCacheTTLSeconds
	}
	if cfg.MaxCommentBytes == 0 {
		cfg.MaxCommentBytes = defaultMaxCommentBytes
	}
	if cfg.TimeoutMS < 0 || cfg.Workers < 0 || cfg.QueueSize < 0 ||
		cfg.CacheMaxEntries < 0 || cfg.CacheTTLSeconds < 0 || cfg.MaxCommentBytes < 0 {
		return fmt.Errorf("toxicity.yaml numeric settings must be >= 0")
	}
	// Validate before consumers convert units or apply their direct-constructor
	// fallback. A positive YAML choice must never silently become another value.
	if cfg.TimeoutMS > math.MaxInt32 {
		return fmt.Errorf("toxicity.yaml timeout_ms must be <= %d", math.MaxInt32)
	}
	if int64(cfg.CacheTTLSeconds) > math.MaxInt64/int64(time.Second) {
		return fmt.Errorf("toxicity.yaml cache_ttl_seconds must be <= %d", math.MaxInt64/int64(time.Second))
	}
	if cfg.Enabled {
		if cfg.Endpoint == "" {
			return fmt.Errorf("toxicity.yaml endpoint must not be empty when enabled")
		}
		parsed, err := url.ParseRequestURI(cfg.Endpoint)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return fmt.Errorf("toxicity.yaml endpoint must be an absolute URL")
		}
		if parsed.Scheme != "https" && parsed.Scheme != "http" {
			return fmt.Errorf("toxicity.yaml endpoint scheme must be http or https")
		}
		if cfg.BearerTokenEnv == "" {
			return fmt.Errorf("toxicity.yaml bearer_token_env must not be empty when enabled")
		}
		if strings.TrimSpace(os.Getenv(cfg.BearerTokenEnv)) == "" {
			return fmt.Errorf("toxicity worker bearer token missing; set %s", cfg.BearerTokenEnv)
		}
	}
	return nil
}

func (c Config) timeout() time.Duration {
	if c.TimeoutMS <= 0 || c.TimeoutMS > math.MaxInt32 {
		return time.Duration(defaultTimeoutMS) * time.Millisecond
	}
	return time.Duration(c.TimeoutMS) * time.Millisecond
}
