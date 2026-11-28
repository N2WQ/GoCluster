package pathreliability

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadFilePropPriorFeedsShippedDefaults(t *testing.T) {
	cfg, warnings, errs, err := LoadFileWithDiagnostics(filepath.Join("..", "data", "config", "path_reliability.yaml"))
	if err != nil {
		t.Fatalf("LoadFileWithDiagnostics: %v", err)
	}
	if len(warnings) != 0 || len(errs) != 0 {
		t.Fatalf("unexpected diagnostics: warnings=%v errs=%v", warnings, errs)
	}
	if cfg.VOACAPFallback.Enabled {
		t.Fatalf("shipped voacap_fallback.enabled must be false")
	}
	if cfg.PropPriorFallback == nil || !cfg.PropPriorFallback.Enabled {
		t.Fatalf("shipped prop_prior_fallback must be enabled by default")
	}
	if cfg.PropPriorFeeds == nil {
		t.Fatalf("shipped path_reliability.yaml must define prop_prior_feeds")
	}
	if !cfg.PropPriorFeeds.Enabled {
		t.Fatalf("shipped prop_prior_feeds.enabled must be true")
	}
	if cfg.PropPriorFeeds.Hp30PollIntervalSeconds != 600 ||
		cfg.PropPriorFeeds.SFIPollIntervalSeconds != 1800 ||
		cfg.PropPriorFeeds.RTSWPollIntervalSeconds != 300 ||
		cfg.PropPriorFeeds.DstPollIntervalSeconds != 900 ||
		cfg.PropPriorFeeds.FailoverConsecutiveErrors != 3 {
		t.Fatalf("unexpected shipped prop_prior_feeds defaults: %+v", cfg.PropPriorFeeds)
	}
}

func TestLoadFilePropPriorFeedsOmittedIsDisabled(t *testing.T) {
	path := writeTempConfigWithoutKey(t, "prop_prior_feeds")
	cfg, warnings, errs, err := LoadFileWithDiagnostics(path)
	if err != nil || len(warnings) != 0 || len(errs) != 0 {
		t.Fatalf("omitted prop_prior_feeds must load cleanly: err=%v warnings=%v errs=%v", err, warnings, errs)
	}
	if cfg.PropPriorFeeds != nil {
		t.Fatalf("omitted prop_prior_feeds should leave pointer nil, got %+v", cfg.PropPriorFeeds)
	}
}

func TestLoadFilePropPriorFeedsRejectsNullMissingAndInvalidValuesEvenWhenDisabled(t *testing.T) {
	t.Run("null section", func(t *testing.T) {
		path := writeTempConfigOverlay(t, "prop_prior_feeds: null\n")
		if _, err := LoadFile(path); err == nil || !strings.Contains(strings.ToLower(err.Error()), "must not be null") {
			t.Fatalf("expected null prop_prior_feeds error, got %v", err)
		}
	})

	t.Run("missing required child key", func(t *testing.T) {
		path := writeTempConfigWithoutKey(t, "prop_prior_feeds", "user_agent")
		if _, err := LoadFile(path); err == nil || !strings.Contains(err.Error(), "prop_prior_feeds.user_agent") {
			t.Fatalf("expected missing user_agent error, got %v", err)
		}
	})

	t.Run("invalid scheme while disabled", func(t *testing.T) {
		path := writeTempConfigOverlay(t, "prop_prior_feeds:\n  enabled: false\n  hp30_primary_url: ftp://example.invalid/hp30\n")
		if _, err := LoadFile(path); err == nil || !strings.Contains(err.Error(), "hp30_primary_url") {
			t.Fatalf("expected invalid scheme error, got %v", err)
		}
	})

	t.Run("zero interval while disabled", func(t *testing.T) {
		path := writeTempConfigOverlay(t, "prop_prior_feeds:\n  enabled: false\n  rtsw_poll_interval_seconds: 0\n")
		if _, err := LoadFile(path); err == nil || !strings.Contains(err.Error(), "rtsw_poll_interval_seconds") {
			t.Fatalf("expected zero interval error, got %v", err)
		}
	})

	t.Run("enabled valid config loads", func(t *testing.T) {
		path := writeTempConfigOverlay(t, "prop_prior_feeds:\n  enabled: true\n")
		cfg, err := LoadFile(path)
		if err != nil {
			t.Fatalf("valid enabled prop_prior_feeds failed: %v", err)
		}
		if cfg.PropPriorFeeds == nil || !cfg.PropPriorFeeds.Enabled {
			t.Fatalf("expected PropPriorFeeds.Enabled=true, got %+v", cfg.PropPriorFeeds)
		}
	})
}
