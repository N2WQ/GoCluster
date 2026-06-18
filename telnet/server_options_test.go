package telnet

import (
	"testing"
	"time"

	"dxcluster/filter"
)

func TestNormalizeServerOptionsBroadcastBatchIntervalDefaultAndExplicitZero(t *testing.T) {
	defaulted := normalizeServerOptions(ServerOptions{})
	if defaulted.BroadcastBatchInterval != defaultBroadcastBatchInterval {
		t.Fatalf("default broadcast batch interval = %v, want %v", defaulted.BroadcastBatchInterval, defaultBroadcastBatchInterval)
	}

	disabled := normalizeServerOptions(ServerOptions{
		BroadcastBatchInterval:    0,
		BroadcastBatchIntervalSet: true,
	})
	if disabled.BroadcastBatchInterval != 0 {
		t.Fatalf("explicit zero broadcast batch interval = %v, want 0", disabled.BroadcastBatchInterval)
	}

	configured := normalizeServerOptions(ServerOptions{
		BroadcastBatchInterval:    time.Millisecond,
		BroadcastBatchIntervalSet: true,
	})
	if configured.BroadcastBatchInterval != time.Millisecond {
		t.Fatalf("configured broadcast batch interval = %v, want 1ms", configured.BroadcastBatchInterval)
	}
}

func TestNormalizeServerOptionsAutoReadPauseBounds(t *testing.T) {
	disabled := normalizeServerOptions(ServerOptions{})
	if disabled.AutoReadPauseMinRows != 0 || disabled.AutoReadPauseDuration != 0 {
		t.Fatalf("default auto read pause = rows:%d duration:%s, want disabled", disabled.AutoReadPauseMinRows, disabled.AutoReadPauseDuration)
	}

	configured := normalizeServerOptions(ServerOptions{
		AutoReadPauseMinRows:  10,
		AutoReadPauseDuration: 30 * time.Second,
	})
	if configured.AutoReadPauseMinRows != 10 || configured.AutoReadPauseDuration != 30*time.Second {
		t.Fatalf("configured auto read pause = rows:%d duration:%s, want rows:10 duration:30s", configured.AutoReadPauseMinRows, configured.AutoReadPauseDuration)
	}

	capped := normalizeServerOptions(ServerOptions{
		AutoReadPauseMinRows:  999,
		AutoReadPauseDuration: time.Hour,
	})
	if capped.AutoReadPauseMinRows != maxAutoReadPauseRows || capped.AutoReadPauseDuration != maxAutoReadPauseDuration {
		t.Fatalf("capped auto read pause = rows:%d duration:%s, want rows:%d duration:%s", capped.AutoReadPauseMinRows, capped.AutoReadPauseDuration, maxAutoReadPauseRows, maxAutoReadPauseDuration)
	}
}

func TestNormalizeServerOptionsDefaultDedupePolicy(t *testing.T) {
	defaulted := normalizeServerOptions(ServerOptions{})
	if defaulted.DefaultDedupePolicy != filter.DedupePolicySlow {
		t.Fatalf("default dedupe policy = %q, want SLOW", defaulted.DefaultDedupePolicy)
	}

	configured := normalizeServerOptions(ServerOptions{DefaultDedupePolicy: filter.DedupePolicyMed})
	if configured.DefaultDedupePolicy != filter.DedupePolicyMed {
		t.Fatalf("configured dedupe policy = %q, want MED", configured.DefaultDedupePolicy)
	}
}

func TestPreLoginTemplateUsesConfiguredDedupeDefault(t *testing.T) {
	server := NewServer(ServerOptions{
		DefaultDedupePolicy: filter.DedupePolicySlow,
		DedupeSlowEnabled:   true,
	}, nil)
	data := server.preLoginTemplateData(time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC))
	if data.dedupePolicy != filter.DedupePolicySlow {
		t.Fatalf("pre-login dedupe policy = %q, want SLOW", data.dedupePolicy)
	}
}

func TestConfiguredDedupeDefaultFallsBackWhenDisabled(t *testing.T) {
	server := NewServer(ServerOptions{
		DefaultDedupePolicy: filter.DedupePolicySlow,
		DedupeFastEnabled:   true,
		DedupeMedEnabled:    true,
		DedupeSlowEnabled:   false,
	}, nil)
	if got := server.effectiveDefaultDedupePolicy(); got != dedupePolicyFast {
		t.Fatalf("effective default policy = %v, want FAST fallback", got)
	}
}
