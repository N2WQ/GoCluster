package toxicity

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"dxcluster/spot"
)

type configDeadlineObservation struct {
	comment  string
	deadline time.Time
	seenAt   time.Time
}

type configDeadlineClient struct {
	calls chan configDeadlineObservation
}

func (c configDeadlineClient) Classify(ctx context.Context, comment string) (Decision, error) {
	deadline, _ := ctx.Deadline()
	c.calls <- configDeadlineObservation{comment: comment, deadline: deadline, seenAt: time.Now()}
	return Decision{Status: spot.ToxicitySafe, Model: "config-fixture"}, nil
}

func checkConfigDeadlineAndPayload(t *testing.T, cfg Config, wantTimeout time.Duration, wantBytes int) *Classifier {
	t.Helper()
	client := configDeadlineClient{calls: make(chan configDeadlineObservation, 1)}
	c := NewClassifier(cfg, testGate(t), client)
	if c == nil {
		t.Fatal("expected active classifier")
	}
	t.Cleanup(c.Stop)
	c.Start()
	s := spot.NewSpot("K1ABC", "W1XYZ", 14074, "CW")
	s.Comment = strings.Repeat("g", 3000)
	before := time.Now()
	if c.ClassifyOrEnqueue(s, before) {
		t.Fatal("fixture comment bypassed consumer")
	}
	select {
	case call := <-client.calls:
		if len(call.comment) != wantBytes || call.deadline.Before(before.Add(wantTimeout)) || call.deadline.After(call.seenAt.Add(wantTimeout)) {
			t.Fatalf("payload bytes=%d deadline=%s, want %d bytes and %s bracket", len(call.comment), call.deadline, wantBytes, wantTimeout)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not call fixture client")
	}
	select {
	case result := <-c.Results():
		if result.Spot != s || result.Decision.Status != spot.ToxicitySafe {
			t.Fatalf("unexpected consumer result: %+v", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not publish completed result")
	}
	return c
}

func TestLoadedConfigZeroDefaultsReachConsumers(t *testing.T) {
	t.Setenv("DXC_TEST_CONFIG_CONTRACT_TOKEN", "fixture-only")
	changes := map[string]string{"timeout_ms": "0", "workers": "0", "queue_size": "0", "cache_max_entries": "0", "cache_ttl_seconds": "0", "max_comment_bytes": "0"}
	cfg, present, err := loadContractConfig(t, true, changes)
	if err != nil || !present {
		t.Fatalf("all-zero config failed: %t %v", present, err)
	}
	c := checkConfigDeadlineAndPayload(t, cfg, 1200*time.Millisecond, 512)
	if cfg.Workers != 2 || cap(c.jobs) != 128 || cap(c.results) != 128 || c.cache.max != 4096 || c.cache.ttl != 86400*time.Second || c.DrainTimeout() != 2650*time.Millisecond {
		t.Fatalf("consumer defaults diverged: cfg=%+v jobs=%d results=%d cache=%+v drain=%s", cfg, cap(c.jobs), cap(c.results), c.cache, c.DrainTimeout())
	}
}

func TestLoadedConfigDurationLimitsReachConsumers(t *testing.T) {
	t.Setenv("DXC_TEST_CONFIG_CONTRACT_TOKEN", "fixture-only")
	for _, timeout := range []string{"1", "2147483647"} {
		cfg, _, err := loadContractConfig(t, true, map[string]string{"timeout_ms": timeout})
		if err != nil {
			t.Fatal(err)
		}
		ms, err := strconv.ParseInt(timeout, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		checkConfigDeadlineAndPayload(t, cfg, time.Duration(ms)*time.Millisecond, 512)
	}
	for _, ttl := range []string{"1", "9223372036"} {
		seconds, err := strconv.ParseInt(ttl, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if strconv.IntSize == 32 && seconds > 2147483647 {
			continue // The loader boundary test proves native integer refusal.
		}
		cfg, _, err := loadContractConfig(t, true, map[string]string{"cache_ttl_seconds": ttl})
		if err != nil {
			t.Fatal(err)
		}
		c := NewClassifier(cfg, testGate(t), errorClient{})
		t.Cleanup(c.Stop)
		now := time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC)
		c.cache.Put("expiry fixture", Decision{Status: spot.ToxicitySafe}, now)
		expiry := now.Add(time.Duration(seconds) * time.Second)
		if _, ok := c.cache.Get("expiry fixture", expiry.Add(-time.Nanosecond)); !ok {
			t.Fatalf("TTL %s expires early", ttl)
		}
		if _, ok := c.cache.Get("expiry fixture", expiry); ok {
			t.Fatalf("TTL %s survives exact expiry", ttl)
		}
	}
}

func TestConfigRawConstructorSemanticsAndDrain(t *testing.T) {
	base := Config{Enabled: true, Workers: 1, QueueSize: 1, CacheMaxEntries: 8, CacheTTLSeconds: 60, MaxCommentBytes: 512, TimeoutMS: 1200}
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"disabled", Config{Workers: 1, QueueSize: 1}},
		{"zero-workers", Config{Enabled: true, QueueSize: 1}},
		{"negative-workers", Config{Enabled: true, Workers: -1, QueueSize: 1}},
		{"zero-queue", Config{Enabled: true, Workers: 1}},
		{"negative-queue", Config{Enabled: true, Workers: 1, QueueSize: -1}},
	} {
		if c := NewClassifier(tc.cfg, testGate(t), errorClient{}); c != nil {
			c.Stop()
			t.Fatalf("raw %s should disable classifier", tc.name)
		}
	}
	if c := NewClassifier(base, testGate(t), nil); c != nil {
		c.Stop()
		t.Fatal("nil client should disable classifier")
	}
	for _, value := range []int{0, -1} {
		for _, field := range []string{"count", "ttl"} {
			cfg := base
			if field == "count" {
				cfg.CacheMaxEntries = value
			} else {
				cfg.CacheTTLSeconds = value
			}
			c := NewClassifier(cfg, testGate(t), errorClient{})
			if c.cache != nil {
				t.Errorf("raw %s=%d should disable cache", field, value)
			}
			c.Stop()
		}
		cfg := base
		cfg.MaxCommentBytes = value
		cfg.TimeoutMS = value
		checkConfigDeadlineAndPayload(t, cfg, 1200*time.Millisecond, 3000)
	}
	if strconv.IntSize == 64 {
		cfg := base
		tooLarge := int64(2147483648)
		cfg.TimeoutMS = int(tooLarge)
		checkConfigDeadlineAndPayload(t, cfg, 1200*time.Millisecond, 512)
	}
	for _, tc := range []struct{ ms, drain int }{
		{1, 500}, {125, 500}, {126, 502}, {1200, 2650},
		{2374, 4998}, {2375, 5000}, {2376, 5000}, {2147483647, 5000},
	} {
		cfg := base
		cfg.TimeoutMS = tc.ms
		c := NewClassifier(cfg, testGate(t), errorClient{})
		if got := c.DrainTimeout(); got != time.Duration(tc.drain)*time.Millisecond {
			t.Errorf("drain for %d ms = %s, want %d ms", tc.ms, got, tc.drain)
		}
		c.Stop()
	}
}
