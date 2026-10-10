package toxicity

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Literal fixtures keep default expectations independent of normalization code.
var configContractLines = []string{
	"enabled: true", "endpoint: https://worker.example.test/classify",
	"bearer_token_env: DXC_TEST_CONFIG_CONTRACT_TOKEN", "timeout_ms: 1200",
	"workers: 2", "queue_size: 128", "cache_max_entries: 4096",
	"cache_ttl_seconds: 86400", "max_comment_bytes: 512", "safe_gate_file: gate.yaml",
}

func loadContractConfig(t *testing.T, enabled bool, changes map[string]string) (Config, bool, error) {
	t.Helper()
	lines := append([]string(nil), configContractLines...)
	lines[0] = fmt.Sprintf("enabled: %t", enabled)
	for i, line := range lines {
		key, _, _ := strings.Cut(line, ":")
		if replacement, ok := changes[key]; ok {
			if replacement == "<omit>" {
				lines[i] = ""
			} else {
				lines[i] = key + ": " + replacement
			}
		}
	}
	path := filepath.Join(t.TempDir(), "toxicity.yaml")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return LoadConfig(path)
}

func configContractNumeric(cfg Config) []int {
	return []int{cfg.TimeoutMS, cfg.Workers, cfg.QueueSize, cfg.CacheMaxEntries, cfg.CacheTTLSeconds, cfg.MaxCommentBytes}
}

func TestLoadConfigContractPresenceAndNumericValues(t *testing.T) {
	t.Setenv("DXC_TEST_CONFIG_CONTRACT_TOKEN", "fixture-only")
	keys := []string{"timeout_ms", "workers", "queue_size", "cache_max_entries", "cache_ttl_seconds", "max_comment_bytes"}
	defaults := []int{1200, 2, 128, 4096, 86400, 512}
	for _, enabled := range []bool{false, true} {
		for _, line := range configContractLines {
			key, _, _ := strings.Cut(line, ":")
			for _, value := range []string{"<omit>", "null", "{}"} {
				t.Run(fmt.Sprintf("enabled=%t/%s/%s", enabled, key, value), func(t *testing.T) {
					cfg, present, err := loadContractConfig(t, enabled, map[string]string{key: value})
					if err == nil || present || cfg != (Config{}) {
						t.Fatalf("invalid required key admitted: cfg=%+v present=%t err=%v", cfg, present, err)
					}
				})
			}
		}
		for i, key := range keys {
			for _, value := range []string{"-1", "zero-word", "0", "1"} {
				t.Run(fmt.Sprintf("enabled=%t/%s/%s", enabled, key, value), func(t *testing.T) {
					cfg, present, err := loadContractConfig(t, enabled, map[string]string{key: value})
					if value == "-1" || value == "zero-word" {
						if err == nil || present || cfg != (Config{}) {
							t.Fatalf("bad numeric value admitted: %+v %t %v", cfg, present, err)
						}
						return
					}
					want := append([]int(nil), defaults...)
					if value == "1" {
						want[i] = 1
					}
					if err != nil || !present || cfg.Enabled != enabled || fmt.Sprint(configContractNumeric(cfg)) != fmt.Sprint(want) {
						t.Fatalf("numeric values = %+v present=%t err=%v, want %v enabled=%t", cfg, present, err, want, enabled)
					}
				})
			}
		}
	}
}

func TestLoadConfigContractDurationBoundaries(t *testing.T) {
	t.Setenv("DXC_TEST_CONFIG_CONTRACT_TOKEN", "fixture-only")
	for _, enabled := range []bool{false, true} {
		for _, tc := range []struct {
			key   string
			value int64
			valid bool
		}{
			{"timeout_ms", 2147483647, true}, {"timeout_ms", 2147483648, false},
			{"cache_ttl_seconds", 9223372036, true}, {"cache_ttl_seconds", 9223372037, false},
		} {
			t.Run(fmt.Sprintf("enabled=%t/%s/%d", enabled, tc.key, tc.value), func(t *testing.T) {
				cfg, present, err := loadContractConfig(t, enabled, map[string]string{tc.key: strconv.FormatInt(tc.value, 10)})
				fitsInt := strconv.IntSize == 64 || tc.value <= 2147483647
				if !tc.valid || !fitsInt {
					if err == nil || present || cfg != (Config{}) || (fitsInt && !strings.Contains(err.Error(), tc.key)) {
						t.Fatalf("expected refusal for %s: %+v %t %v", tc.key, cfg, present, err)
					}
					return
				}
				if err != nil || !present {
					t.Fatalf("valid boundary refused: %t %v", present, err)
				}
				if tc.key == "timeout_ms" {
					if cfg.timeout() != 2147483647*time.Millisecond {
						t.Fatalf("timeout silently changed: %s", cfg.timeout())
					}
				} else {
					cache := NewCache(cfg.CacheMaxEntries, time.Duration(cfg.CacheTTLSeconds)*time.Second)
					if cache.ttl != 9223372036*time.Second {
						t.Fatalf("TTL silently changed: %s", cache.ttl)
					}
				}
			})
		}
	}
}

func TestLoadConfigContractCapacityLimitsAreNativeIntegers(t *testing.T) {
	t.Setenv("DXC_TEST_CONFIG_CONTRACT_TOKEN", "fixture-only")
	maxInt := int(^uint(0) >> 1)
	for _, key := range []string{"workers", "queue_size", "cache_max_entries", "max_comment_bytes"} {
		cfg, present, err := loadContractConfig(t, false, map[string]string{key: strconv.Itoa(maxInt)})
		if err != nil || !present {
			t.Fatalf("%s native max refused: %v", key, err)
		}
		got := map[string]int{"workers": cfg.Workers, "queue_size": cfg.QueueSize, "cache_max_entries": cfg.CacheMaxEntries, "max_comment_bytes": cfg.MaxCommentBytes}[key]
		if got != maxInt {
			t.Fatalf("%s changed to %d", key, got)
		}
		// Deliberately never construct or start these boundary-sized capacities.
	}
}

func TestLoadConfigContractOptionalAndStrict(t *testing.T) {
	for _, path := range []string{"", " \t", filepath.Join(t.TempDir(), "absent.yaml")} {
		cfg, present, err := LoadConfig(path)
		if err != nil || present || cfg != (Config{}) {
			t.Fatalf("optional path %q: %+v %t %v", path, cfg, present, err)
		}
	}
	path := filepath.Join(t.TempDir(), "unknown.yaml")
	if err := os.WriteFile(path, []byte(strings.Join(configContractLines, "\n")+"\nunknown_key: 1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, present, err := LoadConfig(path); err == nil || present || !strings.Contains(err.Error(), "unknown_key") {
		t.Fatalf("unknown key not refused: %t %v", present, err)
	}
}
