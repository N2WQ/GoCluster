package pathreliability

import (
	"fmt"
	"strings"
	"testing"
)

func TestS6SelectionConfigLoadingContract(t *testing.T) {
	for _, key := range []string{"min_fine_weight", "fine_only_weight"} {
		t.Run(key+"/missing", func(t *testing.T) {
			_, err := LoadFile(writeTempConfigWithoutKey(t, key))
			if err == nil || !strings.Contains(err.Error(), key) {
				t.Fatalf("missing %s: %v", key, err)
			}
		})
		for _, value := range []string{"null", "0", "-1"} {
			t.Run(key+"/"+value, func(t *testing.T) {
				_, err := LoadFile(writeTempConfigOverlay(t, fmt.Sprintf("%s: %s\n", key, value)))
				if err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("invalid %s %s: %v", key, value, err)
				}
			})
		}
	}
	cfg, err := LoadFile(writeTempConfigOverlay(t, "min_fine_weight: 30\nfine_only_weight: 20\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MinFineWeight != 30 || cfg.FineOnlyWeight != 30 {
		t.Fatalf("normalization relationship lost: min=%g only=%g", cfg.MinFineWeight, cfg.FineOnlyWeight)
	}
}
