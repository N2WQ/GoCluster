package filter

import (
	"errors"
	"strings"
	"testing"
)

func FuzzStorageAdmissionWork(f *testing.F) {
	for _, seed := range [][]byte{{0, 0}, {255, 100}, {255, 255}, {0, 255}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, choices []byte) {
		if len(choices) < 2 {
			return
		}
		length, occurrences := 1+int(choices[0])*128, 1+int(choices[1])*3
		raw := v5StorageMinimalRecord + "recent_ips: [&text " + strings.Repeat("A", length)
		raw += strings.Repeat(", *text", occurrences-1) + "]\n"
		// Each logical reference must at least pay its scalar's bytes even
		// though normalization retains only five recent-IP entries.
		got, err := DecodeV5UserRecord([]byte(raw))
		if length*occurrences > maxUserRecordWork {
			if !errors.Is(err, ErrStorageAdmission) || !strings.Contains(err.Error(), "work") || got != nil {
				t.Fatal("alias expansion bypassed active shared budget", err)
			}
		} else if err == nil && len(got.RecentIPs) > maxRecentIPs {
			t.Fatal("admitted normalized metadata exceeded cap")
		}
	})
}
