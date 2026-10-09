package filter

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Controlled well-formed shapes cross the production work bounds while raw,
// final configuration and canonical bytes still fit. Hand-derived P+E totals
// and frozen stock decoding are separate oracles; unlimited helpers alone
// cannot demonstrate active admission.
func FuzzStorageAdmissionWork(f *testing.F) {
	for _, seed := range [][]byte{{0, 0, 0}, {255, 100, 0}, {255, 255, 0}, {0, 255, 1}, {255, 100, 1}, {255, 200, 1}, {255, 255, 1}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, choices []byte) {
		if len(choices) < 3 {
			return
		}
		length, occurrences := 1+int(choices[0])*128, 1+int(choices[1])*3
		if choices[2]%2 == 0 {
			raw := "legacy: &text " + strings.Repeat("A", length) + "\nrecent_ips: [" + strings.TrimSuffix(strings.Repeat("*text,", occurrences), ",") + "]\n"
			var document yaml.Node
			if err := yaml.Unmarshal([]byte(raw), &document); err != nil {
				t.Fatal(err)
			}
			got, work, err := decodeStoredUserRecordDocument(&document)
			wantWork := uint64(42 + length + occurrences*(length+3))
			if wantWork > maxUserRecordWork {
				if !errors.Is(err, ErrStorageAdmission) || !strings.Contains(err.Error(), "decoding work") || got != nil {
					t.Fatalf("active record work refusal: %d %v", wantWork, err)
				}
				return
			}
			if err != nil || work.P+work.E != wantWork {
				t.Fatalf("accepted record work %v want %d: %v", work, wantWork, err)
			}
			got.RecentIPs = trimRecentIPs(got.RecentIPs)
			if err := AdmitUserRecord(got); err != nil {
				t.Fatal("work control exceeds another admission", err)
			}
			want, err := frozenDecodeUserRecord([]byte(raw))
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("fitting stock record differs: %v", err)
			}
			return
		}
		raw := storageRepeatedPresets(occurrences, length)
		var document yaml.Node
		if err := yaml.Unmarshal([]byte(raw), &document); err != nil {
			t.Fatal(err)
		}
		got, work, err := decodeStoredPresetCollectionDocument(&document)
		wantWork := uint64(79 + 2*length + occurrences*(55+length))
		if wantWork > maxPresetCollectionWork {
			if !errors.Is(err, ErrStorageAdmission) || !strings.Contains(err.Error(), "decoding work") || got != nil {
				t.Fatalf("active collection work refusal: %d %v", wantWork, err)
			}
			return
		}
		if err != nil || work.P+work.E != wantWork {
			t.Fatalf("accepted collection work %v want %d: %v", work, wantWork, err)
		}
		want, err := frozenDecodePresetCollection([]byte(raw))
		if err != nil || !reflect.DeepEqual(got.Presets, want) {
			t.Fatalf("fitting stock collection differs: %v", err)
		}
	})
}
