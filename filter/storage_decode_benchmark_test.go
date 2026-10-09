package filter

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func storageScaleFixture(shape string, size int) string {
	var raw strings.Builder
	if shape == "legacy roots" {
		for i := 0; i < size; i++ {
			fmt.Fprintf(&raw, "ignored%05d: {unused: false}\n", i)
		}
		return raw.String()
	}
	var rules strings.Builder
	for i := 0; i < size; i++ {
		if i != 0 {
			rules.WriteByte(',')
		}
		fmt.Fprintf(&rules, "X%05d: true", i)
	}
	switch shape {
	case "direct map":
		return "bands: {" + rules.String() + "}\nallbands: false\n"
	case "merged map":
		return "bands: {<<: {" + rules.String() + "}}\nallbands: false\n"
	case "nested baseline":
		return "preset: {name: TEST, baseline: {bands: {" + rules.String() + "}, allbands: false}}\n"
	default:
		panic("unknown storage scale shape")
	}
}

// Correctness is checked before and after timed work. These are storage-load
// measurements, not a claim about the running cluster's latency or capacity.
func BenchmarkStorageDecodeScale(b *testing.B) {
	for _, shape := range []string{"direct map", "merged map", "legacy roots", "nested baseline"} {
		for _, size := range []int{128, 512, 2048, 8192} {
			raw := []byte(storageScaleFixture(shape, size))
			want, err := frozenDecodeUserRecord(raw)
			if err != nil {
				b.Fatal(err)
			}
			for _, decoder := range []struct {
				name string
				load func([]byte) (*UserRecord, error)
			}{{"frozen", frozenDecodeUserRecord}, {"candidate", candidateStorageRecord}} {
				b.Run(fmt.Sprintf("%s/%d/%s", shape, size, decoder.name), func(b *testing.B) {
					got, err := decoder.load(raw)
					if err != nil || !reflect.DeepEqual(got, want) {
						b.Fatalf("scale fixture differs: %v", err)
					}
					b.ReportAllocs()
					b.SetBytes(int64(len(raw)))
					b.ResetTimer()
					for range b.N {
						got, err = decoder.load(raw)
						if err != nil {
							b.Fatal(err)
						}
					}
					b.StopTimer()
					if !reflect.DeepEqual(got, want) {
						b.Fatal("timed decode changed complete result")
					}
				})
			}
		}
	}
}
