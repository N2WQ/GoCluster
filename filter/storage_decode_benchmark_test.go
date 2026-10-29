package filter

import (
	"fmt"
	"strings"
	"testing"
)

func BenchmarkStorageDecodeScale(b *testing.B) {
	for _, size := range []int{128, 512, 2048, 8192} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			raw := []byte(v6StorageMinimalRecord + "recent_ips: [&text " + strings.Repeat("A", size) + ", *text]\n")
			if _, err := DecodeV6UserRecord(raw); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(raw)))
			b.ResetTimer()
			for b.Loop() {
				if _, err := DecodeV6UserRecord(raw); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
