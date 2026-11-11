package cluster

import (
	"fmt"
	"path/filepath"
	"testing"

	"dxcluster/cty"
)

func loadShippedClusterCTY(tb testing.TB) *cty.CTYDatabase {
	tb.Helper()
	db, err := cty.LoadCTYDatabase(filepath.Join("..", "..", "data", "cty", "cty.plist"))
	if err != nil {
		tb.Fatal(err)
	}
	return db
}

func BenchmarkCTYMetadataCache(b *testing.B) {
	db := loadShippedClusterCTY(b)
	for _, name := range []string{"warm_hit", "warm_miss", "cold_churn"} {
		b.Run(name, func(b *testing.B) {
			cache := newCallMetaCache(64, 0)
			call := "K1ABC"
			if name == "warm_miss" {
				call = "QZ9ABC"
			}
			cache.LookupCTY(call, db)
			calls := make([]string, 256)
			for i := range calls {
				calls[i] = fmt.Sprintf("K1ZXAA%04d", i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if name == "cold_churn" {
					call = calls[i&255]
				}
				info, ok, cached := cache.LookupCTY(call, db)
				if name == "warm_miss" {
					if ok || info != nil || !cached {
						b.Fatal("negative cache contract lost")
					}
				} else if !ok || info == nil || (name == "warm_hit" && !cached) || (name == "cold_churn" && cached) {
					b.Fatal("positive cache workload lost its admission shape")
				}
			}
		})
	}
}
