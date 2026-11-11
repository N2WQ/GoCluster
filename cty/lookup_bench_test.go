package cty

import (
	"path/filepath"
	"testing"
)

func loadShippedCTY(tb testing.TB) *CTYDatabase {
	tb.Helper()
	db, err := LoadCTYDatabase(filepath.Join("..", "data", "cty", "cty.plist"))
	if err != nil {
		tb.Fatal(err)
	}
	return db
}

// These fixed calls cover ordinary fallback, a full exception, and a portable
// lookup without timing plist parsing or silently accepting a lookup miss.
func BenchmarkCTYLookup(b *testing.B) {
	db := loadShippedCTY(b)
	for _, call := range []string{"K1ABC", "K1DG", "K1DW", "JA1ABC", "N2WQ/VE3"} {
		b.Run(call, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if info, ok := db.LookupCallsignPortable(call); !ok || info == nil {
					b.Fatal("known CTY lookup missed")
				}
			}
		})
	}
}
