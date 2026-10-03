//go:build sqlite3_qualification && windows

package sqlite3

import (
	"context"
	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
)

type fallbackQualificationKey struct{}

// WithFallbackForQualification selects the supported unavailable-API path in
// qualification builds only. Production selects that path from OS capability.
func WithFallbackForQualification(ctx context.Context) context.Context {
	return context.WithValue(ctx, fallbackQualificationKey{}, true)
}

func configureQualificationMemory(ctx context.Context, mem *sqlite3_wrap.Memory) {
	mem.ForceFallback, _ = ctx.Value(fallbackQualificationKey{}).(bool)
}
