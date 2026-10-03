//go:build sqlite3_qualification

package v15probe

import (
	"context"
	sqlite3 "github.com/ncruces/go-sqlite3"
)

func fallbackContext(ctx context.Context) context.Context {
	return sqlite3.WithFallbackForQualification(ctx)
}

func secondaryMode() string { return "fallback" }
