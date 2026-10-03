//go:build sqlite3_qualification && windows

package peer

import (
	"context"
	sqlite3 "github.com/ncruces/go-sqlite3"
)

func topologyQualificationModes() []string { return []string{"native", "fallback"} }
func topologyQualificationContext(ctx context.Context, mode string) context.Context {
	if mode == "fallback" {
		return sqlite3.WithFallbackForQualification(ctx)
	}
	return ctx
}
