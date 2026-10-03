//go:build sqlite3_qualification

package v15probe

import "context"

func fallbackContext(context.Context) context.Context { panic("Windows fallback requested on Linux") }
func secondaryMode() string                           { return "native" }
