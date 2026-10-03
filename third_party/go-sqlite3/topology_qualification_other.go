//go:build sqlite3_qualification && !windows

package sqlite3

import (
	"context"
	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
)

func configureQualificationMemory(context.Context, *sqlite3_wrap.Memory) {}
