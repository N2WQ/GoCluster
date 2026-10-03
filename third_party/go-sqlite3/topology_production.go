//go:build !sqlite3_qualification

package sqlite3

import (
	"context"
	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
)

func configureQualificationMemory(context.Context, *sqlite3_wrap.Memory) {}

type qualificationState struct{}

func configureQualificationConn(context.Context, *Conn) {}
func qualificationRetirement(*Conn) error               { return nil }
