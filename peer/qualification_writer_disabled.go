//go:build !qualification

package peer

import (
	"context"
	"time"
)

type qualificationWriterState struct{}

func (*qualificationWriterState) wait(context.Context, time.Time) error { return nil }
