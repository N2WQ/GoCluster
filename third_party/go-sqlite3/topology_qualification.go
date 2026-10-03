//go:build sqlite3_qualification

package sqlite3

import (
	"context"
	"errors"
	"sync/atomic"
)

type qualificationState struct{ retirementBlocked *atomic.Bool }
type retirementQualificationKey struct{}

// WithRetirementBlockedForQualification keeps the real owner allocated while a
// fixture rejects retirement. It adds no callback, worker or production switch.
func WithRetirementBlockedForQualification(ctx context.Context, blocked *atomic.Bool) context.Context {
	return context.WithValue(ctx, retirementQualificationKey{}, blocked)
}

func configureQualificationConn(ctx context.Context, c *Conn) {
	c.qualification.retirementBlocked, _ = ctx.Value(retirementQualificationKey{}).(*atomic.Bool)
}

func qualificationRetirement(c *Conn) error {
	if blocked := c.qualification.retirementBlocked; blocked != nil && blocked.Load() {
		return errors.New("qualification: retirement blocked with real owner retained")
	}
	return nil
}
