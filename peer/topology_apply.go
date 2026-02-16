package peer

import (
	"context"
	"time"
)

func (t *topologyStore) applyPC92Frame(ctx context.Context, f *Frame, now time.Time) {
	t.applyPC92(ctx, f, now)
}
