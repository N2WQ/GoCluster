package peer

import "time"

func (t *topologyStore) applyPC92Frame(f *Frame, now time.Time) {
	t.applyPC92(f, now)
}
