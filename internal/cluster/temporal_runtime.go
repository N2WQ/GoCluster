// File role: Owns bounded runtime temporal holds and their ordered release.
// Crawler notes: The output consumer owns pending/heap state. Forced retirement
// selects future-due holds at actual UTC time while preserving decoder gates.
// Related docs: docs/decisions/ADR-0038-fixed-lag-temporal-decoder-main-replay-parity.md,
// docs/decisions/ADR-0275-runtime-output-retirement.md.
package cluster

import (
	"container/heap"
	"time"

	"dxcluster/config"
	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

type runtimeTemporalPending struct {
	id                         uint64
	spot                       *spot.Spot
	evidence                   spot.ResolverEvidence
	hasEvidence                bool
	stabilizerResolverKey      spot.ResolverSignalKey
	hasStabilizerResolverKey   bool
	stabilizerEvidenceEnqueued bool
	selection                  correctionflow.ResolverPrimarySelection
	maxAt                      time.Time
}

type runtimeTemporalRelease struct {
	pending  runtimeTemporalPending
	decision correctionflow.TemporalDecision
}

type runtimeTemporalItem struct {
	id  uint64
	due time.Time
	seq uint64
}

type runtimeTemporalHeap []*runtimeTemporalItem

// runtimeTemporalController owns main-loop temporal pending state, heap order,
// and deterministic request ids. It is used only by the single output
// goroutine; the decoder retains its own internal locking as before.
type runtimeTemporalController struct {
	enabled bool
	decoder *correctionflow.TemporalDecoder
	pending map[uint64]runtimeTemporalPending
	queue   runtimeTemporalHeap
	nextID  uint64
	seq     uint64
}

func (h runtimeTemporalHeap) Len() int { return len(h) }

func (h runtimeTemporalHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].seq < h[j].seq
	}
	return h[i].due.Before(h[j].due)
}

func (h runtimeTemporalHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *runtimeTemporalHeap) Push(x any) {
	item, ok := x.(*runtimeTemporalItem)
	if !ok {
		return
	}
	*h = append(*h, item)
}

func (h *runtimeTemporalHeap) Pop() any {
	old := *h
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func popRuntimeTemporalDue(h *runtimeTemporalHeap, now time.Time, force bool) []*runtimeTemporalItem {
	if h == nil || h.Len() == 0 {
		return nil
	}
	out := make([]*runtimeTemporalItem, 0, 8)
	for h.Len() > 0 {
		head := (*h)[0]
		if head == nil || (!force && head.due.After(now)) {
			break
		}
		itemAny := heap.Pop(h)
		if item, ok := itemAny.(*runtimeTemporalItem); ok && item != nil {
			out = append(out, item)
		}
	}
	return out
}

func runtimeTemporalNextDue(h *runtimeTemporalHeap) (time.Time, bool) {
	if h == nil || h.Len() == 0 {
		return time.Time{}, false
	}
	head := (*h)[0]
	if head == nil {
		return time.Time{}, false
	}
	return head.due, true
}

func newRuntimeTemporalController(cfg config.CallCorrectionConfig) *runtimeTemporalController {
	controller := &runtimeTemporalController{}
	if !cfg.Enabled || !cfg.TemporalDecoder.Enabled {
		return controller
	}
	controller.decoder = correctionflow.NewTemporalDecoder(cfg)
	if controller.decoder == nil || !controller.decoder.Enabled() {
		controller.decoder = nil
		return controller
	}
	controller.enabled = true
	controller.pending = make(map[uint64]runtimeTemporalPending)
	controller.nextID = 1
	heap.Init(&controller.queue)
	return controller
}

func (c *runtimeTemporalController) Enabled() bool {
	return c != nil && c.enabled && c.decoder != nil
}

func (c *runtimeTemporalController) Observe(
	observedAt time.Time,
	key spot.ResolverSignalKey,
	subject string,
	pending runtimeTemporalPending,
) (bool, uint64, string) {
	if !c.Enabled() {
		return false, 0, "disabled"
	}
	observedAt = observedAt.UTC()
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	id := c.nextID
	c.nextID++
	pending.selection.Snapshot = pending.selection.Snapshot.Compact()
	accepted, reason := c.decoder.Observe(correctionflow.TemporalObservation{
		ID:          id,
		ObservedAt:  observedAt,
		Key:         key,
		SubjectCall: subject,
		Selection:   pending.selection,
	})
	if !accepted {
		return false, id, reason
	}
	pending.id = id
	pending.maxAt = observedAt.Add(c.decoder.MaxWaitDuration())
	c.pending[id] = pending
	heap.Push(&c.queue, &runtimeTemporalItem{
		id:  id,
		due: observedAt.Add(c.decoder.LagDuration()),
		seq: c.seq,
	})
	c.seq++
	return true, id, ""
}

func (c *runtimeTemporalController) Drain(now time.Time, force bool) []runtimeTemporalRelease {
	if !c.Enabled() {
		return nil
	}
	now = now.UTC()
	// Force retires future-due requests too; the decoder still receives the
	// actual clock and keeps its existing evidence/score/fallback rules.
	due := popRuntimeTemporalDue(&c.queue, now, force)
	if len(due) == 0 {
		return nil
	}
	releases := make([]runtimeTemporalRelease, 0, len(due))
	for _, item := range due {
		if item == nil {
			continue
		}
		pending, ok := c.pending[item.id]
		if !ok || pending.spot == nil {
			continue
		}
		decision := c.decoder.Evaluate(item.id, now, force)
		if decision.Action == correctionflow.TemporalDecisionActionDefer {
			nextDue := pending.maxAt
			if nextDue.Before(now) {
				nextDue = now
			}
			heap.Push(&c.queue, &runtimeTemporalItem{
				id:  item.id,
				due: nextDue,
				seq: c.seq,
			})
			c.seq++
			continue
		}
		delete(c.pending, item.id)
		releases = append(releases, runtimeTemporalRelease{
			pending:  pending,
			decision: decision,
		})
	}
	return releases
}

func (c *runtimeTemporalController) NextDue() (time.Time, bool) {
	if !c.Enabled() {
		return time.Time{}, false
	}
	return runtimeTemporalNextDue(&c.queue)
}
