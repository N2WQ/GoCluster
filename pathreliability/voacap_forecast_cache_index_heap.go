// File role: Owns indexed persisted-cache victim and actual-hour eligibility
// heaps. A node has exactly one position in each heap; removals clear both slots.
package pathreliability

import (
	"container/heap"
	"time"
)

type voacapForecastCacheRetentionHeap []*voacapForecastCacheIndexEntry

func (h voacapForecastCacheRetentionHeap) Len() int { return len(h) }
func (h voacapForecastCacheRetentionHeap) Less(i, j int) bool {
	return forecastCacheRetentionBetter(h[j].storedAtNS, h[j].dbKey, h[i].storedAtNS, h[i].dbKey)
}
func (h voacapForecastCacheRetentionHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].retentionIndex = i
	h[j].retentionIndex = j
}
func (h *voacapForecastCacheRetentionHeap) Push(value any) {
	e := value.(*voacapForecastCacheIndexEntry) //nolint:errcheck // Private heap callers supply only owned nodes; a type mismatch must panic.
	e.retentionIndex = len(*h)
	*h = append(*h, e)
}
func (h *voacapForecastCacheRetentionHeap) Pop() any {
	last := len(*h) - 1
	e := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	e.retentionIndex = -1
	return e
}

// Subtree aggregates certify each owner's frozen start/end/expiry bounds.
// A represented-hour hole never authorizes deletion; diagnostics count current
// rows separately. Maximum start and minimum end/expiry certify a whole subtree.
type voacapForecastCacheEligibilityHeap []*voacapForecastCacheIndexEntry

func (h voacapForecastCacheEligibilityHeap) Len() int { return len(h) }
func (h voacapForecastCacheEligibilityHeap) Less(i, j int) bool {
	if h[i].windowStartNS != h[j].windowStartNS {
		return h[i].windowStartNS < h[j].windowStartNS
	}
	return h[i].dbKey < h[j].dbKey
}
func (h voacapForecastCacheEligibilityHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].eligibilityIndex = i
	h[j].eligibilityIndex = j
}
func (h *voacapForecastCacheEligibilityHeap) Push(value any) {
	e := value.(*voacapForecastCacheIndexEntry) //nolint:errcheck // Private heap callers supply only owned nodes; a type mismatch must panic.
	e.eligibilityIndex = len(*h)
	*h = append(*h, e)
}
func (h *voacapForecastCacheEligibilityHeap) Pop() any {
	last := len(*h) - 1
	e := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	e.eligibilityIndex = -1
	return e
}

// Recompute only after the final sift, not on each swap. Each affected ancestor
// path is visited once, preserving O(log N) aggregate maintenance.
func (h *voacapForecastCacheEligibilityHeap) add(e *voacapForecastCacheIndexEntry) {
	original := len(*h)
	heap.Push(h, e)
	h.recalculatePath(original)
}

func (h *voacapForecastCacheEligibilityHeap) fix(e *voacapForecastCacheIndexEntry) {
	original := e.eligibilityIndex
	heap.Fix(h, original)
	h.recalculatePath(max(original, e.eligibilityIndex))
}

func (h *voacapForecastCacheEligibilityHeap) remove(e *voacapForecastCacheIndexEntry) {
	original := e.eligibilityIndex
	last := len(*h) - 1
	moved := (*h)[last]
	heap.Remove(h, original)
	if original < len(*h) {
		h.recalculatePath(max(original, moved.eligibilityIndex))
	}
	if last > 0 {
		h.recalculatePath((last - 1) / 2)
	}
}

func (h voacapForecastCacheEligibilityHeap) recalculatePath(index int) {
	for index >= 0 && index < len(h) {
		e := h[index]
		e.subtreeHourMask = e.hourMask
		e.subtreeMinStart = e.windowStartNS
		e.subtreeMaxStart = e.windowStartNS
		e.subtreeMinEnd = e.windowEndNS
		e.subtreeMinExpiry = e.expiresNS
		left := 2*index + 1
		if left > index && left < len(h) {
			h.combineSubtree(e, h[left])
			if left+1 < len(h) {
				h.combineSubtree(e, h[left+1])
			}
		}
		if index == 0 {
			return
		}
		index = (index - 1) / 2
	}
}

func (h voacapForecastCacheEligibilityHeap) combineSubtree(parent, child *voacapForecastCacheIndexEntry) {
	parent.subtreeHourMask &= child.subtreeHourMask
	parent.subtreeMinStart = min(parent.subtreeMinStart, child.subtreeMinStart)
	parent.subtreeMaxStart = max(parent.subtreeMaxStart, child.subtreeMaxStart)
	parent.subtreeMinEnd = min(parent.subtreeMinEnd, child.subtreeMinEnd)
	parent.subtreeMinExpiry = min(parent.subtreeMinExpiry, child.subtreeMinExpiry)
}

func forecastCacheSubtreeEligible(e *voacapForecastCacheIndexEntry, now time.Time, _ int) bool {
	return now.UnixNano() >= e.subtreeMaxStart && now.UnixNano() < e.subtreeMinEnd && now.UnixNano() <= e.subtreeMinExpiry
}

func forecastCacheNodeEligible(e *voacapForecastCacheIndexEntry, now time.Time, _ int) bool {
	return now.UnixNano() >= e.windowStartNS && now.UnixNano() < e.windowEndNS && now.UnixNano() <= e.expiresNS
}

func (h voacapForecastCacheEligibilityHeap) findInvalid(now time.Time, horizonHours int) *voacapForecastCacheIndexEntry {
	if len(h) == 0 || forecastCacheSubtreeEligible(h[0], now, horizonHours) {
		return nil
	}
	for index := 0; ; {
		e := h[index]
		if !forecastCacheNodeEligible(e, now, horizonHours) {
			return e
		}
		left := 2*index + 1
		if left > index && left < len(h) && !forecastCacheSubtreeEligible(h[left], now, horizonHours) {
			index = left
		} else {
			// The current subtree was uncertified and its own node is eligible.
			// If its left child is certified, its right child must contain a victim.
			index = left + 1
		}
	}
}
