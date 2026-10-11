package main

import (
	"container/heap"
	"time"

	"dxcluster/internal/correctionflow"
	"dxcluster/spot"
)

// replayTemporalPending owns one held observation until decoder retirement.
// Its compact saved selection supports proposal decoding; release authority
// comes from current eligible publications. Cardinality is bounded by decoder
// MaxPending, and each owned request has one heap item until final release.
type replayTemporalPending struct {
	id          uint64
	spot        *spot.Spot
	evidence    spot.ResolverEvidence
	hasEvidence bool
	maxAt       time.Time
	selection   correctionflow.ResolverPrimarySelection
}

// replayTemporalItem orders pending temporal observations by release time and
// sequence for deterministic replay artifacts.
type replayTemporalItem struct {
	id  uint64
	due time.Time
	seq uint64
}

type replayTemporalHeap []*replayTemporalItem

func (h replayTemporalHeap) Len() int { return len(h) }

func (h replayTemporalHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].seq < h[j].seq
	}
	return h[i].due.Before(h[j].due)
}

func (h replayTemporalHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *replayTemporalHeap) Push(x any) {
	item, ok := x.(*replayTemporalItem)
	if !ok {
		return
	}
	*h = append(*h, item)
}

func (h *replayTemporalHeap) Pop() any {
	old := *h
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// popReplayTemporalDue returns due items, or every owned heap item when force
// retires EOF holds. Force does not advance the supplied evidence clock. Pop
// clears retired pointers even when the bounded heap backing array is reused.
func popReplayTemporalDue(h *replayTemporalHeap, now time.Time, force bool) []*replayTemporalItem {
	if h == nil || h.Len() == 0 {
		return nil
	}
	out := make([]*replayTemporalItem, 0, 8)
	for h.Len() > 0 {
		head := (*h)[0]
		if head == nil || !force && head.due.After(now) {
			break
		}
		itemAny := heap.Pop(h)
		if item, ok := itemAny.(*replayTemporalItem); ok && item != nil {
			out = append(out, item)
		}
	}
	return out
}
