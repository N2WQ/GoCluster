package main

import (
	"container/heap"
	"time"

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
	item, _ := x.(*runtimeTemporalItem)
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

func popRuntimeTemporalDue(h *runtimeTemporalHeap, now time.Time) []*runtimeTemporalItem {
	if h == nil || h.Len() == 0 {
		return nil
	}
	out := make([]*runtimeTemporalItem, 0, 8)
	for h.Len() > 0 {
		head := (*h)[0]
		if head == nil || head.due.After(now) {
			break
		}
		itemAny := heap.Pop(h)
		item, _ := itemAny.(*runtimeTemporalItem)
		if item == nil {
			continue
		}
		out = append(out, item)
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
