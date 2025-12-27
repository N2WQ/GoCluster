// Package buffer provides a lock-free ring buffer used to fan recent spots to
// telnet clients without blocking the ingest pipeline. Each slot stores an
// atomic pointer so readers either see a complete spot or the previous one,
// never a partially written structure.
package buffer

import (
	"sync/atomic"
	"unsafe"

	"dxcluster/spot"
)

// RingBuffer is a thread-safe circular buffer for storing recent spots. Writers
// atomically publish completed *spot.Spot values, and readers walk backwards
// from the newest index to gather a snapshot for SHOW/DX requests.
type RingBuffer struct {
	// Each slot is an atomic pointer so writers can publish a fully built spot in one step.
	// Combined with the monotonic ID counter, this removes the need for a global mutex.
	slots    []atomic.Pointer[spot.Spot]
	capacity int
	total    atomic.Uint64 // Total spots added (may exceed capacity)
}

// Purpose: Construct a bounded ring buffer for recent spot snapshots.
// Key aspects: Capacity bounds retention; storage is independent of dedup/broadcast.
// Upstream: main.go initializes the shared spot cache.
// Downstream: RingBuffer.Add, RingBuffer.GetRecent, RingBuffer.GetPosition, RingBuffer.GetCount.
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		slots:    make([]atomic.Pointer[spot.Spot], capacity),
		capacity: capacity,
	}
}

// Purpose: Publish a new spot into the ring with a monotonic ID.
// Key aspects: Uses atomic counter and pointer store to avoid partial reads.
// Upstream: main.go spot ingestion/broadcast pipeline.
// Downstream: atomic counter and slot store; Spot.ID mutation.
func (rb *RingBuffer) Add(s *spot.Spot) {
	// Assign monotonic ID using atomic counter
	newID := rb.total.Add(1)
	s.ID = newID

	idx := (newID - 1) % uint64(rb.capacity)
	// Publishing via atomic.Store ensures readers either see the previous spot or this one, never partial state
	rb.slots[idx].Store(s)
}

// Purpose: Return up to N most recent spots in reverse chronological order.
// Key aspects: Walks ID-ordered ring backwards; skips overwritten slots.
// Upstream: Telnet SHOW/DX handlers and spot cache queries.
// Downstream: atomic loads from ring slots.
func (rb *RingBuffer) GetRecent(n int) []*spot.Spot {
	if n <= 0 {
		return []*spot.Spot{}
	}

	// Limit to available spots
	total := rb.total.Load()
	available := int(total)
	if available > rb.capacity {
		available = rb.capacity
	}

	if n > available {
		n = available
	}

	result := make([]*spot.Spot, 0, n)
	if total == 0 {
		return result
	}
	minIndex := total - uint64(available)
	for idx := total; idx > minIndex && len(result) < n; {
		idx--
		slot := idx % uint64(rb.capacity)
		// ID check skips over slots that have been overwritten after wraparound
		if sp := rb.slots[slot].Load(); sp != nil && sp.ID == idx+1 {
			result = append(result, sp)
		}
	}

	return result
}

// Purpose: Return the current write position modulo capacity.
// Key aspects: Derived from atomic total; no locking.
// Upstream: Diagnostics/metrics callers.
// Downstream: atomic total counter.
func (rb *RingBuffer) GetPosition() int {
	total := rb.total.Load()
	return int(total % uint64(rb.capacity))
}

// Purpose: Return total count of spots written to the ring.
// Key aspects: Reads atomic counter; may exceed capacity.
// Upstream: Metrics/diagnostics callers.
// Downstream: atomic total counter.
func (rb *RingBuffer) GetCount() int {
	// total is atomic; no need to lock
	return int(rb.total.Load())
}

// Purpose: Estimate ring buffer memory footprint in kilobytes.
// Key aspects: Accounts for pointer backing + conservative per-spot estimate.
// Upstream: Diagnostics/metrics callers.
// Downstream: unsafe.Sizeof for pointer size.
func (rb *RingBuffer) GetSizeKB() int {
	// pointer size on this architecture
	ptrSize := int(unsafe.Sizeof(uintptr(0)))

	// backing array size
	backingBytes := rb.capacity * ptrSize

	// estimate bytes used by stored Spot objects
	estimatePerSpot := 500 // bytes per spot (approx)
	totalAdded := int(rb.total.Load())
	stored := totalAdded
	if stored > rb.capacity {
		stored = rb.capacity
	}
	spotsBytes := stored * estimatePerSpot

	totalBytes := backingBytes + spotsBytes
	return totalBytes / 1024
}
