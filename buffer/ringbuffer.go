package buffer

import (
	"sync/atomic"
	"unsafe"

	"dxcluster/spot"
)

// RingBuffer is a thread-safe circular buffer for storing recent spots
type RingBuffer struct {
	// Each slot is an atomic pointer so writers can publish a fully built spot in one step.
	// Combined with the monotonic ID counter, this removes the need for a global mutex.
	slots    []atomic.Pointer[spot.Spot]
	capacity int
	total    atomic.Uint64 // Total spots added (may exceed capacity)
}

// NewRingBuffer creates a new ring buffer with the specified capacity
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		slots:    make([]atomic.Pointer[spot.Spot], capacity),
		capacity: capacity,
	}
}

// Add adds a spot to the buffer
func (rb *RingBuffer) Add(s *spot.Spot) {
	// Assign monotonic ID using atomic counter
	newID := rb.total.Add(1)
	s.ID = newID

	idx := (newID - 1) % uint64(rb.capacity)
	// Publishing via atomic.Store ensures readers either see the previous spot or this one, never partial state
	rb.slots[idx].Store(s)
}

// GetRecent returns the N most recent spots
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

// GetPosition returns the current write position in the ring buffer
func (rb *RingBuffer) GetPosition() int {
	total := rb.total.Load()
	return int(total % uint64(rb.capacity))
}

// GetCount returns the total number of spots added (may be > capacity)
func (rb *RingBuffer) GetCount() int {
	// total is atomic; no need to lock
	return int(rb.total.Load())
}

// GetSizeKB returns an approximate size of the ring buffer in kilobytes.
// The estimate includes the backing slice of pointers and an approximation
// of the memory retained by the Spot objects. The per-spot estimate is
// intentionally conservative (default ~500 bytes) because string data and
// other heap allocations vary widely.
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
