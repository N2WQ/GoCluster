package buffer

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"dxcluster/spot"
)

// RingBuffer is a thread-safe circular buffer for storing recent spots
type RingBuffer struct {
	mu       sync.RWMutex
	spots    []*spot.Spot
	capacity int
	writePos int
	total    atomic.Uint64 // Total spots added (may exceed capacity)
}

// NewRingBuffer creates a new ring buffer with the specified capacity
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		spots:    make([]*spot.Spot, capacity),
		capacity: capacity,
		writePos: 0,
	}
}

// Add adds a spot to the buffer
func (rb *RingBuffer) Add(s *spot.Spot) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Assign monotonic ID using atomic counter
	newID := rb.total.Add(1)
	s.ID = newID

	// Write to current position
	rb.spots[rb.writePos] = s

	// Advance write position (wrap around)
	rb.writePos = (rb.writePos + 1) % rb.capacity

	// total already incremented via atomic.Add
}

// GetRecent returns the N most recent spots
func (rb *RingBuffer) GetRecent(n int) []*spot.Spot {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if n <= 0 {
		return []*spot.Spot{}
	}

	// Limit to available spots
	available := int(rb.total.Load())
	if available > rb.capacity {
		available = rb.capacity
	}

	if n > available {
		n = available
	}

	result := make([]*spot.Spot, 0, n)

	// Read backwards from write position
	pos := rb.writePos - 1
	if pos < 0 {
		pos = rb.capacity - 1
	}

	for i := 0; i < n; i++ {
		if rb.spots[pos] != nil {
			result = append(result, rb.spots[pos])
		}

		pos--
		if pos < 0 {
			pos = rb.capacity - 1
		}
	}

	return result
}

// GetPosition returns the current write position in the ring buffer
func (rb *RingBuffer) GetPosition() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.writePos
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
