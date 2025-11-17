// Package buffer provides a thread-safe circular buffer (ring buffer) for storing recent spots.
//
// The ring buffer is a fixed-size FIFO (First In, First Out) data structure that
// automatically overwrites the oldest spots when full. This provides:
//   - Constant-time insertions O(1)
//   - Bounded memory usage (configurable capacity, typically 1000 spots)
//   - Historical queries (SHOW/DX command retrieves recent spots)
//   - Thread-safe concurrent access via read-write mutex
//
// The buffer assigns monotonically increasing IDs to each spot, enabling clients
// to track which spots they've already seen.
//
// Typical capacity: 1000 spots (configurable at creation time)
package buffer

import (
	"sync"

	"dxcluster/spot"
)

// RingBuffer is a thread-safe circular buffer that stores a fixed number of recent spots.
//
// Structure:
//   - Fixed-size array of spot pointers
//   - Head pointer tracks next write position
//   - Size tracks current number of valid spots (≤ capacity)
//   - Monotonic ID counter for unique spot identification
//   - RWMutex for thread-safe concurrent access
//
// Behavior:
//   - When full, new spots overwrite the oldest spots
//   - Spots are always inserted at the head position
//   - Head wraps around to 0 when it reaches capacity
//   - Retrieval returns spots in reverse chronological order (newest first)
//
// Thread Safety:
//   - Add() uses write lock (exclusive access)
//   - GetRecent/GetAll/Size() use read lock (concurrent reads allowed)
//   - Clear() uses write lock (exclusive access)
type RingBuffer struct {
	spots    []*spot.Spot // Fixed-size array of spot pointers
	capacity int          // Maximum number of spots (typically 1000)
	head     int          // Next write position (0 to capacity-1)
	size     int          // Current number of valid spots (0 to capacity)
	nextID   uint64       // Monotonically increasing spot ID counter
	mu       sync.RWMutex // Read-write mutex for thread safety
}

// NewRingBuffer creates a new ring buffer with the specified capacity.
//
// Parameters:
//   - capacity: Maximum number of spots to store (typically 1000)
//
// Returns:
//   - *RingBuffer: Initialized ring buffer ready for use
//
// The buffer is initialized with:
//   - Empty spot array of size 'capacity'
//   - Head at position 0
//   - Size at 0 (empty)
//   - Next ID at 1 (IDs start from 1, not 0)
//
// Typical usage:
//   spotBuffer := buffer.NewRingBuffer(1000)
func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		spots:    make([]*spot.Spot, capacity),
		capacity: capacity,
		head:     0,
		size:     0,
		nextID:   1, // Start IDs from 1 (0 can indicate unassigned)
	}
}

// Add inserts a new spot into the ring buffer.
//
// Parameters:
//   - s: Spot to add to the buffer
//
// Behavior:
//  1. Acquires write lock (blocks other Add/Clear operations, allows no reads)
//  2. Assigns a unique monotonically increasing ID to the spot
//  3. Inserts spot at current head position
//  4. Advances head pointer (wraps to 0 if at end of array)
//  5. Increments size (up to capacity)
//  6. Releases lock
//
// When the buffer is full:
//   - New spots overwrite the oldest spots
//   - Size remains at capacity
//   - Head continues to wrap around
//
// Thread safety: Uses write lock (rb.mu.Lock)
// Time complexity: O(1)
//
// The spot's ID field will be modified by this function.
func (rb *RingBuffer) Add(s *spot.Spot) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Assign unique monotonically increasing ID to this spot
	// IDs are globally unique and never reset
	s.ID = rb.nextID
	rb.nextID++

	// Insert spot at current head position
	rb.spots[rb.head] = s

	// Advance head pointer with wraparound
	// Example: if capacity is 1000, position 999 wraps to 0
	rb.head = (rb.head + 1) % rb.capacity

	// Increment size up to capacity
	// Once full, size stays at capacity (oldest spots are overwritten)
	if rb.size < rb.capacity {
		rb.size++
	}
}

// GetRecent returns the N most recent spots in reverse chronological order.
//
// Parameters:
//   - n: Number of spots to retrieve
//
// Returns:
//   - []*spot.Spot: Slice of spots ordered from newest to oldest
//
// Behavior:
//   - If n > current size, returns all available spots
//   - If n == 0, returns empty slice
//   - Always returns spots in order: newest first, oldest last
//
// Thread safety: Uses read lock (allows concurrent reads)
// Time complexity: O(n)
//
// This is used by the SHOW/DX command to display recent spot activity.
//
// Example:
//   recent := buffer.GetRecent(10) // Get last 10 spots
func (rb *RingBuffer) GetRecent(n int) []*spot.Spot {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	// Clamp n to available size
	if n > rb.size {
		n = rb.size
	}

	// Pre-allocate result slice with exact capacity
	result := make([]*spot.Spot, 0, n)

	// Walk backwards from head (most recent) for n spots
	// Index calculation handles wraparound correctly
	for i := 0; i < n; i++ {
		// Calculate index: (head - 1 - i) with wraparound handling
		// Example: head=5, i=0 → idx=4 (most recent)
		//          head=0, i=0 → idx=capacity-1 (wraparound)
		idx := (rb.head - 1 - i + rb.capacity) % rb.capacity

		// Nil check for safety (should always be non-nil when i < size)
		if rb.spots[idx] != nil {
			result = append(result, rb.spots[idx])
		}
	}

	return result
}

// GetAll returns all spots currently in the buffer in reverse chronological order.
//
// Returns:
//   - []*spot.Spot: All spots ordered from newest to oldest
//
// This is equivalent to GetRecent(buffer.Size()).
//
// Thread safety: Uses read lock via GetRecent()
// Time complexity: O(size)
//
// Example:
//   allSpots := buffer.GetAll()
func (rb *RingBuffer) GetAll() []*spot.Spot {
	return rb.GetRecent(rb.size)
}

// Size returns the current number of spots in the buffer.
//
// Returns:
//   - int: Number of valid spots (0 to capacity)
//
// The size grows from 0 to capacity as spots are added.
// Once full, size remains at capacity even as new spots overwrite old ones.
//
// Thread safety: Uses read lock (allows concurrent reads)
// Time complexity: O(1)
//
// Example:
//   count := buffer.Size() // e.g., 437 spots currently stored
func (rb *RingBuffer) Size() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size
}

// Clear removes all spots from the buffer and resets it to empty state.
//
// Behavior:
//   - Allocates new empty spot array
//   - Resets head to 0
//   - Resets size to 0
//   - nextID counter is NOT reset (IDs remain globally unique)
//
// Thread safety: Uses write lock (exclusive access)
// Time complexity: O(1) plus garbage collection of old array
//
// Note: This operation is rarely used in production. It's primarily for
// testing or administrative reset scenarios.
//
// Example:
//   buffer.Clear() // Remove all spots
func (rb *RingBuffer) Clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Allocate new empty array (old array becomes garbage collected)
	rb.spots = make([]*spot.Spot, rb.capacity)

	// Reset head and size to initial state
	rb.head = 0
	rb.size = 0

	// Note: nextID is NOT reset - IDs remain globally unique across clears
}
