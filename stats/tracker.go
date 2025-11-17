package stats

import (
	"fmt"
	"sync"
	"time"
)

// Tracker tracks spot statistics by source
type Tracker struct {
	mu           sync.RWMutex
	modeCounts   map[string]uint64 // mode -> count (FT8, FT4, MANUAL, etc.)
	sourceCounts map[string]uint64 // source node -> count (RBN, RBN-DIGITAL, PSKREPORTER)
	start        time.Time
}

// NewTracker creates a new stats tracker
func NewTracker() *Tracker {
	return &Tracker{
		modeCounts:   make(map[string]uint64),
		sourceCounts: make(map[string]uint64),
		start:        time.Now(),
	}
}

// IncrementMode increases the count for a mode (FT8, FT4, MANUAL, etc.)
func (t *Tracker) IncrementMode(mode string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.modeCounts[mode]++
}

// IncrementSource increases the count for a source node (RBN, RBN-DIGITAL, PSKREPORTER)
func (t *Tracker) IncrementSource(source string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sourceCounts[source]++
}

// GetCounts returns a copy of all counts
// GetModeCounts returns a copy of mode counts
func (t *Tracker) GetModeCounts() map[string]uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	counts := make(map[string]uint64)
	for k, v := range t.modeCounts {
		counts[k] = v
	}
	return counts
}

// GetSourceCounts returns a copy of source node counts
func (t *Tracker) GetSourceCounts() map[string]uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	counts := make(map[string]uint64)
	for k, v := range t.sourceCounts {
		counts[k] = v
	}
	return counts
}

// GetTotal returns the total count across all sources
// GetTotal returns the total count across all sources (sum of sourceCounts)
func (t *Tracker) GetTotal() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var total uint64
	for _, v := range t.sourceCounts {
		total += v
	}
	return total
}

// GetUptime returns how long the tracker has been running
func (t *Tracker) GetUptime() time.Duration {
	return time.Since(t.start)
}

// Reset resets all counters
func (t *Tracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.modeCounts = make(map[string]uint64)
	t.sourceCounts = make(map[string]uint64)
	t.start = time.Now()
}

// Print displays the current statistics
func (t *Tracker) Print() {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Print source counts (higher-level sources)
	fmt.Printf("Spots by source: ")
	first := true
	for source, count := range t.sourceCounts {
		if !first {
			fmt.Printf(", ")
		}
		fmt.Printf("%s=%d", source, count)
		first = false
	}
	if first {
		fmt.Printf("(none)")
	}
	fmt.Println()

	// Print mode counts separately
	fmt.Printf("Spots by mode: ")
	first = true
	for mode, count := range t.modeCounts {
		if !first {
			fmt.Printf(", ")
		}
		fmt.Printf("%s=%d", mode, count)
		first = false
	}
	if first {
		fmt.Printf("(none)")
	}
	fmt.Println()
}
