package stats

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Tracker tracks spot statistics by source
type Tracker struct {
	// counters live in sync.Map + atomic.Uint64 so per-spot increments don't fight over a mutex
	modeCounts   sync.Map // string -> *atomic.Uint64
	sourceCounts sync.Map // string -> *atomic.Uint64
	start        atomic.Int64
}

// NewTracker creates a new stats tracker
func NewTracker() *Tracker {
	t := &Tracker{}
	t.start.Store(time.Now().UnixNano())
	return t
}

// IncrementMode increases the count for a mode (FT8, FT4, MANUAL, etc.)
func (t *Tracker) IncrementMode(mode string) {
	incrementCounter(&t.modeCounts, mode)
}

// IncrementSource increases the count for a source node (RBN, RBN-DIGITAL, PSKREPORTER)
func (t *Tracker) IncrementSource(source string) {
	incrementCounter(&t.sourceCounts, source)
}

// GetCounts returns a copy of all counts
// GetModeCounts returns a copy of mode counts
func (t *Tracker) GetModeCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	t.modeCounts.Range(func(key, value any) bool {
		counts[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	return counts
}

// GetSourceCounts returns a copy of source node counts
func (t *Tracker) GetSourceCounts() map[string]uint64 {
	counts := make(map[string]uint64)
	t.sourceCounts.Range(func(key, value any) bool {
		counts[key.(string)] = value.(*atomic.Uint64).Load()
		return true
	})
	return counts
}

// GetTotal returns the total count across all sources
// GetTotal returns the total count across all sources (sum of sourceCounts)
func (t *Tracker) GetTotal() uint64 {
	var total uint64
	t.sourceCounts.Range(func(_, value any) bool {
		total += value.(*atomic.Uint64).Load()
		return true
	})
	return total
}

// GetUptime returns how long the tracker has been running
func (t *Tracker) GetUptime() time.Duration {
	start := t.start.Load()
	return time.Since(time.Unix(0, start))
}

// Reset resets all counters
func (t *Tracker) Reset() {
	t.modeCounts.Range(func(key, _ any) bool {
		t.modeCounts.Delete(key)
		return true
	})
	t.sourceCounts.Range(func(key, _ any) bool {
		t.sourceCounts.Delete(key)
		return true
	})
	t.start.Store(time.Now().UnixNano())
}

// Print displays the current statistics
func (t *Tracker) Print() {
	// Print source counts (higher-level sources)
	fmt.Printf("Spots by source: ")
	first := true
	t.sourceCounts.Range(func(key, value any) bool {
		source := key.(string)
		count := value.(*atomic.Uint64).Load()
		if !first {
			fmt.Printf(", ")
		}
		fmt.Printf("%s=%d", source, count)
		first = false
		return true
	})
	if first {
		fmt.Printf("(none)")
	}
	fmt.Println()

	// Print mode counts separately
	fmt.Printf("Spots by mode: ")
	first = true
	t.modeCounts.Range(func(key, value any) bool {
		mode := key.(string)
		count := value.(*atomic.Uint64).Load()
		if !first {
			fmt.Printf(", ")
		}
		fmt.Printf("%s=%d", mode, count)
		first = false
		return true
	})
	if first {
		fmt.Printf("(none)")
	}
	fmt.Println()
}

func incrementCounter(m *sync.Map, key string) {
	if strings.TrimSpace(key) == "" {
		return
	}
	if value, ok := m.Load(key); ok {
		value.(*atomic.Uint64).Add(1)
		return
	}
	counter := &atomic.Uint64{}
	actual, loaded := m.LoadOrStore(key, counter)
	if loaded {
		actual.(*atomic.Uint64).Add(1)
		return
	}
	counter.Add(1)
}
