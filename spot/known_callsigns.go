package spot

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
)

// KnownCallsigns holds a set of normalized callsigns used for confidence boosts.
// Counters track total lookups and DX-only lookups (the latter is what the
// pipeline uses to compute the status hit rate).
type KnownCallsigns struct {
	entries   map[string]struct{}
	lookups   atomic.Uint64
	hits      atomic.Uint64
	dxLookups atomic.Uint64
	dxHits    atomic.Uint64
}

// Purpose: Load a newline-delimited list of known callsigns.
// Key aspects: Skips blanks/comments and normalizes to uppercase.
// Upstream: main startup and refresh tasks.
// Downstream: bufio.Scanner and strings.TrimSpace.
// LoadKnownCallsigns loads a newline-delimited file of callsigns.
func LoadKnownCallsigns(path string) (*KnownCallsigns, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open known callsigns file: %w", err)
	}
	defer file.Close()

	entries := make(map[string]struct{})
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		call := strings.ToUpper(strings.TrimSpace(scanner.Text()))
		if call == "" || strings.HasPrefix(call, "#") {
			continue
		}
		entries[call] = struct{}{}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read known callsigns file: %w", err)
	}

	return &KnownCallsigns{entries: entries}, nil
}

// Purpose: Check membership in the known callsigns set.
// Key aspects: Normalizes input and updates lookup/hit counters.
// Upstream: call correction confidence logic.
// Downstream: atomic counters and map lookup.
// Contains reports whether the callsign appears in the known set.
func (k *KnownCallsigns) Contains(call string) bool {
	if k == nil {
		return false
	}
	call = strings.ToUpper(strings.TrimSpace(call))
	if call == "" {
		return false
	}
	k.lookups.Add(1)
	k.dxLookups.Add(1)
	_, ok := k.entries[call]
	if ok {
		k.hits.Add(1)
		k.dxHits.Add(1)
	}
	return ok
}

// Purpose: Return the number of known callsigns.
// Key aspects: Nil-safe; returns map size.
// Upstream: stats output and logs.
// Downstream: len on map.
// Count returns the number of callsigns in the set.
func (k *KnownCallsigns) Count() int {
	if k == nil {
		return 0
	}
	return len(k.entries)
}

// Purpose: Return lookup and hit counters for all calls.
// Key aspects: Uses atomic counters.
// Upstream: stats output.
// Downstream: atomic loads.
// Stats returns lookup and hit counters for the known calls set.
func (k *KnownCallsigns) Stats() (lookups uint64, hits uint64) {
	if k == nil {
		return 0, 0
	}
	return k.lookups.Load(), k.hits.Load()
}

// Purpose: Return lookup and hit counters for DX calls.
// Key aspects: Uses atomic counters.
// Upstream: stats output.
// Downstream: atomic loads.
// StatsDX returns lookup and hit counters specifically for DX calls.
func (k *KnownCallsigns) StatsDX() (lookups uint64, hits uint64) {
	if k == nil {
		return 0, 0
	}
	return k.dxLookups.Load(), k.dxHits.Load()
}

// Purpose: Return a snapshot slice of all known callsigns.
// Key aspects: Allocates a slice sized to the map.
// Upstream: grid store seeding and exports.
// Downstream: map iteration.
// List returns a snapshot of all known callsigns.
func (k *KnownCallsigns) List() []string {
	if k == nil {
		return nil
	}
	calls := make([]string, 0, len(k.entries))
	for call := range k.entries {
		calls = append(calls, call)
	}
	return calls
}
