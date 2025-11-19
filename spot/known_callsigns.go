package spot

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// KnownCallsigns holds a set of normalized callsigns used for confidence boosts.
type KnownCallsigns struct {
	entries map[string]struct{}
}

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

// Contains reports whether the callsign appears in the known set.
func (k *KnownCallsigns) Contains(call string) bool {
	if k == nil {
		return false
	}
	call = strings.ToUpper(strings.TrimSpace(call))
	if call == "" {
		return false
	}
	_, ok := k.entries[call]
	return ok
}

// Count returns the number of callsigns in the set.
func (k *KnownCallsigns) Count() int {
	if k == nil {
		return 0
	}
	return len(k.entries)
}
