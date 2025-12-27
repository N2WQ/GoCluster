// Package skew fetches and applies per-skimmer frequency correction factors so
// incoming RBN/PSKReporter spots can be normalized before deduplication.
package skew

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Entry describes a single published skew correction entry.
type Entry struct {
	Callsign         string  `json:"callsign"`
	CorrectionFactor float64 `json:"correction_factor"`
	SkewHz           float64 `json:"-"` // only needed while parsing CSV
	Spots            int     `json:"-"` // used for filtering before JSON is written
}

// Table provides lookup access to skew entries keyed by raw skimmer ID (SSID preserved).
type Table struct {
	entries map[string]Entry
}

// Purpose: Construct a lookup table keyed by raw skimmer callsign.
// Key aspects: Normalizes callsigns to uppercase; rejects empty input.
// Upstream: LoadFile, FetchAndWrite pipelines.
// Downstream: Table.Lookup.
func NewTable(entries []Entry) (*Table, error) {
	table := &Table{entries: make(map[string]Entry, len(entries))}
	for _, entry := range entries {
		key := strings.ToUpper(strings.TrimSpace(entry.Callsign))
		if key == "" {
			continue
		}
		table.entries[key] = entry
	}
	if len(table.entries) == 0 {
		return nil, errors.New("skew: no usable entries")
	}
	return table, nil
}

// Purpose: Load a JSON skew table from disk into a lookup table.
// Key aspects: Parses JSON and filters unusable entries.
// Upstream: main.go startup or tooling.
// Downstream: FilterEntries, NewTable.
func LoadFile(path string) (*Table, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("skew: read %s: %w", path, err)
	}
	var entries []Entry
	if err := json.Unmarshal(payload, &entries); err != nil {
		return nil, fmt.Errorf("skew: parse %s: %w", path, err)
	}
	entries = FilterEntries(entries, 0)
	if len(entries) == 0 {
		return nil, fmt.Errorf("skew: %s contained no usable entries", path)
	}
	return NewTable(entries)
}

// Purpose: Return number of entries in the table.
// Key aspects: Safe on nil receiver.
// Upstream: Diagnostics/metrics.
// Downstream: None.
func (t *Table) Count() int {
	if t == nil {
		return 0
	}
	return len(t.entries)
}

// Purpose: Look up the correction factor for a raw skimmer callsign.
// Key aspects: Normalizes callsign; returns (0,false) when missing.
// Upstream: Store.Lookup and ApplyCorrection.
// Downstream: Table.entries map.
func (t *Table) Lookup(call string) (float64, bool) {
	if t == nil {
		return 0, false
	}
	key := strings.ToUpper(strings.TrimSpace(call))
	if key == "" {
		return 0, false
	}
	entry, ok := t.entries[key]
	if !ok {
		return 0, false
	}
	return entry.CorrectionFactor, true
}

// Store provides atomic access to the latest skew table.
type Store struct {
	ptr atomic.Pointer[Table]
}

// Purpose: Construct an empty atomic store for skew tables.
// Key aspects: Uses atomic.Pointer for lock-free reads.
// Upstream: main.go initialization.
// Downstream: Store.Set, Store.Lookup.
func NewStore() *Store {
	return &Store{}
}

// Purpose: Replace the stored skew table atomically.
// Key aspects: Safe for concurrent readers.
// Upstream: Skew refresh pipeline.
// Downstream: atomic pointer store.
func (s *Store) Set(table *Table) {
	if s == nil {
		return
	}
	s.ptr.Store(table)
}

// Purpose: Look up the correction factor via the current table.
// Key aspects: Handles nil store/table safely.
// Upstream: ApplyCorrection and ingest paths.
// Downstream: Table.Lookup.
func (s *Store) Lookup(call string) (float64, bool) {
	if s == nil {
		return 0, false
	}
	table := s.ptr.Load()
	if table == nil {
		return 0, false
	}
	return table.Lookup(call)
}

// Purpose: Apply per-skimmer correction to an incoming frequency (kHz).
// Key aspects: Multiplies by factor and rounds to 0.1 kHz; no-op if missing.
// Upstream: RBN/PSKReporter ingest pipeline.
// Downstream: Store.Lookup.
func ApplyCorrection(store *Store, rawCall string, freqKHz float64) float64 {
	if store == nil || freqKHz <= 0 {
		return freqKHz
	}
	factor, ok := store.Lookup(rawCall)
	if !ok || factor <= 0 {
		return freqKHz
	}
	corrected := freqKHz * factor
	// Half-up rounding to 0.1 kHz to avoid banker's rounding surprises.
	return math.Floor(corrected*10+0.5) / 10
}

// Purpose: Return number of entries in the current stored table.
// Key aspects: Safe on nil store/table.
// Upstream: Diagnostics/metrics.
// Downstream: Table.Count.
func (s *Store) Count() int {
	if s == nil {
		return 0
	}
	table := s.ptr.Load()
	if table == nil {
		return 0
	}
	return table.Count()
}

// Purpose: Filter skew entries by correction factor and minimum spots.
// Key aspects: Drops factor==1 and entries below minSpots; returns a new slice.
// Upstream: LoadFile, FetchAndWrite.
// Downstream: None.
func FilterEntries(entries []Entry, minSpots int) []Entry {
	if minSpots < 0 {
		minSpots = 0
	}
	filtered := make([]Entry, 0, len(entries))
	for _, entry := range entries {
		if entry.CorrectionFactor == 1 {
			continue
		}
		if minSpots > 0 && entry.Spots < minSpots {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

// Purpose: Download the skew CSV and parse entries.
// Key aspects: Validates URL and HTTP status; reads full body before parsing.
// Upstream: FetchAndWrite and tooling.
// Downstream: parseCSV, http.DefaultClient.Do.
func Fetch(ctx context.Context, rawURL string) ([]Entry, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return nil, errors.New("skew: url is required")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("skew: build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("skew: download csv: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("skew: unexpected status %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("skew: read response: %w", err)
	}

	return parseCSV(body)
}

// Purpose: Write skew entries to a JSON file.
// Key aspects: Ensures destination directory exists; writes indented JSON.
// Upstream: FetchAndWrite or tooling.
// Downstream: os.WriteFile, json.MarshalIndent.
func WriteJSON(entries []Entry, path string) error {
	if len(entries) == 0 {
		return errors.New("skew: no entries to write")
	}
	payload, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return fmt.Errorf("skew: marshal json: %w", err)
	}
	dir := filepath.Dir(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("skew: mkdir %s: %w", dir, err)
		}
	}
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		return fmt.Errorf("skew: write file: %w", err)
	}
	return nil
}

// Purpose: Parse the skew CSV into entries.
// Key aspects: Skips headers/comments; validates row length and drops factor==1.
// Upstream: Fetch.
// Downstream: csv.Reader, toEntry.
func parseCSV(raw []byte) ([]Entry, error) {
	reader := csv.NewReader(bytes.NewReader(raw))
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true

	var entries []Entry
	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("skew: parse csv: %w", err)
		}
		if len(record) == 0 {
			continue
		}
		first := strings.TrimSpace(record[0])
		if first == "" || strings.HasPrefix(first, "#") {
			continue
		}
		if strings.EqualFold(first, "callsign") {
			continue
		}
		if len(record) < 4 {
			return nil, fmt.Errorf("skew: invalid row %q", strings.Join(record, ","))
		}

		entry, err := toEntry(record)
		if err != nil {
			return nil, err
		}
		if entry.CorrectionFactor == 1 {
			continue
		}
		entries = append(entries, entry)
	}

	if len(entries) == 0 {
		return nil, errors.New("skew: no data rows found")
	}
	return entries, nil
}

// Purpose: Convert a CSV record into a skew Entry.
// Key aspects: Parses numeric fields and normalizes callsign.
// Upstream: parseCSV.
// Downstream: strconv parsing helpers.
func toEntry(record []string) (Entry, error) {
	normalize := func(idx int) string {
		if idx >= len(record) {
			return ""
		}
		return strings.TrimSpace(record[idx])
	}

	call := strings.ToUpper(normalize(0))
	if call == "" {
		return Entry{}, errors.New("skew: empty callsign field")
	}
	skewValue, err := strconv.ParseFloat(normalize(1), 64)
	if err != nil {
		return Entry{}, fmt.Errorf("skew: parse skew for %s: %w", call, err)
	}
	spots, err := strconv.Atoi(normalize(2))
	if err != nil {
		return Entry{}, fmt.Errorf("skew: parse spots for %s: %w", call, err)
	}
	factor, err := strconv.ParseFloat(normalize(3), 64)
	if err != nil {
		return Entry{}, fmt.Errorf("skew: parse factor for %s: %w", call, err)
	}

	return Entry{
		Callsign:         call,
		SkewHz:           skewValue,
		Spots:            spots,
		CorrectionFactor: factor,
	}, nil
}

// Purpose: Fetch, filter, and persist the skew table as JSON.
// Key aspects: Uses a bounded timeout; returns count of written entries.
// Upstream: Tools or scheduled refresh.
// Downstream: Fetch, FilterEntries, WriteJSON.
func FetchAndWrite(ctx context.Context, url string, minSpots int, path string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	entries, err := Fetch(ctx, url)
	if err != nil {
		return 0, err
	}
	filtered := FilterEntries(entries, minSpots)
	if len(filtered) == 0 {
		return 0, errors.New("skew: no entries after filtering")
	}
	if err := WriteJSON(filtered, path); err != nil {
		return 0, err
	}
	return len(filtered), nil
}
