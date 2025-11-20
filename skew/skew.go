package skew

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Entry describes the skew characteristics published for a single skimmer.
type Entry struct {
	Callsign         string  `json:"callsign"`
	SkewHz           float64 `json:"skew_hz"`
	Spots            int     `json:"spots"`
	CorrectionFactor float64 `json:"correction_factor"`
}

// Table provides lookup access to skew entries keyed by raw skimmer ID (SSID preserved).
type Table struct {
	entries map[string]Entry
}

// NewTable constructs a lookup table from the provided entries.
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

// LoadFile reads the JSON file produced by FetchAndWrite and constructs a lookup table.
func LoadFile(path string) (*Table, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("skew: read %s: %w", path, err)
	}
	var entries []Entry
	if err := json.Unmarshal(payload, &entries); err != nil {
		return nil, fmt.Errorf("skew: parse %s: %w", path, err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("skew: %s contained no entries", path)
	}
	return NewTable(entries)
}

// Count returns the number of skimmers with published corrections.
func (t *Table) Count() int {
	if t == nil {
		return 0
	}
	return len(t.entries)
}

// Lookup returns the multiplicative correction factor for the provided raw DE call.
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

// NewStore constructs an empty store.
func NewStore() *Store {
	return &Store{}
}

// Set replaces the currently stored table.
func (s *Store) Set(table *Table) {
	if s == nil {
		return
	}
	s.ptr.Store(table)
}

// Lookup retrieves the correction factor for the raw skimmer callsign.
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

// Count returns the number of entries currently cached.
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

// Fetch downloads the CSV table and returns parsed skew entries.
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

// WriteJSON marshals the entries to JSON and writes them to the provided path.
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
		entries = append(entries, entry)
	}

	if len(entries) == 0 {
		return nil, errors.New("skew: no data rows found")
	}
	return entries, nil
}

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

// FetchAndWrite is a helper that downloads the CSV and writes it to JSON at the given path.
func FetchAndWrite(ctx context.Context, url, path string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	entries, err := Fetch(ctx, url)
	if err != nil {
		return 0, err
	}
	if err := WriteJSON(entries, path); err != nil {
		return 0, err
	}
	return len(entries), nil
}
