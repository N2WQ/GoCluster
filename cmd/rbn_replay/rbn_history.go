// File role: Parses RBN history CSV rows for replay and correction evaluation.
// Crawler notes: Start here for CSV schema aliases, DX canonicalization,
// spot-class parsing, and historical row admission before replay construction.
// Related docs: README.md, docs/decisions/ADR-0115-dx-numeric-ssid-canonicalization.md.
package main

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	rbnfeed "dxcluster/rbn"
	"dxcluster/spot"
	"dxcluster/strutil"
)

type rbnHistoryRow struct {
	Time      time.Time
	FreqKHz   float64
	Band      string
	DXCall    string
	Spotter   string
	Mode      string
	SpotClass rbnfeed.SpotClass
	ReportDB  int
}

// rbnHistoryCSV streams one RBN history CSV member from a zip. It owns both the
// zip reader and the entry reader so replay can process large days without
// unpacking files or loading rows into memory.
type rbnHistoryCSV struct {
	zipReader *zip.ReadCloser
	entry     io.ReadCloser
	reader    *csv.Reader

	header []string
	col    struct {
		callsign int
		freq     int
		band     int
		dx       int
		db       int
		date     int
		spotMode int
		txMode   int
	}
}

// openRBNHistoryCSV prefers the expected day-named CSV but falls back to the
// first CSV member so older archive layouts remain replayable.
func openRBNHistoryCSV(zipPath string, preferredName string) (*rbnHistoryCSV, error) {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, fmt.Errorf("open zip: %w", err)
	}

	var file *zip.File
	for _, f := range r.File {
		if preferredName != "" && f.Name == preferredName {
			file = f
			break
		}
	}
	if file == nil {
		for _, f := range r.File {
			if strings.HasSuffix(strings.ToLower(f.Name), ".csv") {
				file = f
				break
			}
		}
	}
	if file == nil {
		r.Close()
		return nil, errors.New("zip contains no CSV entries")
	}

	rc, err := file.Open()
	if err != nil {
		r.Close()
		return nil, fmt.Errorf("open zip entry %s: %w", file.Name, err)
	}

	br := bufio.NewReaderSize(rc, 1<<20)
	reader := csv.NewReader(br)
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true

	header, err := reader.Read()
	if err != nil {
		rc.Close()
		r.Close()
		return nil, fmt.Errorf("read CSV header: %w", err)
	}
	trimmed := make([]string, 0, len(header))
	for _, h := range header {
		trimmed = append(trimmed, strings.TrimSpace(h))
	}

	parser := &rbnHistoryCSV{
		zipReader: r,
		entry:     rc,
		reader:    reader,
		header:    trimmed,
	}
	parser.initColumns()
	if err := parser.requiredColumnsPresent(); err != nil {
		parser.Close()
		return nil, err
	}
	return parser, nil
}

// initColumns records header positions once because row parsing is on the
// replay hot path.
func (c *rbnHistoryCSV) initColumns() {
	c.col.callsign = -1
	c.col.freq = -1
	c.col.band = -1
	c.col.dx = -1
	c.col.db = -1
	c.col.date = -1
	c.col.spotMode = -1
	c.col.txMode = -1

	for i, name := range c.header {
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "callsign":
			c.col.callsign = i
		case "freq":
			c.col.freq = i
		case "band":
			c.col.band = i
		case "dx":
			c.col.dx = i
		case "db":
			c.col.db = i
		case "date":
			c.col.date = i
		case "mode":
			c.col.spotMode = i
		case "tx_mode":
			c.col.txMode = i
		}
	}
}

func (c *rbnHistoryCSV) Close() error {
	if c == nil {
		return nil
	}
	if c.entry != nil {
		_ = c.entry.Close()
	}
	if c.zipReader != nil {
		return c.zipReader.Close()
	}
	return nil
}

func (c *rbnHistoryCSV) Header() []string {
	if c == nil {
		return nil
	}
	out := make([]string, len(c.header))
	copy(out, c.header)
	return out
}

// requiredColumnsPresent fails early when an input schema would make replay
// counters misleading.
func (c *rbnHistoryCSV) requiredColumnsPresent() error {
	if c == nil {
		return errors.New("nil CSV parser")
	}
	missing := make([]string, 0, 4)
	if c.col.callsign < 0 {
		missing = append(missing, "callsign")
	}
	if c.col.freq < 0 {
		missing = append(missing, "freq")
	}
	if c.col.band < 0 {
		missing = append(missing, "band")
	}
	if c.col.dx < 0 {
		missing = append(missing, "dx")
	}
	if c.col.db < 0 {
		missing = append(missing, "db")
	}
	if c.col.date < 0 {
		missing = append(missing, "date")
	}
	if c.col.spotMode < 0 {
		missing = append(missing, "mode")
	}
	if c.col.txMode < 0 {
		missing = append(missing, "tx_mode")
	}
	if len(missing) > 0 {
		return fmt.Errorf("CSV missing required columns: %s", strings.Join(missing, ", "))
	}
	return nil
}

// Read returns ok=false for malformed or non-candidate rows so replay can count
// bad input without aborting a full-day run.
func (c *rbnHistoryCSV) Read() (rbnHistoryRow, bool, error) {
	record, err := c.reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return rbnHistoryRow{}, false, io.EOF
		}
		return rbnHistoryRow{}, false, err
	}

	get := func(idx int) string {
		if idx < 0 || idx >= len(record) {
			return ""
		}
		return strings.TrimSpace(record[idx])
	}

	ts, ok := parseRBNHistoryTime(get(c.col.date))
	if !ok {
		return rbnHistoryRow{}, false, nil
	}

	freqKHz, ok := parseRBNHistoryFrequency(get(c.col.freq))
	if !ok {
		return rbnHistoryRow{}, false, nil
	}

	dxCall := spot.NormalizeSpotDXCallsign(get(c.col.dx))
	spotter := spot.NormalizeCallsign(get(c.col.callsign))
	mode := strutil.NormalizeUpper(get(c.col.txMode))
	spotClass, _ := rbnfeed.NormalizeSpotClass(get(c.col.spotMode))
	if dxCall == "" || spotter == "" || mode == "" {
		return rbnHistoryRow{}, false, nil
	}

	reportRaw := get(c.col.db)
	report, err := strconv.Atoi(reportRaw)
	if err != nil {
		if v, floatErr := strconv.ParseFloat(reportRaw, 64); floatErr == nil {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return rbnHistoryRow{}, false, nil
			}
			// Conversion truncates toward zero. Check that result against exact
			// signed-int bounds while preserving historical negative rounding.
			rounded := v + 0.5
			truncated := math.Trunc(rounded)
			limit := math.Ldexp(1, strconv.IntSize-1)
			if truncated < -limit || truncated >= limit {
				return rbnHistoryRow{}, false, nil
			}
			report = int(rounded)
		} else {
			return rbnHistoryRow{}, false, nil
		}
	}

	band := spot.NormalizeBand(get(c.col.band))
	if band == "" {
		band = spot.NormalizeBand(spot.FreqToBand(freqKHz))
	}

	return rbnHistoryRow{
		Time:      ts.UTC(),
		FreqKHz:   freqKHz,
		Band:      band,
		DXCall:    dxCall,
		Spotter:   spotter,
		Mode:      mode,
		SpotClass: spotClass,
		ReportDB:  report,
	}, true, nil
}

func parseRBNHistoryTime(raw string) (time.Time, bool) {
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", raw, time.UTC)
	return ts, err == nil
}

func parseRBNHistoryFrequency(raw string) (float64, bool) {
	freqKHz, err := strconv.ParseFloat(raw, 64)
	return freqKHz, err == nil && freqKHz > 0
}
