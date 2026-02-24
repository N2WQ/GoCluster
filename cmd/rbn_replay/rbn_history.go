package main

import (
	"archive/zip"
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"dxcluster/spot"
	"dxcluster/strutil"
)

type rbnHistoryRow struct {
	Time     time.Time
	FreqKHz  float64
	Band     string
	DXCall   string
	Spotter  string
	Mode     string
	ReportDB int
}

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
		txMode   int
	}
}

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

func (c *rbnHistoryCSV) initColumns() {
	c.col.callsign = -1
	c.col.freq = -1
	c.col.band = -1
	c.col.dx = -1
	c.col.db = -1
	c.col.date = -1
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
	if c.col.txMode < 0 {
		missing = append(missing, "tx_mode")
	}
	if len(missing) > 0 {
		return fmt.Errorf("CSV missing required columns: %s", strings.Join(missing, ", "))
	}
	return nil
}

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

	tsRaw := get(c.col.date)
	ts, err := time.ParseInLocation("2006-01-02 15:04:05", tsRaw, time.UTC)
	if err != nil {
		return rbnHistoryRow{}, false, nil
	}

	freqRaw := get(c.col.freq)
	freqKHz, err := strconv.ParseFloat(freqRaw, 64)
	if err != nil || freqKHz <= 0 {
		return rbnHistoryRow{}, false, nil
	}

	dxCall := spot.NormalizeCallsign(get(c.col.dx))
	spotter := spot.NormalizeCallsign(get(c.col.callsign))
	mode := strutil.NormalizeUpper(get(c.col.txMode))
	if dxCall == "" || spotter == "" || mode == "" {
		return rbnHistoryRow{}, false, nil
	}

	reportRaw := get(c.col.db)
	report, err := strconv.Atoi(reportRaw)
	if err != nil {
		if v, floatErr := strconv.ParseFloat(reportRaw, 64); floatErr == nil {
			report = int(v + 0.5)
		} else {
			return rbnHistoryRow{}, false, nil
		}
	}

	band := spot.NormalizeBand(get(c.col.band))
	if band == "" {
		band = spot.NormalizeBand(spot.FreqToBand(freqKHz))
	}

	return rbnHistoryRow{
		Time:     ts.UTC(),
		FreqKHz:  freqKHz,
		Band:     band,
		DXCall:   dxCall,
		Spotter:  spotter,
		Mode:     mode,
		ReportDB: report,
	}, true, nil
}
