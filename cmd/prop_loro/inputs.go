// File role: Streaming readers for Slice 1 capture artifacts (hourly UTC
// spots-YYYY-MM-DD-HH.csv.gz and daily spaceweather-YYYY-MM-DD.jsonl) with
// strict causal space-weather lookup and bounded hourly chronological sorting.
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"dxcluster/internal/propprior"
)

type capturedSpot struct {
	observedUnix  int64
	arrivedUnix   int64
	band          string
	mode          string
	source        string
	rawSNRDB      int
	ft8DB         float64
	deGrid        string
	dxGrid        string
	deCall        string
	deHash        uint64
	isBeacon      bool
	deGridDerived bool
	dxGridDerived bool
}

type swSnapshot struct {
	TSUnix      int64                          `json:"ts_unix"`
	TSUTC       string                         `json:"ts_utc"`
	Reason      string                         `json:"reason"`
	Gen         uint64                         `json:"gen"`
	SFINow      float64                        `json:"sfi_now"`
	SFI72hMean  float64                        `json:"sfi_72h_mean"`
	Hp30Lag1    float64                        `json:"hp30_lag1"`
	Hp3012hMax  float64                        `json:"hp30_12h_max"`
	Hp3024hMean float64                        `json:"hp30_24h_mean"`
	BzSouth3h   float64                        `json:"bz_south_3h"`
	Vsw3h       float64                        `json:"vsw_3h"`
	DstNow      float64                        `json:"dst_now"`
	Dst24hMin   float64                        `json:"dst_24h_min"`
	VOACAPSSN   struct {
		Available bool   `json:"available"`
		SSN       int    `json:"ssn"`
		Source    string `json:"source"`
	} `json:"voacap_ssn"`
}

func (s swSnapshot) features() propprior.SpaceWeatherFeatures {
	return propprior.SpaceWeatherFeatures{
		SFINow:      s.SFINow,
		SFI72hMean:  s.SFI72hMean,
		Hp30Lag1:    s.Hp30Lag1,
		Hp3012hMax:  s.Hp3012hMax,
		Hp3024hMean: s.Hp3024hMean,
		BzSouth3h:   s.BzSouth3h,
		Vsw3h:       s.Vsw3h,
		DstNow:      s.DstNow,
		Dst24hMin:   s.Dst24hMin,
	}
}

// swTimeline provides strict causal (zero-lookahead) space-weather lookup via
// binary search over snapshots sorted ascending by TSUnix.
type swTimeline struct {
	snaps []swSnapshot
}

func loadSpaceWeatherTimeline(dir string) (*swTimeline, []string, error) {
	pattern := filepath.Join(dir, "spaceweather-*.jsonl")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, nil, fmt.Errorf("glob spaceweather files: %w", err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, nil, fmt.Errorf("no spaceweather-*.jsonl files found in %q", dir)
	}

	var snaps []swSnapshot
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return nil, nil, fmt.Errorf("open %s: %w", path, err)
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		lineNo := 0
		for sc.Scan() {
			lineNo++
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var rec swSnapshot
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				_ = f.Close()
				return nil, nil, fmt.Errorf("parse %s:%d: %w", path, lineNo, err)
			}
			snaps = append(snaps, rec)
		}
		scanErr := sc.Err()
		_ = f.Close()
		if scanErr != nil {
			return nil, nil, fmt.Errorf("scan %s: %w", path, scanErr)
		}
	}
	sort.SliceStable(snaps, func(i, j int) bool {
		if snaps[i].TSUnix != snaps[j].TSUnix {
			return snaps[i].TSUnix < snaps[j].TSUnix
		}
		return snaps[i].Gen < snaps[j].Gen
	})
	return &swTimeline{snaps: snaps}, files, nil
}

// lookupCausal returns the latest snapshot with TSUnix <= tsUnix (strictly causal).
// If tsUnix precedes every snapshot, returns (zero, false).
func (t *swTimeline) lookupCausal(tsUnix int64) (swSnapshot, bool) {
	if t == nil || len(t.snaps) == 0 {
		return swSnapshot{}, false
	}
	// First index with TSUnix > tsUnix.
	idx := sort.Search(len(t.snaps), func(i int) bool {
		return t.snaps[i].TSUnix > tsUnix
	})
	if idx == 0 {
		return swSnapshot{}, false
	}
	return t.snaps[idx-1], true
}

func discoverSpotFiles(dir string) ([]string, error) {
	pattern := filepath.Join(dir, "spots-*.csv.gz")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob spot files: %w", err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("no spots-*.csv.gz files found in %q", dir)
	}
	return files, nil
}

type spotReadStats struct {
	FilesRead int
	RowsRead  int64
	RowsBad   int64
}

// streamSpotFiles reads hourly spots-*.csv.gz files in chronological file order,
// sorts each file's records deterministically by observed_unix (preserving a
// bounded trailing overlap across hour boundaries for late-arriving spots), and
// invokes emit for every record within [startUnix, endUnix] (0 = unbounded).
func streamSpotFiles(files []string, startUnix, endUnix int64, emit func(capturedSpot) error) (spotReadStats, error) {
	var stats spotReadStats
	const overlapSec int64 = 120
	var carry []capturedSpot

	for _, path := range files {
		hourRecs, bad, err := readSpotGzipFile(path, startUnix, endUnix)
		if err != nil {
			return stats, err
		}
		stats.FilesRead++
		stats.RowsRead += int64(len(hourRecs))
		stats.RowsBad += bad

		if len(carry) > 0 {
			hourRecs = append(carry, hourRecs...)
			carry = nil
		}
		sortCapturedSpots(hourRecs)
		if len(hourRecs) == 0 {
			continue
		}

		// Hold back the final overlapSec window to merge cleanly with the next hour file.
		cutoff := hourRecs[len(hourRecs)-1].observedUnix - overlapSec
		splitIdx := len(hourRecs)
		for splitIdx > 0 && hourRecs[splitIdx-1].observedUnix > cutoff {
			splitIdx--
		}
		for i := 0; i < splitIdx; i++ {
			if err := emit(hourRecs[i]); err != nil {
				return stats, err
			}
		}
		if splitIdx < len(hourRecs) {
			carry = append(carry[:0], hourRecs[splitIdx:]...)
		}
	}

	sortCapturedSpots(carry)
	for i := range carry {
		if err := emit(carry[i]); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func readSpotGzipFile(path string, startUnix, endUnix int64) ([]capturedSpot, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		// An empty 0-byte file just created by a live writer has no gzip header yet.
		if errors.Is(err, io.EOF) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("gzip reader %s: %w", path, err)
	}
	defer gz.Close()

	cr := csv.NewReader(bufio.NewReaderSize(gz, 64*1024))
	cr.FieldsPerRecord = -1
	cr.ReuseRecord = true

	var out []capturedSpot
	var bad int64
	for {
		row, err := cr.Read()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// ErrUnexpectedEOF can occur only on the actively open current-hour file
			// between 10s sync flushes; completed rows prior to the tail remain valid.
			break
		}
		if err != nil {
			bad++
			continue
		}
		if len(row) == 0 || row[0] == "observed_unix" {
			continue
		}
		rec, ok := parseCapturedSpotRow(row)
		if !ok {
			bad++
			continue
		}
		if startUnix > 0 && rec.observedUnix < startUnix {
			continue
		}
		if endUnix > 0 && rec.observedUnix > endUnix {
			continue
		}
		out = append(out, rec)
	}
	return out, bad, nil
}

func parseCapturedSpotRow(row []string) (capturedSpot, bool) {
	if len(row) < 14 {
		return capturedSpot{}, false
	}
	obsUnix, err := strconv.ParseInt(row[0], 10, 64)
	if err != nil || obsUnix <= 0 {
		return capturedSpot{}, false
	}
	arrUnix, err := strconv.ParseInt(row[1], 10, 64)
	if err != nil || arrUnix <= 0 {
		arrUnix = obsUnix
	}
	rawSNR, err := strconv.Atoi(row[5])
	if err != nil {
		return capturedSpot{}, false
	}
	ft8DB, err := strconv.ParseFloat(row[6], 64)
	if err != nil {
		return capturedSpot{}, false
	}
	deHash, err := strconv.ParseUint(row[10], 10, 64)
	if err != nil {
		return capturedSpot{}, false
	}
	return capturedSpot{
		observedUnix:  obsUnix,
		arrivedUnix:   arrUnix,
		band:          strings.ToLower(strings.TrimSpace(row[2])),
		mode:          strings.ToUpper(strings.TrimSpace(row[3])),
		source:        strings.TrimSpace(row[4]),
		rawSNRDB:      rawSNR,
		ft8DB:         ft8DB,
		deGrid:        strings.ToUpper(strings.TrimSpace(row[7])),
		dxGrid:        strings.ToUpper(strings.TrimSpace(row[8])),
		deCall:        strings.ToUpper(strings.TrimSpace(row[9])),
		deHash:        deHash,
		isBeacon:      row[11] == "1",
		deGridDerived: row[12] == "1",
		dxGridDerived: row[13] == "1",
	}, true
}

func sortCapturedSpots(spots []capturedSpot) {
	sort.Slice(spots, func(i, j int) bool {
		a, b := &spots[i], &spots[j]
		if a.observedUnix != b.observedUnix {
			return a.observedUnix < b.observedUnix
		}
		if a.arrivedUnix != b.arrivedUnix {
			return a.arrivedUnix < b.arrivedUnix
		}
		if a.deHash != b.deHash {
			return a.deHash < b.deHash
		}
		if a.band != b.band {
			return a.band < b.band
		}
		if a.dxGrid != b.dxGrid {
			return a.dxGrid < b.dxGrid
		}
		if a.mode != b.mode {
			return a.mode < b.mode
		}
		return a.rawSNRDB < b.rawSNRDB
	})
}
