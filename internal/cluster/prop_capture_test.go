package cluster

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/internal/propprior"
	"dxcluster/internal/voacap"
	"dxcluster/spot"
)

type stubPropCaptureSSN struct {
	ssn      int
	ok       bool
	smoothed float64
	obsUTC   time.Time
}

func (s stubPropCaptureSSN) CurrentSSN(time.Time) (int, bool) {
	return s.ssn, s.ok
}

func (s stubPropCaptureSSN) Snapshot() voacap.SunspotMonitorSnapshot {
	return voacap.SunspotMonitorSnapshot{
		SmoothedCountIndex: s.smoothed,
		LastObservedAtUTC:  s.obsUTC,
	}
}

func TestPropCaptureDisabledByDefault(t *testing.T) {
	t.Setenv(envPropCaptureDir, "")
	rt := &clusterRuntime{}
	rt.initializePropCapture()
	if rt.propCapture != nil {
		rt.propCapture.Stop()
		t.Fatal("expected nil propCapture when DXC_PROP_CAPTURE_DIR is unset")
	}
}

func TestPropCaptureInvalidDirDegradesGracefully(t *testing.T) {
	blockingFile := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blockingFile, []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocking file: %v", err)
	}
	t.Setenv(envPropCaptureDir, blockingFile)
	rt := &clusterRuntime{}
	rt.initializePropCapture()
	if rt.propCapture != nil {
		rt.propCapture.Stop()
		t.Fatal("expected nil propCapture when DXC_PROP_CAPTURE_DIR is not a directory")
	}
}

func TestPropCaptureSpotAndSpaceWeatherRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ssnPath := filepath.Join(t.TempDir(), "ssn_state.json")
	ssnJSON := `{
  "version": 3,
  "last_observed_at_utc": "2026-11-26T16:35:00Z",
  "smoothed_count_index": 35.65,
  "input_initialized": true,
  "input": { "ssn": 36 }
}`
	if err := os.WriteFile(ssnPath, []byte(ssnJSON), 0o644); err != nil {
		t.Fatalf("write ssn_state.json: %v", err)
	}

	weather := propprior.NewSpaceWeatherStore()
	baseNow := time.Date(2026, 11, 28, 0, 15, 0, 0, time.UTC)
	weather.RecordSFI(time.Date(2026, 11, 27, 0, 0, 0, 0, time.UTC), 142.5, "noaa_f107")
	weather.RecordHp30(time.Date(2026, 11, 27, 23, 30, 0, 0, time.UTC), 2.33, "gfz_hp30")
	weather.RecordBzGSM(time.Date(2026, 11, 27, 23, 0, 0, 0, time.UTC), -3.5, "noaa_rtsw_mag")
	weather.RecordVsw(time.Date(2026, 11, 27, 23, 0, 0, 0, time.UTC), 460.0, "noaa_rtsw_wind")
	weather.RecordDst(time.Date(2026, 11, 27, 23, 0, 0, 0, time.UTC), -18.0, "noaa_kyoto_dst")

	var nowUnix atomic.Int64
	nowUnix.Store(baseNow.Unix())

	w, err := newPropCaptureWriter(propCaptureConfig{
		Dir:             dir,
		QueueCap:        64,
		FlushInterval:   20 * time.Millisecond,
		SWCheckInterval: 20 * time.Millisecond,
		SWHeartbeat:     time.Hour,
		Weather:         weather,
		SSNStatePath:    ssnPath,
		Logger:          log.New(io.Discard, "", 0),
		NowFn:           func() time.Time { return time.Unix(nowUnix.Load(), 0).UTC() },
	})
	if err != nil {
		t.Fatalf("newPropCaptureWriter: %v", err)
	}
	w.Start()

	s1 := &spot.Spot{
		DECallNorm: "K1TTT",
		ModeNorm:   "CW",
		SourceType: spot.SourceRBN,
		Report:     18,
		IsBeacon:   false,
		DEMetadata: spot.CallMetadata{Grid: "FN32", GridDerived: false},
		DXMetadata: spot.CallMetadata{Grid: "PM95", GridDerived: true},
	}
	s2 := &spot.Spot{
		DECallNorm: "JA1,ABC",
		ModeNorm:   "WSPR",
		SourceType: spot.SourcePSKReporter,
		Report:     -21,
		IsBeacon:   true,
		DEMetadata: spot.CallMetadata{Grid: "PM95", GridDerived: true},
		DXMetadata: spot.CallMetadata{Grid: "FN32", GridDerived: false},
	}
	h1 := uint64(0x123456789abcdef0)
	h2 := uint64(42)
	obs1 := time.Date(2026, 11, 28, 0, 15, 10, 0, time.UTC)
	obs2 := time.Date(2026, 11, 28, 1, 5, 20, 0, time.UTC)

	w.EnqueueSpot(s1, obs1, obs1.Add(time.Second), "20m", "CW", 11.0, h1)
	w.EnqueueSpot(s2, obs2, obs2.Add(2*time.Second), "20m", "WSPR", -21.0, h2)

	// Wait briefly for startup snapshot, then bump space-weather generation.
	deadline := time.Now().Add(2 * time.Second)
	for w.WrittenCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	nowUnix.Store(obs2.Unix())
	weather.RecordHp30(time.Date(2026, 11, 28, 0, 30, 0, 0, time.UTC), 3.67, "gfz_hp30")
	time.Sleep(60 * time.Millisecond)

	// Idempotent Stop.
	w.Stop()
	w.Stop()

	if got := w.WrittenCount(); got != 2 {
		t.Fatalf("WrittenCount = %d, want 2", got)
	}
	if got := w.DroppedCount(); got != 0 {
		t.Fatalf("DroppedCount = %d, want 0", got)
	}

	// Verify hourly rotation produced two valid gzipped CSV files.
	rows00 := readGzipCSVRows(t, filepath.Join(dir, "spots-2026-11-28-00.csv.gz"))
	if len(rows00) != 2 {
		t.Fatalf("hour 00 rows = %d, want 2 (header + 1 row)", len(rows00))
	}
	if strings.Join(rows00[0], ",")+"\n" != propCaptureCSVHeader {
		t.Fatalf("unexpected header: %q", strings.Join(rows00[0], ","))
	}
	if rows00[1][2] != "20m" || rows00[1][3] != "CW" || rows00[1][4] != "RBN" ||
		rows00[1][5] != "18" || rows00[1][6] != "11.00" ||
		rows00[1][7] != "FN32" || rows00[1][8] != "PM95" ||
		rows00[1][9] != "K1TTT" || rows00[1][10] != strconv.FormatUint(h1, 10) ||
		rows00[1][11] != "0" || rows00[1][12] != "0" || rows00[1][13] != "1" {
		t.Fatalf("unexpected hour 00 row: %#v", rows00[1])
	}

	rows01 := readGzipCSVRows(t, filepath.Join(dir, "spots-2026-11-28-01.csv.gz"))
	if len(rows01) != 2 {
		t.Fatalf("hour 01 rows = %d, want 2 (header + 1 row)", len(rows01))
	}
	// Comma inside DECall must be stripped so CSV columns stay exact.
	if rows01[1][3] != "WSPR" || rows01[1][4] != "PSK" || rows01[1][9] != "JA1ABC" || rows01[1][11] != "1" {
		t.Fatalf("unexpected hour 01 row: %#v", rows01[1])
	}

	// Verify spaceweather JSONL captured startup, gen_change, and shutdown with SSN.
	swRecords := readSWJSONL(t, filepath.Join(dir, "spaceweather-2026-11-28.jsonl"))
	if len(swRecords) < 3 {
		t.Fatalf("expected >= 3 spaceweather records (startup, gen_change, shutdown), got %d", len(swRecords))
	}
	if swRecords[0].Reason != "startup" {
		t.Fatalf("first reason = %q, want startup", swRecords[0].Reason)
	}
	last := swRecords[len(swRecords)-1]
	if last.Reason != "shutdown" {
		t.Fatalf("last reason = %q, want shutdown", last.Reason)
	}
	if last.SFINow != 142.5 || last.Hp30Lag1 != 3.67 || last.DstNow != -18.0 {
		t.Fatalf("unexpected final spaceweather features: %+v", last)
	}
	if !last.VOACAPSSN.Available || last.VOACAPSSN.SSN != 36 || last.VOACAPSSN.Source != "ssn_state_file" {
		t.Fatalf("unexpected VOACAP SSN from file: %+v", last.VOACAPSSN)
	}
	if last.SpotsWritten != 2 || last.SpotsDropped != 0 {
		t.Fatalf("unexpected spot counters in shutdown record: written=%d dropped=%d", last.SpotsWritten, last.SpotsDropped)
	}
}

func TestPropCaptureSSNMonitorPrecedence(t *testing.T) {
	dir := t.TempDir()
	ssnPath := filepath.Join(t.TempDir(), "ssn_state.json")
	if err := os.WriteFile(ssnPath, []byte(`{"version":3,"input_initialized":true,"input":{"ssn":36}}`), 0o644); err != nil {
		t.Fatalf("write ssn_state.json: %v", err)
	}
	obsAt := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	w, err := newPropCaptureWriter(propCaptureConfig{
		Dir:          dir,
		SSNMonitor:   stubPropCaptureSSN{ssn: 51, ok: true, smoothed: 50.8, obsUTC: obsAt},
		SSNStatePath: ssnPath,
		Logger:       log.New(io.Discard, "", 0),
		NowFn:        func() time.Time { return obsAt },
	})
	if err != nil {
		t.Fatalf("newPropCaptureWriter: %v", err)
	}
	w.Start()
	w.Stop()

	recs := readSWJSONL(t, filepath.Join(dir, "spaceweather-2026-11-28.jsonl"))
	if len(recs) == 0 {
		t.Fatal("expected spaceweather records")
	}
	if !recs[0].VOACAPSSN.Available || recs[0].VOACAPSSN.SSN != 51 || recs[0].VOACAPSSN.Source != "sunspot_monitor" {
		t.Fatalf("expected live SunspotMonitor SSN=51, got %+v", recs[0].VOACAPSSN)
	}
}

func TestPropCaptureSameHourAppendValidGzip(t *testing.T) {
	dir := t.TempDir()
	ts := time.Date(2026, 11, 28, 2, 10, 0, 0, time.UTC)
	sp := &spot.Spot{
		DECallNorm: "W1AW",
		ModeNorm:   "FT8",
		SourceType: spot.SourcePSKReporter,
		Report:     -12,
		DEMetadata: spot.CallMetadata{Grid: "FN31"},
		DXMetadata: spot.CallMetadata{Grid: "JO22"},
	}

	for run := range 2 {
		w, err := newPropCaptureWriter(propCaptureConfig{
			Dir:    dir,
			Logger: log.New(io.Discard, "", 0),
			NowFn:  func() time.Time { return ts },
		})
		if err != nil {
			t.Fatalf("run %d newPropCaptureWriter: %v", run, err)
		}
		w.Start()
		w.EnqueueSpot(sp, ts.Add(time.Duration(run)*time.Second), ts, "40m", "FT8", -12.0, uint64(run+1))
		w.Stop()
	}

	rows := readGzipCSVRows(t, filepath.Join(dir, "spots-2026-11-28-02.csv.gz"))
	if len(rows) != 3 {
		t.Fatalf("expected 1 header + 2 rows across concatenated gzip members, got %d", len(rows))
	}
}

func TestPropCaptureNonBlockingDropAndConcurrentRace(t *testing.T) {
	dir := t.TempDir()
	ts := time.Date(2026, 11, 28, 3, 0, 0, 0, time.UTC)
	w, err := newPropCaptureWriter(propCaptureConfig{
		Dir:             dir,
		QueueCap:        4,
		FlushInterval:   5 * time.Millisecond,
		SWCheckInterval: 5 * time.Millisecond,
		Logger:          log.New(io.Discard, "", 0),
		NowFn:           func() time.Time { return ts },
	})
	if err != nil {
		t.Fatalf("newPropCaptureWriter: %v", err)
	}
	w.Start()

	const workers = 4
	const perWorker = 250
	var wg sync.WaitGroup
	wg.Add(workers)
	for g := range workers {
		go func(id int) {
			defer wg.Done()
			sp := &spot.Spot{
				DECallNorm: "K1ABC",
				ModeNorm:   "CW",
				SourceType: spot.SourceRBN,
				Report:     10,
				DEMetadata: spot.CallMetadata{Grid: "FN31"},
				DXMetadata: spot.CallMetadata{Grid: "JO22"},
			}
			for i := range perWorker {
				w.EnqueueSpot(sp, ts, ts, "20m", "CW", 3.0, uint64(id*1000+i))
			}
		}(g)
	}
	wg.Wait()
	w.Stop()

	total := w.WrittenCount() + w.DroppedCount()
	if total != workers*perWorker {
		t.Fatalf("written(%d) + dropped(%d) = %d, want %d", w.WrittenCount(), w.DroppedCount(), total, workers*perWorker)
	}
	// Enqueue after Stop must be a safe no-op.
	w.EnqueueSpot(&spot.Spot{DECallNorm: "W1AW"}, ts, ts, "20m", "CW", 0, 1)
}

func readGzipCSVRows(t *testing.T, path string) [][]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gzip reader %s: %v", path, err)
	}
	defer gz.Close()
	rows, err := csv.NewReader(gz).ReadAll()
	if err != nil {
		t.Fatalf("csv read %s: %v", path, err)
	}
	return rows
}

func readSWJSONL(t *testing.T, path string) []propCaptureSWRecord {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	var out []propCaptureSWRecord
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec propCaptureSWRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("unmarshal %q: %v", line, err)
		}
		out = append(out, rec)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return out
}
