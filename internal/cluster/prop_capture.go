// File role: Opt-in, bounded asynchronous capture of accepted path-reliability
// spot observations (hourly UTC .csv.gz) and causal space-weather + VOACAP SSN
// snapshots (daily UTC .jsonl) activated solely by DXC_PROP_CAPTURE_DIR.
// Crawler notes: Start here for weekend/offline LORO dataset capture. Disabled
// by default (nil writer: zero goroutines, zero allocations, zero file handles).
// Producers enqueue flat value structs non-blockingly and never close ch;
// clusterRuntime.close() stops all spot/weather producers before calling Stop().
package cluster

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/internal/propprior"
	proppriorfeed "dxcluster/internal/propprior/feed"
	"dxcluster/internal/voacap"
	"dxcluster/spot"
)

const (
	envPropCaptureDir           = "DXC_PROP_CAPTURE_DIR"
	propCaptureQueueCap         = 16384
	propCaptureFlushInterval    = 10 * time.Second
	propCaptureSWCheckInterval  = 60 * time.Second
	propCaptureSWHeartbeat      = 5 * time.Minute
	propCaptureDropLogCooldown  = 5 * time.Minute
	propCaptureBufSize          = 64 * 1024
	propCaptureMaxSSNStateBytes = 64 * 1024
	propCaptureCSVHeader        = "observed_unix,arrived_unix,band,mode,source,raw_snr_db,ft8_db,de_grid,dx_grid,de_call,de_hash,is_beacon,de_grid_derived,dx_grid_derived\n"
)

type propCaptureSpot struct {
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

type propCaptureSWFeed struct {
	HasValid      bool    `json:"has_valid"`
	LastValidUnix int64   `json:"last_valid_unix,omitempty"`
	GapHours      float64 `json:"gap_hours"`
	Source        string  `json:"source,omitempty"`
}

type propCaptureSWHealth struct {
	State             string `json:"state"`
	ActiveSource      string `json:"active_source,omitempty"`
	UsingFallback     bool   `json:"using_fallback,omitempty"`
	ConsecutiveErrors int    `json:"consecutive_errors,omitempty"`
	NewestBinUnix     int64  `json:"newest_bin_unix,omitempty"`
}

type propCaptureSSN struct {
	Available    bool    `json:"available"`
	SSN          int     `json:"ssn,omitempty"`
	Smoothed     float64 `json:"smoothed_count_index,omitempty"`
	ObservedUnix int64   `json:"observed_unix,omitempty"`
	Source       string  `json:"source,omitempty"`
}

type propCaptureSWRecord struct {
	TSUnix       int64                          `json:"ts_unix"`
	TSUTC        string                         `json:"ts_utc"`
	Reason       string                         `json:"reason"`
	Gen          uint64                         `json:"gen"`
	SFINow       float64                        `json:"sfi_now"`
	SFI72hMean   float64                        `json:"sfi_72h_mean"`
	Hp30Lag1     float64                        `json:"hp30_lag1"`
	Hp3012hMax   float64                        `json:"hp30_12h_max"`
	Hp3024hMean  float64                        `json:"hp30_24h_mean"`
	BzSouth3h    float64                        `json:"bz_south_3h"`
	Vsw3h        float64                        `json:"vsw_3h"`
	DstNow       float64                        `json:"dst_now"`
	Dst24hMin    float64                        `json:"dst_24h_min"`
	SFIDelta     float64                        `json:"sfi_delta"`
	DstRecovery  float64                        `json:"dst_recovery"`
	Feeds        map[string]propCaptureSWFeed   `json:"feeds"`
	Health       map[string]propCaptureSWHealth `json:"health,omitempty"`
	VOACAPSSN    propCaptureSSN                 `json:"voacap_ssn"`
	SpotsWritten uint64                         `json:"spots_written"`
	SpotsDropped uint64                         `json:"spots_dropped"`
}

type propCaptureSSNSource interface {
	CurrentSSN(now time.Time) (int, bool)
	Snapshot() voacap.SunspotMonitorSnapshot
}

type propCaptureConfig struct {
	Dir             string
	QueueCap        int
	FlushInterval   time.Duration
	SWCheckInterval time.Duration
	SWHeartbeat     time.Duration
	Weather         *propprior.SpaceWeatherStore
	Poller          *proppriorfeed.Poller
	SSNMonitor      propCaptureSSNSource
	SSNStatePath    string
	Logger          *log.Logger
	NowFn           func() time.Time
}

// propCaptureWriter owns a single bounded background writer goroutine that
// persists accepted path-predictor observations and causal space-weather/SSN
// snapshots without blocking hot paths or retaining *spot.Spot pointers.
type propCaptureWriter struct {
	dir             string
	flushInterval   time.Duration
	swCheckInterval time.Duration
	swHeartbeat     time.Duration
	weather         *propprior.SpaceWeatherStore
	poller          *proppriorfeed.Poller
	ssnMonitor      propCaptureSSNSource
	ssnStatePath    string
	logger          *log.Logger
	nowFn           func() time.Time

	ch   chan propCaptureSpot
	stop chan struct{}
	done chan struct{}

	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool
	stopped   atomic.Bool

	written atomic.Uint64
	dropped atomic.Uint64

	// Single-goroutine writer state owned exclusively by run().
	spotFile          *os.File
	spotBuf           *bufio.Writer
	spotGZ            *gzip.Writer
	spotHourKey       string
	spotDirty         bool
	lineBuf           []byte
	swFile            *os.File
	swDayKey          string
	lastSWGen         uint64
	hasLastSWGen      bool
	lastSWWrite       time.Time
	lastDropLog       time.Time
	lastReportedDrops uint64
}

func newPropCaptureWriter(cfg propCaptureConfig) (*propCaptureWriter, error) {
	dir := strings.TrimSpace(cfg.Dir)
	if dir == "" {
		return nil, errors.New("prop capture directory is empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create prop capture dir %q: %w", dir, err)
	}
	// Verify writability up front so bad paths fail safely at startup.
	probe, err := os.CreateTemp(dir, ".prop_capture_probe_*")
	if err != nil {
		return nil, fmt.Errorf("prop capture dir %q not writable: %w", dir, err)
	}
	probeName := probe.Name()
	_ = probe.Close()
	_ = os.Remove(probeName)

	qcap := cfg.QueueCap
	if qcap <= 0 {
		qcap = propCaptureQueueCap
	}
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = propCaptureFlushInterval
	}
	swCheck := cfg.SWCheckInterval
	if swCheck <= 0 {
		swCheck = propCaptureSWCheckInterval
	}
	swHeartbeat := cfg.SWHeartbeat
	if swHeartbeat <= 0 {
		swHeartbeat = propCaptureSWHeartbeat
	}
	nowFn := cfg.NowFn
	if nowFn == nil {
		nowFn = func() time.Time { return time.Now().UTC() }
	}
	return &propCaptureWriter{
		dir:             dir,
		flushInterval:   flushInterval,
		swCheckInterval: swCheck,
		swHeartbeat:     swHeartbeat,
		weather:         cfg.Weather,
		poller:          cfg.Poller,
		ssnMonitor:      cfg.SSNMonitor,
		ssnStatePath:    strings.TrimSpace(cfg.SSNStatePath),
		logger:          cfg.Logger,
		nowFn:           nowFn,
		ch:              make(chan propCaptureSpot, qcap),
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
		lineBuf:         make([]byte, 0, 256),
	}, nil
}

// Start launches the background capture goroutine once.
func (w *propCaptureWriter) Start() {
	if w == nil || w.stopped.Load() {
		return
	}
	w.startOnce.Do(func() {
		w.started.Store(true)
		go w.run()
	})
}

// Stop signals shutdown, drains buffered spot records, writes a final
// space-weather snapshot, and closes all open files. Safe to call multiple times.
func (w *propCaptureWriter) Stop() {
	if w == nil {
		return
	}
	w.stopOnce.Do(func() {
		w.stopped.Store(true)
		close(w.stop)
		if w.started.Load() {
			<-w.done
		}
	})
}

// EnqueueSpot copies scalar/string fields from an accepted path observation and
// queues it non-blockingly. Drops newest when full to protect ingest/output hot paths.
func (w *propCaptureWriter) EnqueueSpot(
	s *spot.Spot,
	observedAt, arrivedAt time.Time,
	band, mode string,
	ft8DB float64,
	receiverHash uint64,
) {
	if w == nil || s == nil || w.stopped.Load() {
		return
	}
	deCall := strings.TrimSpace(s.DECallNorm)
	if deCall == "" {
		deCall = strings.TrimSpace(s.DECall)
	}
	deGrid := strings.TrimSpace(s.DEGridNorm)
	if deGrid == "" {
		deGrid = strings.TrimSpace(s.DEMetadata.Grid)
	}
	dxGrid := strings.TrimSpace(s.DXGridNorm)
	if dxGrid == "" {
		dxGrid = strings.TrimSpace(s.DXMetadata.Grid)
	}
	rec := propCaptureSpot{
		observedUnix:  observedAt.UTC().Unix(),
		arrivedUnix:   arrivedAt.UTC().Unix(),
		band:          band,
		mode:          strings.ToUpper(strings.TrimSpace(mode)),
		source:        sourceBucket(s.SourceType),
		rawSNRDB:      s.Report,
		ft8DB:         ft8DB,
		deGrid:        deGrid,
		dxGrid:        dxGrid,
		deCall:        deCall,
		deHash:        receiverHash,
		isBeacon:      s.IsBeacon,
		deGridDerived: s.DEMetadata.GridDerived,
		dxGridDerived: s.DXMetadata.GridDerived,
	}
	select {
	case w.ch <- rec:
	default:
		w.dropped.Add(1)
	}
}

func (w *propCaptureWriter) WrittenCount() uint64 {
	if w == nil {
		return 0
	}
	return w.written.Load()
}

func (w *propCaptureWriter) DroppedCount() uint64 {
	if w == nil {
		return 0
	}
	return w.dropped.Load()
}

func (w *propCaptureWriter) run() {
	defer close(w.done)
	defer w.closeAllFiles()

	w.writeSpaceWeatherSnapshot(w.nowFn().UTC(), "startup")

	flushTicker := time.NewTicker(w.flushInterval)
	defer flushTicker.Stop()
	swTicker := time.NewTicker(w.swCheckInterval)
	defer swTicker.Stop()

	for {
		select {
		case <-w.stop:
			w.drainSpots()
			w.flushSpots()
			w.writeSpaceWeatherSnapshot(w.nowFn().UTC(), "shutdown")
			return
		case rec := <-w.ch:
			w.writeSpotRecord(rec)
		case <-flushTicker.C:
			w.flushSpots()
			w.maybeLogDrops(w.nowFn().UTC())
		case <-swTicker.C:
			w.maybeWriteSpaceWeather(w.nowFn().UTC())
		}
	}
}

func (w *propCaptureWriter) drainSpots() {
	for {
		select {
		case rec := <-w.ch:
			w.writeSpotRecord(rec)
		default:
			return
		}
	}
}

func (w *propCaptureWriter) writeSpotRecord(rec propCaptureSpot) {
	ts := time.Unix(rec.observedUnix, 0).UTC()
	hourKey := ts.Format("2006-01-02-15")
	if w.spotGZ == nil || w.spotHourKey != hourKey {
		if err := w.rotateSpotFile(hourKey); err != nil {
			w.logf("Warning: prop capture spot file open (%s): %v", hourKey, err)
			return
		}
	}
	w.lineBuf = appendPropCaptureCSVRow(w.lineBuf[:0], rec)
	if _, err := w.spotGZ.Write(w.lineBuf); err != nil {
		w.logf("Warning: prop capture spot write (%s): %v", hourKey, err)
		w.closeSpotFile()
		return
	}
	w.spotDirty = true
	w.written.Add(1)
}

func (w *propCaptureWriter) rotateSpotFile(hourKey string) error {
	w.closeSpotFile()
	path := filepath.Join(w.dir, "spots-"+hourKey+".csv.gz")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	info, statErr := f.Stat()
	writeHeader := statErr == nil && info.Size() == 0

	bw := bufio.NewWriterSize(f, propCaptureBufSize)
	gz, err := gzip.NewWriterLevel(bw, gzip.BestSpeed)
	if err != nil {
		_ = f.Close()
		return err
	}
	if writeHeader {
		if _, err := gz.Write([]byte(propCaptureCSVHeader)); err != nil {
			_ = gz.Close()
			_ = bw.Flush()
			_ = f.Close()
			return err
		}
	}
	w.spotFile = f
	w.spotBuf = bw
	w.spotGZ = gz
	w.spotHourKey = hourKey
	w.spotDirty = writeHeader
	return nil
}

func (w *propCaptureWriter) flushSpots() {
	if !w.spotDirty || w.spotGZ == nil || w.spotBuf == nil {
		return
	}
	if err := w.spotGZ.Flush(); err != nil {
		w.logf("Warning: prop capture gzip flush failed: %v", err)
		return
	}
	if err := w.spotBuf.Flush(); err != nil {
		w.logf("Warning: prop capture buffer flush failed: %v", err)
		return
	}
	w.spotDirty = false
}

func (w *propCaptureWriter) closeSpotFile() {
	if w.spotGZ != nil {
		_ = w.spotGZ.Close()
		w.spotGZ = nil
	}
	if w.spotBuf != nil {
		_ = w.spotBuf.Flush()
		w.spotBuf = nil
	}
	if w.spotFile != nil {
		_ = w.spotFile.Close()
		w.spotFile = nil
	}
	w.spotHourKey = ""
	w.spotDirty = false
}

func (w *propCaptureWriter) maybeWriteSpaceWeather(now time.Time) {
	gen := uint64(0)
	if w.weather != nil {
		gen = w.weather.Generation()
	}
	if !w.hasLastSWGen || gen != w.lastSWGen {
		w.writeSpaceWeatherSnapshot(now, "gen_change")
		return
	}
	if now.Sub(w.lastSWWrite) >= w.swHeartbeat {
		w.writeSpaceWeatherSnapshot(now, "heartbeat")
	}
}

func (w *propCaptureWriter) writeSpaceWeatherSnapshot(now time.Time, reason string) {
	utc := now.UTC()
	dayKey := utc.Format("2006-01-02")
	if w.swFile == nil || w.swDayKey != dayKey {
		if err := w.rotateSWFile(dayKey); err != nil {
			w.logf("Warning: prop capture spaceweather file open (%s): %v", dayKey, err)
			return
		}
	}

	var snap propprior.SpaceWeatherSnapshot
	if w.weather != nil {
		snap = w.weather.Evaluate(utc)
	} else {
		snap = propprior.NewSpaceWeatherStore().Evaluate(utc)
	}

	rec := propCaptureSWRecord{
		TSUnix:      utc.Unix(),
		TSUTC:       utc.Format(time.RFC3339),
		Reason:      reason,
		Gen:         snap.Generation,
		SFINow:      snap.Features.SFINow,
		SFI72hMean:  snap.Features.SFI72hMean,
		Hp30Lag1:    snap.Features.Hp30Lag1,
		Hp3012hMax:  snap.Features.Hp3012hMax,
		Hp3024hMean: snap.Features.Hp3024hMean,
		BzSouth3h:   snap.Features.BzSouth3h,
		Vsw3h:       snap.Features.Vsw3h,
		DstNow:      snap.Features.DstNow,
		Dst24hMin:   snap.Features.Dst24hMin,
		SFIDelta:    snap.SFIDelta,
		DstRecovery: snap.DstRecovery,
		Feeds: map[string]propCaptureSWFeed{
			"sfi":  toPropCaptureSWFeed(snap.SFI),
			"hp30": toPropCaptureSWFeed(snap.Hp30),
			"bz":   toPropCaptureSWFeed(snap.Bz),
			"vsw":  toPropCaptureSWFeed(snap.Vsw),
			"dst":  toPropCaptureSWFeed(snap.Dst),
		},
		VOACAPSSN:    w.readSSN(utc),
		SpotsWritten: w.written.Load(),
		SpotsDropped: w.dropped.Load(),
	}
	if w.poller != nil {
		h := w.poller.Health()
		rec.Health = map[string]propCaptureSWHealth{
			"hp30":      toPropCaptureSWHealth(h.Hp30),
			"sfi":       toPropCaptureSWHealth(h.SFI),
			"rtsw_mag":  toPropCaptureSWHealth(h.RTSWMag),
			"rtsw_wind": toPropCaptureSWHealth(h.RTSWWind),
			"dst":       toPropCaptureSWHealth(h.Dst),
		}
	}

	line, err := json.Marshal(rec)
	if err != nil {
		w.logf("Warning: prop capture spaceweather marshal failed: %v", err)
		return
	}
	line = append(line, '\n')
	if _, err := w.swFile.Write(line); err != nil {
		w.logf("Warning: prop capture spaceweather write failed: %v", err)
		w.closeSWFile()
		return
	}
	w.lastSWGen = snap.Generation
	w.hasLastSWGen = true
	w.lastSWWrite = utc
}

func (w *propCaptureWriter) rotateSWFile(dayKey string) error {
	w.closeSWFile()
	path := filepath.Join(w.dir, "spaceweather-"+dayKey+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w.swFile = f
	w.swDayKey = dayKey
	return nil
}

func (w *propCaptureWriter) closeSWFile() {
	if w.swFile != nil {
		_ = w.swFile.Close()
		w.swFile = nil
	}
	w.swDayKey = ""
}

func (w *propCaptureWriter) closeAllFiles() {
	w.closeSpotFile()
	w.closeSWFile()
}

func (w *propCaptureWriter) maybeLogDrops(now time.Time) {
	dropped := w.dropped.Load()
	if dropped == w.lastReportedDrops {
		return
	}
	if !w.lastDropLog.IsZero() && now.Sub(w.lastDropLog) < propCaptureDropLogCooldown {
		return
	}
	delta := dropped - w.lastReportedDrops
	w.lastReportedDrops = dropped
	w.lastDropLog = now
	w.logf("Warning: prop capture dropped %d spots since last report (total_dropped=%d total_written=%d)", delta, dropped, w.written.Load())
}

func (w *propCaptureWriter) readSSN(now time.Time) propCaptureSSN {
	if w.ssnMonitor != nil {
		if ssn, ok := w.ssnMonitor.CurrentSSN(now); ok {
			snap := w.ssnMonitor.Snapshot()
			out := propCaptureSSN{
				Available: true,
				SSN:       ssn,
				Smoothed:  snap.SmoothedCountIndex,
				Source:    "sunspot_monitor",
			}
			if !snap.LastObservedAtUTC.IsZero() {
				out.ObservedUnix = snap.LastObservedAtUTC.UTC().Unix()
			}
			return out
		}
	}
	if w.ssnStatePath == "" {
		return propCaptureSSN{}
	}
	return readSSNStateFile(w.ssnStatePath)
}

func readSSNStateFile(path string) propCaptureSSN {
	f, err := os.Open(path)
	if err != nil {
		return propCaptureSSN{}
	}
	defer f.Close()

	body, err := io.ReadAll(io.LimitReader(f, propCaptureMaxSSNStateBytes+1))
	if err != nil || len(body) == 0 || len(body) > propCaptureMaxSSNStateBytes {
		return propCaptureSSN{}
	}
	var parsed struct {
		Version            int       `json:"version"`
		LastObservedAtUTC  time.Time `json:"last_observed_at_utc"`
		SmoothedCountIndex float64   `json:"smoothed_count_index"`
		InputInitialized   bool      `json:"input_initialized"`
		Input              struct {
			SSN int `json:"ssn"`
		} `json:"input"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return propCaptureSSN{}
	}
	if parsed.Version != 3 || !parsed.InputInitialized || parsed.Input.SSN <= 0 {
		return propCaptureSSN{}
	}
	out := propCaptureSSN{
		Available: true,
		SSN:       parsed.Input.SSN,
		Smoothed:  parsed.SmoothedCountIndex,
		Source:    "ssn_state_file",
	}
	if !parsed.LastObservedAtUTC.IsZero() {
		out.ObservedUnix = parsed.LastObservedAtUTC.UTC().Unix()
	}
	return out
}

func toPropCaptureSWFeed(d propprior.FeedDiagnostics) propCaptureSWFeed {
	out := propCaptureSWFeed{
		HasValid: d.HasValid,
		GapHours: d.GapHours,
		Source:   d.Source,
	}
	if d.HasValid && !d.LastValidUTC.IsZero() {
		out.LastValidUnix = d.LastValidUTC.UTC().Unix()
	}
	return out
}

func toPropCaptureSWHealth(h proppriorfeed.StreamHealth) propCaptureSWHealth {
	out := propCaptureSWHealth{
		State:             string(h.State),
		ActiveSource:      h.ActiveSource,
		UsingFallback:     h.UsingFallback,
		ConsecutiveErrors: h.ConsecutiveErrors,
	}
	if !h.NewestBinUTC.IsZero() {
		out.NewestBinUnix = h.NewestBinUTC.UTC().Unix()
	}
	return out
}

func appendPropCaptureCSVRow(dst []byte, rec propCaptureSpot) []byte {
	dst = strconv.AppendInt(dst, rec.observedUnix, 10)
	dst = append(dst, ',')
	dst = strconv.AppendInt(dst, rec.arrivedUnix, 10)
	dst = append(dst, ',')
	dst = appendSanitizedCSVToken(dst, rec.band)
	dst = append(dst, ',')
	dst = appendSanitizedCSVToken(dst, rec.mode)
	dst = append(dst, ',')
	dst = appendSanitizedCSVToken(dst, rec.source)
	dst = append(dst, ',')
	dst = strconv.AppendInt(dst, int64(rec.rawSNRDB), 10)
	dst = append(dst, ',')
	dst = strconv.AppendFloat(dst, rec.ft8DB, 'f', 2, 64)
	dst = append(dst, ',')
	dst = appendSanitizedCSVToken(dst, rec.deGrid)
	dst = append(dst, ',')
	dst = appendSanitizedCSVToken(dst, rec.dxGrid)
	dst = append(dst, ',')
	dst = appendSanitizedCSVToken(dst, rec.deCall)
	dst = append(dst, ',')
	dst = strconv.AppendUint(dst, rec.deHash, 10)
	dst = append(dst, ',')
	dst = appendBoolDigit(dst, rec.isBeacon)
	dst = append(dst, ',')
	dst = appendBoolDigit(dst, rec.deGridDerived)
	dst = append(dst, ',')
	dst = appendBoolDigit(dst, rec.dxGridDerived)
	dst = append(dst, '\n')
	return dst
}

func appendSanitizedCSVToken(dst []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b == ',' || b == '\r' || b == '\n' || b == '"' {
			continue
		}
		dst = append(dst, b)
	}
	return dst
}

func appendBoolDigit(dst []byte, v bool) []byte {
	if v {
		return append(dst, '1')
	}
	return append(dst, '0')
}

func (w *propCaptureWriter) logf(format string, args ...any) {
	if w != nil && w.logger != nil {
		w.logger.Printf(format, args...)
		return
	}
	log.Printf(format, args...)
}
