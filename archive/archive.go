package archive

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dxcluster/config"
	"dxcluster/internal/byteutil"
	"dxcluster/internal/numutil"
	"dxcluster/spot"
	"dxcluster/strutil"

	"github.com/cockroachdb/pebble"
)

const (
	spotPrefix = "s|"
	spotKeyLen = len(spotPrefix) + 8 + 4
)

var (
	spotPrefixBytes = []byte(spotPrefix)
	spotIterLower   = append([]byte(nil), spotPrefixBytes...)
	spotIterUpper   = byteutil.PrefixUpperBound(spotIterLower)
	// spotIterOptions is read-only and safe to reuse across iterators.
	spotIterOptions = &pebble.IterOptions{
		LowerBound: spotIterLower,
		UpperBound: spotIterUpper,
	}
)

const (
	recordVersionV2         = 2
	recordVersionV3         = 3
	recordVersionV4         = 4
	recordVersion           = 5
	recordFixedHeaderSizeV2 = 28
	recordFixedHeaderSize   = 36
)

const (
	fieldDXCall = iota
	fieldDECall
	fieldDECallStripped
	fieldMode
	fieldComment
	fieldSource
	fieldSourceNode
	fieldConfidence
	fieldBand
	fieldDXGrid
	fieldDEGrid
	fieldDXCont
	fieldDECont
	fieldEvents
	fieldToxicityStatus
	fieldToxicityCategories
	fieldToxicityModel
	fieldCount
)

const (
	fieldCountV3         = fieldEvents
	fieldCountV4         = fieldEvents + 1
	recordHeaderSizeV2   = recordFixedHeaderSizeV2 + fieldCountV3*2
	recordHeaderSizeV3   = recordFixedHeaderSize + fieldCountV3*2
	recordHeaderSizeV4   = recordFixedHeaderSize + fieldCountV4*2
	recordHeaderSize     = recordFixedHeaderSize + fieldCount*2
	recentScanMultiplier = 20
	recentScanGrowth     = 4
	recentScanMax        = 200000
)

const (
	flagHasReport = 1 << iota
	flagIsHuman
	flagDXGridDerived
	flagDEGridDerived
)

var errInvalidRecord = errors.New("archive: invalid record encoding")

func recordLayout(version byte) (fixedHeaderSize int, headerSize int, fieldN int, ok bool) {
	switch version {
	case recordVersionV2:
		return recordFixedHeaderSizeV2, recordHeaderSizeV2, fieldCountV3, true
	case recordVersionV3:
		return recordFixedHeaderSize, recordHeaderSizeV3, fieldCountV3, true
	case recordVersionV4:
		return recordFixedHeaderSize, recordHeaderSizeV4, fieldCountV4, true
	case recordVersion:
		return recordFixedHeaderSize, recordHeaderSize, fieldCount, true
	default:
		return 0, 0, 0, false
	}
}

// Writer persists spots to Pebble asynchronously with one archive retention window.
// It is designed to be removable: the hot path never blocks on the writer,
// and backpressure results in dropped archive writes.
type Writer struct {
	cfg        config.ArchiveConfig
	db         *pebble.DB
	queue      chan *spot.Spot
	stop       chan struct{}
	doneInsert chan struct{}
	doneClean  chan struct{}
	writeOpts  *pebble.WriteOptions

	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool
	seq       uint32
}

// NewWriter initializes archive storage and returns a writer instance.
// Key aspects: Creates Pebble DB directory and queue defaults.
// Upstream: main.go archive setup.
// Downstream: openArchiveDB.
func NewWriter(cfg config.ArchiveConfig) (*Writer, error) {
	db, err := openArchiveDB(cfg)
	if err != nil {
		return nil, err
	}
	writeOpts, err := archiveWriteOptions(cfg)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if cfg.RetentionSeconds <= 0 {
		cfg.RetentionSeconds = config.DefaultArchiveRetentionSeconds
	}
	qsize := cfg.QueueSize
	if qsize <= 0 {
		qsize = 10000
	}
	return &Writer{
		cfg:        cfg,
		db:         db,
		queue:      make(chan *spot.Spot, qsize),
		stop:       make(chan struct{}),
		doneInsert: make(chan struct{}),
		doneClean:  make(chan struct{}),
		writeOpts:  writeOpts,
	}, nil
}

// Purpose: Open the archive Pebble DB with optional corruption recovery.
// Key aspects: Ensures a directory exists; deletes corrupt DB when configured.
// Upstream: NewWriter.
// Downstream: pebble.Open.
func openArchiveDB(cfg config.ArchiveConfig) (*pebble.DB, error) {
	path := strings.TrimSpace(cfg.DBPath)
	if path == "" {
		return nil, errors.New("archive: db_path is empty")
	}
	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			if cfg.AutoDeleteCorruptDB {
				if err := DropDB(path); err != nil {
					return nil, fmt.Errorf("archive: delete non-directory path: %w", err)
				}
				log.Printf("archive: removed non-directory path at %s", path)
			} else {
				return nil, fmt.Errorf("archive: %s exists and is not a directory", path)
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("archive: stat path: %w", err)
	}

	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("archive: ensure directory: %w", err)
	}
	db, err := pebble.Open(path, &pebble.Options{})
	if err == nil {
		return db, nil
	}
	if cfg.AutoDeleteCorruptDB && pebble.IsCorruptionError(err) {
		if err := DropDB(path); err != nil {
			return nil, fmt.Errorf("archive: delete corrupt db: %w", err)
		}
		if err := os.MkdirAll(path, 0o755); err != nil {
			return nil, fmt.Errorf("archive: ensure directory: %w", err)
		}
		db, err = pebble.Open(path, &pebble.Options{})
		if err != nil {
			return nil, fmt.Errorf("archive: open after delete: %w", err)
		}
		log.Printf("archive: deleted corrupt db at %s", path)
		return db, nil
	}
	return nil, fmt.Errorf("archive: open: %w", err)
}

// Purpose: Map archive synchronous mode to Pebble write options.
// Key aspects: "off" disables fsync; other modes enable sync.
// Upstream: NewWriter.
// Downstream: pebble.WriteOptions.
func archiveWriteOptions(cfg config.ArchiveConfig) (*pebble.WriteOptions, error) {
	mode := strings.ToLower(strings.TrimSpace(cfg.Synchronous))
	if mode == "" {
		mode = "off"
	}
	switch mode {
	case "off":
		return pebble.NoSync, nil
	case "normal", "full", "extra":
		return pebble.Sync, nil
	default:
		return nil, fmt.Errorf("archive: invalid synchronous mode %q", cfg.Synchronous)
	}
}

// Start starts background loops for inserts and retention cleanup.
// Key aspects: Runs goroutines; writer remains non-blocking for callers.
// Upstream: main.go startup.
// Downstream: insertLoop goroutine, cleanupLoop goroutine.
func (w *Writer) Start() {
	if w == nil {
		return
	}
	w.startOnce.Do(func() {
		w.started.Store(true)
		go w.insertLoop()
		go w.cleanupLoop()
	})
}

// Stop stops the writer and closes the underlying DB.
// Key aspects: Signals loops to exit; waits for completion before closing Pebble.
// Upstream: main.go shutdown.
// Downstream: insertLoop, cleanupLoop, db.Close.
func (w *Writer) Stop() {
	if w == nil {
		return
	}
	w.stopOnce.Do(func() { close(w.stop) })
	if w.started.Load() {
		<-w.doneInsert
		<-w.doneClean
	}
	if w.db != nil {
		_ = w.db.Close()
	}
}

// Enqueue tries to enqueue a spot snapshot for archival without blocking.
// Key aspects: Reuses an already-owned async snapshot when available; otherwise
// clones before queueing so later normalization cannot race with the pipeline.
// Upstream: main.go spot ingest/broadcast.
// Downstream: writer queue channel.
func (w *Writer) Enqueue(s *spot.Spot) {
	if w == nil || s == nil {
		return
	}
	snapshot := s.SnapshotForAsync()
	if snapshot == nil {
		return
	}
	w.EnqueueOwned(snapshot)
}

// EnqueueOwned enqueues a caller-owned immutable snapshot without cloning it
// again. Callers must guarantee the spot will not be mutated after handoff.
func (w *Writer) EnqueueOwned(snapshot *spot.Spot) {
	if w == nil || snapshot == nil {
		return
	}
	select {
	case <-w.stop:
		return
	default:
	}
	select {
	case w.queue <- snapshot:
	default:
		// Drop silently to avoid interfering with the hot path.
	}
}

// Purpose: Batch and insert queued spots into Pebble.
// Key aspects: Uses a size/time batch; flushes on stop signal.
// Upstream: Start goroutine.
// Downstream: flush, time.Timer.
func (w *Writer) insertLoop() {
	defer close(w.doneInsert)

	batchSize := w.cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	interval := time.Duration(w.cfg.BatchIntervalMS) * time.Millisecond
	if interval <= 0 {
		interval = 200 * time.Millisecond
	}
	batch := make([]*spot.Spot, 0, batchSize)
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-w.stop:
			batch = w.drainQueue(batch, batchSize)
			w.flush(batch)
			return
		case s := <-w.queue:
			batch = append(batch, s)
			if len(batch) >= batchSize {
				w.flush(batch)
				batch = batch[:0]
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(interval)
			}
		case <-timer.C:
			if len(batch) > 0 {
				w.flush(batch)
				batch = batch[:0]
			}
			timer.Reset(interval)
		}
	}
}

func (w *Writer) drainQueue(batch []*spot.Spot, batchSize int) []*spot.Spot {
	for {
		select {
		case s := <-w.queue:
			batch = append(batch, s)
			if len(batch) >= batchSize {
				w.flush(batch)
				batch = batch[:0]
			}
		default:
			return batch
		}
	}
}

// Purpose: Flush a batch of spots into Pebble in a single write batch.
// Key aspects: Best-effort logging on errors; batch commit honors sync config.
// Upstream: insertLoop.
// Downstream: pebble.Batch.
func (w *Writer) flush(batch []*spot.Spot) {
	if len(batch) == 0 {
		return
	}
	pebbleBatch := w.db.NewBatch()
	defer pebbleBatch.Close()

	for _, s := range batch {
		if s == nil {
			continue
		}
		s.EnsureNormalized()
		key := spotKeyBytes(normalizeUnixNano(s.Time), w.nextSeq())
		val := encodeRecord(s)
		if err := pebbleBatch.Set(key, val, nil); err != nil {
			log.Printf("archive: batch set failed: %v", err)
		}
	}
	if err := pebbleBatch.Commit(w.writeOpts); err != nil {
		log.Printf("archive: batch commit: %v", err)
	}
}

func (w *Writer) nextSeq() uint32 {
	w.seq++
	return w.seq
}

// Purpose: Periodically enforce retention policy by deleting old rows.
// Key aspects: Uses a ticker; exits on stop signal.
// Upstream: Start goroutine.
// Downstream: cleanupOnce, time.Ticker.
func (w *Writer) cleanupLoop() {
	defer close(w.doneClean)
	interval := time.Duration(w.cfg.CleanupIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = time.Hour
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
			w.cleanupOnce()
		}
	}
}

func (w *Writer) cleanupOnce() {
	w.cleanupOnceAt(time.Now().UTC())
}

// Purpose: Run one retention cleanup pass.
// Key aspects: Deletes expired timestamp keys with one Pebble range tombstone
// after a bounded oldest-key probe. Repeated cleanup intentionally reuses the
// full expired range so late-arriving old spots are removed on the next pass.
// Upstream: cleanupLoop.
// Downstream: pebble.DeleteRange.
func (w *Writer) cleanupOnceAt(now time.Time) {
	if w == nil || w.db == nil {
		return
	}
	cutoff := retentionCutoff(now.UTC().UnixNano(), w.cfg.RetentionSeconds)
	if cutoff <= 0 {
		return
	}
	rangeEnd := spotKeyBytes(cutoff, 0)

	iter, err := w.db.NewIter(spotIterOptions)
	if err != nil {
		log.Printf("archive: cleanup iterator: %v", err)
		return
	}

	hasExpired := false
	if iter.First() {
		hasExpired = bytes.Compare(iter.Key(), rangeEnd) < 0
	} else if err := iter.Error(); err != nil {
		log.Printf("archive: cleanup iterate: %v", err)
		_ = iter.Close()
		return
	}
	if err := iter.Error(); err != nil {
		log.Printf("archive: cleanup iterate: %v", err)
		_ = iter.Close()
		return
	}
	_ = iter.Close()

	if !hasExpired {
		return
	}
	if err := w.db.DeleteRange(spotIterLower, rangeEnd, w.writeOpts); err != nil {
		log.Printf("archive: cleanup delete range: %v", err)
	}
}

func retentionCutoff(now int64, seconds int) int64 {
	if seconds <= 0 {
		return math.MinInt64
	}
	return now - int64(seconds)*int64(time.Second)
}

// DropDB deletes the archive DB files (test helper).
// Key aspects: Removes the DB directory tree or file path.
// Upstream: Tests or maintenance tools.
// Downstream: os.RemoveAll.
func DropDB(path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("archive: empty path")
	}
	return os.RemoveAll(path)
}

// Recent returns the most recent N archived spots, newest-first.
// Key aspects: Read-only iterator; reconstructs Spot objects from records.
// Upstream: Telnet SHOW/DX handlers when archive is enabled.
// Downstream: decodeSpot.
func (w *Writer) Recent(limit int) ([]*spot.Spot, error) {
	return w.RecentFiltered(limit, nil)
}

// RecentFiltered returns the most recent N archived spots that match a predicate.
// Key aspects: Progressive bounded scan to avoid unbounded reads on narrow filters.
// Upstream: Telnet SHOW MYDX handlers.
// Downstream: decodeSpot, predicate match.
func (w *Writer) RecentFiltered(limit int, match func(*spot.Spot) bool) ([]*spot.Spot, error) {
	if w == nil || w.db == nil {
		return nil, fmt.Errorf("archive: writer is nil")
	}
	if limit <= 0 {
		return []*spot.Spot{}, nil
	}
	iter, err := w.db.NewIter(spotIterOptions)
	if err != nil {
		return nil, fmt.Errorf("archive: recent iterator: %w", err)
	}
	defer iter.Close()

	results := make([]*spot.Spot, 0, limit)
	scanned := 0
	decodeErrors := 0
	scanLimit := limit * recentScanMultiplier
	if scanLimit < limit {
		scanLimit = limit
	}
	if scanLimit > recentScanMax {
		scanLimit = recentScanMax
	}

	for ok := iter.Last(); ok && len(results) < limit; ok = iter.Prev() {
		// Expand scan window for narrow filters while keeping a hard cap.
		if scanned >= scanLimit {
			if scanLimit >= recentScanMax {
				break
			}
			if scanLimit > recentScanMax/recentScanGrowth {
				scanLimit = recentScanMax
			} else {
				scanLimit *= recentScanGrowth
				if scanLimit > recentScanMax {
					scanLimit = recentScanMax
				}
			}
		}
		scanned++
		ts, ok := parseSpotKey(iter.Key())
		if !ok {
			continue
		}
		s, err := decodeSpot(ts, iter.Value())
		if err != nil {
			decodeErrors++
			if decodeErrors == 1 {
				log.Printf("archive: recent decode: %v", err)
			}
			continue
		}
		if match != nil && !match(s) {
			continue
		}
		results = append(results, s)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("archive: recent iterate: %w", err)
	}
	return results, nil
}

type archiveRecord struct {
	dxCall             string
	deCall             string
	deCallStripped     string
	mode               string
	comment            string
	source             string
	sourceNode         string
	confidence         string
	band               string
	dxGrid             string
	deGrid             string
	dxGridDerived      bool
	deGridDerived      bool
	dxCont             string
	deCont             string
	events             spot.EventMask
	freq               float64
	observedFreq       float64
	report             int
	hasReport          bool
	isHuman            bool
	ttl                uint8
	dxCQZone           int
	deCQZone           int
	dxADIF             int
	deADIF             int
	toxicityStatus     string
	toxicityCategories string
	toxicityModel      string
}

func encodeRecord(s *spot.Spot) []byte {
	dxCall := strings.TrimSpace(s.DXCallNorm)
	if dxCall == "" {
		dxCall = strings.TrimSpace(s.DXCall)
	}
	deCall := strings.TrimSpace(s.DECallNorm)
	if deCall == "" {
		deCall = strings.TrimSpace(s.DECall)
	}
	deCallStripped := strings.TrimSpace(s.DECallNormStripped)
	if deCallStripped == "" {
		deCallStripped = strings.TrimSpace(s.DECallStripped)
	}
	mode := strings.TrimSpace(s.ModeNorm)
	if mode == "" {
		mode = strings.TrimSpace(s.Mode)
	}
	mode = strings.ToUpper(mode)
	comment := strings.TrimSpace(s.Comment)
	source := strings.TrimSpace(string(s.SourceType))
	sourceNode := strings.TrimSpace(s.SourceNode)
	confidence := strings.TrimSpace(s.Confidence)
	band := strings.TrimSpace(s.BandNorm)
	if band == "" {
		band = strings.TrimSpace(s.Band)
	}
	dxGrid := strings.TrimSpace(s.DXGridNorm)
	if dxGrid == "" {
		dxGrid = strings.TrimSpace(s.DXMetadata.Grid)
	}
	deGrid := strings.TrimSpace(s.DEGridNorm)
	if deGrid == "" {
		deGrid = strings.TrimSpace(s.DEMetadata.Grid)
	}
	dxCont := strutil.NormalizeUpper(s.DXMetadata.Continent)
	deCont := strutil.NormalizeUpper(s.DEMetadata.Continent)
	events := spot.EventString(s.Events)
	toxicityStatus := string(spot.NormalizeToxicityStatus(string(s.ToxicityStatus)))
	toxicityCategories := strings.Join(s.ToxicityCategories, ",")
	toxicityModel := strings.TrimSpace(s.ToxicityModel)

	lengths := [fieldCount]int{
		len(dxCall),
		len(deCall),
		len(deCallStripped),
		len(mode),
		len(comment),
		len(source),
		len(sourceNode),
		len(confidence),
		len(band),
		len(dxGrid),
		len(deGrid),
		len(dxCont),
		len(deCont),
		len(events),
		len(toxicityStatus),
		len(toxicityCategories),
		len(toxicityModel),
	}
	total := recordHeaderSize
	for _, l := range lengths {
		total += l
	}
	buf := make([]byte, total)
	buf[0] = recordVersion
	flags := uint8(0)
	if s.HasReport {
		flags |= flagHasReport
	}
	if s.IsHuman {
		flags |= flagIsHuman
	}
	if s.DXMetadata.GridDerived {
		flags |= flagDXGridDerived
	}
	if s.DEMetadata.GridDerived {
		flags |= flagDEGridDerived
	}
	buf[1] = flags
	buf[2] = s.TTL
	buf[3] = 0
	binary.BigEndian.PutUint64(buf[4:], math.Float64bits(s.Frequency))
	binary.BigEndian.PutUint32(buf[12:], uint32(int32(s.Report)))
	binary.BigEndian.PutUint16(buf[16:], numutil.ClampUint16(s.DXMetadata.CQZone))
	binary.BigEndian.PutUint16(buf[18:], numutil.ClampUint16(s.DEMetadata.CQZone))
	binary.BigEndian.PutUint32(buf[20:], uint32(clampInt(s.DXMetadata.ADIF)))
	binary.BigEndian.PutUint32(buf[24:], uint32(clampInt(s.DEMetadata.ADIF)))
	binary.BigEndian.PutUint64(buf[28:], math.Float64bits(s.EffectiveObservedFrequency()))

	offset := recordFixedHeaderSize
	for i := 0; i < fieldCount; i++ {
		binary.BigEndian.PutUint16(buf[offset:], uint16(lengths[i]))
		offset += 2
	}
	writeOffset := recordHeaderSize
	writeString := func(value string) {
		if value == "" {
			return
		}
		copy(buf[writeOffset:], value)
		writeOffset += len(value)
	}
	writeString(dxCall)
	writeString(deCall)
	writeString(deCallStripped)
	writeString(mode)
	writeString(comment)
	writeString(source)
	writeString(sourceNode)
	writeString(confidence)
	writeString(band)
	writeString(dxGrid)
	writeString(deGrid)
	writeString(dxCont)
	writeString(deCont)
	writeString(events)
	writeString(toxicityStatus)
	writeString(toxicityCategories)
	writeString(toxicityModel)
	return buf
}

func decodeRecord(raw []byte) (archiveRecord, error) {
	if len(raw) < recordHeaderSizeV2 {
		return archiveRecord{}, errInvalidRecord
	}
	fixedHeaderSize, headerSize, fieldN, ok := recordLayout(raw[0])
	if !ok || len(raw) < headerSize {
		return archiveRecord{}, errInvalidRecord
	}
	flags := raw[1]
	ttl := raw[2]
	freq := math.Float64frombits(binary.BigEndian.Uint64(raw[4:]))
	report := int32(binary.BigEndian.Uint32(raw[12:]))
	dxCQ := int(binary.BigEndian.Uint16(raw[16:]))
	deCQ := int(binary.BigEndian.Uint16(raw[18:]))
	dxADIF := int(binary.BigEndian.Uint32(raw[20:]))
	deADIF := int(binary.BigEndian.Uint32(raw[24:]))
	observedFreq := freq
	if raw[0] == recordVersionV3 || raw[0] == recordVersionV4 || raw[0] == recordVersion {
		observedFreq = math.Float64frombits(binary.BigEndian.Uint64(raw[28:]))
	}

	offset := fixedHeaderSize
	lengths := [fieldCount]int{}
	for i := 0; i < fieldN; i++ {
		lengths[i] = int(binary.BigEndian.Uint16(raw[offset:]))
		offset += 2
	}
	dataOffset := headerSize
	fields := [fieldCount]string{}
	for i := 0; i < fieldN; i++ {
		l := lengths[i]
		if l == 0 {
			continue
		}
		if dataOffset+l > len(raw) {
			return archiveRecord{}, errInvalidRecord
		}
		fields[i] = string(raw[dataOffset : dataOffset+l])
		dataOffset += l
	}
	if dataOffset != len(raw) {
		return archiveRecord{}, errInvalidRecord
	}
	events := spot.EventMask(0)
	if fieldN > fieldEvents {
		events = spot.ParseEventString(fields[fieldEvents])
	}
	if events == 0 {
		events = spot.ParseSpotEvents(fields[fieldComment])
	}
	toxicityStatus := string(spot.ToxicityUnknown)
	toxicityCategories := ""
	toxicityModel := ""
	if fieldN > fieldToxicityStatus {
		toxicityStatus = string(spot.NormalizeToxicityStatus(fields[fieldToxicityStatus]))
	}
	if fieldN > fieldToxicityCategories {
		toxicityCategories = fields[fieldToxicityCategories]
	}
	if fieldN > fieldToxicityModel {
		toxicityModel = fields[fieldToxicityModel]
	}

	return archiveRecord{
		dxCall:             fields[fieldDXCall],
		deCall:             fields[fieldDECall],
		deCallStripped:     fields[fieldDECallStripped],
		mode:               fields[fieldMode],
		comment:            fields[fieldComment],
		source:             fields[fieldSource],
		sourceNode:         fields[fieldSourceNode],
		confidence:         fields[fieldConfidence],
		band:               fields[fieldBand],
		dxGrid:             fields[fieldDXGrid],
		deGrid:             fields[fieldDEGrid],
		dxGridDerived:      flags&flagDXGridDerived != 0,
		deGridDerived:      flags&flagDEGridDerived != 0,
		dxCont:             fields[fieldDXCont],
		deCont:             fields[fieldDECont],
		events:             events,
		freq:               freq,
		observedFreq:       observedFreq,
		report:             int(report),
		hasReport:          flags&flagHasReport != 0,
		isHuman:            flags&flagIsHuman != 0,
		ttl:                ttl,
		dxCQZone:           dxCQ,
		deCQZone:           deCQ,
		dxADIF:             dxADIF,
		deADIF:             deADIF,
		toxicityStatus:     toxicityStatus,
		toxicityCategories: toxicityCategories,
		toxicityModel:      toxicityModel,
	}, nil
}

func decodeSpot(ts int64, raw []byte) (*spot.Spot, error) {
	rec, err := decodeRecord(raw)
	if err != nil {
		return nil, err
	}
	band := strings.TrimSpace(rec.band)
	if band == "" {
		band = spot.FreqToBand(rec.freq)
	}
	s := &spot.Spot{
		DXCall:             rec.dxCall,
		DECall:             rec.deCall,
		DECallStripped:     rec.deCallStripped,
		Frequency:          rec.freq,
		ObservedFrequency:  rec.observedFreq,
		Mode:               rec.mode,
		Report:             rec.report,
		Time:               time.Unix(0, ts).UTC(),
		Comment:            rec.comment,
		Events:             rec.events,
		SourceType:         spot.SourceType(rec.source),
		SourceNode:         rec.sourceNode,
		TTL:                rec.ttl,
		IsHuman:            rec.isHuman,
		HasReport:          rec.hasReport,
		Confidence:         rec.confidence,
		Band:               band,
		ToxicityStatus:     spot.NormalizeToxicityStatus(rec.toxicityStatus),
		ToxicityCategories: splitArchiveCSV(rec.toxicityCategories),
		ToxicityModel:      rec.toxicityModel,
	}
	if rec.deCallStripped != "" {
		s.DECallNormStripped = rec.deCallStripped
	}
	s.DXMetadata = spot.CallMetadata{
		Grid:        rec.dxGrid,
		GridDerived: rec.dxGridDerived,
		Continent:   rec.dxCont,
		CQZone:      rec.dxCQZone,
		ADIF:        rec.dxADIF,
	}
	s.DEMetadata = spot.CallMetadata{
		Grid:        rec.deGrid,
		GridDerived: rec.deGridDerived,
		Continent:   rec.deCont,
		CQZone:      rec.deCQZone,
		ADIF:        rec.deADIF,
	}
	s.EnsureNormalized()
	s.RefreshBeaconFlag()
	return s, nil
}

func splitArchiveCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func spotKeyBytes(ts int64, seq uint32) []byte {
	buf := make([]byte, spotKeyLen)
	copy(buf, spotPrefix)
	binary.BigEndian.PutUint64(buf[len(spotPrefix):], uint64(ts))
	binary.BigEndian.PutUint32(buf[len(spotPrefix)+8:], seq)
	return buf
}

func parseSpotKey(key []byte) (int64, bool) {
	if len(key) != spotKeyLen || !bytes.HasPrefix(key, spotPrefixBytes) {
		return 0, false
	}
	ts := int64(binary.BigEndian.Uint64(key[len(spotPrefix):]))
	return ts, true
}

func normalizeUnixNano(t time.Time) int64 {
	if t.IsZero() {
		return time.Now().UTC().UnixNano()
	}
	ts := t.UTC().UnixNano()
	if ts < 0 {
		return 0
	}
	return ts
}

func clampInt(value int) int {
	if value < 0 {
		return 0
	}
	return value
}
