// Package gridstore persists known calls and grids in a Pebble key/value store,
// providing durable lookup and aggregation for grid statistics and TTL purges.
package gridstore

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

const (
	recordVersion       = 2
	recordHeaderSize    = 48
	maintenanceBatchCap = 1024
)

const (
	recordFlagKnown       = 1 << 0
	recordFlagCTYValid    = 1 << 1
	recordFlagGridDerived = 1 << 2
)

const (
	callPrefix    = "c|"
	updatedPrefix = "u|"
	metaCountKey  = "meta|count"
)

const expiresNone = int64(-1)

var (
	errStoreClosed   = errors.New("gridstore: store is closed")
	errInvalidCount  = errors.New("gridstore: invalid count metadata")
	errInvalidRecord = errors.New("gridstore: invalid record encoding")
)

const (
	defaultCacheSizeBytes        = int64(64 << 20)  // 64MB shared block cache for hot reads
	defaultBloomFilterBits       = 10               // Bits per key for bloom filters on SSTables
	defaultMemTableSizeBytes     = uint64(32 << 20) // 32MB write buffer for burst tolerance
	defaultL0CompactionThreshold = 4                // Keep compactions reactive for low write amp
	defaultL0StopWritesThreshold = 16               // Higher stop threshold to absorb short spikes
	defaultWriteQueueDepth       = 64               // Buffered channel depth feeding the single writer
)

// Options controls Pebble tuning and writer buffering for the grid store.
// All zero/negative fields are replaced with safe defaults via sanitizeOptions.
type Options struct {
	CacheSizeBytes        int64
	BloomFilterBitsPerKey int
	MemTableSizeBytes     uint64
	L0CompactionThreshold int
	L0StopWritesThreshold int
	WriteQueueDepth       int
}

// Record represents a single callsign entry with optional grid and known-call flag.
type Record struct {
	Call         string
	IsKnown      bool
	Grid         sql.NullString
	GridDerived  bool
	CTYValid     bool
	CTYADIF      int
	CTYCQZone    int
	CTYITUZone   int
	CTYContinent string
	CTYCountry   string
	Observations int
	FirstSeen    time.Time
	UpdatedAt    time.Time
	ExpiresAt    *time.Time
}

type recordValue struct {
	isKnown      bool
	grid         string
	gridValid    bool
	gridDerived  bool
	ctyValid     bool
	ctyADIF      uint32
	ctyCQZone    uint16
	ctyITUZone   uint16
	ctyContinent string
	ctyCountry   string
	observations uint64
	firstSeen    int64
	updatedAt    int64
	expiresAt    int64
}

// Store manages the Pebble database that holds call metadata.
type Store struct {
	db     *pebble.DB
	writes chan writeRequest
	done   chan struct{}
	cache  *pebble.Cache // owned cache for the DB; unref'd on Close

	mu     sync.Mutex
	closed bool
	count  atomic.Int64
}

type writeKind int

const (
	writeUpsertBatch writeKind = iota
	writeClearKnown
	writePurge
)

type writeRequest struct {
	kind   writeKind
	recs   []Record
	cutoff time.Time
	resp   chan writeResult
}

type writeResult struct {
	removed int64
	err     error
}

func sanitizeOptions(opts Options) Options {
	if opts.CacheSizeBytes <= 0 {
		opts.CacheSizeBytes = defaultCacheSizeBytes
	}
	if opts.BloomFilterBitsPerKey <= 0 {
		opts.BloomFilterBitsPerKey = defaultBloomFilterBits
	}
	if opts.MemTableSizeBytes <= 0 {
		opts.MemTableSizeBytes = defaultMemTableSizeBytes
	}
	if opts.L0CompactionThreshold <= 0 {
		opts.L0CompactionThreshold = defaultL0CompactionThreshold
	}
	if opts.L0StopWritesThreshold <= opts.L0CompactionThreshold {
		opts.L0StopWritesThreshold = defaultL0StopWritesThreshold
		if opts.L0StopWritesThreshold <= opts.L0CompactionThreshold {
			opts.L0StopWritesThreshold = opts.L0CompactionThreshold + 4
		}
	}
	if opts.WriteQueueDepth <= 0 {
		opts.WriteQueueDepth = defaultWriteQueueDepth
	}
	return opts
}

// Entries loads all call records from the store.
// Key aspects: Reads full table; returns a slice of Record values.
// Upstream: Administrative tools or diagnostics.
// Downstream: Pebble iterator.
func (s *Store) Entries() ([]Record, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("gridstore: store is not initialized")
	}
	iter, err := s.db.NewIter(iterOptionsForPrefix(callPrefix))
	if err != nil {
		return nil, fmt.Errorf("gridstore: entries iterator: %w", err)
	}
	defer iter.Close()

	var list []Record
	for iter.First(); iter.Valid(); iter.Next() {
		call, ok := parseCallKey(iter.Key())
		if !ok {
			continue
		}
		val, err := decodeRecordValue(iter.Value())
		if err != nil {
			return nil, fmt.Errorf("gridstore: decode entry: %w", err)
		}
		list = append(list, recordValueToRecord(call, val))
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("gridstore: iterate entries: %w", err)
	}
	return list, nil
}

// Open opens or creates the gridstore Pebble database.
// Key aspects: Initializes metadata and spins a single writer goroutine.
// Upstream: main.go startup and tools.
// Downstream: Pebble open, writer loop.
func Open(path string, opts Options) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("gridstore: database path is empty")
	}
	opts = sanitizeOptions(opts)

	if info, err := os.Stat(path); err == nil {
		if !info.IsDir() {
			return nil, fmt.Errorf("gridstore: %s exists and is not a directory", path)
		}
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("gridstore: stat path: %w", err)
	}

	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, fmt.Errorf("gridstore: ensure directory: %w", err)
	}

	pebbleOpts := &pebble.Options{
		Cache:                 nil,
		MemTableSize:          opts.MemTableSizeBytes,
		L0CompactionThreshold: opts.L0CompactionThreshold,
		L0StopWritesThreshold: opts.L0StopWritesThreshold,
	}
	if opts.CacheSizeBytes > 0 {
		pebbleOpts.Cache = pebble.NewCache(opts.CacheSizeBytes)
	}
	if opts.BloomFilterBitsPerKey > 0 {
		filter := bloom.FilterPolicy(opts.BloomFilterBitsPerKey)
		level := pebble.LevelOptions{
			FilterPolicy: filter,
			FilterType:   pebble.TableFilter,
		}
		// Apply the same table filter policy to all default levels (Pebble defaults to 7).
		pebbleOpts.Levels = make([]pebble.LevelOptions, 7)
		for i := range pebbleOpts.Levels {
			pebbleOpts.Levels[i] = level
		}
	}

	db, err := pebble.Open(path, pebbleOpts)
	if err != nil {
		if pebbleOpts.Cache != nil {
			pebbleOpts.Cache.Unref()
		}
		return nil, fmt.Errorf("gridstore: open: %w", err)
	}

	count, err := loadCount(db)
	if err != nil {
		_ = db.Close()
		if pebbleOpts.Cache != nil {
			pebbleOpts.Cache.Unref()
		}
		return nil, err
	}

	store := &Store{
		db:     db,
		writes: make(chan writeRequest, opts.WriteQueueDepth),
		done:   make(chan struct{}),
		cache:  pebbleOpts.Cache,
	}
	store.count.Store(count)
	go store.writeLoop()
	return store, nil
}

// Close closes the underlying database handle.
// Key aspects: Drains writer goroutine before closing Pebble.
// Upstream: main.go shutdown or tests.
// Downstream: writer loop, db.Close.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	if s.closeWriter() {
		<-s.done
	}
	err := s.db.Close()
	if s.cache != nil {
		s.cache.Unref()
		s.cache = nil
	}
	return err
}

// PurgeOlderThan deletes records older than the cutoff time.
// Key aspects: Uses updated_at index; returns rows removed.
// Upstream: Periodic maintenance in main pipeline.
// Downstream: Pebble deletes.
func (s *Store) PurgeOlderThan(cutoff time.Time) (int64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("gridstore: store is not initialized")
	}
	resp := make(chan writeResult, 1)
	req := writeRequest{kind: writePurge, cutoff: cutoff, resp: resp}
	if err := s.enqueue(req); err != nil {
		return 0, err
	}
	result := <-resp
	return result.removed, result.err
}

// ClearKnownFlags clears the known-call flag for all entries.
// Key aspects: Bulk update across the calls table.
// Upstream: Admin reset flows or tests.
// Downstream: writer loop.
func (s *Store) ClearKnownFlags() error {
	if s == nil || s.db == nil {
		return errors.New("gridstore: store is not initialized")
	}
	resp := make(chan writeResult, 1)
	req := writeRequest{kind: writeClearKnown, resp: resp}
	if err := s.enqueue(req); err != nil {
		return err
	}
	result := <-resp
	return result.err
}

// Upsert inserts or updates a call record atomically.
// Key aspects: Normalizes callsign; uses single writer goroutine.
// Upstream: Spot processing and enrichment.
// Downstream: UpsertBatch.
func (s *Store) Upsert(rec Record) error {
	return s.UpsertBatch([]Record{rec})
}

// UpsertBatch inserts or updates multiple records in a single batch.
// Key aspects: Serializes writes through a single goroutine and Syncs to disk.
// Upstream: Batch updates from spot pipelines or tools.
// Downstream: writer loop.
func (s *Store) UpsertBatch(recs []Record) error {
	if s == nil || s.db == nil {
		return errors.New("gridstore: store is not initialized")
	}
	if len(recs) == 0 {
		return nil
	}
	resp := make(chan writeResult, 1)
	req := writeRequest{kind: writeUpsertBatch, recs: recs, resp: resp}
	if err := s.enqueue(req); err != nil {
		return err
	}
	result := <-resp
	return result.err
}

// Get fetches a record by callsign.
// Key aspects: Returns (nil, nil) when not found; normalizes callsign.
// Upstream: Spot enrichment queries.
// Downstream: Pebble get.
func (s *Store) Get(call string) (*Record, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("gridstore: store is not initialized")
	}
	call = normalizeCall(call)
	if call == "" {
		return nil, errors.New("gridstore: call is empty")
	}
	value, closer, err := s.db.Get(callKeyBytes(call))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("gridstore: get %s: %w", call, err)
	}
	defer closer.Close()
	val, err := decodeRecordValue(value)
	if err != nil {
		return nil, fmt.Errorf("gridstore: decode %s: %w", call, err)
	}
	rec := recordValueToRecord(call, val)
	return &rec, nil
}

// Count returns the total number of stored calls.
// Key aspects: Uses cached count maintained by the writer.
// Upstream: Metrics/diagnostics.
// Downstream: atomic count.
func (s *Store) Count() (int64, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("gridstore: store is not initialized")
	}
	return s.count.Load(), nil
}

// IsBusyError retry logic for legacy callers; Pebble has no SQLITE_BUSY analog.
// Key aspects: Always returns false to indicate no busy retry.
// Upstream: startGridWriter retry guards.
// Downstream: None.
func IsBusyError(err error) bool {
	return false
}

func (s *Store) enqueue(req writeRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errStoreClosed
	}
	s.writes <- req
	return nil
}

func (s *Store) closeWriter() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.closed = true
	close(s.writes)
	return true
}

func (s *Store) writeLoop() {
	defer close(s.done)
	for req := range s.writes {
		result := writeResult{}
		switch req.kind {
		case writeUpsertBatch:
			result.err = s.applyUpsertBatch(req.recs)
		case writeClearKnown:
			result.err = s.applyClearKnownFlags()
		case writePurge:
			result.removed, result.err = s.applyPurgeOlderThan(req.cutoff)
		default:
			result.err = fmt.Errorf("gridstore: unknown write request")
		}
		if req.resp != nil {
			req.resp <- result
		}
	}
}

// applyUpsertBatch merges incoming records with existing data and commits a
// single Pebble batch with Sync for durability.
func (s *Store) applyUpsertBatch(recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	batch := s.db.NewBatch()
	defer batch.Close()

	now := time.Now().UTC()
	count := s.count.Load()
	countDelta := int64(0)

	for i := range recs {
		rec := recs[i]
		call := normalizeCall(rec.Call)
		if call == "" {
			continue
		}
		incoming := normalizeRecordValue(rec, now)
		existing, found, err := s.getRecordValue(call)
		if err != nil {
			return err
		}
		merged := mergeRecordValue(existing, found, incoming)

		if err := batch.Set(callKeyBytes(call), encodeRecordValue(merged), nil); err != nil {
			return fmt.Errorf("gridstore: batch set %s: %w", call, err)
		}

		if !found {
			countDelta++
		}

		if found && existing.updatedAt != 0 && existing.updatedAt != merged.updatedAt {
			if err := batch.Delete(updatedKeyBytes(existing.updatedAt, call), nil); err != nil {
				return fmt.Errorf("gridstore: batch delete idx %s: %w", call, err)
			}
		}

		if !found || existing.updatedAt != merged.updatedAt {
			if err := batch.Set(updatedKeyBytes(merged.updatedAt, call), nil, nil); err != nil {
				return fmt.Errorf("gridstore: batch set idx %s: %w", call, err)
			}
		}
	}

	if countDelta != 0 {
		count += countDelta
		if count < 0 {
			count = 0
		}
		if err := batch.Set([]byte(metaCountKey), encodeCount(count), nil); err != nil {
			return fmt.Errorf("gridstore: batch set count: %w", err)
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("gridstore: batch commit: %w", err)
	}

	if countDelta != 0 {
		s.count.Store(count)
	}
	return nil
}

// applyClearKnownFlags walks all records and clears known-call flags in batches.
func (s *Store) applyClearKnownFlags() error {
	iter, err := s.db.NewIter(iterOptionsForPrefix(callPrefix))
	if err != nil {
		return fmt.Errorf("gridstore: clear known iterator: %w", err)
	}
	defer iter.Close()

	batch := s.db.NewBatch()
	defer batch.Close()

	pending := 0
	for iter.First(); iter.Valid(); iter.Next() {
		call, ok := parseCallKey(iter.Key())
		if !ok {
			continue
		}
		val, err := decodeRecordValue(iter.Value())
		if err != nil {
			return err
		}
		if !val.isKnown {
			continue
		}
		val.isKnown = false
		if err := batch.Set(callKeyBytes(call), encodeRecordValue(val), nil); err != nil {
			return fmt.Errorf("gridstore: clear known %s: %w", call, err)
		}
		pending++
		if pending >= maintenanceBatchCap {
			if err := batch.Commit(pebble.Sync); err != nil {
				return fmt.Errorf("gridstore: clear known commit: %w", err)
			}
			batch.Reset()
			pending = 0
		}
	}
	if err := iter.Error(); err != nil {
		return fmt.Errorf("gridstore: clear known iterate: %w", err)
	}
	if pending > 0 {
		if err := batch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("gridstore: clear known commit: %w", err)
		}
	}
	return nil
}

// applyPurgeOlderThan deletes records older than the cutoff using the updated-at index.
func (s *Store) applyPurgeOlderThan(cutoff time.Time) (int64, error) {
	if cutoff.IsZero() {
		return 0, nil
	}
	cutoffUnix := cutoff.UTC().Unix()
	if cutoffUnix <= 0 {
		return 0, nil
	}
	upper := updatedUpperBound(cutoffUnix)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(updatedPrefix),
		UpperBound: upper,
	})
	if err != nil {
		return 0, fmt.Errorf("gridstore: purge iterator: %w", err)
	}
	defer iter.Close()

	batch := s.db.NewBatch()
	defer batch.Close()

	count := s.count.Load()
	pending := int64(0)
	removedTotal := int64(0)

	commitBatch := func() error {
		if pending == 0 {
			return nil
		}
		count -= pending
		if count < 0 {
			count = 0
		}
		if err := batch.Set([]byte(metaCountKey), encodeCount(count), nil); err != nil {
			return fmt.Errorf("gridstore: purge set count: %w", err)
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return fmt.Errorf("gridstore: purge commit: %w", err)
		}
		batch.Reset()
		removedTotal += pending
		pending = 0
		return nil
	}

	for iter.First(); iter.Valid(); iter.Next() {
		ts, call, ok := parseUpdatedKey(iter.Key())
		if !ok {
			continue
		}
		if ts > cutoffUnix {
			break
		}
		if err := batch.Delete(iter.Key(), nil); err != nil {
			return removedTotal, fmt.Errorf("gridstore: purge delete idx %s: %w", call, err)
		}
		if err := batch.Delete(callKeyBytes(call), nil); err != nil {
			return removedTotal, fmt.Errorf("gridstore: purge delete %s: %w", call, err)
		}
		pending++
		if pending >= maintenanceBatchCap {
			if err := commitBatch(); err != nil {
				return removedTotal, err
			}
		}
	}
	if err := iter.Error(); err != nil {
		return removedTotal, fmt.Errorf("gridstore: purge iterate: %w", err)
	}
	if err := commitBatch(); err != nil {
		return removedTotal, err
	}
	s.count.Store(count)
	return removedTotal, nil
}

func (s *Store) getRecordValue(call string) (recordValue, bool, error) {
	value, closer, err := s.db.Get(callKeyBytes(call))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return recordValue{}, false, nil
		}
		return recordValue{}, false, fmt.Errorf("gridstore: get %s: %w", call, err)
	}
	defer closer.Close()
	val, err := decodeRecordValue(value)
	if err != nil {
		return recordValue{}, false, fmt.Errorf("gridstore: decode %s: %w", call, err)
	}
	return val, true, nil
}

func normalizeRecordValue(rec Record, now time.Time) recordValue {
	updatedAt := rec.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = now
	}
	firstSeen := rec.FirstSeen
	if firstSeen.IsZero() {
		firstSeen = updatedAt
	}
	expires := expiresNone
	if rec.ExpiresAt != nil {
		expires = rec.ExpiresAt.UTC().Unix()
	}
	grid, gridValid := normalizeGrid(rec.Grid)
	gridDerived := rec.GridDerived && gridValid
	ctyContinent, ctyCountry, ctyValid := normalizeCTY(rec)
	ctyADIF := clampUint32(rec.CTYADIF)
	ctyCQZone := clampUint16(rec.CTYCQZone)
	ctyITUZone := clampUint16(rec.CTYITUZone)
	if !ctyValid {
		ctyADIF = 0
		ctyCQZone = 0
		ctyITUZone = 0
		ctyContinent = ""
		ctyCountry = ""
	}
	obs := rec.Observations
	if obs < 0 {
		obs = 0
	}
	return recordValue{
		isKnown:      rec.IsKnown,
		grid:         grid,
		gridValid:    gridValid,
		gridDerived:  gridDerived,
		ctyValid:     ctyValid,
		ctyADIF:      ctyADIF,
		ctyCQZone:    ctyCQZone,
		ctyITUZone:   ctyITUZone,
		ctyContinent: ctyContinent,
		ctyCountry:   ctyCountry,
		observations: uint64(obs),
		firstSeen:    firstSeen.UTC().Unix(),
		updatedAt:    updatedAt.UTC().Unix(),
		expiresAt:    expires,
	}
}

func mergeRecordValue(existing recordValue, found bool, incoming recordValue) recordValue {
	if !found {
		return incoming
	}
	merged := incoming
	merged.isKnown = existing.isKnown || incoming.isKnown
	if !incoming.gridValid {
		merged.grid = existing.grid
		merged.gridValid = existing.gridValid
		merged.gridDerived = existing.gridDerived
	} else if incoming.gridDerived && existing.gridValid && !existing.gridDerived {
		merged.grid = existing.grid
		merged.gridValid = true
		merged.gridDerived = false
	}
	if !incoming.ctyValid {
		merged.ctyValid = existing.ctyValid
		merged.ctyADIF = existing.ctyADIF
		merged.ctyCQZone = existing.ctyCQZone
		merged.ctyITUZone = existing.ctyITUZone
		merged.ctyContinent = existing.ctyContinent
		merged.ctyCountry = existing.ctyCountry
	}
	merged.observations = existing.observations + incoming.observations
	if existing.firstSeen != 0 && (merged.firstSeen == 0 || existing.firstSeen < merged.firstSeen) {
		merged.firstSeen = existing.firstSeen
	}
	if existing.updatedAt > merged.updatedAt {
		merged.updatedAt = existing.updatedAt
	}
	if merged.expiresAt == expiresNone {
		merged.expiresAt = existing.expiresAt
	}
	return merged
}

func recordValueToRecord(call string, val recordValue) Record {
	var grid sql.NullString
	if val.gridValid && strings.TrimSpace(val.grid) != "" {
		grid = sql.NullString{String: val.grid, Valid: true}
	}
	var expires *time.Time
	if val.expiresAt != expiresNone {
		t := time.Unix(val.expiresAt, 0).UTC()
		expires = &t
	}
	ctyContinent := ""
	ctyCountry := ""
	ctyADIF := 0
	ctyCQZone := 0
	ctyITUZone := 0
	if val.ctyValid {
		ctyContinent = val.ctyContinent
		ctyCountry = val.ctyCountry
		ctyADIF = int(val.ctyADIF)
		ctyCQZone = int(val.ctyCQZone)
		ctyITUZone = int(val.ctyITUZone)
	}
	return Record{
		Call:         call,
		IsKnown:      val.isKnown,
		Grid:         grid,
		GridDerived:  val.gridDerived && val.gridValid,
		CTYValid:     val.ctyValid,
		CTYADIF:      ctyADIF,
		CTYCQZone:    ctyCQZone,
		CTYITUZone:   ctyITUZone,
		CTYContinent: ctyContinent,
		CTYCountry:   ctyCountry,
		Observations: int(val.observations),
		FirstSeen:    time.Unix(val.firstSeen, 0).UTC(),
		UpdatedAt:    time.Unix(val.updatedAt, 0).UTC(),
		ExpiresAt:    expires,
	}
}

func encodeRecordValue(val recordValue) []byte {
	grid := ""
	if val.gridValid {
		grid = val.grid
	}
	ctyContinent := ""
	ctyCountry := ""
	if val.ctyValid {
		ctyContinent = val.ctyContinent
		ctyCountry = val.ctyCountry
	}
	gridLen := len(grid)
	contLen := len(ctyContinent)
	countryLen := len(ctyCountry)
	buf := make([]byte, recordHeaderSize+gridLen+contLen+countryLen)
	buf[0] = recordVersion
	flags := byte(0)
	if val.isKnown {
		flags |= recordFlagKnown
	}
	if val.ctyValid {
		flags |= recordFlagCTYValid
	}
	if val.gridValid && val.gridDerived {
		flags |= recordFlagGridDerived
	}
	buf[1] = flags
	binary.BigEndian.PutUint64(buf[2:], val.observations)
	binary.BigEndian.PutUint64(buf[10:], uint64(val.firstSeen))
	binary.BigEndian.PutUint64(buf[18:], uint64(val.updatedAt))
	expires := uint64(^uint64(0))
	if val.expiresAt != expiresNone {
		expires = uint64(val.expiresAt)
	}
	binary.BigEndian.PutUint64(buf[26:], expires)
	binary.BigEndian.PutUint32(buf[34:], val.ctyADIF)
	binary.BigEndian.PutUint16(buf[38:], val.ctyCQZone)
	binary.BigEndian.PutUint16(buf[40:], val.ctyITUZone)
	binary.BigEndian.PutUint16(buf[42:], uint16(gridLen))
	binary.BigEndian.PutUint16(buf[44:], uint16(contLen))
	binary.BigEndian.PutUint16(buf[46:], uint16(countryLen))
	offset := recordHeaderSize
	copy(buf[offset:], grid)
	offset += gridLen
	copy(buf[offset:], ctyContinent)
	offset += contLen
	copy(buf[offset:], ctyCountry)
	return buf
}

func decodeRecordValue(raw []byte) (recordValue, error) {
	if len(raw) < recordHeaderSize {
		return recordValue{}, errInvalidRecord
	}
	if raw[0] != recordVersion {
		return recordValue{}, errInvalidRecord
	}
	flags := raw[1]
	isKnown := (flags & recordFlagKnown) != 0
	ctyValid := (flags & recordFlagCTYValid) != 0
	gridDerived := (flags & recordFlagGridDerived) != 0
	observations := binary.BigEndian.Uint64(raw[2:])
	firstSeen := int64(binary.BigEndian.Uint64(raw[10:]))
	updatedAt := int64(binary.BigEndian.Uint64(raw[18:]))
	expiresRaw := binary.BigEndian.Uint64(raw[26:])
	ctyADIF := binary.BigEndian.Uint32(raw[34:])
	ctyCQZone := binary.BigEndian.Uint16(raw[38:])
	ctyITUZone := binary.BigEndian.Uint16(raw[40:])
	gridLen := int(binary.BigEndian.Uint16(raw[42:]))
	contLen := int(binary.BigEndian.Uint16(raw[44:]))
	countryLen := int(binary.BigEndian.Uint16(raw[46:]))
	if recordHeaderSize+gridLen+contLen+countryLen > len(raw) {
		return recordValue{}, errInvalidRecord
	}
	grid := ""
	gridValid := false
	ctyContinent := ""
	ctyCountry := ""
	offset := recordHeaderSize
	if gridLen > 0 {
		grid = string(raw[offset : offset+gridLen])
		gridValid = true
		offset += gridLen
	} else {
		offset += gridLen
	}
	if contLen > 0 {
		ctyContinent = string(raw[offset : offset+contLen])
	}
	offset += contLen
	if countryLen > 0 {
		ctyCountry = string(raw[offset : offset+countryLen])
	}
	expires := expiresNone
	if expiresRaw != ^uint64(0) {
		expires = int64(expiresRaw)
	}
	if !ctyValid {
		ctyADIF = 0
		ctyCQZone = 0
		ctyITUZone = 0
		ctyContinent = ""
		ctyCountry = ""
	}
	return recordValue{
		isKnown:      isKnown,
		grid:         grid,
		gridValid:    gridValid,
		gridDerived:  gridDerived && gridValid,
		ctyValid:     ctyValid,
		ctyADIF:      ctyADIF,
		ctyCQZone:    ctyCQZone,
		ctyITUZone:   ctyITUZone,
		ctyContinent: ctyContinent,
		ctyCountry:   ctyCountry,
		observations: observations,
		firstSeen:    firstSeen,
		updatedAt:    updatedAt,
		expiresAt:    expires,
	}, nil
}

func encodeCount(count int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(count))
	return buf
}

func loadCount(db *pebble.DB) (int64, error) {
	count, err := readCountMeta(db)
	if err == nil {
		return count, nil
	}
	if !errors.Is(err, pebble.ErrNotFound) && !errors.Is(err, errInvalidCount) {
		return 0, fmt.Errorf("gridstore: read count: %w", err)
	}
	count, err = computeCount(db)
	if err != nil {
		return 0, err
	}
	if err := db.Set([]byte(metaCountKey), encodeCount(count), pebble.Sync); err != nil {
		return 0, fmt.Errorf("gridstore: write count: %w", err)
	}
	return count, nil
}

func readCountMeta(db *pebble.DB) (int64, error) {
	value, closer, err := db.Get([]byte(metaCountKey))
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(value) != 8 {
		return 0, errInvalidCount
	}
	return int64(binary.BigEndian.Uint64(value)), nil
}

func computeCount(db *pebble.DB) (int64, error) {
	iter, err := db.NewIter(iterOptionsForPrefix(callPrefix))
	if err != nil {
		return 0, fmt.Errorf("gridstore: count iterator: %w", err)
	}
	defer iter.Close()
	count := int64(0)
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	if err := iter.Error(); err != nil {
		return 0, fmt.Errorf("gridstore: count iterate: %w", err)
	}
	return count, nil
}

func normalizeCall(call string) string {
	return strings.ToUpper(strings.TrimSpace(call))
}

func normalizeGrid(grid sql.NullString) (string, bool) {
	if !grid.Valid {
		return "", false
	}
	value := strings.ToUpper(strings.TrimSpace(grid.String))
	if value == "" {
		return "", false
	}
	return value, true
}

func normalizeCTY(rec Record) (string, string, bool) {
	continent := strings.ToUpper(strings.TrimSpace(rec.CTYContinent))
	country := strings.TrimSpace(rec.CTYCountry)
	ctyValid := rec.CTYValid
	if !ctyValid && (rec.CTYADIF > 0 || rec.CTYCQZone > 0 || rec.CTYITUZone > 0 || continent != "" || country != "") {
		ctyValid = true
	}
	if !ctyValid {
		return "", "", false
	}
	return continent, country, true
}

func clampUint16(v int) uint16 {
	if v <= 0 {
		return 0
	}
	max := int(^uint16(0))
	if v > max {
		return ^uint16(0)
	}
	return uint16(v)
}

func clampUint32(v int) uint32 {
	if v <= 0 {
		return 0
	}
	max := int(^uint32(0))
	if v > max {
		return ^uint32(0)
	}
	return uint32(v)
}

func callKeyBytes(call string) []byte {
	return append([]byte(callPrefix), call...)
}

func parseCallKey(key []byte) (string, bool) {
	prefix := []byte(callPrefix)
	if len(key) <= len(prefix) || !bytes.HasPrefix(key, prefix) {
		return "", false
	}
	return string(key[len(prefix):]), true
}

func updatedKeyBytes(updatedAt int64, call string) []byte {
	buf := make([]byte, len(updatedPrefix)+8+len(call))
	copy(buf, updatedPrefix)
	binary.BigEndian.PutUint64(buf[len(updatedPrefix):], uint64(updatedAt))
	copy(buf[len(updatedPrefix)+8:], call)
	return buf
}

func parseUpdatedKey(key []byte) (int64, string, bool) {
	prefix := []byte(updatedPrefix)
	if len(key) <= len(prefix)+8 {
		return 0, "", false
	}
	if !bytes.HasPrefix(key, prefix) {
		return 0, "", false
	}
	ts := int64(binary.BigEndian.Uint64(key[len(prefix):]))
	call := string(key[len(prefix)+8:])
	if call == "" {
		return 0, "", false
	}
	return ts, call, true
}

func updatedUpperBound(cutoffUnix int64) []byte {
	if cutoffUnix == int64(^uint64(0)>>1) {
		return nil
	}
	return updatedKeyBytes(cutoffUnix+1, "")
}

func iterOptionsForPrefix(prefix string) *pebble.IterOptions {
	lower := []byte(prefix)
	upper := prefixUpperBound(lower)
	return &pebble.IterOptions{LowerBound: lower, UpperBound: upper}
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xFF {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}
