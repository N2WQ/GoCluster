// File role: Owns the Pebble-backed Custom SCP retained-evidence store used by
// correction, stabilizer, and S-floor support paths.
// Crawler notes: Start here for Custom SCP admission, recent/static support
// lookup cost, retained-state bounds, persistence convergence, and cleanup
// coupling between entries, static calls, expiry indexes, active counters, and
// interned strings.
// Related docs: docs/decisions/ADR-0080-custom-scp-retained-heap-layout.md,
// docs/decisions/ADR-0143-custom-scp-support-lookup-duplicate-work-removal.md,
// docs/decisions/ADR-0279-custom-scp-cleanup-and-database-ownership.md,
// docs/decisions/ADR-0285-individual-historical-receiver-freshness.md.
// Consumer query requirements: docs/decisions/ADR-0286-consumer-specific-historical-requirements.md.
// Cleanup ownership: spot/custom_scp_cleanup.go. Related tests:
// spot/custom_scp_store_test.go, spot/custom_scp_cleanup_test.go,
// spot/custom_scp_lifecycle_test.go, spot/custom_scp_persistence_bench_test.go.
package spot

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"dxcluster/pathreliability"
	"dxcluster/strutil"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

const (
	customSCPDefaultHorizonDays       = 60
	customSCPDefaultStaticHorizonDays = 395
	customSCPDefaultMaxKeys           = 500000
	customSCPDefaultMaxSpotters       = 64
	customSCPDefaultCleanupInterval   = 10 * time.Minute
	customSCPDefaultCacheSizeBytes    = int64(64 << 20)
	customSCPDefaultBloomBits         = 10
	customSCPDefaultMemTableSize      = uint64(32 << 20)
	customSCPDefaultL0Compaction      = 4
	customSCPDefaultL0StopWrites      = 16
	customSCPDefaultCoreMinScore      = 5
	customSCPDefaultCoreMinH3Cells    = 2
	customSCPDefaultFloorMinScore     = 3
	customSCPDefaultFloorExactCells   = 2
	customSCPDefaultFloorFamilyCells  = 3

	customSCPMetaPrefix = "m|"
	customSCPObsPrefix  = "o|"
)

var (
	customSCPMetaPrefixBytes = []byte(customSCPMetaPrefix)
	customSCPObsPrefixBytes  = []byte(customSCPObsPrefix)
)

const (
	customSCPScoreT0 = 10 // <= 12h
	customSCPScoreT1 = 7  // <= 48h
	customSCPScoreT2 = 5  // <= 7d
	customSCPScoreT3 = 3  // <= 30d
	customSCPScoreT4 = 2  // <= 90d
	customSCPScoreT5 = 1  // <= 180d
	customSCPScoreT6 = 0  // <= 395d
)

const (
	customSCPTierT0 = iota
	customSCPTierT1
	customSCPTierT2
	customSCPTierT3
	customSCPTierT4
	customSCPTierT5
	customSCPTierT6
)

type customSCPKey struct {
	call   string
	band   string
	bucket string
}

type customSCPSpotterObs struct {
	seenUnix int64
	cellRes1 uint16
}

type customSCPEntry struct {
	spotters       []customSCPSpotterEntry
	lastSeen       int64
	oldestSeenUnix int64
}

// CustomSCPOptions configures runtime and persistence behavior. The max/horizon
// fields are the memory contract: runtime evidence is useful only while it
// remains bounded and recent enough to support correction decisions.
type CustomSCPOptions struct {
	Path string

	HorizonDays       int
	StaticHorizonDays int
	MaxKeys           int
	MaxSpottersPerKey int
	CleanupInterval   time.Duration

	CacheSizeBytes        int64
	BloomFilterBitsPerKey int
	MemTableSizeBytes     uint64
	L0CompactionThreshold int
	L0StopWritesThreshold int

	CoreMinScore   int
	CoreMinH3Cells int

	SFloorMinScore         int
	SFloorExactMinH3Cells  int
	SFloorFamilyMinH3Cells int

	MinSNRDBCW   int
	MinSNRDBRTTY int
}

type customSCPSnapshot struct {
	uniqueSpotters int
	uniqueCells    int
	latestSeenUnix int64
	score          int
}

// CustomSCPStatsSnapshot reports bounded custom-SCP cardinality and cleanup
// counters for opt-in diagnostics.
type CustomSCPStatsSnapshot struct {
	StaticCalls                int
	ObservationKeys            int
	ObservationSpotters        int
	InternedStrings            int
	InternedRefs               int
	InternReleaseMisses        uint64
	EntryExpiryItems           int
	StaticExpiryItems          int
	OversizedKeysSeenOnLoad    uint64
	OverflowObservationsPruned uint64
	StaleObservationsPruned    uint64
	StaleStaticPruned          uint64
}

type customSCPDiagnostics struct {
	oversizedKeysSeenOnLoad    uint64
	overflowObservationsPruned uint64
	staleObservationsPruned    uint64
	staleStaticPruned          uint64
}

// CustomSCPStore holds runtime-learned static membership and recent-evidence
// rails for the correction/confidence path. The store owns all maps, expiry
// heaps and interned strings under s.mu; delete paths must
// release secondary indexes and intern refs before an observation key or static
// call leaves the primary maps.
type CustomSCPStore struct {
	mu sync.RWMutex

	opts CustomSCPOptions

	entries      map[customSCPKey]*customSCPEntry
	entryExpiry  customSCPEntryExpiryQueue
	static       map[string]int64
	staticExpiry customSCPStaticExpiryQueue
	active       activeBandCallCounter

	observationSpotters int
	diag                customSCPDiagnostics
	interner            customSCPInterner

	// Lock order is dbMu -> diskMu -> mu. A complete disk operation holds one
	// dbMu reader, including disposal of iterators/batches. diskMu excludes live
	// writes from cleanup's current-value check through its commit; evidence
	// readers need only mu and can continue during cleanup disk I/O.
	dbMu   sync.RWMutex
	diskMu sync.Mutex
	db     *pebble.DB

	cleanupMu     sync.Mutex
	cleanupPassMu sync.Mutex
	quit          chan struct{}
	cleanupDone   chan struct{}
	closing       bool
	closeOnce     sync.Once
	closeErr      error
}

type customSCPInternRef struct {
	value string
	refs  int
}

// customSCPInterner is owned by CustomSCPStore under s.mu. It retains only
// strings that are reachable from current static membership, observation keys,
// or retained spotters, so deleting an owner must release the matching ref.
type customSCPInterner struct {
	refs          map[string]customSCPInternRef
	totalRefs     int
	releaseMisses uint64
}

func (i *customSCPInterner) retain(value string) string {
	if value == "" {
		return value
	}
	if i.refs == nil {
		i.refs = make(map[string]customSCPInternRef, 1024)
	}
	ref, ok := i.refs[value]
	if ok {
		ref.refs++
		i.refs[value] = ref
		i.totalRefs++
		return ref.value
	}
	i.refs[value] = customSCPInternRef{value: value, refs: 1}
	i.totalRefs++
	return value
}

func (i *customSCPInterner) release(value string) {
	if value == "" {
		return
	}
	ref, ok := i.refs[value]
	if !ok || ref.refs <= 0 {
		i.releaseMisses++
		return
	}
	i.totalRefs--
	if ref.refs == 1 {
		delete(i.refs, value)
		return
	}
	ref.refs--
	i.refs[value] = ref
}

func (i *customSCPInterner) canonical(value string) string {
	if value == "" || i.refs == nil {
		return value
	}
	if ref, ok := i.refs[value]; ok {
		return ref.value
	}
	return value
}

func (i *customSCPInterner) distinct() int {
	if i == nil {
		return 0
	}
	return len(i.refs)
}

// OpenCustomSCPStore opens (or creates) the Pebble-backed custom SCP store and
// rebuilds bounded in-memory indexes before the runtime starts using evidence.
// Startup fails rather than running with a partially loaded correction memory.
func OpenCustomSCPStore(opts CustomSCPOptions) (*CustomSCPStore, error) {
	opts = sanitizeCustomSCPOptions(opts)
	if strings.TrimSpace(opts.Path) == "" {
		return nil, errors.New("custom_scp: path is empty")
	}
	if err := os.MkdirAll(opts.Path, 0o755); err != nil {
		return nil, fmt.Errorf("custom_scp: mkdir: %w", err)
	}
	pebbleOpts := &pebble.Options{
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
		pebbleOpts.Levels = make([]pebble.LevelOptions, 7)
		for i := range pebbleOpts.Levels {
			pebbleOpts.Levels[i] = level
		}
	}
	db, err := pebble.Open(opts.Path, pebbleOpts)
	if err != nil {
		if pebbleOpts.Cache != nil {
			pebbleOpts.Cache.Unref()
		}
		return nil, fmt.Errorf("custom_scp: open: %w", err)
	}
	store := &CustomSCPStore{
		opts:         opts,
		entries:      make(map[customSCPKey]*customSCPEntry, 1024),
		entryExpiry:  newCustomSCPEntryExpiryQueue(1024),
		static:       make(map[string]int64, 1024),
		staticExpiry: newCustomSCPStaticExpiryQueue(1024),
		db:           db,
	}
	if err := store.loadFromDB(); err != nil {
		_ = store.Close()
		return nil, err
	}
	return store, nil
}

// Close stops cleanup before closing Pebble so no background cleanup can race
// the durable store during shutdown.
func (s *CustomSCPStore) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.cleanupMu.Lock()
		s.closing = true
		s.stopCleanupLocked()
		s.cleanupMu.Unlock()
		// Join the cleanup owner before waiting for all remaining disk users.
		s.dbMu.Lock()
		defer s.dbMu.Unlock()
		if s.db != nil {
			s.closeErr = s.db.Close()
			s.db = nil
		}
	})
	return s.closeErr
}

// StartCleanup starts periodic in-memory and on-disk stale-data cleanup. The
// cleanup loop is the owner of horizon reclamation between live observations.
func (s *CustomSCPStore) StartCleanup() {
	if s == nil || s.opts.CleanupInterval <= 0 {
		return
	}
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	if s.closing || s.quit != nil {
		return
	}
	s.quit = make(chan struct{})
	s.cleanupDone = make(chan struct{})
	go s.runCleanup(s.quit, s.cleanupDone)
}

// StopCleanup ends the periodic cleanup owner before shutdown or tests inspect
// retained state.
func (s *CustomSCPStore) StopCleanup() {
	if s == nil {
		return
	}
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	s.stopCleanupLocked()
}

// Checkpoint creates a Pebble checkpoint at dest.
func (s *CustomSCPStore) Checkpoint(dest string) error {
	if s == nil {
		return errors.New("custom_scp: store not initialized")
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.db == nil {
		return errors.New("custom_scp: store not initialized")
	}
	if strings.TrimSpace(dest) == "" {
		return errors.New("custom_scp: checkpoint destination is empty")
	}
	if err := s.db.Checkpoint(dest, pebble.WithFlushedWAL()); err != nil {
		return fmt.Errorf("custom_scp: checkpoint: %w", err)
	}
	return nil
}

// Verify performs a bounded full scan over observation and static-membership keys.
func (s *CustomSCPStore) Verify(ctx context.Context, maxDuration time.Duration) (int64, error) {
	if s == nil {
		return 0, errors.New("custom_scp: store not initialized")
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	if s.db == nil {
		return 0, errors.New("custom_scp: store not initialized")
	}
	return verifyCustomSCPDB(ctx, s.db, maxDuration)
}

// VerifyCustomSCPCheckpoint verifies a checkpoint directory by opening it
// read-only and scanning all records.
func VerifyCustomSCPCheckpoint(ctx context.Context, path string, maxDuration time.Duration) (int64, error) {
	if strings.TrimSpace(path) == "" {
		return 0, errors.New("custom_scp: checkpoint path is empty")
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("custom_scp: checkpoint stat %s: %w", path, err)
	}
	if !info.IsDir() {
		return 0, fmt.Errorf("custom_scp: checkpoint %s is not a directory", path)
	}
	db, err := pebble.Open(path, &pebble.Options{ReadOnly: true})
	if err != nil {
		return 0, fmt.Errorf("custom_scp: checkpoint open %s: %w", path, err)
	}
	defer db.Close()
	return verifyCustomSCPDB(ctx, db, maxDuration)
}

// verifyCustomSCPDB intentionally checks only record shape and scan liveness.
// Semantic pruning is handled by loadFromDB/cleanup so checkpoint verification
// can stay bounded and safe for operational use.
func verifyCustomSCPDB(ctx context.Context, db *pebble.DB, maxDuration time.Duration) (int64, error) {
	if db == nil {
		return 0, errors.New("custom_scp: database is nil")
	}
	start := time.Now().UTC()
	deadline := time.Time{}
	if maxDuration > 0 {
		deadline = start.Add(maxDuration)
	}
	iter, err := db.NewIter(nil)
	if err != nil {
		return 0, fmt.Errorf("custom_scp: verify iterator: %w", err)
	}
	defer iter.Close()
	var scanned int64
	for iter.First(); iter.Valid(); iter.Next() {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return scanned, ctx.Err()
			default:
			}
		}
		if !deadline.IsZero() && time.Now().UTC().After(deadline) {
			return scanned, errors.New("custom_scp: integrity scan timed out")
		}
		key := iter.Key()
		if strings.HasPrefix(string(key), customSCPMetaPrefix) {
			if len(iter.Value()) != 8 {
				return scanned, errors.New("custom_scp: invalid static-membership value")
			}
		}
		if strings.HasPrefix(string(key), customSCPObsPrefix) {
			if len(iter.Value()) != 10 {
				return scanned, errors.New("custom_scp: invalid observation value")
			}
		}
		scanned++
	}
	if err := iter.Error(); err != nil {
		return scanned, fmt.Errorf("custom_scp: verify iterate: %w", err)
	}
	return scanned, nil
}

// RecordSpot records one accepted V-confidence spot into custom SCP runtime
// evidence. The V-only gate prevents the learned SCP memory from reinforcing
// uncertain or SCP-promoted outputs.
func (s *CustomSCPStore) RecordSpot(sp *Spot) {
	if s == nil || sp == nil || sp.IsBeacon {
		return
	}
	// Admission is intentionally limited to strongest-confidence output to avoid
	// self-reinforcement loops where SCP-backed S/C outputs feed SCP evidence.
	if strutil.NormalizeUpper(sp.Confidence) != "V" {
		return
	}
	mode := sp.ModeNorm
	if mode == "" {
		mode = sp.Mode
	}
	if _, ok := customSCPBucketForMode(mode); !ok {
		return
	}
	call := sp.DXCallNorm
	if call == "" {
		call = sp.DXCall
	}
	call = NormalizeCallsign(call)
	if call == "" {
		return
	}
	band := sp.BandNorm
	if band == "" || band == "???" {
		band = FreqToBand(sp.Frequency)
	}
	band = NormalizeBand(band)
	if band == "" || band == "???" {
		return
	}
	spotter := sp.DECallNorm
	if spotter == "" {
		spotter = sp.DECall
	}
	spotter = strutil.NormalizeUpper(spotter)
	if spotter == "" {
		return
	}
	seenAt := sp.Time.UTC()
	if seenAt.IsZero() {
		seenAt = time.Now().UTC()
	}
	cell := uint16(0)
	grid := strings.TrimSpace(sp.DEGridNorm)
	if grid == "" {
		grid = strings.TrimSpace(sp.DEMetadata.Grid)
	}
	if grid != "" {
		cell = uint16(pathreliability.EncodeCoarseCell(grid))
	}

	key, baseKey, ok := CorrectionFamilyKeyPair(call)
	if !ok {
		key = call
	}
	// Cover both family keys with one lifetime reader; nested readers can
	// deadlock when Close is waiting for the writer lock between the two keys.
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	s.diskMu.Lock()
	defer s.diskMu.Unlock()
	if key != "" {
		s.recordObservationWithDiskLocked(key, band, mode, spotter, cell, sp.Report, sp.HasReport, seenAt)
	}
	if baseKey != "" {
		s.recordObservationWithDiskLocked(baseKey, band, mode, spotter, cell, sp.Report, sp.HasReport, seenAt)
	}
}

// recordObservation owns the mutation path for one retained observation. It
// couples the primary entries map, static membership, expiry heaps, interner,
// active counters, and Pebble writes so all retained views converge under churn.
func (s *CustomSCPStore) recordObservation(call, band, mode, spotter string, cellRes1 uint16, report int, hasReport bool, seenAt time.Time) {
	if s == nil {
		return
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	s.diskMu.Lock()
	defer s.diskMu.Unlock()
	s.recordObservationWithDiskLocked(call, band, mode, spotter, cellRes1, report, hasReport, seenAt)
}

// Callers hold dbMu and diskMu, including for memory-only stores after Close.
func (s *CustomSCPStore) recordObservationWithDiskLocked(call, band, mode, spotter string, cellRes1 uint16, report int, hasReport bool, seenAt time.Time) {
	call = NormalizeCallsign(call)
	band = NormalizeBand(band)
	bucket, ok := customSCPBucketForMode(mode)
	if !ok || call == "" || band == "" || band == "???" || spotter == "" {
		return
	}
	if !s.snrPasses(bucket, report, hasReport) {
		return
	}
	seenUnix := seenAt.UTC().Unix()
	if seenUnix <= 0 {
		seenUnix = time.Now().UTC().Unix()
	}
	cutoff := s.observationHorizonCutoffUnix(time.Now().UTC())
	if seenUnix < cutoff {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	call, staticChanged := s.retainStaticCallLocked(call, seenUnix)
	if staticChanged {
		s.upsertStaticExpiryLocked(call, s.static[call])
	}

	key := customSCPKey{call: call, band: band, bucket: bucket}
	entry := s.entries[key]
	wasActive := customSCPEntryActive(entry)
	if entry == nil {
		if len(s.entries) >= s.opts.MaxKeys {
			s.evictOldestKeyLocked()
		}
		if len(s.entries) >= s.opts.MaxKeys {
			return
		}
		key = customSCPKey{
			call:   s.retainInternedStringLocked(call),
			band:   s.retainInternedStringLocked(band),
			bucket: s.retainInternedStringLocked(bucket),
		}
		entry = &customSCPEntry{spotters: makeCustomSCPSpotters(s.opts.MaxSpottersPerKey)}
		s.entries[key] = entry
	}
	var removed []string
	s.pruneEntryLocked(entry, cutoff, &removed)
	obs := customSCPSpotterObs{seenUnix: seenUnix, cellRes1: cellRes1}
	persistSpotter := spotter
	updated := false
	idx, exists := findSpotterIndex(entry.spotters, spotter)
	if !exists {
		persistSpotter = s.retainInternedStringLocked(spotter)
		entry.spotters = insertCustomSCPSpotter(
			entry.spotters,
			idx,
			customSCPSpotterEntry{
				spotter:  persistSpotter,
				seenUnix: encodeCustomSCPSpotterSeenUnix(obs.seenUnix),
				cellRes1: obs.cellRes1,
			},
			s.opts.MaxSpottersPerKey,
		)
		s.observationSpotters++
		updated = true
	} else if seenUnix > decodeCustomSCPSpotterSeenUnix(entry.spotters[idx].seenUnix) {
		entry.spotters[idx].seenUnix = encodeCustomSCPSpotterSeenUnix(obs.seenUnix)
		entry.spotters[idx].cellRes1 = obs.cellRes1
		persistSpotter = entry.spotters[idx].spotter
		updated = true
	}
	if seenUnix > entry.lastSeen {
		entry.lastSeen = seenUnix
	}
	if len(entry.spotters) > s.opts.MaxSpottersPerKey {
		overflow := s.trimSpottersLocked(entry, &removed)
		s.diag.overflowObservationsPruned += uint64(overflow)
	}
	if len(entry.spotters) == 0 {
		s.deleteEntryLocked(key)
		s.deleteObservationPrefixLocked(key)
		return
	}
	s.markEntryForCleanupLocked(key, entry)
	if !wasActive {
		s.active.Add(key.call, key.band)
	}
	if staticChanged {
		s.persistStaticLocked(call, s.static[call])
	}
	if obs, ok := entrySpotterObs(entry, persistSpotter); updated && ok {
		s.persistObservationLocked(key, spotter, obs)
	}
	s.deleteObservationSpottersLocked(key, entry, removed, nil)
}

// StaticContains reports whether call is in persisted custom SCP membership.
func (s *CustomSCPStore) StaticContains(call string) bool {
	if s == nil {
		return false
	}
	call = NormalizeCallsign(call)
	if call == "" {
		return false
	}
	s.mu.RLock()
	seen, ok := s.static[call]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	return seen >= s.staticHorizonCutoffUnix(time.Now().UTC())
}

// StaticCallCount returns the number of static custom-SCP calls that are still
// valid within the configured static horizon. It intentionally does not mutate
// or prune retained state; cleanup owns reclamation.
func (s *CustomSCPStore) StaticCallCount(now time.Time) int {
	if s == nil {
		return 0
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cutoff := s.staticHorizonCutoffUnix(now.UTC())
	s.mu.RLock()
	defer s.mu.RUnlock()
	total := 0
	for _, seen := range s.static {
		if seen >= cutoff {
			total++
		}
	}
	return total
}

// StatsSnapshot reports current custom-SCP cardinalities and bounded cleanup
// counters for opt-in diagnostics.
func (s *CustomSCPStore) StatsSnapshot() CustomSCPStatsSnapshot {
	if s == nil {
		return CustomSCPStatsSnapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return CustomSCPStatsSnapshot{
		StaticCalls:                len(s.static),
		ObservationKeys:            len(s.entries),
		ObservationSpotters:        s.observationSpotters,
		InternedStrings:            s.interner.distinct(),
		InternedRefs:               s.interner.totalRefs,
		InternReleaseMisses:        s.interner.releaseMisses,
		EntryExpiryItems:           s.entryExpiry.Len(),
		StaticExpiryItems:          s.staticExpiry.Len(),
		OversizedKeysSeenOnLoad:    s.diag.oversizedKeysSeenOnLoad,
		OverflowObservationsPruned: s.diag.overflowObservationsPruned,
		StaleObservationsPruned:    s.diag.staleObservationsPruned,
		StaleStaticPruned:          s.diag.staleStaticPruned,
	}
}

// HasRecentSupport implements RecentSupportStore for resolver/stabilizer rails.
func (s *CustomSCPStore) HasRecentSupport(call, band, mode string, minUnique int, now time.Time) bool {
	snapshot := s.snapshotFor(call, band, mode, now, s.opts.CoreMinScore)
	if snapshot.score < s.opts.CoreMinScore {
		return false
	}
	if snapshot.uniqueCells < s.opts.CoreMinH3Cells {
		return false
	}
	return snapshot.uniqueSpotters >= normalizeMinUnique(minUnique, s.opts.MaxSpottersPerKey)
}

// RecentSupportCount implements RecentSupportStore for resolver/stabilizer rails.
// Count is returned only when core score/cell gates pass.
func (s *CustomSCPStore) RecentSupportCount(call, band, mode string, now time.Time) int {
	snapshot := s.snapshotFor(call, band, mode, now, s.opts.CoreMinScore)
	if snapshot.score < s.opts.CoreMinScore || snapshot.uniqueCells < s.opts.CoreMinH3Cells {
		return 0
	}
	return snapshot.uniqueSpotters
}

// RecentSupportCountNormalized is the no-cache lookup path for callers that
// already own a correction-normalized call key. Synthetic edit-neighbor probes
// use this to avoid filling the global callsign normalization cache with
// one-off misses; raw external callers must keep using RecentSupportCount.
func (s *CustomSCPStore) RecentSupportCountNormalized(call, band, mode string, now time.Time) int {
	snapshot := s.snapshotForNormalizedCall(call, band, mode, now, s.opts.CoreMinScore)
	if snapshot.score < s.opts.CoreMinScore || snapshot.uniqueCells < s.opts.CoreMinH3Cells {
		return 0
	}
	return snapshot.uniqueSpotters
}

// RecentSupportCountWithRequirements counts eligible historical receivers under
// one consumer's score/cell gates. It preserves partial counts below receiver
// qualification minima for Bayesian prior accounting and diagnostics.
func (s *CustomSCPStore) RecentSupportCountWithRequirements(call, band, mode string, requirements CustomSCPRequirements, now time.Time) int {
	snapshot := s.snapshotFor(call, band, mode, now, requirements.MinScore)
	if snapshot.score < requirements.MinScore || snapshot.uniqueCells < requirements.MinH3Cells {
		return 0
	}
	return snapshot.uniqueSpotters
}

// RecentSupportCountNormalizedWithRequirements avoids normalization-cache entries
// for synthetic probes already owned as correction-normalized call keys.
func (s *CustomSCPStore) RecentSupportCountNormalizedWithRequirements(call, band, mode string, requirements CustomSCPRequirements, now time.Time) int {
	snapshot := s.snapshotForNormalizedCall(call, band, mode, now, requirements.MinScore)
	if snapshot.score < requirements.MinScore || snapshot.uniqueCells < requirements.MinH3Cells {
		return 0
	}
	return snapshot.uniqueSpotters
}

// HasRecentSupportWithRequirements applies the receiver qualification gate after
// score/cell eligibility. Capacity normalization retains its existing contract.
func (s *CustomSCPStore) HasRecentSupportWithRequirements(call, band, mode string, minUnique int, requirements CustomSCPRequirements, now time.Time) bool {
	if s == nil {
		return false
	}
	return s.RecentSupportCountWithRequirements(call, band, mode, requirements, now) >= normalizeMinUnique(minUnique, s.opts.MaxSpottersPerKey)
}

// HasSFloorSupportExact reports exact-call support for S-floor promotion.
func (s *CustomSCPStore) HasSFloorSupportExact(call, band, mode string, minUnique int, now time.Time) bool {
	snapshot := s.snapshotFor(call, band, mode, now, s.opts.SFloorMinScore)
	if snapshot.score < s.opts.SFloorMinScore {
		return false
	}
	if snapshot.uniqueCells < s.opts.SFloorExactMinH3Cells {
		return false
	}
	return snapshot.uniqueSpotters >= normalizeMinUnique(minUnique, s.opts.MaxSpottersPerKey)
}

// HasSFloorSupportFamily reports family-fallback support for S-floor promotion.
func (s *CustomSCPStore) HasSFloorSupportFamily(calls []string, band, mode string, minUnique int, now time.Time) bool {
	if s == nil {
		return false
	}
	best := customSCPSnapshot{}
	for _, call := range calls {
		best = s.bestSFloorFamilySnapshot(best, call, band, mode, now)
	}
	return s.hasSFloorFamilySupport(best, minUnique)
}

// HasSFloorSupportFamilyKeyPair reports family-fallback support without forcing
// hot callers to allocate a temporary key slice for the common single-key case.
func (s *CustomSCPStore) HasSFloorSupportFamilyKeyPair(voteKey, baseKey, band, mode string, minUnique int, now time.Time) bool {
	if s == nil {
		return false
	}
	best := s.bestSFloorFamilySnapshot(customSCPSnapshot{}, voteKey, band, mode, now)
	if baseKey != "" && baseKey != voteKey {
		best = s.bestSFloorFamilySnapshot(best, baseKey, band, mode, now)
	}
	return s.hasSFloorFamilySupport(best, minUnique)
}

func (s *CustomSCPStore) bestSFloorFamilySnapshot(best customSCPSnapshot, call, band, mode string, now time.Time) customSCPSnapshot {
	snap := s.snapshotFor(call, band, mode, now, s.opts.SFloorMinScore)
	if snap.score > best.score || (snap.score == best.score && snap.uniqueSpotters > best.uniqueSpotters) {
		return snap
	}
	return best
}

func (s *CustomSCPStore) hasSFloorFamilySupport(best customSCPSnapshot, minUnique int) bool {
	if best.score < s.opts.SFloorMinScore {
		return false
	}
	if best.uniqueCells < s.opts.SFloorFamilyMinH3Cells {
		return false
	}
	return best.uniqueSpotters >= normalizeMinUnique(minUnique, s.opts.MaxSpottersPerKey)
}

// ActiveCallCount returns the maintained distinct-call snapshot within the
// observation horizon. Read-side queries remain side-effect free; freshness
// advances on writes and cleanup passes.
func (s *CustomSCPStore) ActiveCallCount(_ time.Time) int {
	if s == nil {
		return 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active.Total()
}

// ActiveCallCountsByBand returns the maintained distinct active-call snapshot
// per band.
func (s *CustomSCPStore) ActiveCallCountsByBand(_ time.Time) map[string]int {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active.CountsByBand()
}

func (s *CustomSCPStore) snapshotFor(call, band, mode string, now time.Time, minScore int) customSCPSnapshot {
	if s == nil {
		return customSCPSnapshot{}
	}
	call = NormalizeCallsign(call)
	return s.snapshotForNormalizedCall(call, band, mode, now, minScore)
}

func (s *CustomSCPStore) snapshotForNormalizedCall(call, band, mode string, now time.Time, minScore int) customSCPSnapshot {
	if s == nil {
		return customSCPSnapshot{}
	}
	band = NormalizeBand(band)
	bucket, ok := customSCPBucketForMode(mode)
	if !ok || call == "" || band == "" || band == "???" {
		return customSCPSnapshot{}
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	cutoff := s.observationHorizonCutoffUnix(now.UTC())

	s.mu.Lock()
	defer s.mu.Unlock()
	key := customSCPKey{call: call, band: band, bucket: bucket}
	entry := s.entries[key]
	if entry == nil {
		return customSCPSnapshot{}
	}
	s.pruneEntryLocked(entry, cutoff, nil)
	if len(entry.spotters) == 0 {
		s.deleteEntryLocked(key)
		return customSCPSnapshot{}
	}
	s.markEntryForCleanupLocked(key, entry)
	return eligibleCustomSCPSnapshot(entry, now.UTC().Unix(), minScore)
}

func (s *CustomSCPStore) loadFromDB() error {
	if s == nil || s.db == nil {
		return nil
	}
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return fmt.Errorf("custom_scp: load iterator: %w", err)
	}
	defer iter.Close()
	observationCutoff := s.observationHorizonCutoffUnix(time.Now().UTC())
	staticCutoff := s.staticHorizonCutoffUnix(time.Now().UTC())
	batch := s.db.NewBatch()
	defer batch.Close()
	pendingDeletes := 0
	var (
		pendingKey      customSCPKey
		pendingEntry    *customSCPEntry
		pendingOverflow bool
	)
	flushPending := func() {
		if pendingEntry == nil {
			return
		}
		pendingDeletes += s.trimPendingEntryOnLoadLocked(pendingKey, pendingEntry, &pendingOverflow, batch)
		if len(pendingEntry.spotters) == 0 {
			pendingEntry = nil
			pendingKey = customSCPKey{}
			pendingOverflow = false
			return
		}
		if len(s.entries) >= s.opts.MaxKeys {
			s.evictOldestKeyLocked()
		}
		if len(s.entries) >= s.opts.MaxKeys {
			pendingEntry = nil
			pendingKey = customSCPKey{}
			pendingOverflow = false
			return
		}
		retainedKey := customSCPKey{
			call:   s.retainInternedStringLocked(pendingKey.call),
			band:   s.retainInternedStringLocked(pendingKey.band),
			bucket: s.retainInternedStringLocked(pendingKey.bucket),
		}
		pendingEntry.spotters = compactCustomSCPSpotters(pendingEntry.spotters)
		for i := range pendingEntry.spotters {
			pendingEntry.spotters[i].spotter = s.retainInternedStringLocked(pendingEntry.spotters[i].spotter)
		}
		s.entries[retainedKey] = pendingEntry
		s.active.Add(retainedKey.call, retainedKey.band)
		s.observationSpotters += len(pendingEntry.spotters)
		s.markEntryForCleanupLocked(retainedKey, pendingEntry)
		pendingEntry = nil
		pendingKey = customSCPKey{}
		pendingOverflow = false
	}
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		v := iter.Value()
		switch {
		case bytes.HasPrefix(k, customSCPMetaPrefixBytes):
			if len(v) != 8 {
				ignoreCustomSCPDiskError(batch.Delete(iter.Key(), nil))
				pendingDeletes++
				continue
			}
			call := NormalizeCallsign(string(k[len(customSCPMetaPrefixBytes):]))
			seen := int64(binary.BigEndian.Uint64(v))
			if call == "" {
				continue
			}
			if seen < staticCutoff {
				ignoreCustomSCPDiskError(batch.Delete(iter.Key(), nil))
				s.diag.staleStaticPruned++
				pendingDeletes++
				continue
			}
			call, _ = s.retainStaticCallLocked(call, seen)
			s.upsertStaticExpiryLocked(call, seen)
		case bytes.HasPrefix(k, customSCPObsPrefixBytes):
			call, band, bucket, spotter, ok := parseObservationKeyBytes(k)
			if !ok || len(v) != 10 {
				ignoreCustomSCPDiskError(batch.Delete(iter.Key(), nil))
				pendingDeletes++
				continue
			}
			seen := int64(binary.BigEndian.Uint64(v[:8]))
			if seen < observationCutoff {
				ignoreCustomSCPDiskError(batch.Delete(iter.Key(), nil))
				s.diag.staleObservationsPruned++
				pendingDeletes++
				continue
			}
			key := customSCPKey{call: call, band: band, bucket: bucket}
			if pendingEntry == nil || key != pendingKey {
				flushPending()
				pendingKey = key
				pendingEntry = &customSCPEntry{spotters: makeCustomSCPSpotters(s.opts.MaxSpottersPerKey)}
				pendingOverflow = false
			}
			cell := binary.BigEndian.Uint16(v[8:10])
			entryUpsertSpotter(pendingEntry, spotter, customSCPSpotterObs{seenUnix: seen, cellRes1: cell}, s.opts.MaxSpottersPerKey)
			if seen > pendingEntry.lastSeen {
				pendingEntry.lastSeen = seen
			}
			if len(pendingEntry.spotters) > s.opts.MaxSpottersPerKey {
				pendingDeletes += s.trimPendingEntryOnLoadLocked(key, pendingEntry, &pendingOverflow, batch)
			}
		}
		if pendingDeletes >= 512 {
			ignoreCustomSCPDiskError(batch.Commit(pebble.NoSync))
			batch.Reset()
			pendingDeletes = 0
		}
	}
	flushPending()
	if err := iter.Error(); err != nil {
		return fmt.Errorf("custom_scp: load iterate: %w", err)
	}
	if pendingDeletes > 0 {
		ignoreCustomSCPDiskError(batch.Commit(pebble.NoSync))
	}
	return nil
}

func (s *CustomSCPStore) persistStaticLocked(call string, seenUnix int64) {
	if s == nil || s.db == nil || call == "" {
		return
	}
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, uint64(seenUnix))
	ignoreCustomSCPDiskError(s.db.Set([]byte(customSCPMetaPrefix+call), value, pebble.NoSync))
}

func (s *CustomSCPStore) persistObservationLocked(key customSCPKey, spotter string, obs customSCPSpotterObs) {
	if s == nil || s.db == nil {
		return
	}
	value := make([]byte, 10)
	binary.BigEndian.PutUint64(value[:8], uint64(obs.seenUnix))
	binary.BigEndian.PutUint16(value[8:10], obs.cellRes1)
	ignoreCustomSCPDiskError(s.db.Set([]byte(observationKeyString(key, spotter)), value, pebble.NoSync))
}

func (s *CustomSCPStore) evictOldestKeyLocked() {
	var victim customSCPKey
	set := false
	var oldest int64
	for key, entry := range s.entries {
		if !set || entry.lastSeen < oldest || (entry.lastSeen == oldest && customSCPKeyLess(key, victim)) {
			victim = key
			oldest = entry.lastSeen
			set = true
		}
	}
	if !set {
		return
	}
	s.deleteEntryLocked(victim)
	s.deleteObservationPrefixLocked(victim)
}

func (s *CustomSCPStore) retainInternedStringLocked(value string) string {
	if s == nil || value == "" {
		return value
	}
	return s.interner.retain(value)
}

func (s *CustomSCPStore) releaseInternedStringLocked(value string) {
	if s == nil || value == "" {
		return
	}
	s.interner.release(value)
}

func (s *CustomSCPStore) retainStaticCallLocked(call string, seenUnix int64) (string, bool) {
	if s == nil || call == "" {
		return call, false
	}
	if s.static == nil {
		s.static = make(map[string]int64, 1024)
	}
	if current, ok := s.static[call]; ok {
		if seenUnix > current {
			s.static[call] = seenUnix
			return s.interner.canonical(call), true
		}
		return s.interner.canonical(call), false
	}
	retained := s.retainInternedStringLocked(call)
	s.static[retained] = seenUnix
	return retained, true
}

func (s *CustomSCPStore) pruneEntryLocked(entry *customSCPEntry, cutoff int64, removed *[]string) {
	s.pruneEntryLimitedLocked(entry, cutoff, removed, 0)
}

// limit bounds cleanup scratch; zero preserves the full live/load pruning path.
func (s *CustomSCPStore) pruneEntryLimitedLocked(entry *customSCPEntry, cutoff int64, removed *[]string, limit int) int {
	if entry == nil || len(entry.spotters) == 0 {
		return 0
	}
	removedCount := 0
	write := 0
	for _, spotter := range entry.spotters {
		if decodeCustomSCPSpotterSeenUnix(spotter.seenUnix) < cutoff && (limit <= 0 || removedCount < limit) {
			if s.observationSpotters > 0 {
				s.observationSpotters--
			}
			s.releaseInternedStringLocked(spotter.spotter)
			removedCount++
			if removed != nil {
				*removed = append(*removed, spotter.spotter)
			}
			continue
		}
		entry.spotters[write] = spotter
		write++
	}
	for i := write; i < len(entry.spotters); i++ {
		entry.spotters[i] = customSCPSpotterEntry{}
	}
	entry.spotters = entry.spotters[:write]
	if len(entry.spotters) == 0 {
		entry.spotters = nil
	}
	s.refreshEntryAgesLocked(entry)
	return removedCount
}

func (s *CustomSCPStore) trimSpottersLocked(entry *customSCPEntry, removed *[]string) int {
	return s.trimSpottersLimitedLocked(entry, removed, 0)
}

func (s *CustomSCPStore) trimSpottersLimitedLocked(entry *customSCPEntry, removed *[]string, limit int) int {
	if entry == nil || len(entry.spotters) <= s.opts.MaxSpottersPerKey {
		return 0
	}
	trimmed := 0
	for len(entry.spotters) > s.opts.MaxSpottersPerKey && (limit <= 0 || trimmed < limit) {
		idx, ok := oldestCustomSCPSpotterIndex(entry)
		if !ok {
			break
		}
		spotter := entryDeleteSpotterAt(entry, idx)
		if s.observationSpotters > 0 {
			s.observationSpotters--
		}
		s.releaseInternedStringLocked(spotter.spotter)
		if removed != nil {
			*removed = append(*removed, spotter.spotter)
		}
		trimmed++
	}
	return trimmed
}

func (s *CustomSCPStore) trimPendingEntryOnLoadLocked(key customSCPKey, entry *customSCPEntry, pendingOverflow *bool, batch *pebble.Batch) int {
	if entry == nil || len(entry.spotters) <= s.opts.MaxSpottersPerKey {
		return 0
	}
	deleted := 0
	for len(entry.spotters) > s.opts.MaxSpottersPerKey {
		if pendingOverflow != nil && !*pendingOverflow {
			*pendingOverflow = true
			s.diag.oversizedKeysSeenOnLoad++
		}
		idx, ok := oldestCustomSCPSpotterIndex(entry)
		if !ok {
			break
		}
		spotter := entryDeleteSpotterAt(entry, idx)
		s.diag.overflowObservationsPruned++
		if s.deleteObservationSpotterLocked(key, nil, spotter.spotter, batch) {
			deleted++
		}
	}
	return deleted
}

func ignoreCustomSCPDiskError(error) {
	// The in-memory bounded view is authoritative. These disk operations are
	// best-effort convergence; cleanup/load passes retry stale record removal.
}

func (s *CustomSCPStore) observationHorizonCutoffUnix(now time.Time) int64 {
	return now.UTC().Add(-time.Duration(s.opts.HorizonDays) * 24 * time.Hour).Unix()
}

func (s *CustomSCPStore) staticHorizonCutoffUnix(now time.Time) int64 {
	return now.UTC().Add(-time.Duration(s.opts.StaticHorizonDays) * 24 * time.Hour).Unix()
}

func (s *CustomSCPStore) deleteEntryLocked(key customSCPKey) {
	s.deleteEntryWithExpiryLocked(key, true)
}

func (s *CustomSCPStore) deleteEntryAfterExpiryPopLocked(key customSCPKey) {
	s.deleteEntryWithExpiryLocked(key, false)
}

func (s *CustomSCPStore) deleteEntryWithExpiryLocked(key customSCPKey, deleteExpiry bool) {
	entry := s.entries[key]
	if entry == nil {
		return
	}
	if deleteExpiry {
		s.deleteEntryExpiryLocked(key)
	}
	s.active.Remove(key.call, key.band)
	s.observationSpotters -= len(entry.spotters)
	if s.observationSpotters < 0 {
		s.observationSpotters = 0
	}
	delete(s.entries, key)
	for _, spotter := range entry.spotters {
		s.releaseInternedStringLocked(spotter.spotter)
	}
	s.releaseInternedStringLocked(key.call)
	s.releaseInternedStringLocked(key.band)
	s.releaseInternedStringLocked(key.bucket)
}

func (s *CustomSCPStore) deleteStaticAfterExpiryPopLocked(call string) bool {
	if s == nil || call == "" {
		return false
	}
	if _, ok := s.static[call]; !ok {
		return false
	}
	delete(s.static, call)
	s.releaseInternedStringLocked(call)
	return true
}

func (s *CustomSCPStore) deleteObservationSpotterLocked(key customSCPKey, entry *customSCPEntry, spotter string, batch *pebble.Batch) bool {
	if s == nil || s.db == nil || spotter == "" {
		return false
	}
	if entryHasSpotter(entry, spotter) {
		return false
	}
	keyBytes := []byte(observationKeyString(key, spotter))
	if batch != nil {
		ignoreCustomSCPDiskError(batch.Delete(keyBytes, nil))
	} else {
		ignoreCustomSCPDiskError(s.db.Delete(keyBytes, pebble.NoSync))
	}
	return true
}

func (s *CustomSCPStore) deleteObservationSpottersLocked(key customSCPKey, entry *customSCPEntry, spotters []string, batch *pebble.Batch) int {
	if s == nil || s.db == nil || len(spotters) == 0 {
		return 0
	}
	uniq := make(map[string]struct{}, len(spotters))
	deleted := 0
	for _, spotter := range spotters {
		if spotter == "" {
			continue
		}
		if entryHasSpotter(entry, spotter) {
			continue
		}
		if _, ok := uniq[spotter]; ok {
			continue
		}
		uniq[spotter] = struct{}{}
		if s.deleteObservationSpotterLocked(key, nil, spotter, batch) {
			deleted++
		}
	}
	return deleted
}

func (s *CustomSCPStore) deleteObservationPrefixLocked(key customSCPKey) {
	if s == nil || s.db == nil {
		return
	}
	prefix := []byte(observationPrefixForKey(key))
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: append(prefix, 0xff),
	})
	if err != nil {
		return
	}
	defer iter.Close()
	batch := s.db.NewBatch()
	defer batch.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		ignoreCustomSCPDiskError(batch.Delete(iter.Key(), nil))
	}
	ignoreCustomSCPDiskError(batch.Commit(pebble.NoSync))
}

func (s *CustomSCPStore) snrPasses(bucket string, report int, hasReport bool) bool {
	switch bucket {
	case "cw":
		if s.opts.MinSNRDBCW > 0 {
			return hasReport && report >= s.opts.MinSNRDBCW
		}
	case "rtty":
		if s.opts.MinSNRDBRTTY > 0 {
			return hasReport && report >= s.opts.MinSNRDBRTTY
		}
	}
	return true
}

func sanitizeCustomSCPOptions(opts CustomSCPOptions) CustomSCPOptions {
	if strings.TrimSpace(opts.Path) == "" {
		opts.Path = filepath.Join("data", "scp")
	}
	if opts.HorizonDays <= 0 {
		opts.HorizonDays = customSCPDefaultHorizonDays
	}
	if opts.StaticHorizonDays <= 0 {
		opts.StaticHorizonDays = customSCPDefaultStaticHorizonDays
	}
	if opts.MaxKeys <= 0 {
		opts.MaxKeys = customSCPDefaultMaxKeys
	}
	if opts.MaxSpottersPerKey <= 0 {
		opts.MaxSpottersPerKey = customSCPDefaultMaxSpotters
	}
	if opts.CleanupInterval <= 0 {
		opts.CleanupInterval = customSCPDefaultCleanupInterval
	}
	if opts.CacheSizeBytes <= 0 {
		opts.CacheSizeBytes = customSCPDefaultCacheSizeBytes
	}
	if opts.BloomFilterBitsPerKey <= 0 {
		opts.BloomFilterBitsPerKey = customSCPDefaultBloomBits
	}
	if opts.MemTableSizeBytes <= 0 {
		opts.MemTableSizeBytes = customSCPDefaultMemTableSize
	}
	if opts.L0CompactionThreshold <= 0 {
		opts.L0CompactionThreshold = customSCPDefaultL0Compaction
	}
	if opts.L0StopWritesThreshold <= opts.L0CompactionThreshold {
		opts.L0StopWritesThreshold = customSCPDefaultL0StopWrites
		if opts.L0StopWritesThreshold <= opts.L0CompactionThreshold {
			opts.L0StopWritesThreshold = opts.L0CompactionThreshold + 4
		}
	}
	if opts.CoreMinScore <= 0 {
		opts.CoreMinScore = customSCPDefaultCoreMinScore
	}
	if opts.CoreMinH3Cells <= 0 {
		opts.CoreMinH3Cells = customSCPDefaultCoreMinH3Cells
	}
	if opts.SFloorMinScore <= 0 {
		opts.SFloorMinScore = customSCPDefaultFloorMinScore
	}
	if opts.SFloorExactMinH3Cells <= 0 {
		opts.SFloorExactMinH3Cells = customSCPDefaultFloorExactCells
	}
	if opts.SFloorFamilyMinH3Cells <= 0 {
		opts.SFloorFamilyMinH3Cells = customSCPDefaultFloorFamilyCells
	}
	return opts
}

func customSCPBucketForMode(mode string) (string, bool) {
	bucket := CustomSCPBucketForMode(mode)
	if bucket == "" {
		return "", false
	}
	return bucket, true
}

func normalizeMinUnique(minUnique int, max int) int {
	if minUnique <= 0 {
		minUnique = 2
	}
	if max > 0 && minUnique > max {
		return max
	}
	return minUnique
}

func tierForAge(ageSeconds int64) int {
	switch {
	case ageSeconds <= int64((12 * time.Hour).Seconds()):
		return customSCPTierT0
	case ageSeconds <= int64((48 * time.Hour).Seconds()):
		return customSCPTierT1
	case ageSeconds <= int64((7 * 24 * time.Hour).Seconds()):
		return customSCPTierT2
	case ageSeconds <= int64((30 * 24 * time.Hour).Seconds()):
		return customSCPTierT3
	case ageSeconds <= int64((90 * 24 * time.Hour).Seconds()):
		return customSCPTierT4
	case ageSeconds <= int64((180 * 24 * time.Hour).Seconds()):
		return customSCPTierT5
	default:
		return customSCPTierT6
	}
}

func scoreForTier(tier int) int {
	switch tier {
	case customSCPTierT0:
		return customSCPScoreT0
	case customSCPTierT1:
		return customSCPScoreT1
	case customSCPTierT2:
		return customSCPScoreT2
	case customSCPTierT3:
		return customSCPScoreT3
	case customSCPTierT4:
		return customSCPScoreT4
	case customSCPTierT5:
		return customSCPScoreT5
	default:
		return customSCPScoreT6
	}
}

func observationKeyString(key customSCPKey, spotter string) string {
	return customSCPObsPrefix + key.call + "|" + key.band + "|" + key.bucket + "|" + strutil.NormalizeUpper(spotter)
}

func observationPrefixForKey(key customSCPKey) string {
	return customSCPObsPrefix + key.call + "|" + key.band + "|" + key.bucket + "|"
}

func parseObservationKeyBytes(key []byte) (call, band, bucket, spotter string, ok bool) {
	if !bytes.HasPrefix(key, customSCPObsPrefixBytes) {
		return "", "", "", "", false
	}
	raw := key[len(customSCPObsPrefixBytes):]
	first := bytes.IndexByte(raw, '|')
	if first <= 0 {
		return "", "", "", "", false
	}
	secondRel := bytes.IndexByte(raw[first+1:], '|')
	if secondRel <= 0 {
		return "", "", "", "", false
	}
	second := first + 1 + secondRel
	thirdRel := bytes.IndexByte(raw[second+1:], '|')
	if thirdRel <= 0 {
		return "", "", "", "", false
	}
	third := second + 1 + thirdRel
	if third+1 >= len(raw) {
		return "", "", "", "", false
	}
	call = NormalizeCallsign(string(raw[:first]))
	band = NormalizeBand(string(raw[first+1 : second]))
	bucket = strings.ToLower(strings.TrimSpace(string(raw[second+1 : third])))
	spotter = strutil.NormalizeUpper(string(raw[third+1:]))
	if call == "" || band == "" || spotter == "" {
		return "", "", "", "", false
	}
	return call, band, bucket, spotter, true
}

func customSCPEntryActive(entry *customSCPEntry) bool {
	return entry != nil && len(entry.spotters) > 0
}

func customSCPKeyLess(a, b customSCPKey) bool {
	if a.call != b.call {
		return a.call < b.call
	}
	if a.band != b.band {
		return a.band < b.band
	}
	return a.bucket < b.bucket
}

// eligibleCustomSCPSnapshot describes the qualifying population after retention
// pruning. Filtering never mutates the retained observations; a fresh receiver
// cannot refresh other receivers, and cells come from the same eligible rows.
func eligibleCustomSCPSnapshot(entry *customSCPEntry, nowUnix int64, minScore int) customSCPSnapshot {
	snapshot := customSCPSnapshot{}
	if entry == nil || len(entry.spotters) == 0 {
		return snapshot
	}
	var cells [customSCPDefaultMaxSpotters]uint16
	used := 0
	var overflow map[uint16]struct{}
	for _, spotter := range entry.spotters {
		seenUnix := int64(spotter.seenUnix)
		if seenUnix > nowUnix {
			continue
		}
		score := scoreForTier(tierForAge(nowUnix - seenUnix))
		if score < minScore {
			continue
		}
		snapshot.uniqueSpotters++
		if seenUnix > snapshot.latestSeenUnix {
			snapshot.latestSeenUnix = seenUnix
		}
		if score > snapshot.score {
			snapshot.score = score
		}
		cell := spotter.cellRes1
		if cell == 0 {
			continue
		}
		if overflow != nil {
			overflow[cell] = struct{}{}
			continue
		}
		found := false
		for i := 0; i < used; i++ {
			if cells[i] == cell {
				found = true
				break
			}
		}
		if found {
			continue
		}
		if used < len(cells) {
			cells[used] = cell
			used++
			continue
		}
		overflow = make(map[uint16]struct{}, used+1)
		for i := 0; i < used; i++ {
			overflow[cells[i]] = struct{}{}
		}
		overflow[cell] = struct{}{}
	}
	if overflow != nil {
		snapshot.uniqueCells = len(overflow)
	} else {
		snapshot.uniqueCells = used
	}
	return snapshot
}
