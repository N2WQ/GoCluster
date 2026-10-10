// File role: Owns Custom SCP cleanup chunks and background-worker retirement.
// Cleanup rechecks current evidence and disk values before deleting snapshot
// candidates. See ADR-0279 and the persistence/delivery regressions.
package spot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
)

const customSCPCleanupChunkSize = 512

// These pass-local candidates retain only keys, never iterator values. Each
// chunk is capped at 512; no server-lifetime journal or side table is added.
// evicted means a fresh observation was discarded by the existing spotter cap,
// so freshness alone cannot protect it. Reinserted retained evidence still can.
type customSCPCleanupDelete struct {
	key     string
	cutoff  int64
	evicted bool
}

func (s *CustomSCPStore) runCleanup(quit <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(s.opts.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-quit:
			return
		case <-ticker.C:
			s.cleanup(time.Now().UTC())
		}
	}
}

// cleanupMu remains held while joining, preventing Start from overlapping the
// retiring owner. The worker never takes cleanupMu. Close sets closing first.
func (s *CustomSCPStore) stopCleanupLocked() {
	if s.quit == nil {
		return
	}
	close(s.quit)
	<-s.cleanupDone
	s.quit = nil
	s.cleanupDone = nil
}

func (s *CustomSCPStore) cleanup(now time.Time) {
	if s == nil {
		return
	}
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()
	s.cleanupPassMu.Lock()
	defer s.cleanupPassMu.Unlock()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	obsCutoff := s.observationHorizonCutoffUnix(now)
	staticCutoff := s.staticHorizonCutoffUnix(now)
	for {
		candidates, more := s.pruneCleanupChunk(obsCutoff, staticCutoff)
		s.applyCleanupDeletes(candidates)
		if !more {
			break
		}
	}
	if s.db == nil {
		return
	}
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return
	}
	defer iter.Close()
	iter.First()
	for iter.Valid() {
		s.applyCleanupDeletes(scanCustomSCPCleanupChunk(iter, obsCutoff, staticCutoff))
	}
}

// Callers own a dbMu reader. Memory pruning is chunked by removed observations
// and static calls, including partially pruned oversized keys. All secondary
// ownership is updated before mu is released; a live refresh between this
// capture and apply is protected by applyCleanupDeletes.
func (s *CustomSCPStore) pruneCleanupChunk(obsCutoff, staticCutoff int64) ([]customSCPCleanupDelete, bool) {
	s.diskMu.Lock()
	defer s.diskMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	var candidates []customSCPCleanupDelete
	for len(candidates) < customSCPCleanupChunkSize {
		key, entry, ok := s.popDueEntryExpiryLocked(obsCutoff)
		if !ok {
			break
		}
		if candidates == nil {
			candidates = make([]customSCPCleanupDelete, 0, customSCPCleanupChunkSize)
		}
		var removed []string
		count := s.pruneEntryLimitedLocked(entry, obsCutoff, &removed, customSCPCleanupChunkSize-len(candidates))
		s.diag.staleObservationsPruned += uint64(count)
		for _, de := range removed {
			candidates = append(candidates, customSCPCleanupDelete{key: observationKeyString(key, de), cutoff: obsCutoff})
		}
		removed = removed[:0]
		if len(candidates) < customSCPCleanupChunkSize {
			count = s.trimSpottersLimitedLocked(entry, &removed, customSCPCleanupChunkSize-len(candidates))
			s.diag.overflowObservationsPruned += uint64(count)
			for _, de := range removed {
				candidates = append(candidates, customSCPCleanupDelete{key: observationKeyString(key, de), evicted: true})
			}
		}
		if len(entry.spotters) == 0 {
			s.deleteEntryAfterExpiryPopLocked(key)
		} else {
			s.markEntryForCleanupLocked(key, entry)
		}
	}
	for len(candidates) < customSCPCleanupChunkSize {
		call, ok := s.popDueStaticExpiryLocked(staticCutoff)
		if !ok {
			break
		}
		if s.deleteStaticAfterExpiryPopLocked(call) {
			s.diag.staleStaticPruned++
			if candidates == nil {
				candidates = make([]customSCPCleanupDelete, 0, customSCPCleanupChunkSize)
			}
			candidates = append(candidates, customSCPCleanupDelete{key: customSCPMetaPrefix + call, cutoff: staticCutoff})
		}
	}
	// Live writes and startup already enforce MaxKeys. Preserve cleanup's repair
	// of an oversized view, one key at a time, without retaining victim lists.
	if len(s.entries) > s.opts.MaxKeys {
		s.evictOldestKeyLocked()
		return candidates, true
	}
	return candidates, len(candidates) == customSCPCleanupChunkSize
}

// The iterator is a historical snapshot. Even malformed rows can be repaired
// after discovery, so this function only proposes deletes; it never commits.
// Scan work as well as candidate count is bounded, including fresh-only scans.
func scanCustomSCPCleanupChunk(iter *pebble.Iterator, obsCutoff, staticCutoff int64) []customSCPCleanupDelete {
	var candidates []customSCPCleanupDelete
	for scanned := 0; iter.Valid() && scanned < customSCPCleanupChunkSize; scanned++ {
		key := iter.Key()
		cutoff, size := int64(0), 0
		switch {
		case bytes.HasPrefix(key, customSCPMetaPrefixBytes):
			cutoff, size = staticCutoff, 8
		case bytes.HasPrefix(key, customSCPObsPrefixBytes):
			cutoff, size = obsCutoff, 10
		}
		if size != 0 && customSCPDiskValueExpired(iter.Value(), size, cutoff) {
			if candidates == nil {
				candidates = make([]customSCPCleanupDelete, 0, customSCPCleanupChunkSize)
			}
			candidates = append(candidates, customSCPCleanupDelete{key: string(key), cutoff: cutoff})
		}
		iter.Next()
	}
	return candidates
}

func customSCPDiskValueExpired(value []byte, size int, cutoff int64) bool {
	return len(value) != size || int64(binary.BigEndian.Uint64(value[:8])) < cutoff
}

// dbMu is already held by the complete operation. diskMu prevents a write
// after the final recheck and before Sync commit, including writes that have
// not yet acquired mu. Never retain mu across this cleanup disk I/O.
func (s *CustomSCPStore) applyCleanupDeletes(candidates []customSCPCleanupDelete) {
	if s.db == nil || len(candidates) == 0 {
		return
	}
	s.diskMu.Lock()
	defer s.diskMu.Unlock()
	batch := s.db.NewBatch()
	defer batch.Close()
	pending := 0
	for _, candidate := range candidates {
		s.mu.RLock()
		retained := s.cleanupDeleteRetainedLocked(candidate)
		s.mu.RUnlock()
		if retained {
			continue
		}
		if !candidate.evicted {
			value, closer, err := s.db.Get([]byte(candidate.key))
			if err != nil {
				if closer != nil {
					ignoreCustomSCPDiskError(closer.Close())
				}
				// Not-found needs no tombstone; unknown errors cannot prove expiry.
				if !errors.Is(err, pebble.ErrNotFound) {
					ignoreCustomSCPDiskError(err)
				}
				continue
			}
			size := 10
			if strings.HasPrefix(candidate.key, customSCPMetaPrefix) {
				size = 8
			}
			expired := customSCPDiskValueExpired(value, size, candidate.cutoff)
			ignoreCustomSCPDiskError(closer.Close())
			if !expired {
				continue
			}
		}
		if err := batch.Delete([]byte(candidate.key), nil); err == nil {
			pending++
		}
	}
	if pending > 0 {
		ignoreCustomSCPDiskError(batch.Commit(pebble.Sync))
	}
}

func (s *CustomSCPStore) cleanupDeleteRetainedLocked(candidate customSCPCleanupDelete) bool {
	if strings.HasPrefix(candidate.key, customSCPMetaPrefix) {
		seen, ok := s.static[strings.TrimPrefix(candidate.key, customSCPMetaPrefix)]
		return ok && seen >= candidate.cutoff
	}
	call, band, bucket, de, ok := parseObservationKeyBytes([]byte(candidate.key))
	return ok && entryHasSpotter(s.entries[customSCPKey{call: call, band: band, bucket: bucket}], de)
}
