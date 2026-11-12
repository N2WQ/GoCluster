// File role: Owns bounded grid-update coalescing and the final persistence drain.
// The caller owns update admission and channel closure; this loop owns pending
// records through their batch write and closes done only after the final attempt.
package cluster

import (
	"log"
	"time"

	"dxcluster/gridstore"
)

// Private per-instance fault witnesses run outside authority/storage locks.
// Production supplies the zero value; tests observe actual worker responses and
// the real write boundary without replacing either implementation.
type gridWriterOps struct {
	afterRead   func(gridLookupRequest, *gridstore.Record)
	beforeWrite func([]gridstore.Record)
	afterWrite  func([]gridstore.Record, error)
}

// mergePendingGridRecord coalesces one call without changing its batch lifetime.
// A locator and its provenance are one value: approximate CTY-derived grids
// cannot replace an observed grid, but their other metadata still contributes.
func mergePendingGridRecord(existing, incoming gridstore.Record) gridstore.Record {
	merged := existing
	if incoming.Grid.Valid && (!incoming.GridDerived || !existing.Grid.Valid || existing.GridDerived) {
		merged.Grid = incoming.Grid
		merged.GridDerived = incoming.GridDerived
	}
	if incoming.CTYValid {
		merged.CTYValid = true
		merged.CTYADIF = incoming.CTYADIF
		merged.CTYCQZone = incoming.CTYCQZone
		merged.CTYITUZone = incoming.CTYITUZone
		merged.CTYContinent = incoming.CTYContinent
		merged.CTYCountry = incoming.CTYCountry
	}
	if incoming.Observations > 0 {
		merged.Observations += incoming.Observations
	}
	if !incoming.FirstSeen.IsZero() {
		if merged.FirstSeen.IsZero() || incoming.FirstSeen.Before(merged.FirstSeen) {
			merged.FirstSeen = incoming.FirstSeen
		}
	}
	if !incoming.UpdatedAt.IsZero() {
		if merged.UpdatedAt.IsZero() || incoming.UpdatedAt.After(merged.UpdatedAt) {
			merged.UpdatedAt = incoming.UpdatedAt
		}
	}
	if incoming.ExpiresAt != nil {
		merged.ExpiresAt = incoming.ExpiresAt
	}
	return merged
}

// runGridWriter is the production writer loop. Its pending map flushes at 500
// distinct calls or the configured interval; producers never wait on storage.
func runGridWriter(coord *gridCoordinator, updates <-chan gridWriteRequest, done chan<- struct{}, flushInterval, ttl time.Duration, metrics *gridMetrics, ops gridWriterOps) {
	defer close(done)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	var ttlTicker *time.Ticker
	var ttlCh <-chan time.Time
	if ttl > 0 {
		ttlTicker = time.NewTicker(ttl)
		defer ttlTicker.Stop()
		ttlCh = ttlTicker.C
	}

	pending := make(map[string]gridWriteRequest)
	// Purpose: Flush pending updates to the database in a batch.
	// Key aspects: Retains batch on busy errors; clears on success.
	// Upstream: update loop and ticker ticks.
	// Downstream: store.UpsertBatch, metrics.learnedTotal.
	flush := func() {
		if len(pending) == 0 {
			return
		}
		batch, store := coord.prepareBatch(pending)
		if store == nil || len(batch) == 0 {
			return
		}
		gridUpdates := 0
		for i := range batch {
			if batch[i].Grid.Valid {
				gridUpdates++
			}
		}
		start := time.Now().UTC()
		if ops.beforeWrite != nil {
			ops.beforeWrite(batch)
		}
		err := store.UpsertBatch(batch)
		if ops.afterWrite != nil {
			ops.afterWrite(batch, err)
		}
		if err != nil {
			if gridstore.IsBusyError(err) {
				// Keep the batch in-memory so a later flush can retry after the lock clears.
				log.Printf("Warning: gridstore batch upsert busy (retaining %d pending): %v", len(batch), err)
				return
			}
			log.Printf("Warning: gridstore batch upsert failed (dropping %d pending): %v", len(batch), err)
			coord.finishBatch(pending, err)
			return
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			log.Printf("Gridstore: batch upsert %d records in %s", len(batch), elapsed)
		}
		if gridUpdates > 0 {
			metrics.learnedTotal.Add(uint64(gridUpdates))
		}
		coord.finishBatch(pending, nil)
	}

	for {
		select {
		case req, ok := <-updates:
			if !ok {
				flush()
				return
			}
			// Admission finalized this key before registering its owner. Re-keying
			// here would separate the request from its cache and release identity.
			coord.coalesce(pending, req)
			if len(pending) >= 500 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-ttlCh:
			store := coord.handle.Store()
			if store == nil {
				continue
			}
			cutoff := time.Now().UTC().Add(-ttl)
			if removed, err := store.PurgeOlderThan(cutoff); err != nil {
				log.Printf("Warning: gridstore TTL purge failed: %v", err)
			} else if removed > 0 {
				log.Printf("Gridstore: purged %d entries older than %v", removed, ttl)
			}
		}
	}
}
