// File role: Orders grid authority across active reads, reports and persistence.
// Only existing queue/batch owners and the five lookup workers retain entries.
// Warm cache hits remain shard-local. Changed work takes authority then shard;
// no storage, callback or blocking admission runs under either lock. ADR-0345.
package cluster

import (
	"strings"
	"sync"
	"time"

	"dxcluster/gridstore"
)

// Nonzero identities cannot wrap; an outstanding owner prevents pointer ABA.
type gridIdentity struct{ _ byte }

type gridWriteRequest struct {
	record           gridstore.Record
	storeEpoch       *gridIdentity
	reportedRevision *gridIdentity
}

type gridStoredReport struct {
	grid    string
	updated time.Time
}

type gridActiveCall struct {
	call                           string
	writes, currentWrites, lookups int
	live, committed, negative      *gridIdentity
	pin                            *gridStoredReport
}

type gridLookupToken struct {
	entry                                   *gridActiveCall
	store, cache, live, committed, negative *gridIdentity
	contested                               bool
}

type gridCoordinator struct {
	mu                          sync.Mutex
	cache                       *callMetaCache
	handle                      *gridStoreHandle
	storeEpoch, cacheEpoch      *gridIdentity
	entries                     map[string]*gridActiveCall
	keyBytes, pinBytes, pending uint64
	peak, changes, rebuilds     int
	metrics                     *gridMetrics
}

func newGridCoordinator(cache *callMetaCache) *gridCoordinator {
	return &gridCoordinator{cache: cache, storeEpoch: &gridIdentity{}, cacheEpoch: &gridIdentity{}}
}

// Binding precedes recovery/lookup producers. The cache's owner exists before
// the CTY scheduler can call Clear; handle binding never replaces a live owner.
func bindGridCoordinator(handle *gridStoreHandle, cache *callMetaCache) *gridCoordinator {
	handle.bindMu.Lock()
	defer handle.bindMu.Unlock()
	if handle.authority != nil {
		if cache != nil && handle.authority != cache.authority {
			panic("grid authority bound to a different cache")
		}
		return handle.authority
	}
	g := newGridCoordinator(nil)
	if cache != nil {
		g = cache.authority
	}
	g.mu.Lock()
	g.handle = handle
	g.mu.Unlock()
	handle.authority = g
	return g
}

func (g *gridCoordinator) entryLocked(call string) *gridActiveCall {
	if e := g.entries[call]; e != nil {
		return e
	}
	if g.entries == nil {
		g.entries = make(map[string]*gridActiveCall)
	}
	e := &gridActiveCall{call: strings.Clone(call), live: &gridIdentity{}, committed: &gridIdentity{}, negative: &gridIdentity{}}
	g.entries[e.call] = e
	g.keyBytes += uint64(len(e.call))
	g.changes++
	if len(g.entries) > g.peak {
		g.peak = len(g.entries)
	}
	return e
}

func (g *gridCoordinator) dropPinLocked(e *gridActiveCall) {
	if e.pin != nil {
		g.pinBytes -= uint64(len(e.pin.grid))
		e.pin = nil
	}
}

func (g *gridCoordinator) releaseLocked(e *gridActiveCall) {
	if e.lookups == 0 {
		g.dropPinLocked(e)
	}
	if e.lookups == 0 && e.writes == 0 {
		delete(g.entries, e.call)
		g.keyBytes -= uint64(len(e.call))
		g.changes++
	}
	if len(g.entries) == 0 {
		g.entries = nil
		g.peak, g.changes = 0, 0
	} else if g.peak > 64 && len(g.entries)*4 < g.peak || g.changes >= max(256, 4*g.peak) {
		// Bound historical/tombstone backing even with one permanently held
		// owner. Reuse compact strings and stable entry pointers during overlap.
		fresh := make(map[string]*gridActiveCall, len(g.entries))
		for k, v := range g.entries {
			fresh[k] = v
		}
		g.entries = fresh
		g.peak, g.changes = len(fresh), 0
		g.rebuilds++
	}
	g.gaugesLocked()
}

func (g *gridCoordinator) gaugesLocked() {
	if g.metrics == nil {
		return
	}
	g.metrics.activeKeys.Store(uint64(len(g.entries)))
	g.metrics.copiedTextBytes.Store(g.keyBytes + g.pinBytes)
	g.metrics.pendingReports.Store(g.pending)
}

func (g *gridCoordinator) invalidateCache() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.invalidateCacheLocked()
}

func (g *gridCoordinator) invalidateCacheLocked() {
	g.cacheEpoch = &gridIdentity{}
	for _, e := range g.entries {
		g.dropPinLocked(e)
	}
	if g.cache != nil {
		g.cache.clearEntries()
	}
	g.gaugesLocked()
}

// Set(nil) preserves live offline cache availability. A nonnil replacement
// atomically retires old-store authority and the recovery cache image together.
func (g *gridCoordinator) replaceStore(store *gridstore.Store) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.handle.store.Load() == store {
		return
	}
	g.storeEpoch = &gridIdentity{}
	for _, e := range g.entries {
		g.dropPinLocked(e)
		e.currentWrites = 0
	}
	g.handle.store.Store(store)
	if store != nil {
		g.invalidateCacheLocked()
	}
	g.gaugesLocked()
}

func (g *gridCoordinator) beginLookup(call string) (gridLookupToken, *gridstore.Store) {
	g.mu.Lock()
	defer g.mu.Unlock()
	e := g.entryLocked(call)
	e.lookups++
	t := gridLookupToken{entry: e, store: g.storeEpoch, cache: g.cacheEpoch, live: e.live, committed: e.committed, negative: e.negative, contested: e.currentWrites != 0}
	g.gaugesLocked()
	return t, g.handle.Store()
}

func recordGridResult(rec *gridstore.Record) gridLookupResult {
	if rec == nil || !rec.Grid.Valid {
		return gridLookupResult{}
	}
	grid := strings.TrimSpace(strings.ToUpper(rec.Grid.String))
	if grid == "" {
		return gridLookupResult{}
	}
	return gridLookupResult{grid: grid, derived: rec.GridDerived, ok: true}
}

func (g *gridCoordinator) cachedReported(call string) gridLookupResult {
	if g.cache != nil {
		grid, derived, ok := g.cache.LookupGrid(call, nil)
		if ok && !derived {
			return gridLookupResult{grid: grid, ok: true}
		}
	}
	return gridLookupResult{}
}

func (g *gridCoordinator) supersededLocked() {
	if g.metrics != nil {
		g.metrics.supersededCandidates.Add(1)
	}
}

func (g *gridCoordinator) endLookup(t gridLookupToken) {
	g.mu.Lock()
	defer g.mu.Unlock()
	t.entry.lookups--
	g.releaseLocked(t.entry)
}

// finishLookup is the only worker selection/publication boundary. Temporary
// copies can arbitrate positive reported reads only; absence never borrows one.
// Cache TTL still applies to cache hits, not a successful stored-row response.
func (g *gridCoordinator) finishLookup(t gridLookupToken, rec *gridstore.Record, err error) (gridLookupResult, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	e := t.entry
	if t.store != g.storeEpoch || t.cache != g.cacheEpoch {
		g.supersededLocked()
		return gridLookupResult{}, false
	}
	candidate := recordGridResult(rec)
	replaced := false
	if err != nil || !candidate.ok {
		e.negative = &gridIdentity{}
		g.dropPinLocked(e)
		return gridLookupResult{}, false
	}
	if t.live != e.live || t.contested || e.currentWrites != 0 {
		g.supersededLocked()
		return g.cachedReported(e.call), false
	}
	if t.committed != e.committed && (candidate.derived || e.pin == nil) {
		g.supersededLocked()
		return g.cachedReported(e.call), false
	}
	if candidate.derived {
		e.negative = &gridIdentity{}
		g.dropPinLocked(e)
		if selected := g.cachedReported(e.call); selected.ok {
			g.supersededLocked()
			return selected, false
		}
	} else {
		// Absence fences pin reuse, not an independently successful positive
		// read (including a raw-call fallback the absent base did not cover).
		if t.negative == e.negative && e.pin != nil && !rec.UpdatedAt.After(e.pin.updated) {
			replaced = e.pin.grid != candidate.grid || !e.pin.updated.Equal(rec.UpdatedAt)
			copyRec := *rec
			copyRec.Grid, copyRec.UpdatedAt = sqlNullString(e.pin.grid), e.pin.updated
			rec = &copyRec
			candidate.grid = e.pin.grid
		}
	}
	storedWinner := true
	if g.cache != nil {
		// Expire the previous resident before applying, preserving persisted age.
		g.cache.LookupGrid(e.call, nil)
		candidate, storedWinner = g.cache.selectGridRecordSource(*rec)
	}
	if replaced || !storedWinner {
		g.supersededLocked()
	}
	if candidate.ok && !candidate.derived {
		e.committed = &gridIdentity{}
		g.dropPinLocked(e)
		if storedWinner && t.negative == e.negative {
			e.pin = &gridStoredReport{grid: strings.Clone(candidate.grid), updated: rec.UpdatedAt}
			g.pinBytes += uint64(len(candidate.grid))
		}
	}
	g.gaugesLocked()
	// A synthesized record may persist only when that record actually won;
	// selecting a newer cached estimate cannot authorize enqueueing the old one.
	return candidate, candidate.ok && candidate.derived && storedWinner && !replaced
}

// enqueueReport advances live authority even when persistence is refused. Only
// successful nonblocking sends add owners; the writer takes this same mutex
// before transfer, so registration after the send is visible before processing.
func (g *gridCoordinator) enqueueReport(rec gridstore.Record, updates chan<- gridWriteRequest) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.cache != nil && !g.cache.UpdateGrid(rec.Call, rec.Grid.String, false) {
		return
	}
	revision := &gridIdentity{}
	if e := g.entries[rec.Call]; e != nil {
		e.live = revision
		g.dropPinLocked(e)
	}
	if !g.handle.Available() {
		g.gaugesLocked()
		return
	}
	req := gridWriteRequest{record: rec, storeEpoch: g.storeEpoch, reportedRevision: revision}
	select {
	case updates <- req:
		e := g.entryLocked(rec.Call)
		e.live = revision
		e.writes++
		e.currentWrites++
		g.pending++
	default:
	}
	g.gaugesLocked()
}

func (g *gridCoordinator) enqueueMetadata(rec gridstore.Record, epoch *gridIdentity, updates chan<- gridWriteRequest) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if epoch == nil {
		epoch = g.storeEpoch
	}
	if epoch != g.storeEpoch || !g.handle.Available() {
		return
	}
	select {
	case updates <- gridWriteRequest{record: rec, storeEpoch: epoch}:
	default:
	}
}

func (g *gridCoordinator) releaseWriteLocked(req gridWriteRequest) {
	if req.reportedRevision == nil {
		return
	}
	e := g.entries[req.record.Call]
	if e == nil || e.writes == 0 {
		panic("grid report owner missing")
	}
	e.writes--
	if req.storeEpoch == g.storeEpoch {
		e.currentWrites--
	}
	g.pending--
	g.releaseLocked(e)
}

// coalesce is called after receive, before the writer uses its handoff. Exactly
// one pending owner survives duplicate reports; CTY timestamps do not replace
// the chosen reported locator's revision.
func (g *gridCoordinator) coalesce(pending map[string]gridWriteRequest, req gridWriteRequest) {
	g.mu.Lock()
	defer g.mu.Unlock()
	call := req.record.Call
	if req.storeEpoch != g.storeEpoch {
		g.releaseWriteLocked(req)
		return
	}
	old, exists := pending[call]
	if exists && old.storeEpoch != req.storeEpoch {
		g.releaseWriteLocked(old)
		delete(pending, call)
		exists = false
	}
	if exists {
		rec := mergePendingGridRecord(old.record, req.record)
		if req.reportedRevision != nil {
			g.releaseWriteLocked(old)
			req.record = rec
			pending[call] = req
		} else {
			old.record = rec
			pending[call] = old
		}
	} else {
		pending[call] = req
	}
}

func (g *gridCoordinator) prepareBatch(pending map[string]gridWriteRequest) ([]gridstore.Record, *gridstore.Store) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for call := range pending {
		req := pending[call]
		if req.storeEpoch != g.storeEpoch {
			g.releaseWriteLocked(req)
			delete(pending, call)
		}
	}
	store := g.handle.Store()
	if store == nil {
		for call := range pending {
			req := pending[call]
			g.releaseWriteLocked(req)
			delete(pending, call)
		}
		return nil, nil
	}
	batch := make([]gridstore.Record, 0, len(pending))
	for call := range pending {
		batch = append(batch, pending[call].record)
	}
	return batch, store
}

func (g *gridCoordinator) finishBatch(pending map[string]gridWriteRequest, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for call := range pending {
		req := pending[call]
		if req.reportedRevision != nil && req.storeEpoch == g.storeEpoch && err == nil {
			e := g.entries[call]
			if e.live == req.reportedRevision && g.cache != nil {
				g.cache.selectGridRecord(req.record)
			}
			// Commit ordering is separate from live revisions: old A must fence
			// reads without invalidating the still-pending newer B ticket.
			e.committed = &gridIdentity{}
		}
		g.releaseWriteLocked(req)
		delete(pending, call)
	}
}
