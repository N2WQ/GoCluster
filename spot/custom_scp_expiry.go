// File role: Custom SCP secondary expiry heaps and key indexes. The structures
// are bounded by CustomSCPStore primary maps, and delete/pop paths keep indexes
// coupled so cleanup does not retain stale side state.
package spot

const customSCPImmediateCleanupDueUnix int64 = -1 << 63

// customSCPEntryExpiryItem links one observation key to the next time cleanup
// should inspect it. The heap is a secondary index for CustomSCPStore.entries,
// so delete paths must remove or invalidate the matching item.
type customSCPEntryExpiryItem struct {
	key     customSCPKey
	dueUnix int64
	index   int
}

// customSCPEntryExpiryQueue is a min-heap plus key index. It exists so cleanup
// can find stale or oversized observation entries without scanning every key on
// every interval; root pops maintain the index incrementally so cleanup churn
// stays proportional to removed entries instead of active retained state.
type customSCPEntryExpiryQueue struct {
	items        []customSCPEntryExpiryItem
	indexes      map[customSCPKey]int
	indexesDirty bool
}

// newCustomSCPEntryExpiryQueue pre-sizes the secondary index to the expected
// active observation cardinality; the primary MaxKeys cap still owns the bound.
func newCustomSCPEntryExpiryQueue(capacity int) customSCPEntryExpiryQueue {
	return customSCPEntryExpiryQueue{
		items:   make([]customSCPEntryExpiryItem, 0, capacity),
		indexes: make(map[customSCPKey]int, capacity),
	}
}

func (h *customSCPEntryExpiryQueue) Len() int {
	if h == nil {
		return 0
	}
	return len(h.items)
}

// Less orders by cleanup due time first and key second so repeated runs are
// deterministic when multiple entries expire at the same second.
func (h *customSCPEntryExpiryQueue) Less(i, j int) bool {
	if h.items[i].dueUnix != h.items[j].dueUnix {
		return h.items[i].dueUnix < h.items[j].dueUnix
	}
	return customSCPKeyLess(h.items[i].key, h.items[j].key)
}

func (h *customSCPEntryExpiryQueue) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
	if !h.indexesDirty {
		h.indexes[h.items[i].key] = i
		h.indexes[h.items[j].key] = j
	}
}

func (h *customSCPEntryExpiryQueue) push(item customSCPEntryExpiryItem) {
	h.ensureIndexes()
	item.index = len(h.items)
	h.items = append(h.items, item)
	h.indexes[item.key] = item.index
	h.up(item.index)
}

func (h *customSCPEntryExpiryQueue) popLast() customSCPEntryExpiryItem {
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = customSCPEntryExpiryItem{}
	item.index = -1
	h.items = old[:n-1]
	if !h.indexesDirty {
		delete(h.indexes, item.key)
	}
	return item
}

func (h *customSCPEntryExpiryQueue) popRoot() {
	h.remove(0)
}

func (h *customSCPEntryExpiryQueue) remove(idx int) {
	n := h.Len() - 1
	if idx < 0 || idx > n {
		return
	}
	if idx != n {
		h.Swap(idx, n)
		if !h.down(idx, n) {
			h.up(idx)
		}
	}
	h.popLast()
}

func (h *customSCPEntryExpiryQueue) fix(idx int) {
	if !h.down(idx, h.Len()) {
		h.up(idx)
	}
}

func (h *customSCPEntryExpiryQueue) up(idx int) {
	for {
		parent := (idx - 1) / 2
		if idx == 0 || !h.Less(idx, parent) {
			break
		}
		h.Swap(parent, idx)
		idx = parent
	}
}

func (h *customSCPEntryExpiryQueue) down(idx, n int) bool {
	start := idx
	for {
		left := 2*idx + 1
		if left >= n || left < 0 {
			break
		}
		child := left
		if right := left + 1; right < n && h.Less(right, left) {
			child = right
		}
		if !h.Less(child, idx) {
			break
		}
		h.Swap(idx, child)
		idx = child
	}
	return idx > start
}

func (h *customSCPEntryExpiryQueue) ensureIndexes() {
	if h.indexes != nil && !h.indexesDirty {
		return
	}
	h.indexes = make(map[customSCPKey]int, len(h.items)+1)
	for i := range h.items {
		h.items[i].index = i
		h.indexes[h.items[i].key] = i
	}
	h.indexesDirty = false
}

type customSCPStaticExpiryItem struct {
	call    string
	dueUnix int64
	index   int
}

// customSCPStaticExpiryQueue mirrors the observation expiry heap for the
// static-membership map. It is separate because static calls have a longer
// horizon and different primary owner than observation entries.
type customSCPStaticExpiryQueue struct {
	items        []customSCPStaticExpiryItem
	indexes      map[string]int
	indexesDirty bool
}

// newCustomSCPStaticExpiryQueue pre-sizes the static secondary index; the
// static map and horizon cleanup own its retained cardinality.
func newCustomSCPStaticExpiryQueue(capacity int) customSCPStaticExpiryQueue {
	return customSCPStaticExpiryQueue{
		items:   make([]customSCPStaticExpiryItem, 0, capacity),
		indexes: make(map[string]int, capacity),
	}
}

func (h *customSCPStaticExpiryQueue) Len() int {
	if h == nil {
		return 0
	}
	return len(h.items)
}

// Less keeps static cleanup deterministic for equal timestamps.
func (h *customSCPStaticExpiryQueue) Less(i, j int) bool {
	if h.items[i].dueUnix != h.items[j].dueUnix {
		return h.items[i].dueUnix < h.items[j].dueUnix
	}
	return h.items[i].call < h.items[j].call
}

func (h *customSCPStaticExpiryQueue) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
	if !h.indexesDirty {
		h.indexes[h.items[i].call] = i
		h.indexes[h.items[j].call] = j
	}
}

func (h *customSCPStaticExpiryQueue) push(item customSCPStaticExpiryItem) {
	h.ensureIndexes()
	item.index = len(h.items)
	h.items = append(h.items, item)
	h.indexes[item.call] = item.index
	h.up(item.index)
}

func (h *customSCPStaticExpiryQueue) popLast() customSCPStaticExpiryItem {
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = customSCPStaticExpiryItem{}
	item.index = -1
	h.items = old[:n-1]
	if !h.indexesDirty {
		delete(h.indexes, item.call)
	}
	return item
}

func (h *customSCPStaticExpiryQueue) popRoot() {
	h.remove(0)
}

func (h *customSCPStaticExpiryQueue) remove(idx int) {
	n := h.Len() - 1
	if idx < 0 || idx > n {
		return
	}
	if idx != n {
		h.Swap(idx, n)
		if !h.down(idx, n) {
			h.up(idx)
		}
	}
	h.popLast()
}

func (h *customSCPStaticExpiryQueue) fix(idx int) {
	if !h.down(idx, h.Len()) {
		h.up(idx)
	}
}

func (h *customSCPStaticExpiryQueue) up(idx int) {
	for {
		parent := (idx - 1) / 2
		if idx == 0 || !h.Less(idx, parent) {
			break
		}
		h.Swap(parent, idx)
		idx = parent
	}
}

func (h *customSCPStaticExpiryQueue) down(idx, n int) bool {
	start := idx
	for {
		left := 2*idx + 1
		if left >= n || left < 0 {
			break
		}
		child := left
		if right := left + 1; right < n && h.Less(right, left) {
			child = right
		}
		if !h.Less(child, idx) {
			break
		}
		h.Swap(idx, child)
		idx = child
	}
	return idx > start
}

func (h *customSCPStaticExpiryQueue) ensureIndexes() {
	if h.indexes != nil && !h.indexesDirty {
		return
	}
	h.indexes = make(map[string]int, len(h.items)+1)
	for i := range h.items {
		h.items[i].index = i
		h.indexes[h.items[i].call] = i
	}
	h.indexesDirty = false
}

// refreshEntryAgesLocked recomputes age bounds from retained spotters after
// inserts, trims, or load. Cleanup uses oldestSeenUnix to know when an entry can
// still contain stale observations.
func (s *CustomSCPStore) refreshEntryAgesLocked(entry *customSCPEntry) {
	if entry == nil || len(entry.spotters) == 0 {
		if entry != nil {
			entry.lastSeen = 0
			entry.oldestSeenUnix = 0
		}
		return
	}
	latest := int64(0)
	oldest := int64(0)
	for _, spotter := range entry.spotters {
		seenUnix := decodeCustomSCPSpotterSeenUnix(spotter.seenUnix)
		if seenUnix > latest {
			latest = seenUnix
		}
		if oldest == 0 || seenUnix < oldest {
			oldest = seenUnix
		}
	}
	entry.lastSeen = latest
	entry.oldestSeenUnix = oldest
}

// entryCleanupDueUnix returns the next cleanup trigger for one observation
// entry. Oversized entries are marked immediately so overflow cannot remain
// until the normal age horizon.
func (s *CustomSCPStore) entryCleanupDueUnix(entry *customSCPEntry) int64 {
	if entry == nil || len(entry.spotters) == 0 {
		return 0
	}
	if len(entry.spotters) > s.opts.MaxSpottersPerKey {
		return customSCPImmediateCleanupDueUnix
	}
	return entry.oldestSeenUnix
}

// upsertEntryExpiryLocked keeps the observation expiry heap coupled to the
// primary entries map. Callers hold s.mu.
func (s *CustomSCPStore) upsertEntryExpiryLocked(key customSCPKey, entry *customSCPEntry) {
	if s == nil {
		return
	}
	if entry == nil || len(entry.spotters) == 0 {
		s.deleteEntryExpiryLocked(key)
		return
	}
	dueUnix := s.entryCleanupDueUnix(entry)
	if dueUnix == 0 {
		s.deleteEntryExpiryLocked(key)
		return
	}
	s.entryExpiry.ensureIndexes()
	if idx, ok := s.entryExpiry.indexes[key]; ok {
		// Warm reads and same-second refreshes commonly preserve both ranks.
		if s.entryExpiry.items[idx].dueUnix == dueUnix {
			return
		}
		s.entryExpiry.items[idx].dueUnix = dueUnix
		s.entryExpiry.fix(idx)
		return
	}
	s.entryExpiry.push(customSCPEntryExpiryItem{key: key, dueUnix: dueUnix})
}

// deleteEntryExpiryLocked removes the secondary heap/index item when the primary
// observation entry is deleted. Callers hold s.mu.
func (s *CustomSCPStore) deleteEntryExpiryLocked(key customSCPKey) {
	if s == nil {
		return
	}
	s.entryExpiry.ensureIndexes()
	idx, ok := s.entryExpiry.indexes[key]
	if !ok {
		return
	}
	s.entryExpiry.remove(idx)
}

// markEntryForCleanupLocked refreshes age metadata shared by the two indexes.
// Cleanup uses the oldest retained receiver and capacity eviction the newest.
func (s *CustomSCPStore) markEntryForCleanupLocked(key customSCPKey, entry *customSCPEntry) {
	if s == nil {
		return
	}
	s.refreshEntryAgesLocked(entry)
	s.upsertEntryExpiryLocked(key, entry)
	if entry != nil && len(entry.spotters) > 0 {
		s.eviction.upsert(key, entry, s.opts.MaxKeys)
	}
}

// popDueEntryExpiryLocked returns one currently due primary entry and discards
// stale heap entries caused by deletes or timestamp updates. Callers hold s.mu.
func (s *CustomSCPStore) popDueEntryExpiryLocked(observationCutoff int64) (customSCPKey, *customSCPEntry, bool) {
	for s != nil && s.entryExpiry.Len() > 0 {
		item := s.entryExpiry.items[0]
		entry := s.entries[item.key]
		if entry == nil {
			s.entryExpiry.popRoot()
			continue
		}
		if item.dueUnix != s.entryCleanupDueUnix(entry) {
			s.upsertEntryExpiryLocked(item.key, entry)
			continue
		}
		if item.dueUnix >= observationCutoff {
			return customSCPKey{}, nil, false
		}
		s.entryExpiry.popRoot()
		return item.key, entry, true
	}
	return customSCPKey{}, nil, false
}

// upsertStaticExpiryLocked keeps the static-membership expiry heap coupled to
// the static map. Callers hold s.mu.
func (s *CustomSCPStore) upsertStaticExpiryLocked(call string, seenUnix int64) {
	if s == nil || call == "" || seenUnix <= 0 {
		s.deleteStaticExpiryLocked(call)
		return
	}
	s.staticExpiry.ensureIndexes()
	if idx, ok := s.staticExpiry.indexes[call]; ok {
		s.staticExpiry.items[idx].dueUnix = seenUnix
		s.staticExpiry.fix(idx)
		return
	}
	s.staticExpiry.push(customSCPStaticExpiryItem{call: call, dueUnix: seenUnix})
}

// deleteStaticExpiryLocked removes the secondary heap/index item for a static
// call when the primary static map no longer owns it. Callers hold s.mu.
func (s *CustomSCPStore) deleteStaticExpiryLocked(call string) {
	if s == nil || call == "" {
		return
	}
	s.staticExpiry.ensureIndexes()
	idx, ok := s.staticExpiry.indexes[call]
	if !ok {
		return
	}
	s.staticExpiry.remove(idx)
}

// popDueStaticExpiryLocked returns one currently stale static call and drops
// obsolete heap entries after updates/deletes. Callers hold s.mu.
func (s *CustomSCPStore) popDueStaticExpiryLocked(staticCutoff int64) (string, bool) {
	for s != nil && s.staticExpiry.Len() > 0 {
		item := s.staticExpiry.items[0]
		seen := s.static[item.call]
		if seen <= 0 {
			s.staticExpiry.popRoot()
			continue
		}
		if item.dueUnix != seen {
			s.upsertStaticExpiryLocked(item.call, seen)
			continue
		}
		if item.dueUnix >= staticCutoff {
			return "", false
		}
		s.staticExpiry.popRoot()
		return item.call, true
	}
	return "", false
}
