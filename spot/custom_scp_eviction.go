// File role: Tracks capacity victims independently of receiver expiry.
// The evidence lock protects the index and the primary entries it references.
package spot

type customSCPEvictionItem struct {
	key      customSCPKey
	entry    *customSCPEntry
	lastSeen int64
}

// Each primary entry owns one slot. Keep the indexed timestamp separate from
// the mutable entry so a refresh can compare ranks before repairing the heap.
type customSCPEvictionHeap []customSCPEvictionItem

func (h *customSCPEvictionHeap) upsert(key customSCPKey, entry *customSCPEntry, maxKeys int) {
	if entry.evictionSlot != 0 {
		i := entry.evictionSlot - 1
		old := (*h)[i].lastSeen
		if old == entry.lastSeen {
			return
		}
		(*h)[i].lastSeen = entry.lastSeen
		if entry.lastSeen < old {
			h.up(i)
		} else {
			h.down(i)
		}
		return
	}

	// Admission checks MaxKeys before creating a primary owner. Explicit growth
	// keeps backing slots at or below the same cap, including the last step.
	n := len(*h)
	if n == cap(*h) {
		capacity := n + min(max(n, 1024), maxKeys-n)
		grown := make(customSCPEvictionHeap, n, capacity)
		copy(grown, *h)
		*h = grown
	}
	*h = append(*h, customSCPEvictionItem{key: key, entry: entry, lastSeen: entry.lastSeen})
	entry.evictionSlot = n + 1
	h.up(n)
}

func (h *customSCPEvictionHeap) remove(entry *customSCPEntry) {
	if entry.evictionSlot == 0 {
		return
	}
	i := entry.evictionSlot - 1
	last := len(*h) - 1
	if i != last {
		h.swap(i, last)
	}
	entry.evictionSlot = 0
	(*h)[last] = customSCPEvictionItem{}
	*h = (*h)[:last]
	if last == 0 {
		*h = nil
	} else if i != last && !h.up(i) {
		h.down(i)
	}
}

func (h customSCPEvictionHeap) less(i, j int) bool {
	if h[i].lastSeen != h[j].lastSeen {
		return h[i].lastSeen < h[j].lastSeen
	}
	return customSCPKeyLess(h[i].key, h[j].key)
}

func (h customSCPEvictionHeap) swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].entry.evictionSlot = i + 1
	h[j].entry.evictionSlot = j + 1
}

func (h customSCPEvictionHeap) up(i int) bool {
	start := i
	for i > 0 {
		parent := (i - 1) / 2
		if !h.less(i, parent) {
			break
		}
		h.swap(i, parent)
		i = parent
	}
	return i != start
}

func (h customSCPEvictionHeap) down(i int) {
	for i < len(h)/2 {
		child := i*2 + 1
		if child+1 < len(h) && h.less(child+1, child) {
			child++
		}
		if !h.less(child, i) {
			break
		}
		h.swap(i, child)
		i = child
	}
}
