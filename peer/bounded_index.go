package peer

import (
	"hash/maphash"
	"iter"
	"math/bits"
	"unsafe"
)

// boundedIndex owns exact keys in explicitly sized buckets. Unlike a runtime
// map, hash skew cannot grow a directory or allocate additional buckets. The
// caller supplies the existing cardinality limit and owns synchronization and
// key/value storage; strings must already have the required ownership lifetime.
// Keys used by this package are strings, string pairs, or session pointers.
type boundedIndex[K comparable, V any] struct {
	buckets []*boundedEntry[K, V]
	seed    maphash.Seed
	count   int
	limit   int
	fixed   bool
}

type boundedEntry[K comparable, V any] struct {
	key   K
	value V
	next  *boundedEntry[K, V]
}

func newBoundedIndex[K comparable, V any](limit int) *boundedIndex[K, V] {
	return &boundedIndex[K, V]{seed: maphash.MakeSeed(), limit: max(limit, 0)}
}

func newFixedIndex[K comparable, V any](limit int) *boundedIndex[K, V] {
	index := newBoundedIndex[K, V](limit)
	index.fixed = true
	if index.limit > 0 {
		index.buckets = make([]*boundedEntry[K, V], indexBucketCount(index.limit))
	}
	return index
}

func indexBucketCount(count int) int {
	if count <= 0 {
		return 0
	}
	return 1 << bits.Len(uint(count-1))
}

func (m *boundedIndex[K, V]) Len() int {
	if m == nil {
		return 0
	}
	return m.count
}

func (m *boundedIndex[K, V]) bucket(key K) int {
	return int(maphash.Comparable(m.seed, key) & uint64(len(m.buckets)-1))
}

func (m *boundedIndex[K, V]) Entry(key K) *boundedEntry[K, V] {
	if m == nil || len(m.buckets) == 0 {
		return nil
	}
	for entry := m.buckets[m.bucket(key)]; entry != nil; entry = entry.next {
		if entry.key == key {
			return entry
		}
	}
	return nil
}

func (m *boundedIndex[K, V]) Get(key K) (V, bool) {
	if entry := m.Entry(key); entry != nil {
		return entry.value, true
	}
	var zero V
	return zero, false
}

func (m *boundedIndex[K, V]) Value(key K) V {
	value, _ := m.Get(key)
	return value
}

// Set returns nil only for a new key beyond the caller's cardinality limit.
// Updates remain valid at capacity. A transaction owner must reserve all of its
// new entries and bucket overlap before its first Set; this method does not
// replace the graph's atomic admission checks. No iterator may span a Set.
func (m *boundedIndex[K, V]) Set(key K, value V) *boundedEntry[K, V] {
	if entry := m.Entry(key); entry != nil {
		entry.key, entry.value = key, value
		return entry
	}
	if m == nil || m.count >= m.limit {
		return nil
	}
	if !m.fixed && m.count >= len(m.buckets) {
		m.resize(max(1, 2*len(m.buckets)))
	}
	i := m.bucket(key)
	entry := &boundedEntry[K, V]{key: key, value: value, next: m.buckets[i]}
	m.buckets[i] = entry
	m.count++
	return entry
}

// Delete never resizes or relinks surviving entries. All permits deleting the
// currently yielded entry; callers compact only after that traversal ends.
func (m *boundedIndex[K, V]) Delete(key K) bool {
	if m == nil || len(m.buckets) == 0 {
		return false
	}
	link := &m.buckets[m.bucket(key)]
	for *link != nil {
		entry := *link
		if entry.key == key {
			*link = entry.next
			m.count--
			// A just-popped cache heap item or iterator can still hold the node.
			// Release its key/value immediately, before admitting a replacement.
			*entry = boundedEntry[K, V]{}
			return true
		}
		link = &entry.next
	}
	return false
}

// All has no snapshot allocation. The owner excludes concurrent mutation.
// Delete of the currently yielded key is allowed; inserting, compacting, or
// deleting another not-yet-yielded key during this traversal is not allowed.
func (m *boundedIndex[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		if m == nil {
			return
		}
		for _, head := range m.buckets {
			for entry := head; entry != nil; {
				next := entry.next
				if !yield(entry.key, entry.value) {
					return
				}
				entry = next
			}
		}
	}
}

// Compact is explicit so deletion cannot invalidate a traversal. The graph
// calls it at its existing quarter-occupancy boundary. Until resize returns,
// its admission charge must include both bucket arrays under the old high-water
// reservation. Entries themselves are relinked, never copied into a new set.
func (m *boundedIndex[K, V]) Compact() {
	if m == nil || m.fixed {
		return
	}
	if m.count == 0 {
		m.buckets = nil
		return
	}
	if count := indexBucketCount(m.count); count < len(m.buckets) {
		m.resize(count)
	}
}

func (m *boundedIndex[K, V]) resize(count int) {
	previous := m.buckets
	m.buckets = make([]*boundedEntry[K, V], count)
	for _, head := range previous {
		for entry := head; entry != nil; {
			next := entry.next
			i := m.bucket(entry.key)
			entry.next = m.buckets[i]
			m.buckets[i] = entry
			entry = next
		}
	}
}

// AllocationBytes counts this index's current backing, excluding separately
// owned strings/values. Transaction proofs also include old/new bucket overlap;
// this sampled current size alone cannot establish a mutation's peak.
func (m *boundedIndex[K, V]) AllocationBytes() int {
	if m == nil {
		return 0
	}
	return pointerAllocationBytes(int(unsafe.Sizeof(*m))) +
		pointerAllocationBytes(len(m.buckets)*int(unsafe.Sizeof((*boundedEntry[K, V])(nil)))) +
		m.count*pointerAllocationBytes(int(unsafe.Sizeof(boundedEntry[K, V]{})))
}
