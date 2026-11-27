// File role: Exact oldest-path selection for delay overflows under the fallback mutex.
package pathreliability

import (
	"slices"
	"time"
)

const voacapDelayShortlistSize = 64

// The delay map owns the full entry. A deleted or refreshed snapshot can
// remain in these fixed slots until pop; it cannot evict without validation.
type voacapDelayVictim struct {
	key      voacapDelayKey
	lastSeen time.Time
}

func compareDelayVictim(at time.Time, key *voacapDelayKey, b *voacapDelayVictim) int {
	if order := at.Compare(b.lastSeen); order != 0 {
		return order
	}
	return -compareLiveDelayKey(key, &b.key)
}

// Keep the least eligible of the selected entries at the root while scanning.
func siftDelayVictim(list []voacapDelayVictim, parent int) {
	item := list[parent]
	for child := 2*parent + 1; child < len(list); child = 2*parent + 1 {
		right := child + 1
		if right < len(list) && compareDelayVictim(list[right].lastSeen, &list[right].key, &list[child]) > 0 {
			child = right
		}
		if compareDelayVictim(item.lastSeen, &item.key, &list[child]) >= 0 {
			break
		}
		list[parent] = list[child]
		parent = child
	}
	list[parent] = item
}

// Allocate only on the first full-table admission. Later refills reuse at most
// min(64, MaxDelayEntries) slots, regardless of traffic, removals or sweep size.
func (f *VOACAPClosedFallback) rebuildDelayShortlistLocked() {
	if f.delayShortlist == nil {
		f.delayShortlist = make([]voacapDelayVictim, 0, min(voacapDelayShortlistSize, f.fallback.MaxDelayEntries))
	}
	list := f.delayShortlist[:0]
	for key, entry := range f.delays {
		if len(list) < cap(list) {
			list = append(list, voacapDelayVictim{key, entry.lastSeen})
			if len(list) == cap(list) {
				for parent := len(list)/2 - 1; parent >= 0; parent-- {
					siftDelayVictim(list, parent)
				}
			}
		} else if compareDelayVictim(entry.lastSeen, &key, &list[0]) < 0 {
			list[0] = voacapDelayVictim{key, entry.lastSeen}
			siftDelayVictim(list, 0)
		}
	}
	slices.SortFunc(list, func(a, b voacapDelayVictim) int {
		return compareDelayVictim(b.lastSeen, &b.key, &a)
	})
	// Every admitted band equals a configured band literal. Use that literal
	// so a stale snapshot cannot pin a caller's larger input string.
	for i := range list {
		for _, freq := range f.fallback.CenterFrequenciesMHz {
			if band := bandForMHz(freq); band == list[i].key.band {
				list[i].key.band = band
				break
			}
		}
	}
	f.delayCutoff = list[0].lastSeen
	f.delayShortlist = list
}

func (f *VOACAPClosedFallback) evictDelayLocked() {
	for {
		if len(f.delayShortlist) == 0 {
			f.rebuildDelayShortlistLocked()
		}
		last := len(f.delayShortlist) - 1
		victim := f.delayShortlist[last]
		f.delayShortlist[last] = voacapDelayVictim{}
		f.delayShortlist = f.delayShortlist[:last]
		if entry, ok := f.delays[victim.key]; ok && entry.lastSeen.Compare(victim.lastSeen) == 0 {
			delete(f.delays, victim.key)
			return
		}
	}
}

// Writes at or before the captured cutoff can newly outrank a remaining
// candidate, including through the typed equal-time tie. Later writes can only
// move a key away from eviction and are checked lazily at pop.
func (f *VOACAPClosedFallback) invalidateDelayShortlistLocked() {
	clear(f.delayShortlist)
	f.delayShortlist = f.delayShortlist[:0]
}
