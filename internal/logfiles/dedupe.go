package logfiles

import (
	"bytes"
	"math"
	"strconv"
	"time"
	"unsafe"

	"dxcluster/internal/asynclog"
)

const dedupeSlots = 512

// PeerProcessorBacking estimates the fixed table and formatting workspace;
// it does not include queues, paths, filesystem work, or producer state.
func PeerProcessorBacking() uint64 {
	const page = uint64(8192)
	size := uint64(unsafe.Sizeof(deduper{}))
	return (size+page-1)&^(page-1) + 2*page
}

// NewPeerProcessor creates a worker-owned deduper. Non-positive windows
// intentionally disable deduplication.
func NewPeerProcessor(window time.Duration) Processor {
	if window <= 0 {
		return nil
	}
	return &deduper{window: window}
}

type dedupeEntry struct {
	key                [asynclog.MaxRecordBytes]byte
	length             uint16
	nextEmit, lastSeen int64
	suppressed         uint64
	saturated          bool
}

// deduper is owned by exactly one configured stream, used only by its group's
// worker. It retains at most 512 exact keys;
// eviction follows the prior event/peer oldest-last-seen policy. Candidate
// writes remain staged until the sink confirms their complete physical write.
type deduper struct {
	entries                [dedupeSlots]dedupeEntry
	used                   int
	window                 time.Duration
	pending                bool
	pendingIndex           int
	pendingAt, pendingNext int64
}

// Prepare commits intentional suppression, but a candidate output never clears
// prior suppression until Complete(true). It uses only the supplied scratch.
func (d *deduper) Prepare(at time.Time, line []byte, output *[asynclog.MaxRecordBytes]byte) (int, ProcessDecision) {
	if d.pending || len(line) > len(output) {
		return 0, ProcessDrop
	}
	if d.window <= 0 {
		return copy(output[:], line), ProcessWrite
	}
	now := at.UnixNano()
	oldest := 0
	for i := 0; i < d.used; i++ {
		entry := &d.entries[i]
		if entry.lastSeen < d.entries[oldest].lastSeen {
			oldest = i
		}
		if int(entry.length) != len(line) || !bytes.Equal(entry.key[:entry.length], line) {
			continue
		}
		if now < entry.nextEmit {
			entry.lastSeen = now
			if entry.suppressed == math.MaxUint64 {
				entry.saturated = true
			} else {
				entry.suppressed++
			}
			return 0, ProcessSuppress
		}
		n := copy(output[:], line)
		if entry.suppressed > 0 {
			var suffixStorage [128]byte
			suffix := suffixStorage[:0]
			if entry.saturated {
				suffix = append(suffix, " suppressed>="...)
			} else {
				suffix = append(suffix, " suppressed="...)
			}
			suffix = strconv.AppendUint(suffix, entry.suppressed, 10)
			suffix = append(suffix, " window="...)
			suffix = append(suffix, d.window.String()...)
			if len(line)+len(suffix) <= len(output) {
				n += copy(output[n:], suffix)
			}
		}
		d.pending = true
		d.pendingIndex = i
		d.pendingAt = now
		d.pendingNext = at.Add(d.window).UnixNano()
		return n, ProcessWrite
	}
	index := oldest
	if d.used < len(d.entries) {
		index = d.used
	}
	d.pending = true
	d.pendingIndex = index
	d.pendingAt = now
	d.pendingNext = at.Add(d.window).UnixNano()
	return copy(output[:], line), ProcessWrite
}

func (d *deduper) Complete(input []byte, written bool) {
	if !d.pending {
		return
	}
	defer func() { d.pending = false }()
	if !written {
		return
	}
	entry := &d.entries[d.pendingIndex]
	*entry = dedupeEntry{length: uint16(len(input)), nextEmit: d.pendingNext, lastSeen: d.pendingAt}
	copy(entry.key[:], input)
	if d.pendingIndex == d.used {
		d.used++
	}
}
