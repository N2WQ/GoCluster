// File role: Bounded live CW arrival evidence for the ordinary-rename safeguard.
// Ingest owns admission. Held deliveries retain a small sequence token and can
// read only rows admitted before that token, including on equal clock readings.
package correctionflow

import (
	"cmp"
	"crypto/sha256"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"dxcluster/spot"

	"github.com/zeebo/xxh3"
)

const (
	cwBurstBuckets    = 128
	cwBurstBucketSize = 256
	cwBurstLookback   = int64(5 * time.Minute)
	cwBurstWidth      = int64(3 * time.Second)
	cwPredictorPrefix = "ccv2-reference-v1\x00"
)

// CWReceiverFamily follows the archive's receiver-base rule: SSIDs and
// portable variants of a skimmer do not constitute independent receivers.
// All 467 groups in the study's frozen alias metadata were singletons.
func CWReceiverFamily(reporter string) string {
	if reporter == "" || len(reporter) > 64 {
		return ""
	}
	key, _, _ := strings.Cut(spot.NormalizeCallsign(reporter), "-")
	if !spot.IsValidNormalizedCallsign(key) {
		return ""
	}
	// Most live reports already name the base call. The slash path uses the
	// same family authority as the study.
	if !strings.Contains(key, "/") {
		return key
	}
	vote, base, ok := spot.CorrectionFamilyKeyPair(key)
	if !ok {
		return ""
	}
	if base != "" {
		return base
	}
	return vote
}

func cwPredictorFamily(family string) bool {
	if family == "" || len(family) > 32 {
		return false
	}
	var data [len(cwPredictorPrefix) + 32]byte
	copy(data[:], cwPredictorPrefix)
	copy(data[len(cwPredictorPrefix):], family)
	sum := sha256.Sum256(data[:len(cwPredictorPrefix)+len(family)])
	return sum[0]&1 == 0
}

type cwBurstLabel [32]byte

func cwLabel(s string) (label cwBurstLabel) {
	copy(label[:], s)
	return label
}

// Labels are inline so a sliced callsign cannot keep an entire line or a
// parser allocation alive. Band is a constant from the fixed frequency table.
type cwBurstRow struct {
	call, family cwBurstLabel
	band         string
	at, hz       int64
	order        uint64
	wpm          int
}

type cwBurstBucket struct {
	rows        [cwBurstBucketSize]cwBurstRow
	next, count int
	lostThrough int64
}

// CWBurstHistory has no unbounded maps, timers or background workers. Each
// bucket owns a fixed ring. A full ring evicts one row. Any overlapping focal
// lookback fails closed even when the overwritten row was for a colliding call.
type CWBurstHistory struct {
	mu        sync.RWMutex
	order     uint64
	evictions uint64
	count     int
	buckets   [cwBurstBuckets]cwBurstBucket
}

func NewCWBurstHistory() *CWBurstHistory {
	return new(CWBurstHistory)
}

// Stats reports actual occupied slots and total overwrites in this bounded
// owner. It is independent of whether an individual veto was delivered.
func (h *CWBurstHistory) Stats() (rows int, evictions uint64) {
	if h == nil {
		return 0, 0
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.count, h.evictions
}

func (h *CWBurstHistory) bucket(call string) *cwBurstBucket {
	return &h.buckets[xxh3.HashString(call)%cwBurstBuckets]
}

func liveCWSpeed(labels []string) int {
	if len(labels) != 1 {
		return 0
	}
	digits, ok := strings.CutSuffix(labels[0], " WPM")
	if !ok {
		return 0
	}
	value, err := strconv.Atoi(digits)
	if err != nil || value < 1 || value > 200 {
		return 0
	}
	return value
}

// Observe records the original, once, before deduplication or rename. Reports
// with ambiguous/missing WPM remain in chronology but cannot confirm speed.
func (h *CWBurstHistory) Observe(s *spot.Spot) {
	if h == nil || s == nil || s.CWBurstFocal.Sequence != 0 || s.SourceType != spot.SourceRBN ||
		s.Mode != "CW" || s.IsHuman || s.IsBeacon || s.RBNCQReceivedAt.IsZero() {
		return
	}
	family := CWReceiverFamily(s.DECall)
	if !cwPredictorFamily(family) {
		return
	}
	original := s.DXCall
	band := spot.FreqToBand(s.RBNCQRawKHz)
	if band == "???" || len(original) == 0 || len(original) > 32 || !spot.IsValidNormalizedCallsign(original) {
		return
	}
	row := cwBurstRow{call: cwLabel(original), family: cwLabel(family), band: band,
		at: s.RBNCQReceivedAt.UnixNano(), hz: int64(math.Round(s.RBNCQRawKHz * 1000)), wpm: liveCWSpeed(s.CWSpeeds)}
	h.mu.Lock()
	defer h.mu.Unlock()
	b := h.bucket(original)
	if b.count == len(b.rows) {
		b.lostThrough = max(b.lostThrough, b.rows[b.next].at)
		h.evictions++
	} else {
		b.count++
		h.count++
	}
	h.order++
	row.order = h.order
	b.rows[b.next] = row
	s.CWBurstFocal = spot.CWBurstFocal{Original: original, Sequence: row.order, Slot: uint16(b.next)}
	b.next = (b.next + 1) % len(b.rows)
}

// eligible indexes are oldest first, sorted by ingress time then sequence.
// The fast path visits ring contents in admission order without sorting.
func (b *cwBurstBucket) eligible(dst []uint16, call cwBurstLabel, anchor *cwBurstRow) []uint16 {
	start := (b.next - b.count + len(b.rows)) % len(b.rows)
	ordered := true
	for n := 0; n < b.count; n++ {
		index := (start + n) % len(b.rows)
		r := &b.rows[index]
		if r.order >= anchor.order || r.call != call || r.band != anchor.band || r.family == anchor.family ||
			r.at < anchor.at-cwBurstLookback || r.at > anchor.at || r.hz < anchor.hz-300 || r.hz > anchor.hz+300 {
			continue
		}
		if len(dst) > 0 && b.rows[dst[len(dst)-1]].at > r.at {
			ordered = false
		}
		dst = append(dst, uint16(index))
	}
	if !ordered {
		slices.SortFunc(dst, func(i, j uint16) int {
			if byTime := cmp.Compare(b.rows[i].at, b.rows[j].at); byTime != 0 {
				return byTime
			}
			return cmp.Compare(b.rows[i].order, b.rows[j].order)
		})
	}
	return dst
}

// Retain answers the burst/WPM portion only. Its caller separately establishes
// the ordinary rename and an independent same-release decision to retain.
func (h *CWBurstHistory) Retain(focal spot.CWBurstFocal, winner string) bool {
	if h == nil || focal.Sequence == 0 || focal.Slot >= cwBurstBucketSize || winner == "" || len(winner) > 32 || winner == focal.Original {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	orig, other := h.bucket(focal.Original), h.bucket(winner)
	anchor := &orig.rows[focal.Slot]
	lostThrough := max(orig.lostThrough, other.lostThrough)
	if anchor.order != focal.Sequence || anchor.call != cwLabel(focal.Original) || anchor.wpm == 0 ||
		(lostThrough != 0 && lostThrough >= anchor.at-cwBurstLookback) {
		return false
	}
	var oi, wi [cwBurstBucketSize]uint16
	var op, wp [cwBurstBucketSize]cwBurstPair
	original := orig.pairs(op[:0], orig.eligible(oi[:0], anchor.call, anchor))
	replacement := other.pairs(wp[:0], other.eligible(wi[:0], cwLabel(winner), anchor))
	return cwBurstRetains(original, replacement, anchor.wpm)
}
