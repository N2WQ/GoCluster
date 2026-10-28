// Package bulletinhistory retains the last day of WWV and WCY bulletins.
// Its single process owner bounds records and deduplication identities, and
// never performs filesystem I/O while receiving or rendering a bulletin.
package bulletinhistory

import (
	"crypto/sha256"
	"fmt"
	"log"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	retention        = 24 * time.Hour
	maxRecords       = 48
	maxCharacters    = 256
	maxOriginalBytes = 128 << 10 // All currently admitted feed frames fit.
	maxDedupeEntries = 4096
	maxSnapshotBytes = 2 << 20
)

// Record is the displayable, bounded portion of an accepted original bulletin.
type Record struct {
	Time      time.Time `json:"time"`
	Text      string    `json:"text"`
	Truncated bool      `json:"truncated,omitempty"`
	Sequence  uint64    `json:"sequence"`
}

type identity struct {
	Kind string    `json:"kind"`
	Hash [32]byte  `json:"hash"`
	Time time.Time `json:"time"`
}

// Stats describes the bounded archive and detected loss/health conditions.
type Stats struct {
	WWVRecords, WCYRecords      int
	DedupeEntries, DedupeLimit  int
	Duplicates, DedupeEvictions uint64
	WWVLosses, WCYLosses        uint64
	StorageHealthy              bool
	StorageError                string
}

// Store owns one in-memory history and one coalescing persistence worker.
// The mutex protects records, identities and status; no disk I/O holds it.
type Store struct {
	mu               sync.Mutex
	path             string
	window           time.Duration
	maxKeys          int
	state            snapshot
	stats            Stats
	unknownUntil     time.Time
	lastStorageError string
	dirty            chan struct{}
	stop             chan struct{}
	done             chan struct{}
	closed           bool
	closeOnce        sync.Once
	closeErr         error
}

// Open loads a bounded snapshot and starts the persistence owner. Missing or
// invalid storage degrades to an empty, visibly uncertain history; live traffic
// must remain available even if the file cannot be written.
func Open(path string, dedupeWindow time.Duration, dedupeMaxEntries int) *Store {
	if dedupeWindow < 0 {
		dedupeWindow = 0
	}
	if dedupeMaxEntries < 0 {
		dedupeMaxEntries = 0
	}
	if dedupeMaxEntries > maxDedupeEntries {
		log.Printf("Bulletin history duplicate identity limit capped at %d", maxDedupeEntries)
		dedupeMaxEntries = maxDedupeEntries
	}
	s := &Store{path: path, window: dedupeWindow, maxKeys: dedupeMaxEntries,
		dirty: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
		unknownUntil: time.Now().UTC().Add(retention)}
	s.state.Version = 1
	s.stats.DedupeLimit = dedupeMaxEntries
	if err := s.load(); err != nil {
		s.storageErrorLocked(err)
		log.Printf("Bulletin history storage unavailable: %v", err)
	}
	go s.worker()
	s.signal()
	return s
}

func canonicalKind(kind string) string {
	switch strings.ToUpper(strings.TrimSpace(kind)) {
	case "WWV", "PC23":
		return "WWV"
	case "WCY", "PC73":
		return "WCY"
	default:
		return ""
	}
}

func (s *Store) signal() {
	select {
	case s.dirty <- struct{}{}:
	default:
	}
}

func (s *Store) list(kind string) *[]Record {
	if kind == "WWV" {
		return &s.state.WWV
	}
	return &s.state.WCY
}

// Record stores an accepted bulletin without waiting for disk. Identical full
// originals are suppressed before truncation; identity storage has its own cap.
func (s *Store) Record(kind, line string, received time.Time) {
	if s == nil {
		return
	}
	kind = canonicalKind(kind)
	line = strings.TrimRight(line, "\r\n")
	if kind == "" || strings.TrimSpace(line) == "" {
		return
	}
	if received.IsZero() {
		received = time.Now().UTC()
	} else {
		received = received.UTC()
	}
	if len(line) > maxOriginalBytes {
		s.ReportLoss(kind, received)
		return
	}
	originalHash := sha256.Sum256([]byte(line))
	text, truncated := truncate(line)
	// Keep at most the bounded display text, not the original's backing array.
	text = strings.Clone(text)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	changed := s.pruneLocked(received)
	if received.Before(s.state.Watermark.Add(-retention)) {
		if changed {
			s.signal()
		}
		return
	}
	if s.window > 0 && s.maxKeys > 0 {
		for _, key := range s.state.Identities {
			if key.Kind != kind || key.Hash != originalHash {
				continue
			}
			delta := received.Sub(key.Time)
			if delta < 0 {
				delta = key.Time.Sub(received)
			}
			if delta <= s.window {
				s.stats.Duplicates++
				if changed {
					s.signal()
				}
				return
			}
		}
		for len(s.state.Identities) >= s.maxKeys {
			s.state.Identities = slices.Delete(s.state.Identities, 0, 1)
			s.stats.DedupeEvictions++
		}
		s.state.Identities = append(s.state.Identities, identity{Kind: kind, Hash: originalHash, Time: received})
	}
	s.state.Sequence++
	items := s.list(kind)
	*items = append(*items, Record{Time: received, Text: text, Truncated: truncated, Sequence: s.state.Sequence})
	slices.SortFunc(*items, compareRecord)
	for len(*items) > maxRecords {
		removed := (*items)[0]
		*items = slices.Delete(*items, 0, 1)
		s.reportLossLocked(kind, removed.Time)
	}
	s.signal()
}

func compareRecord(a, b Record) int {
	if a.Time.Before(b.Time) {
		return -1
	}
	if a.Time.After(b.Time) {
		return 1
	}
	if a.Sequence < b.Sequence {
		return -1
	}
	if a.Sequence > b.Sequence {
		return 1
	}
	return 0
}

func truncate(line string) (string, bool) {
	if !utf8.ValidString(line) {
		line = strings.ToValidUTF8(line, "\uFFFD")
	}
	if utf8.RuneCountInString(line) <= maxCharacters {
		return line, false
	}
	count := 0
	for i := range line {
		if count == maxCharacters {
			return line[:i], true
		}
		count++
	}
	return line, false
}

// ReportLoss marks a known dropped bulletin for the affected type.
func (s *Store) ReportLoss(kind string, at time.Time) {
	if s == nil {
		return
	}
	kind = canonicalKind(kind)
	if kind == "" {
		return
	}
	if at.IsZero() {
		at = time.Now().UTC()
	} else {
		at = at.UTC()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.pruneLocked(at)
	s.reportLossLocked(kind, at)
	s.signal()
}

func (s *Store) reportLossLocked(kind string, at time.Time) {
	if kind == "WWV" {
		s.stats.WWVLosses++
		if at.After(s.state.WWVLossAt) {
			s.state.WWVLossAt = at
		}
	} else {
		s.stats.WCYLosses++
		if at.After(s.state.WCYLossAt) {
			s.state.WCYLossAt = at
		}
	}
}

// pruneLocked never treats a future timestamp as expired; decoded future
// timestamps are rejected at load rather than resurrecting future records.
func (s *Store) pruneLocked(now time.Time) bool {
	if now.After(s.state.Watermark) {
		s.state.Watermark = now
	} else {
		now = s.state.Watermark
	}
	before := len(s.state.WWV) + len(s.state.WCY) + len(s.state.Identities)
	cutoff := now.Add(-retention)
	s.state.WWV = slices.DeleteFunc(s.state.WWV, func(r Record) bool { return r.Time.Before(cutoff) })
	s.state.WCY = slices.DeleteFunc(s.state.WCY, func(r Record) bool { return r.Time.Before(cutoff) })
	if s.window <= 0 || s.maxKeys <= 0 {
		s.state.Identities = nil
		return before != len(s.state.WWV)+len(s.state.WCY)
	}
	// A delayed bulletin can arrive up to a full retained window after another
	// feed supplied the same original. Keep older identities for that delay as
	// well as the configured dedupe window. The cardinality cap still governs.
	span := s.window
	if span > time.Duration(math.MaxInt64)-retention {
		span = time.Duration(math.MaxInt64)
	} else {
		span += retention
	}
	dedupeCutoff := now.Add(-span)
	s.state.Identities = slices.DeleteFunc(s.state.Identities, func(k identity) bool { return k.Time.Before(dedupeCutoff) })
	return before != len(s.state.WWV)+len(s.state.WCY)+len(s.state.Identities)
}

// Render snapshots one type independently of the caller's delivery filter.
func (s *Store) Render(kind string, full bool, now time.Time) string {
	limit := 1
	if full {
		limit = 0
	}
	return s.render(kind, limit, now)
}

// RenderRecent returns the newest five bulletins in receipt order.
func (s *Store) RenderRecent(kind string, now time.Time) string {
	return s.render(kind, 5, now)
}

func (s *Store) render(kind string, limit int, now time.Time) string {
	if s == nil {
		return "Bulletin history is unavailable.\n"
	}
	kind = canonicalKind(kind)
	if kind == "" {
		return "Unknown bulletin type.\n"
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	s.mu.Lock()
	if s.pruneLocked(now) {
		s.signal()
	}
	effectiveNow := s.state.Watermark
	items := slices.Clone(*s.list(kind))
	lossAt := s.state.WWVLossAt
	if kind == "WCY" {
		lossAt = s.state.WCYLossAt
	}
	unknown := effectiveNow.Before(s.unknownUntil) || !s.stats.StorageHealthy
	s.mu.Unlock()
	if limit > 0 && len(items) > limit {
		items = items[len(items)-limit:]
	}
	var b strings.Builder
	if len(items) == 0 {
		fmt.Fprintf(&b, "No %s bulletin in the last 24 hours.\n", kind)
	}
	for _, record := range items {
		fmt.Fprintf(&b, "%s received %s UTC: %s", kind, record.Time.UTC().Format("2006-01-02 15:04:05"), record.Text)
		if record.Truncated {
			b.WriteString(" [truncated]")
		}
		b.WriteByte('\n')
	}
	if !lossAt.IsZero() && !lossAt.Before(effectiveNow.Add(-retention)) {
		fmt.Fprintf(&b, "%s history is incomplete: one or more bulletins were lost.\n", kind)
	} else if unknown {
		fmt.Fprintf(&b, "%s history may be incomplete.\n", kind)
	}
	return b.String()
}

func (s *Store) Stats() Stats {
	if s == nil {
		return Stats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pruneLocked(time.Now().UTC()) {
		s.signal()
	}
	out := s.stats
	out.WWVRecords, out.WCYRecords = len(s.state.WWV), len(s.state.WCY)
	out.DedupeEntries = len(s.state.Identities)
	return out
}

func (s *Store) storageErrorLocked(err error) bool {
	s.stats.StorageHealthy = false
	s.stats.StorageError = err.Error()
	s.unknownUntil = time.Now().UTC().Add(retention)
	changed := s.lastStorageError != err.Error()
	s.lastStorageError = err.Error()
	return changed
}

// Close stops the worker and attempts the final snapshot exactly once.
func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.stop)
		<-s.done
	})
	return s.closeErr
}
