// File role: Owns bounded, coherent path-store diagnostic scans and publications.
// Crawler notes: Cached counts and fresh weight histograms expose sampling time
// and availability; they never add bucket-count-sized scratch history.
// Related docs: pathreliability/README.md, docs/decisions/ADR-0300-path-observation-time-accounting.md.
// Related tests: stats_test.go, store_reporting_test.go.
package pathreliability

import (
	"math"
	"time"
)

type pathStoreStatsSnapshot struct {
	available   bool
	hasSnapshot bool
	asOfUnix    int64
	fine        int
	coarse      int
	byBand      []bandCounts
}

type pathWeightHistogramSnapshot struct {
	available bool
	asOfUnix  int64
	byBand    []weightHistogram
}

type bandCounts struct {
	fine   int
	coarse int
}

type weightHistogram struct {
	total int
	bins  []int
}

// Stats returns the last refreshed counts of active fine/coarse buckets.
func (s *Store) Stats(_ time.Time) (fine int, coarse int) {
	if s == nil || !s.cfg.Enabled {
		return 0, 0
	}
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	return s.stats.fine, s.stats.coarse
}

// StatsByBand returns the last refreshed counts of active fine/coarse buckets
// per band.
func (s *Store) StatsByBand(_ time.Time) []bandCounts {
	return s.statsSnapshot().byBand
}

// statsSnapshot copies one cached publication, so totals and per-band counts
// cannot come from different refreshes. A failed refresh preserves the dated
// previous counts with available=false; hasSnapshot distinguishes those from
// initial zero counters. The copied slice is caller-owned and band-bounded.
func (s *Store) statsSnapshot() pathStoreStatsSnapshot {
	if s == nil || !s.cfg.Enabled {
		return pathStoreStatsSnapshot{}
	}
	s.statsMu.RLock()
	defer s.statsMu.RUnlock()
	result := s.stats
	result.byBand = append([]bandCounts(nil), result.byBand...)
	return result
}

// RefreshStatsSnapshot recomputes the cached active-bucket counts used by
// operator-facing stats displays. It is intentionally explicit so ingest writes
// never scan the whole store.
func (s *Store) RefreshStatsSnapshot(now time.Time) {
	if s == nil || !s.cfg.Enabled {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	} else {
		now = now.UTC()
	}
	nowSec := now.Unix()

	s.statsRefreshMu.Lock()
	defer s.statsRefreshMu.Unlock()

	bands := s.bandIndex.idxToBand
	counts := make([]bandCounts, len(bands))
	staleAfterByBand := make([]int64, len(bands))
	for i, band := range bands {
		halfLife := s.bandIndex.HalfLifeSeconds(band, s.cfg)
		staleAfterByBand[i] = s.staleAfterSeconds(halfLife)
	}
	fine, coarse := 0, 0
	asOf, available := boundedReportScan(nowSec, func(reference int64) (int64, bool) {
		clear(counts)
		fine, coarse = 0, 0
		latest := reference
		for i := range s.shards {
			sh := &s.shards[i]
			sh.mu.RLock()
			for key, b := range sh.buckets {
				if b == nil {
					continue
				}
				if b.lastUpdate > latest {
					latest = b.lastUpdate
				}
				idx, isCoarse := reportBucketBand(key)
				if idx >= len(counts) || reportBucketExpired(b, reference, staleAfterByBand[idx]) {
					continue
				}
				if isCoarse {
					counts[idx].coarse++
					coarse++
				} else {
					counts[idx].fine++
					fine++
				}
			}
			sh.mu.RUnlock()
		}
		return latest, true
	})
	s.publishStatsSnapshot(pathStoreStatsSnapshot{
		available: available, hasSnapshot: available,
		asOfUnix: asOf, fine: fine, coarse: coarse, byBand: counts,
	})
}

// publishStatsSnapshot is called under statsRefreshMu by the sole refresh
// owner. Older coherent attempts cannot rewind a newer cached publication or
// restore its availability after a failed scan. No historical snapshots are kept.
func (s *Store) publishStatsSnapshot(snapshot pathStoreStatsSnapshot) {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	if !snapshot.available {
		s.stats.available = false
		return
	}
	if s.stats.hasSnapshot && snapshot.asOfUnix < s.stats.asOfUnix {
		return
	}
	s.stats = snapshot
}

// WeightHistogramByBand returns per-band bucket weight histograms for active buckets.
// edges defines the ascending bin boundaries (len+1 bins, last bin is >= last edge).
func (s *Store) WeightHistogramByBand(now time.Time, edges []float64) []weightHistogram {
	snapshot := s.weightHistogramSnapshot(now, edges)
	if !snapshot.available {
		return nil
	}
	return snapshot.byBand
}

// weightHistogramSnapshot scans without retaining bucket-sized scratch. Each
// bin classifies mass decayed from its accounting reference at one common time;
// expiry uses the independent newest observation. Failure discards the entire
// attempted distribution, preventing a partial histogram from appearing valid.
func (s *Store) weightHistogramSnapshot(now time.Time, edges []float64) pathWeightHistogramSnapshot {
	if s == nil || !s.cfg.Enabled {
		return pathWeightHistogramSnapshot{}
	}
	bands := s.bandIndex.idxToBand
	if len(bands) == 0 || len(edges) == 0 {
		return pathWeightHistogramSnapshot{}
	}
	binCount := len(edges) + 1
	counts := make([]weightHistogram, len(bands))
	for i := range counts {
		counts[i].bins = make([]int, binCount)
	}
	halfLives := make([]int, len(bands))
	for i, band := range bands {
		halfLives[i] = s.bandIndex.HalfLifeSeconds(band, s.cfg)
	}
	staleAfterByBand := make([]int64, len(bands))
	for i, halfLife := range halfLives {
		staleAfterByBand[i] = s.staleAfterSeconds(halfLife)
	}
	asOf, available := boundedReportScan(now.UTC().Unix(), func(reference int64) (int64, bool) {
		for i := range counts {
			counts[i].total = 0
			clear(counts[i].bins)
		}
		latest := reference
		usable := true
		for i := range s.shards {
			sh := &s.shards[i]
			sh.mu.RLock()
			for key, b := range sh.buckets {
				if b == nil {
					continue
				}
				if b.lastUpdate > latest {
					latest = b.lastUpdate
				}
				idx, _ := reportBucketBand(key)
				if idx >= len(counts) || reportBucketExpired(b, reference, staleAfterByBand[idx]) {
					continue
				}
				if b.weight < 0 || math.IsNaN(b.weight) || math.IsInf(b.weight, 0) {
					usable = false
					continue
				}
				weight, _ := agedContribution(b.weight, nonnegativeSecondsSince(reference, b.lastUpdate), halfLives[idx])
				if weight == 0 {
					continue
				}
				counts[idx].total++
				counts[idx].bins[weightBinIndex(weight, edges)]++
			}
			sh.mu.RUnlock()
		}
		return latest, usable
	})
	if !available {
		return pathWeightHistogramSnapshot{asOfUnix: asOf}
	}
	return pathWeightHistogramSnapshot{available: true, asOfUnix: asOf, byBand: counts}
}

// boundedReportScan discovers the horizon while doing the first attempt. Only
// a coherent pass can publish: references newer than that pass's time require
// exactly one complete retry. Even a provisionally unsafe pass must finish
// horizon discovery, since its unsafe bucket might expire at the final time.
// Scratch is band-bounded; there is no global watermark or bucket history.
func boundedReportScan(query int64, scan func(int64) (int64, bool)) (int64, bool) {
	latest, usable := scan(query)
	if latest <= query {
		return query, usable
	}
	reference := latest
	latest, usable = scan(reference)
	return reference, usable && latest <= reference
}

func reportBucketBand(key uint64) (index int, coarse bool) {
	if key&0xFFFF != 0 {
		return int(uint16(key >> 32)), true
	}
	return int(uint16(key >> 48)), false
}

func reportBucketExpired(b *bucket, reference, staleAfter int64) bool {
	return staleAfter > 0 && nonnegativeSecondsSince(reference, b.newestObservation) > staleAfter
}

func weightBinIndex(weight float64, edges []float64) int {
	for i, edge := range edges {
		if weight < edge {
			return i
		}
	}
	return len(edges)
}
