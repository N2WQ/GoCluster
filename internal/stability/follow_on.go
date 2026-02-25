package stability

import (
	"math"
	"sort"
)

// Config controls follow-on stability evaluation.
type Config struct {
	BucketMinutes   int
	WindowMinutes   int
	MinFollowOn     int
	FreqToleranceHz float64
}

// Spot is one winner-sighting observation in event time.
type Spot struct {
	Ts   int64
	Freq float64
}

// Correction is one applied correction event to evaluate.
type Correction struct {
	Ts     int64
	Winner string
	Freq   float64
	Band   string
}

// Bucket accumulates stability counts for one time bucket.
type Bucket struct {
	Start  int64
	Total  int
	Stable int
}

// BandResult tracks stability counts per band.
type BandResult struct {
	Total  int
	Stable int
}

// Result is the aggregate output of follow-on stability evaluation.
type Result struct {
	StableCount int
	TotalCount  int
	Buckets     []Bucket
	Bands       map[string]BandResult
}

// NormalizeConfig applies stable defaults that match daily-analysis behavior.
func NormalizeConfig(cfg Config) Config {
	if cfg.BucketMinutes <= 0 {
		cfg.BucketMinutes = 60
	}
	if cfg.WindowMinutes <= 0 {
		cfg.WindowMinutes = 60
	}
	if cfg.MinFollowOn <= 0 {
		cfg.MinFollowOn = 2
	}
	if cfg.FreqToleranceHz <= 0 {
		cfg.FreqToleranceHz = 1000
	}
	return cfg
}

// EvaluateFollowOn computes winner follow-on stability.
//
// Semantics intentionally match the legacy daily follow-on stability method:
//   - stable when winner has at least MinFollowOn spots within WindowMinutes
//     and within FreqToleranceHz (around correction frequency),
//   - bucketed by correction timestamp relative to minTs.
func EvaluateFollowOn(corrections []Correction, winnerSpots map[string][]Spot, minTs int64, cfg Config) Result {
	cfg = NormalizeConfig(cfg)
	tolKhz := cfg.FreqToleranceHz / 1000.0
	horizon := int64(cfg.WindowMinutes * 60)
	bucketSize := int64(cfg.BucketMinutes * 60)

	bucketMap := make(map[int64]*Bucket)
	bandMap := make(map[string]BandResult)
	stableCount := 0

	for _, corr := range corrections {
		list := winnerSpots[corr.Winner]
		totalHorizon := 0
		if len(list) > 0 {
			startIdx := binarySearchSpots(list, corr.Ts)
			for i := startIdx; i < len(list); i++ {
				if list[i].Ts > corr.Ts+horizon {
					break
				}
				if math.Abs(list[i].Freq-corr.Freq) <= tolKhz {
					totalHorizon++
					if totalHorizon >= cfg.MinFollowOn {
						break
					}
				}
			}
		}

		stable := totalHorizon >= cfg.MinFollowOn
		if stable {
			stableCount++
		}

		band := corr.Band
		if band == "" {
			band = "unknown"
		}
		bs := bandMap[band]
		bs.Total++
		if stable {
			bs.Stable++
		}
		bandMap[band] = bs

		bucketStart := minTs
		if bucketSize > 0 {
			bucketStart = minTs + ((corr.Ts - minTs) / bucketSize * bucketSize)
		}
		b := bucketMap[bucketStart]
		if b == nil {
			b = &Bucket{Start: bucketStart}
			bucketMap[bucketStart] = b
		}
		b.Total++
		if stable {
			b.Stable++
		}
	}

	buckets := make([]Bucket, 0, len(bucketMap))
	for _, v := range bucketMap {
		buckets = append(buckets, *v)
	}
	sortBucketsByStart(buckets)

	return Result{
		StableCount: stableCount,
		TotalCount:  len(corrections),
		Buckets:     buckets,
		Bands:       bandMap,
	}
}

func binarySearchSpots(sp []Spot, target int64) int {
	lo, hi := 0, len(sp)
	for lo < hi {
		mid := (lo + hi) / 2
		if sp[mid].Ts < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func sortBucketsByStart(buckets []Bucket) {
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].Start < buckets[j].Start })
}
