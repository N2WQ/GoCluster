// File role: Owns fixed-bin SNR histograms for PATHP50 shadow diagnostics.
// Crawler notes: Start here when changing p50 bin geometry, retained bucket
// size, or the CPU cost of PATHP50 distribution scans. The arrays are embedded
// in path buckets, so bin-count changes directly affect retained heap.
// Related docs: pathreliability/README.md, docs/decisions/ADR-0122-path-p50-shadow-diagnostics.md.
// Related tests: pathreliability/snr_histogram_test.go, pathreliability/store_bench_test.go.
package pathreliability

import "math"

const (
	snrHistogramUnderflowBin = 0
	snrHistogramOverflowBin  = snrHistogramBinCount - 1
	snrHistogramMinDB        = -24
	snrHistogramMaxDB        = 24
	snrHistogramStepDB       = 1
	snrHistogramRangeBins    = (snrHistogramMaxDB - snrHistogramMinDB) / snrHistogramStepDB
	snrHistogramBinCount     = snrHistogramRangeBins + 2
)

type snrHistogram [snrHistogramBinCount]float32

func snrHistogramBinIndex(db float64) int {
	if math.IsNaN(db) {
		return -1
	}
	if db < snrHistogramMinDB {
		return snrHistogramUnderflowBin
	}
	if db >= snrHistogramMaxDB {
		return snrHistogramOverflowBin
	}
	return int(math.Floor((db-snrHistogramMinDB)/snrHistogramStepDB)) + 1
}

func snrHistogramBinLowerDB(index int) float64 {
	if index <= snrHistogramUnderflowBin {
		return snrHistogramMinDB
	}
	if index >= snrHistogramOverflowBin {
		return snrHistogramMaxDB
	}
	return snrHistogramMinDB + float64(index-1)*snrHistogramStepDB
}

func (h *snrHistogram) add(bin int, weight float64) {
	if h == nil || bin < 0 || bin >= snrHistogramBinCount || weight <= 0 || math.IsNaN(weight) {
		return
	}
	h[bin] += float32(weight)
}

func (h *snrHistogram) addScaled(src snrHistogram, factor float64) {
	if h == nil || factor <= 0 || math.IsNaN(factor) {
		return
	}
	f := float32(factor)
	for i, v := range src {
		if v > 0 {
			h[i] += v * f
		}
	}
}

func (h *snrHistogram) decay(factor float64) {
	if h == nil || factor == 1 {
		return
	}
	if factor <= 0 || math.IsNaN(factor) {
		*h = snrHistogram{}
		return
	}
	f := float32(factor)
	for i, v := range h {
		h[i] = v * f
	}
}

func (h snrHistogram) shifted(deltaDB float64) snrHistogram {
	if deltaDB == 0 || math.IsNaN(deltaDB) {
		return h
	}
	var shifted snrHistogram
	for i, weight := range h {
		if weight <= 0 {
			continue
		}
		db := snrHistogramBinLowerDB(i) + deltaDB
		bin := snrHistogramBinIndex(db)
		if bin >= 0 {
			shifted[bin] += weight
		}
	}
	return shifted
}

func (h snrHistogram) p50DB() (float64, bool) {
	var total float32
	for _, weight := range h {
		total += weight
	}
	if total <= 0 {
		return 0, false
	}
	target := total / 2
	var cumulative float32
	for i, weight := range h {
		cumulative += weight
		if cumulative >= target {
			return snrHistogramBinLowerDB(i), true
		}
	}
	return snrHistogramBinLowerDB(snrHistogramOverflowBin), true
}
