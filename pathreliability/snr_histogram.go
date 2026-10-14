// File role: Owns fixed-bin SNR histograms for active p50 path scoring.
// Crawler notes: Start here when changing p50 bin geometry, retained bucket
// size, or active p50 scoring cost. The arrays are embedded in path buckets, so
// bin-count changes directly affect retained heap.
// Related docs: pathreliability/README.md, docs/decisions/ADR-0126-active-p50-path-scoring.md.
// Related tests: pathreliability/snr_histogram_test.go, pathreliability/snr_histogram_precision_test.go, pathreliability/store_bench_test.go.
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

func snrHistogramBinRepresentativeDB(index int) float64 {
	if index <= snrHistogramUnderflowBin {
		return snrHistogramMinDB
	}
	if index >= snrHistogramOverflowBin {
		return snrHistogramMaxDB
	}
	return snrHistogramBinLowerDB(index) + float64(snrHistogramStepDB)/2
}

func finiteHistogramBin(weight float32) bool {
	// IEEE binary32 encodings let the usual nonnegative finite case use one
	// integer comparison. Negative zero is also a valid empty bin. This check
	// is repeated across fixed arrays, so it avoids widened floating comparisons.
	bits := math.Float32bits(weight)
	return bits <= 0x7f7fffff || bits == 0x80000000
}

// usableHistogramIncrement tests the weighted contribution, not its factor.
// A representable increment remains usable when addition rounds it away.
func usableHistogramIncrement(weight float64) bool {
	if !(weight > 0) {
		return false
	}
	rounded := float32(weight)
	return rounded > 0 && finiteHistogramBin(rounded)
}

func (h *snrHistogram) addition(bin int, weight float64) (float32, bool) {
	if h == nil || bin < 0 || bin >= snrHistogramBinCount || !usableHistogramIncrement(weight) || !finiteHistogramBin(h[bin]) {
		return 0, false
	}
	next := float32(float64(h[bin]) + weight)
	return next, finiteHistogramBin(next)
}

// canAdd lets the bucket validate a prospective write before changing counters
// or receiver ownership. It does not mutate the histogram.
func (h *snrHistogram) canAdd(bin int, weight float64) bool {
	_, ok := h.addition(bin, weight)
	return ok
}

func (h *snrHistogram) add(bin int, weight float64) bool {
	next, ok := h.addition(bin, weight)
	if !ok {
		return false
	}
	h[bin] = next
	return true
}

// addScaled admits one complete component into scratch built from checked
// writes. It validates the entire source and every prospective destination
// write, committing atomically. Final p50 validation checks the whole assembly.
// Zero excludes the source; a positive component whose weighted bins all vanish
// fails even if other destination bins remain usable. Neither pointer escapes.
func (h *snrHistogram) addScaled(src *snrHistogram, factor float64) bool {
	if h == nil || factor < 0 || math.IsNaN(factor) || math.IsInf(factor, 0) {
		return false
	}
	if factor == 0 {
		return true
	}
	next := *h
	hasSource, hasIncrement := false, false
	for i := range src {
		weight := src[i]
		if !finiteHistogramBin(weight) {
			return false
		}
		if weight == 0 {
			continue
		}
		hasSource = true
		product := float64(weight) * factor
		rounded := float32(product)
		if !finiteHistogramBin(rounded) {
			return false
		}
		if rounded == 0 {
			continue
		}
		hasIncrement = true
		if !finiteHistogramBin(next[i]) {
			return false
		}
		next[i] = float32(float64(next[i]) + product)
		if !finiteHistogramBin(next[i]) {
			return false
		}
	}
	if factor > 0 && (!hasSource || !hasIncrement) {
		return false
	}
	*h = next
	return true
}

// decay multiplies before binary32 rounding. Underflow is normal quantization,
// including an all-zero result; callers detect lost evidence with hasUsableMass
// or p50DB. Invalid arithmetic leaves all prior bins intact.
func (h *snrHistogram) decay(factor float64) bool {
	if h == nil || factor < 0 || math.IsNaN(factor) || math.IsInf(factor, 0) {
		return false
	}
	if factor == 0 {
		*h = snrHistogram{}
		return true
	}
	if factor == 1 {
		for _, weight := range h {
			if !finiteHistogramBin(weight) {
				return false
			}
		}
		return true
	}
	var next snrHistogram
	for i := range h {
		weight := h[i]
		if weight == 0 {
			continue
		}
		// Check the source before rounding: a negative subnormal could otherwise
		// become valid negative zero and hide an unsafe contributing component.
		if !finiteHistogramBin(weight) {
			return false
		}
		next[i] = float32(float64(weight) * factor)
		if !finiteHistogramBin(next[i]) {
			return false
		}
	}
	*h = next
	return true
}

func (h *snrHistogram) hasUsableMass() bool {
	hasMass := false
	for _, weight := range h {
		if !finiteHistogramBin(weight) {
			return false
		}
		hasMass = hasMass || weight > 0
	}
	return hasMass
}

func (h *snrHistogram) shifted(deltaDB float64) (snrHistogram, bool) {
	if math.IsNaN(deltaDB) || math.IsInf(deltaDB, 0) {
		return snrHistogram{}, false
	}
	var shifted snrHistogram
	for i := range h {
		weight := h[i]
		if !finiteHistogramBin(weight) {
			return snrHistogram{}, false
		}
		if weight == 0 {
			continue
		}
		bin := snrHistogramBinIndex(snrHistogramBinLowerDB(i) + deltaDB)
		if !shifted.add(bin, float64(weight)) {
			return snrHistogram{}, false
		}
	}
	return shifted, true
}

func (h *snrHistogram) p50DB() (float64, bool) {
	var total snrHistogramMass
	hasMass := false
	for _, weight := range h {
		if !finiteHistogramBin(weight) {
			return 0, false
		}
		if weight > 0 {
			total.add(weight)
			hasMass = true
		}
	}
	if !hasMass {
		return 0, false
	}
	// Validate every bin before selecting. Exact integer comparison prevents
	// tiny retained bins from manufacturing a tie or hiding a genuine one.
	var cumulative snrHistogramMass
	for i, weight := range h {
		if weight == 0 {
			continue
		}
		cumulative.add(weight)
		comparison := cumulative.compareDouble(&total)
		if comparison >= 0 {
			selected := snrHistogramBinRepresentativeDB(i)
			if comparison == 0 {
				if next, ok := h.nextNonEmptyRepresentativeDB(i + 1); ok {
					return (selected + next) / 2, true
				}
			}
			return selected, true
		}
	}
	return 0, false
}

func (h *snrHistogram) nextNonEmptyRepresentativeDB(start int) (float64, bool) {
	if start < 0 {
		start = 0
	}
	for i := start; i < len(h); i++ {
		if h[i] > 0 {
			return snrHistogramBinRepresentativeDB(i), true
		}
	}
	return 0, false
}
