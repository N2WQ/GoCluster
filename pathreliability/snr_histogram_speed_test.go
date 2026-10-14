package pathreliability

import (
	"math"
	"math/big"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

func TestExactP50SparseSelections(t *testing.T) {
	q := float32(math.SmallestNonzeroFloat32)
	weights := []float32{q, math.Float32frombits(0x007fffff), math.Float32frombits(0x00800000),
		math.Float32frombits(0x3f7fffff), 1, math.Float32frombits(0x3f800001), math.MaxFloat32}
	for i := 0; i < 50; i++ {
		for _, weight := range weights {
			var h snrHistogram
			h[i] = weight
			checkExactP50Reference(t, h)
		}
	}
	pairs := [][2]float32{{q, q}, {q, 2 * q}, {q, math.MaxFloat32}, {1, 1},
		{1, math.Float32frombits(0x3f7fffff)}, {1, math.Float32frombits(0x3f800001)},
		{math.Float32frombits(0x007fffff), math.Float32frombits(0x00800000)}, {math.MaxFloat32, math.MaxFloat32}}
	for i := 0; i < 50; i++ {
		for j := i + 1; j < 50; j++ {
			for _, pair := range pairs {
				var h snrHistogram
				h[i], h[j] = pair[0], pair[1]
				checkExactP50Reference(t, h)
				h[i], h[j] = pair[1], pair[0]
				checkExactP50Reference(t, h)
			}
		}
	}
}

func TestExactP50SparseValidation(t *testing.T) {
	for _, seed := range []snrHistogram{{}, {0: 1}, {0: 1, 1: 1},
		{0: 1, 1: math.SmallestNonzeroFloat32, 2: 1}} {
		for i := 0; i < 50; i++ {
			for _, encoding := range []uint32{0x80000001, 0xbf800000, 0x7f800000, 0xff800000, 0x7fc00001, 0x7f800001} {
				h := seed
				h[i] = math.Float32frombits(encoding)
				checkExactP50Reference(t, h)
			}
		}
		for i := range seed {
			if seed[i] == 0 {
				seed[i] = math.Float32frombits(0x80000000)
			}
		}
		checkExactP50Reference(t, seed)
	}
	// An apparently complete pair cannot select before a third late mass.
	checkExactP50Reference(t, snrHistogram{0: 1, 1: 1, 49: math.SmallestNonzeroFloat32})
	checkExactP50Reference(t, snrHistogram{0: .5, 25: 1, 49: .5})
	checkExactP50Reference(t, snrHistogram{0: math.SmallestNonzeroFloat32,
		25: math.Float32frombits(0x00800000), 49: math.SmallestNonzeroFloat32})
}

func TestExactP50CertificateBoundaries(t *testing.T) {
	for _, tc := range []struct {
		count, span int
		want        bool
	}{{3, 27, true}, {3, 28, false}, {50, 23, true}, {50, 24, false},
		{4, 27, true}, {5, 27, false}, {8, 26, true}, {9, 26, false},
		{16, 25, true}, {17, 25, false}, {32, 24, true}, {33, 24, false}} {
		if got := snrHistogramFloat64Exact(tc.count, tc.span); got != tc.want {
			t.Fatalf("count=%d span=%d certificate=%v want=%v", tc.count, tc.span, got, tc.want)
		}
		h := speedCertificateHistogram(tc.count, tc.span, 0x7fffff)
		checkExactP50Reference(t, h)
	}
	// Subnormal and smallest-normal values use the same lattice shift, 0.
	checkExactP50Reference(t, snrHistogram{0: math.SmallestNonzeroFloat32,
		25: math.Float32frombits(0x007fffff), 49: math.Float32frombits(0x00800000)})
}

// The first mass is one quantum; all other masses have encoded exponent
// span+1. Placing the last at bin49 exercises late count growth independently
// of the extrema. The construction does not call the production certificate.
func speedCertificateHistogram(count, span int, tailFraction uint32) snrHistogram {
	h := snrHistogram{0: math.SmallestNonzeroFloat32}
	for i := 1; i < count-1; i++ {
		h[i] = math.Float32frombits(uint32(span+1) << 23)
	}
	h[49] = math.Float32frombits(uint32(span+1)<<23 | tailFraction)
	return h
}

func TestExactP50FallbackHandoff(t *testing.T) {
	q := float32(math.SmallestNonzeroFloat32)
	for _, h := range []snrHistogram{
		{2: 2, 19: float32(math.Ldexp(1, -56)), 35: 2},
		{5: 1, 30: float32(math.Ldexp(1, -100)), 35: 1, 45: float32(math.Ldexp(1, -100))},
		{0: 2, 10: 1, 20: 1, 49: q},
		{0: q, 10: q, 20: 2 * q, 35: 1, 49: 1},
		{0: q, 10: q, 20: 2 * q, 35: 1, 49: 2},
	} {
		checkExactP50Reference(t, h)
	}
	for _, count := range []int{5, 9, 17, 33} {
		ceiling := 0
		for capacity := 1; capacity < count-1; capacity *= 2 {
			ceiling++
		}
		for _, fraction := range []uint32{0, 1, 0x3fffff, 0x7fffff} {
			h := speedCertificateHistogram(count, 29-ceiling, fraction)
			checkExactP50Reference(t, h)
			// Invalid evidence after a late handoff still invalidates everything.
			h[47], h[49] = h[49], 0
			checkExactP50Reference(t, h)
			h[48] = math.Float32frombits(0x7fc00001)
			checkExactP50Reference(t, h)
		}
	}
}

func TestExactP50FallbackCarries(t *testing.T) {
	var h snrHistogram
	for i := 0; i < 11; i++ {
		h[i] = float32(math.Ldexp((1<<24)-1, 24*(10-i)-149))
	}
	// Fill consecutive quantum bits first, then carry from the lowest bit
	// through four complete words. The late quantum must survive handoff.
	h[49] = math.SmallestNonzeroFloat32
	checkExactP50Reference(t, h)
	checkExactMassSequence(t, append(append([]float32(nil), h[:11]...), h[49]))
}

func TestExactP50CertifiedSums(t *testing.T) {
	random := rand.New(rand.NewPCG(0x0302, 0x9f34b88))
	for sample := 0; sample < 400; sample++ {
		count := 3 + random.IntN(48)
		ceiling := 0
		for capacity := 1; capacity < count; capacity *= 2 {
			ceiling++
		}
		span := random.IntN(30 - ceiling)
		minimum := random.IntN(254 - span)
		positions := random.Perm(50)
		var h snrHistogram
		for i := 0; i < count; i++ {
			shift := minimum + random.IntN(span+1)
			switch i {
			case 0:
				shift = minimum
			case 1:
				shift = minimum + span
			}
			fraction := random.Uint32() & 0x7fffff
			encoding := uint32(shift+1)<<23 | fraction
			if shift == 0 && i%2 == 0 {
				encoding = max(fraction, 1)
			}
			h[positions[i]] = math.Float32frombits(encoding)
		}
		checkExactP50Reference(t, h)
		// Independently check the certificate's exact total/prefix claim, not
		// only a median that might coincide despite inaccurate sums.
		var sum float64
		rational := new(big.Rat)
		for i, weight := range h {
			sum += float64(weight)
			rational.Add(rational, new(big.Rat).SetFloat64(float64(weight)))
			if new(big.Rat).SetFloat64(sum).Cmp(rational) != 0 {
				t.Fatalf("certified prefix rounded: sample=%d bin=%d", sample, i)
			}
		}
	}
}

func speedDispatchHistograms() []snrHistogram {
	var dense snrHistogram
	for i := range dense {
		dense[i] = math.MaxFloat32
	}
	return []snrHistogram{{}, {49: math.SmallestNonzeroFloat32},
		{0: math.SmallestNonzeroFloat32, 49: math.MaxFloat32},
		{5: 1, 30: 2, 49: 1}, dense,
		{2: 2, 19: float32(math.Ldexp(1, -56)), 35: 2},
		speedCertificateHistogram(5, 27, 0x7fffff)}
}

func TestExactP50Dispatch(t *testing.T) {
	for _, h := range speedDispatchHistograms() {
		checkExactP50Reference(t, h)
	}
}

func TestExactP50SpeedAllocs(t *testing.T) {
	for i, h := range speedDispatchHistograms() {
		want, wantOK := exactP50Reference(h)
		var got float64
		var ok bool
		allocations := testing.AllocsPerRun(1000, func() { got, ok = h.p50DB() })
		if allocations != 0 || got != want || ok != wantOK {
			t.Fatalf("branch=%d allocs=%g p50=%g/%v want=%g/%v", i, allocations, got, ok, want, wantOK)
		}
	}
}

func TestExactP50Concurrent(t *testing.T) {
	histograms := speedDispatchHistograms()
	wants := make([]float64, len(histograms))
	available := make([]bool, len(histograms))
	snapshots := make([][50]uint32, len(histograms))
	for i := range histograms {
		wants[i], available[i] = exactP50Reference(histograms[i])
		snapshots[i] = exactP50Snapshot(&histograms[i])
	}
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Go(func() {
			for iteration := 0; iteration < 200; iteration++ {
				i := (iteration + worker) % len(histograms)
				got, ok := histograms[i].p50DB()
				if got != wants[i] || ok != available[i] || exactP50Snapshot(&histograms[i]) != snapshots[i] {
					t.Errorf("concurrent branch=%d p50=%g/%v expected=%g/%v", i, got, ok, wants[i], available[i])
					return
				}
			}
		})
	}
	workers.Wait()
}

func TestExactP50ScalarLookup(t *testing.T) {
	for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
		cfg := DefaultConfig()
		cfg.ReceiverContributionMode = mode
		p := NewPredictor(cfg, []string{"20m"})
		at := time.Unix(1_700_000_000, 0).UTC()
		for _, db := range []float64{-5, 5} {
			p.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", db, 1,
				ObservationTimes{ObservedAt: at, ArrivedAt: at}, false, 1)
		}
		fine, coarse := p.combined.Lookup(1, 2, 3, 4, "20m", at)
		scalars := p.combined.lookupCandidateSet(1, 2, 3, 4, "20m", at, false, sampleScalars)
		for i, sample := range []Sample{fine, coarse} {
			if sample.Weight != 2 || sample.Count != 2 ||
				sample.RawCount != 2 || sample.AgeSec != 0 || !sample.HasSamplingTime || sample.SamplingUnix != at.Unix() {
				t.Fatalf("%s scalar-only lookup changed: %+v", mode, sample)
			}
			if scalars[i].HasP50 || scalars[i].P50DB != 0 || sample != scalars[i].Sample {
				t.Fatalf("%s scalar route computed a quantile or differs from public lookup: %+v", mode, scalars[i])
			}
		}
		quantiles := p.combined.lookupCandidateSet(1, 2, 3, 4, "20m", at, false, sampleQuantile)
		for _, sample := range quantiles[:2] {
			want, ok := exactP50Reference(sample.snrBins)
			if !ok || want != .5 || !sample.HasP50 || sample.P50DB != want || sample.distributionUnavailable {
				t.Fatalf("%s explicit quantile differs from independent oracle: %+v want=%g/%v", mode, sample.Sample, want, ok)
			}
		}
	}
}

func FuzzExactP50Certificate(f *testing.F) {
	for _, seed := range [][]byte{{0, 0, 0}, {47, 0, 23, 255}, {2, 200, 255}, {30, 0, 24, 1}} {
		f.Add(seed, false)
		f.Add(seed, true)
	}
	f.Fuzz(func(t *testing.T, data []byte, outside bool) {
		if len(data) < 3 || len(data) > 200 {
			return
		}
		count := 3 + int(data[0])%48
		ceiling := 0
		for capacity := 1; capacity < count; capacity *= 2 {
			ceiling++
		}
		span := int(data[2]) % (30 - ceiling)
		if outside {
			span = 30 - ceiling
		}
		minimum := int(data[1]) % (254 - span)
		var h snrHistogram
		for i := 0; i < count; i++ {
			shift := minimum + int(data[i%len(data)])%(span+1)
			switch i {
			case 0:
				shift = minimum
			case 1:
				shift = minimum + span
			}
			fraction := uint32(data[(i+1)%len(data)]) * 0x10101 & 0x7fffff
			h[(17*i+int(data[2]))%50] = math.Float32frombits(uint32(shift+1)<<23 | fraction)
		}
		checkExactP50Reference(t, h)
	})
}
