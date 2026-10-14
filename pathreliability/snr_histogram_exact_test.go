package pathreliability

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"testing"
	"time"
)

// The reference converts each stored mass through exact binary64 widening into
// rational arithmetic. It deliberately does not decode IEEE fields or reuse
// production accumulation, validation, or representative helpers.
func exactP50Reference(h snrHistogram) (float64, bool) {
	total := new(big.Rat)
	for _, weight := range h {
		value := float64(weight)
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, false
		}
		total.Add(total, new(big.Rat).SetFloat64(value))
	}
	if total.Sign() == 0 {
		return 0, false
	}
	cumulative := new(big.Rat)
	for i, weight := range h {
		if weight == 0 {
			continue
		}
		cumulative.Add(cumulative, new(big.Rat).SetFloat64(float64(weight)))
		comparison := new(big.Rat).Mul(cumulative, big.NewRat(2, 1)).Cmp(total)
		if comparison < 0 {
			continue
		}
		selected := exactP50Representative(i)
		if comparison == 0 {
			for j := i + 1; j < len(h); j++ {
				if h[j] > 0 {
					return (selected + exactP50Representative(j)) / 2, true
				}
			}
		}
		return selected, true
	}
	return 0, false
}

func exactP50Representative(i int) float64 {
	if i == 0 {
		return -24
	}
	if i == 49 {
		return 24
	}
	return float64(i) - 24.5
}

func exactP50Snapshot(h *snrHistogram) [50]uint32 {
	var snapshot [50]uint32
	for i, value := range h {
		snapshot[i] = math.Float32bits(value)
	}
	return snapshot
}

func checkExactP50Reference(t *testing.T, h snrHistogram) {
	t.Helper()
	before := exactP50Snapshot(&h)
	want, wantOK := exactP50Reference(h)
	got, ok := h.p50DB()
	if ok != wantOK || got != want {
		t.Fatalf("p50=%g available=%v, rational=%g/%v, bins=%v", got, ok, want, wantOK, before)
	}
	if after := exactP50Snapshot(&h); after != before {
		t.Fatalf("p50 mutated histogram bits: before=%v after=%v", before, after)
	}
}

func TestExactP50Golden(t *testing.T) {
	q := float32(math.Ldexp(1, -149))
	tiny := float32(math.Ldexp(1, -100))
	cases := []struct {
		name string
		h    snrHistogram
		want float64
	}{
		{"owner_false_tie", snrHistogram{5: 1, 30: tiny, 45: 1}, 5.5},
		{"owner_true_tie", snrHistogram{5: 1, 30: tiny, 35: 1, 45: tiny}, 8},
		{"owner_smallest_tail", snrHistogram{49: q}, 24},
		{"reviewer_false_tie", snrHistogram{2: 2, 19: float32(math.Ldexp(1, -56)), 35: 2}, -5.5},
		{"full_range", snrHistogram{0: math.MaxFloat32, 25: q, 49: math.MaxFloat32}, .5},
		{"smallest_tie", snrHistogram{0: q, 49: q}, 0},
		{"smallest_imbalance_left", snrHistogram{0: 2 * q, 49: q}, -24},
		{"smallest_imbalance_right", snrHistogram{0: q, 49: 2 * q}, 24},
		{"empty_gap_tie", snrHistogram{0: 1, 10: 1, 40: 2}, .5},
		{"adjacent_tail_tie", snrHistogram{0: 1, 1: 1}, -23.75},
		{"adjacent_float_above", snrHistogram{0: 1, 49: math.Float32frombits(0x3f800001)}, 24},
		{"adjacent_float_below", snrHistogram{0: 1, 49: math.Float32frombits(0x3f7fffff)}, -24},
	}
	var lostTie snrHistogram
	lostTie[0], lostTie[40], lostTie[49] = 1, float32(math.Ldexp(1, -50)), 1
	for i := 1; i <= 8; i++ {
		lostTie[i] = float32(math.Ldexp(1, -53))
	}
	cases = append(cases, struct {
		name string
		h    snrHistogram
		want float64
	}{"lost_true_tie", lostTie, -.5})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, ok := tc.h.p50DB(); !ok || got != tc.want {
				t.Fatalf("p50=%g/%v, literal=%g", got, ok, tc.want)
			}
			checkExactP50Reference(t, tc.h)
		})
	}
}

func TestExactP50FullDomain(t *testing.T) {
	for exponent := uint32(0); exponent < 255; exponent++ {
		for _, fraction := range []uint32{0, 1, 0x3fffff, 0x7fffff} {
			weight := math.Float32frombits(exponent<<23 | fraction)
			checkExactP50Reference(t, snrHistogram{0: weight, 25: math.SmallestNonzeroFloat32, 49: weight})
		}
	}
	for _, weight := range []float32{math.MaxFloat32, math.SmallestNonzeroFloat32} {
		var h snrHistogram
		for i := range h {
			h[i] = weight
		}
		checkExactP50Reference(t, h)
	}
	// Eleven 24-bit chunks plus one quantum sum to 2^264 quanta. This
	// propagates carries through every lower word, including into word five.
	var carry snrHistogram
	carry[0] = math.SmallestNonzeroFloat32
	for i := 0; i < 11; i++ {
		carry[i+1] = float32(math.Ldexp((1<<24)-1, 24*i-149))
	}
	carry[49] = float32(math.Ldexp(1, 115))
	for _, perturbation := range []float32{0, math.SmallestNonzeroFloat32, 2 * math.SmallestNonzeroFloat32} {
		carry[0] = perturbation
		checkExactP50Reference(t, carry)
	}
	random := rand.New(rand.NewPCG(0x684d145, 0x0301))
	for sample := 0; sample < 400; sample++ {
		var h snrHistogram
		for i := range h {
			encoding := random.Uint32() & 0x7fffffff
			if encoding >= 0x7f800000 {
				encoding &^= 0x00800000
			}
			h[i] = math.Float32frombits(encoding)
		}
		checkExactP50Reference(t, h)
	}
}

func TestExactP50ValidationAndImmutability(t *testing.T) {
	checkExactP50Reference(t, snrHistogram{})
	var negativeZeros snrHistogram
	for i := range negativeZeros {
		negativeZeros[i] = math.Float32frombits(0x80000000)
	}
	checkExactP50Reference(t, negativeZeros)
	negativeZeros[49] = math.SmallestNonzeroFloat32
	checkExactP50Reference(t, negativeZeros)
	for i := 0; i < 50; i++ {
		for _, encoding := range []uint32{0x80000001, 0xbf800000, 0x7f800000, 0xff800000, 0x7fc00001, 0x7f800001} {
			h := snrHistogram{0: 100}
			h[i] = math.Float32frombits(encoding)
			checkExactP50Reference(t, h)
		}
	}
}

func TestExactP50ShippedPrediction(t *testing.T) {
	cfg, err := LoadFile("../data/config/path_reliability.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReceiverContributionMode != ReceiverContributionEnforce || cfg.MinObservationCount != 21 || cfg.ReceiverMaxEffectiveCount != 8 {
		t.Fatalf("fixture requires shipped enforcement gates: %+v", cfg)
	}
	for _, mode := range []string{ReceiverContributionEnforce, ReceiverContributionOff, ReceiverContributionShadow} {
		t.Run(mode, func(t *testing.T) {
			modeConfig := cfg
			modeConfig.ReceiverContributionMode = mode
			p := NewPredictor(modeConfig, cfg.AllowedBands)
			start := time.Unix(1_700_000_000, 0).UTC()
			halfLife := time.Duration(p.combined.bandIndex.HalfLifeSeconds("20m", cfg)) * time.Second
			p.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", -6, 1, ObservationTimes{ObservedAt: start, ArrivedAt: start}, false, 1)
			for i := 1; i <= 56; i++ {
				at := start.Add(time.Duration(i) * halfLife)
				p.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", -23, 1, ObservationTimes{ObservedAt: at, ArrivedAt: at}, false, uint64(i%3+1))
				p.UpdateWithReceiverHash(BucketCombined, 1, 2, 3, 4, "20m", 10, 1, ObservationTimes{ObservedAt: at, ArrivedAt: at}, false, uint64((i+1)%3+1))
			}
			query := start.Add(56 * halfLife)
			index, present := p.combined.bandIndex.Lookup("20m")
			if !present {
				t.Fatal("shipped 20m band missing")
			}
			key := packCoarseKey(3, 4, index)
			state := p.combined.shards[key%uint64(len(p.combined.shards))].buckets[key]
			if state == nil {
				t.Fatal("shipped coarse bucket missing")
			}
			bins := state.snrBins
			if bins[2] != 2 || bins[19] != float32(math.Ldexp(1, -56)) || bins[35] != 2 {
				t.Fatalf("shipped retained bins differ from reviewed vector: %v", bins)
			}
			for _, beacon := range []bool{false, true} {
				t.Run(fmt.Sprintf("beacon=%v", beacon), func(t *testing.T) {
					result := p.Predict(1, 2, 3, 4, "20m", "FT8", 0, query)
					if beacon {
						result = p.PredictBeaconReceiveOnlyWithMinObservationCount(1, 2, 3, 4, "20m", "FT8", 0, cfg.BeaconMinObservationCount, query)
					}
					if result.Source != SourceCombined || !result.HasP50 || !result.ObservedDistributionAvailable || !result.HasObservedSamplingTime || result.ObservedSamplingUnix != query.Unix() || result.P50DB != -5.5 || result.Class != classHigh || result.RawCount != 113 {
						t.Fatalf("shipped observed prediction=%+v", result)
					}
					wantRequired := uint32(3)
					if beacon {
						wantRequired = 2 // Shipped beacon floor 11 with count cap 8.
					}
					if mode == ReceiverContributionEnforce && (result.ReceiverCount != 3 || result.ReceiverRequired != wantRequired) {
						t.Fatalf("shipped receiver gates were bypassed: %+v", result)
					}
				})
			}
		})
	}
}

func FuzzExactP50(f *testing.F) {
	for _, h := range []snrHistogram{
		{}, {49: math.SmallestNonzeroFloat32},
		{5: 1, 30: float32(math.Ldexp(1, -100)), 45: 1},
		{0: math.MaxFloat32, 25: math.SmallestNonzeroFloat32, 49: math.MaxFloat32},
		{0: math.Float32frombits(0x80000000), 25: math.Float32frombits(0x7fc00001)},
	} {
		data := make([]byte, 200)
		for i, weight := range h {
			binary.LittleEndian.PutUint32(data[4*i:], math.Float32bits(weight))
		}
		f.Add(data, false)
		f.Add(data, true)
	}
	f.Fuzz(func(t *testing.T, data []byte, validOnly bool) {
		if len(data) > 200 {
			return
		}
		var h snrHistogram
		for i := 0; i < len(data)/4; i++ {
			encoding := binary.LittleEndian.Uint32(data[4*i:])
			if validOnly && encoding != 0x80000000 {
				encoding &= 0x7fffffff
				if encoding >= 0x7f800000 {
					encoding &^= 0x00800000
				}
			}
			h[i] = math.Float32frombits(encoding)
		}
		checkExactP50Reference(t, h)
	})
}
