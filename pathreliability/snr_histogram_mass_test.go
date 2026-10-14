package pathreliability

import (
	"math"
	"math/big"
	"testing"
)

func exactMassInteger(mass snrHistogramMass) *big.Int {
	result := new(big.Int)
	for i := len(mass) - 1; i >= 0; i-- {
		result.Lsh(result, 64)
		result.Add(result, new(big.Int).SetUint64(mass[i]))
	}
	return result
}

func checkExactMassSequence(t *testing.T, weights []float32) {
	t.Helper()
	var mass snrHistogramMass
	want := new(big.Rat)
	quantumInverse := new(big.Rat).SetInt(new(big.Int).Lsh(big.NewInt(1), 149))
	for _, weight := range weights {
		mass.add(weight)
		want.Add(want, new(big.Rat).Mul(new(big.Rat).SetFloat64(float64(weight)), quantumInverse))
		if !want.IsInt() || exactMassInteger(mass).Cmp(want.Num()) != 0 {
			t.Fatalf("integer mass=%s rational=%s after weight=%g", exactMassInteger(mass), want, weight)
		}
		for _, total := range []snrHistogramMass{mass, {}, {0: 1}, {4: 1 << 26}} {
			before, beforeTotal := mass, total
			wantComparison := new(big.Int).Lsh(exactMassInteger(mass), 1).Cmp(exactMassInteger(total))
			got := mass.compareDouble(&total)
			if got != wantComparison || mass != before || total != beforeTotal {
				t.Fatalf("doubled comparison=%d want=%d mass=%v total=%v", got, wantComparison, mass, total)
			}
		}
	}
}

func TestExactP50MassEncodingAndCarries(t *testing.T) {
	for exponent := uint32(0); exponent < 255; exponent++ {
		for _, fraction := range []uint32{0, 1, 0x3fffff, 0x7fffff} {
			weight := math.Float32frombits(exponent<<23 | fraction)
			checkExactMassSequence(t, []float32{math.SmallestNonzeroFloat32, weight, weight})
		}
	}
	weights := []float32{math.SmallestNonzeroFloat32}
	for i := 0; i < 11; i++ {
		weights = append(weights, float32(math.Ldexp((1<<24)-1, 24*i-149)))
	}
	checkExactMassSequence(t, weights)
	// Fill all consecutive quantum bits before the final unit, forcing carry
	// through several complete words rather than only the current chunk.
	checkExactMassSequence(t, append(append([]float32(nil), weights[1:]...), weights[0]))
	maximum := make([]float32, 50)
	for i := range maximum {
		maximum[i] = math.MaxFloat32
	}
	checkExactMassSequence(t, maximum)
	checkExactMassSequence(t, []float32{0, math.Float32frombits(0x80000000)})
}
