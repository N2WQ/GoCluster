package pathreliability

import (
	"math"
	"testing"
)

const (
	precisionWeakBin   = 5  // -20 dB lower edge, -19.5 dB representative.
	precisionStrongBin = 11 // -14 dB lower edge, -13.5 dB representative.
)

func TestSNRHistogramPrecisionFiniteEncodings(t *testing.T) {
	for _, c := range []struct {
		bits   uint32
		finite bool
	}{
		{0, true}, {0x80000000, true}, {1, true}, {0x7f7fffff, true},
		{0xbf800000, false}, {0x7f800000, false}, {0xff800000, false},
		{0x7f800001, false}, {0xffc00000, false},
	} {
		if got := finiteHistogramBin(math.Float32frombits(c.bits)); got != c.finite {
			t.Fatalf("encoding=%08x finite=%v want=%v", c.bits, got, c.finite)
		}
	}
}

func TestSNRHistogramPrecisionDecayCannotSanitizeNegativeUnderflow(t *testing.T) {
	var hist snrHistogram
	hist[precisionWeakBin] = -math.SmallestNonzeroFloat32
	hist[precisionStrongBin] = 1
	before := hist
	if math.Float32bits(float32(float64(hist[precisionWeakBin])*.5)) != 0x80000000 {
		t.Fatal("fixture did not produce negative zero")
	}
	if hist.decay(.5) || hist != before {
		t.Fatal("decay hid a negative source by rounding it to zero")
	}
	if _, available := hist.p50DB(); available {
		t.Fatal("invalid source supplied a surviving median")
	}
}

func TestSNRHistogramPrecisionIncrementBoundary(t *testing.T) {
	quantum := math.Ldexp(1, -149)
	halfQuantum := math.Ldexp(1, -150)
	for _, tc := range []struct {
		name   string
		weight float64
		ok     bool
		want   float64
	}{
		{"half_quantum_tie", halfQuantum, false, 0},
		{"above_half_quantum", math.Nextafter(halfQuantum, math.Inf(1)), true, quantum},
		{"quantum", quantum, true, quantum},
		{"finite_maximum", math.MaxFloat32, true, math.MaxFloat32},
		{"finite_conversion_overflow", math.Ldexp(1, 128), false, 0},
		{"zero", 0, false, 0},
		{"negative", -quantum, false, 0},
		{"nan", math.NaN(), false, 0},
		{"infinity", math.Inf(1), false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hist snrHistogram
			if ok := hist.canAdd(precisionWeakBin, tc.weight); ok != tc.ok || hist != (snrHistogram{}) {
				t.Fatalf("prospective admission=%v want=%v; preview mutated bins=%v", ok, tc.ok, hist)
			}
			if ok := hist.add(precisionWeakBin, tc.weight); ok != tc.ok {
				t.Fatalf("admission=%v want=%v", ok, tc.ok)
			}
			// Exact powers-of-two assertions cannot accept a vanished quantum.
			if got := float64(hist[precisionWeakBin]); got != tc.want {
				t.Fatalf("stored increment=%g want=%g", got, tc.want)
			}
		})
	}
}

func TestSNRHistogramPrecisionAbsorbedIncrement(t *testing.T) {
	var hist snrHistogram
	hist[precisionWeakBin] = 1
	before := hist
	quantum := math.Ldexp(1, -149)
	if !hist.canAdd(precisionWeakBin, quantum) || !hist.add(precisionWeakBin, quantum) {
		t.Fatal("representable increment was rejected because addition absorbed it")
	}
	if hist != before {
		t.Fatalf("quantum unexpectedly changed the unit bin: got=%v want=%v", hist, before)
	}
	var component snrHistogram
	component[precisionWeakBin] = float32(quantum)
	if !hist.addScaled(&component, 1) || hist != before {
		t.Fatal("component admission required a measurable destination-bin change")
	}
}

func TestSNRHistogramPrecisionProductsBeforeRounding(t *testing.T) {
	var source snrHistogram
	source[precisionWeakBin] = float32(math.Ldexp(1, 100))
	factor := math.Ldexp(1, -160)
	want := math.Ldexp(1, -60)
	if float32(factor) != 0 {
		t.Fatal("fixture does not exercise a factor lost by premature conversion")
	}
	decayed := source
	if !decayed.decay(factor) || float64(decayed[precisionWeakBin]) != want || !decayed.hasUsableMass() {
		t.Fatalf("representable decayed product lost: bins=%v want=%g", decayed, want)
	}
	var merged snrHistogram
	if !merged.addScaled(&source, factor) || float64(merged[precisionWeakBin]) != want || !merged.hasUsableMass() {
		t.Fatalf("representable scaled product lost: bins=%v want=%g", merged, want)
	}
	if source[precisionWeakBin] != float32(math.Ldexp(1, 100)) {
		t.Fatal("scaled merge changed its source")
	}
}

func TestSNRHistogramPrecisionDecayUnderflow(t *testing.T) {
	var hist snrHistogram
	hist[precisionWeakBin] = float32(math.Ldexp(1, -149))
	if !hist.decay(0.5) || hist != (snrHistogram{}) || hist.hasUsableMass() {
		t.Fatalf("half-quantum tie did not quantize to unavailable mass: %v", hist)
	}
	if db, ok := hist.p50DB(); ok || db != 0 {
		t.Fatalf("empty distribution produced p50=%g available=%v", db, ok)
	}
	hist[precisionWeakBin] = float32(math.Ldexp(1, -149))
	hist[precisionStrongBin] = 1
	if !hist.decay(0.5) || hist[precisionWeakBin] != 0 || hist[precisionStrongBin] != 0.5 || !hist.hasUsableMass() {
		t.Fatalf("ordinary per-bin quantization rejected surviving distribution: %v", hist)
	}
}

func TestSNRHistogramPrecisionScaledComponentUnderflow(t *testing.T) {
	var component snrHistogram
	component[precisionWeakBin] = float32(math.Ldexp(1, -149))
	for _, factor := range []float64{0.25, 0.5} {
		var destination snrHistogram
		destination[precisionStrongBin] = 4
		before := destination
		if destination.addScaled(&component, factor) || destination != before {
			t.Fatalf("vanished component was accepted or partially committed: factor=%g bins=%v", factor, destination)
		}
	}
	var destination snrHistogram
	if !destination.addScaled(&component, math.Nextafter(0.5, math.Inf(1))) || destination[precisionWeakBin] != component[precisionWeakBin] {
		t.Fatalf("just-above-half scaled quantum lost: %v", destination)
	}
}

func TestSNRHistogramPrecisionAtomicOverflow(t *testing.T) {
	for _, tc := range []struct {
		name      string
		existing  float32
		increment float64
	}{
		{"same_bin_large_sum", float32(math.Ldexp(1, 127)), math.Ldexp(1, 127)},
		{"maximum_plus_ulp", math.MaxFloat32, math.Ldexp(1, 104)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var hist snrHistogram
			hist[precisionWeakBin] = tc.existing
			hist[precisionStrongBin] = 3
			before := hist
			if hist.canAdd(precisionWeakBin, tc.increment) || hist.add(precisionWeakBin, tc.increment) || hist != before {
				t.Fatalf("overflow admission committed nonfinite bins: %v", hist)
			}
		})
	}
	var destination, component snrHistogram
	destination[precisionWeakBin] = 1
	destination[precisionStrongBin] = float32(math.Ldexp(1, 127))
	component[precisionWeakBin] = 2 // Processed before the later failing bin.
	component[precisionStrongBin] = float32(math.Ldexp(1, 127))
	before := destination
	if destination.addScaled(&component, 1) || destination != before {
		t.Fatalf("merge overflow left an earlier partial write: %v", destination)
	}
	hist := component
	before = hist
	if hist.decay(2) || hist != before {
		t.Fatalf("product overflow left a partial decay: %v", hist)
	}
}

func TestSNRHistogramPrecisionShiftCollision(t *testing.T) {
	var source snrHistogram
	source[2] = float32(math.Ldexp(1, 127))
	source[3] = float32(math.Ldexp(1, 127))
	before := source
	shifted, ok := source.shifted(-100)
	if ok || shifted != (snrHistogram{}) || source != before {
		t.Fatalf("shift collision produced partial/nonfinite output: ok=%v bins=%v", ok, shifted)
	}
	identity, ok := source.shifted(0)
	if !ok || identity != source {
		t.Fatal("identity shift changed finite independent bins")
	}
}

func TestSNRHistogramPrecisionP50WideTotal(t *testing.T) {
	var hist snrHistogram
	hist[precisionWeakBin] = float32(math.Ldexp(1, 127))
	hist[precisionStrongBin] = float32(math.Ldexp(1, 127))
	// The exact total is 2^128, exceeding binary32 despite finite input bins.
	if db, ok := hist.p50DB(); !ok || db != -16.5 {
		t.Fatalf("wide even-split median=%g available=%v want=-16.5", db, ok)
	}
}

func TestSNRHistogramPrecisionP50TinyMass(t *testing.T) {
	var hist snrHistogram
	hist[precisionWeakBin] = float32(math.Ldexp(1, -149))
	if db, ok := hist.p50DB(); !ok || db != -19.5 {
		t.Fatalf("one quantum selected an empty earlier bin: db=%g available=%v", db, ok)
	}
	hist[precisionStrongBin] = hist[precisionWeakBin]
	if db, ok := hist.p50DB(); !ok || db != -16.5 {
		t.Fatalf("tiny exact even split lost midpoint policy: db=%g available=%v", db, ok)
	}
}

func TestSNRHistogramPrecisionInvalidArithmetic(t *testing.T) {
	var destination, component snrHistogram
	destination[precisionStrongBin] = 1
	component[precisionWeakBin] = 2
	for _, factor := range []float64{-1, math.NaN(), math.Inf(1), math.Inf(-1)} {
		before := destination
		if destination.addScaled(&component, factor) || destination != before {
			t.Fatalf("invalid scale committed bins: factor=%g bins=%v", factor, destination)
		}
		if destination.decay(factor) || destination != before {
			t.Fatalf("invalid decay committed bins: factor=%g bins=%v", factor, destination)
		}
	}
	for _, invalid := range []float32{-1, float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1))} {
		component[precisionWeakBin] = invalid
		before := destination
		if destination.addScaled(&component, 1) || destination != before {
			t.Fatalf("invalid source entered merged distribution: %v", component)
		}
		if !destination.addScaled(&component, 0) || destination != before {
			t.Fatal("excluded zero-coefficient source poisoned valid destination")
		}
		if _, ok := component.p50DB(); ok || component.hasUsableMass() {
			t.Fatalf("invalid distribution was available: %v", component)
		}
	}
	for _, delta := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if shifted, ok := destination.shifted(delta); ok || shifted != (snrHistogram{}) {
			t.Fatalf("invalid shift returned usable bins: delta=%g bins=%v", delta, shifted)
		}
	}
}
