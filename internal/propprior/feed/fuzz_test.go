package feed

import (
	"math"
	"testing"
	"time"

	"dxcluster/internal/propprior"
)

var fuzzNow = time.Date(2026, 11, 27, 17, 16, 0, 0, time.UTC)

func assertValidSamples(t *testing.T, samples []propprior.Sample, minVal, maxVal float64, maxExclusiveBin int64, stepSec int64) {
	t.Helper()
	var prev time.Time
	for i, s := range samples {
		if s.BinStartUTC.IsZero() {
			t.Fatalf("sample[%d] has zero timestamp", i)
		}
		if i > 0 && !s.BinStartUTC.After(prev) {
			t.Fatalf("samples not strictly ascending at %d: %v <= %v", i, s.BinStartUTC, prev)
		}
		prev = s.BinStartUTC
		if math.IsNaN(s.Value) || math.IsInf(s.Value, 0) || s.Value < minVal || s.Value > maxVal {
			t.Fatalf("sample[%d] out of range [%v,%v]: %v", i, minVal, maxVal, s.Value)
		}
		if bin := floorDiv(s.BinStartUTC.Unix(), stepSec); bin >= maxExclusiveBin {
			t.Fatalf("sample[%d] leaked in-progress/future bin %d >= %d", i, bin, maxExclusiveBin)
		}
	}
}

func FuzzParseGFZHp30(f *testing.F) {
	f.Add([]byte(`{"Hp30":[0.0,null,1.333],"datetime":["2026-11-27T15:00:00Z","2026-11-27T15:30:00Z","2026-11-27T16:00:00Z"]}`))
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		samples, err := ParseGFZHp30(data, fuzzNow)
		if err != nil {
			return
		}
		assertValidSamples(t, samples, hp30MinValid, hp30MaxValid, floorDiv(fuzzNow.Unix(), hp30StepSec), hp30StepSec)
	})
}

func FuzzParseNOAAKp(f *testing.F) {
	f.Add([]byte(`[{"time_tag":"2026-11-27T12:00:00","Kp":1.67}]`))
	f.Add([]byte(`[["time_tag","Kp"],["2026-11-27 12:00:00.000","1.67"]]`))
	f.Fuzz(func(t *testing.T, data []byte) {
		samples, err := ParseNOAAKp(data, fuzzNow)
		if err != nil {
			return
		}
		assertValidSamples(t, samples, kpMinValid, kpMaxValid, floorDiv(fuzzNow.Unix(), hp30StepSec), hp30StepSec)
	})
}

func FuzzParseNOAAF107(f *testing.F) {
	f.Add([]byte(`[{"time_tag":"2026-11-26T20:00:00","flux":87.0,"reporting_schedule":"Noon"}]`))
	f.Fuzz(func(t *testing.T, data []byte) {
		samples, err := ParseNOAAF107(data, fuzzNow)
		if err != nil {
			return
		}
		assertValidSamples(t, samples, sfiMinValid, sfiMaxValid, floorDiv(fuzzNow.Unix(), sfiStepSec)+1, sfiStepSec)
	})
}

func FuzzParseRTSWMag(f *testing.F) {
	f.Add([]byte(`[{"time_tag":"2026-11-27T16:00:00","active":true,"source":"SOLAR1","bz_gsm":-3.5}]`))
	f.Fuzz(func(t *testing.T, data []byte) {
		samples, err := ParseRTSWMag(data, fuzzNow)
		if err != nil {
			return
		}
		assertValidSamples(t, samples, bzMinValid, bzMaxValid, floorDiv(fuzzNow.Unix(), omniStepSec), omniStepSec)
	})
}

func FuzzParseRTSWWind(f *testing.F) {
	f.Add([]byte(`[{"time_tag":"2026-11-27T16:00:00","active":true,"source":"SOLAR1","proton_speed":420.0}]`))
	f.Fuzz(func(t *testing.T, data []byte) {
		samples, err := ParseRTSWWind(data, fuzzNow)
		if err != nil {
			return
		}
		assertValidSamples(t, samples, vswMinValid, vswMaxValid, floorDiv(fuzzNow.Unix(), omniStepSec), omniStepSec)
	})
}

func FuzzParseKyotoDst(f *testing.F) {
	f.Add([]byte(`[{"time_tag":"2026-11-27T16:00:00","dst":-25}]`))
	f.Add([]byte(`[["time_tag","dst"],["2026-11-27 16:00:00","-25"]]`))
	f.Fuzz(func(t *testing.T, data []byte) {
		samples, err := ParseKyotoDst(data, fuzzNow)
		if err != nil {
			return
		}
		assertValidSamples(t, samples, dstMinValid, dstMaxValid, floorDiv(fuzzNow.Unix(), omniStepSec), omniStepSec)
	})
}

func FuzzParseGeospaceDst(f *testing.F) {
	f.Add([]byte(`[{"time_tag":"2026-11-27T16:00:00","dst":-24.19}]`))
	f.Fuzz(func(t *testing.T, data []byte) {
		samples, err := ParseGeospaceDst(data, fuzzNow)
		if err != nil {
			return
		}
		assertValidSamples(t, samples, dstMinValid, dstMaxValid, floorDiv(fuzzNow.Unix(), omniStepSec), omniStepSec)
	})
}
