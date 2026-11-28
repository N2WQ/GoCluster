package propprior

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"testing"
	"time"
)

func TestLoadEmbedded(t *testing.T) {
	m, err := LoadEmbedded()
	if err != nil {
		t.Fatalf("LoadEmbedded failed: %v", err)
	}
	if m.FormatVersion() != FormatVersion {
		t.Fatalf("FormatVersion=%d, want %d", m.FormatVersion(), FormatVersion)
	}
	if m.FeatureSchemaVersion() != FeatureSchemaVersion {
		t.Fatalf("FeatureSchemaVersion=%d, want %d", m.FeatureSchemaVersion(), FeatureSchemaVersion)
	}
	if m.LPMinDistKM() != 4000.0 {
		t.Fatalf("LPMinDistKM=%v, want 4000.0", m.LPMinDistKM())
	}
	if m.MuFloor() != -120.0 {
		t.Fatalf("MuFloor=%v, want -120.0", m.MuFloor())
	}
	if m.SigmaFloor() != 2.0 {
		t.Fatalf("SigmaFloor=%v, want 2.0", m.SigmaFloor())
	}

	var nilModel *Model
	if nilModel.FormatVersion() != 0 || nilModel.FeatureSchemaVersion() != 0 ||
		nilModel.LPMinDistKM() != 0 || nilModel.MuFloor() != 0 || nilModel.SigmaFloor() != 0 {
		t.Fatalf("nil Model getters should return zero values")
	}
}

func TestLoadFromBytesCopiesCallerSlices(t *testing.T) {
	w := append([]byte(nil), embeddedWeights...)
	om := append([]byte(nil), embeddedOceanMask...)
	m, err := LoadFromBytes(w, om)
	if err != nil {
		t.Fatalf("LoadFromBytes failed: %v", err)
	}
	req := PathRequest{
		TXLat: 40.7, TXLon: -74.0, RXLat: 51.5, RXLon: -0.1,
		FreqMHz: 14.095,
		Time:    time.Date(2025, 3, 21, 14, 0, 0, 0, time.UTC),
		SpaceWeatherFeatures: SpaceWeatherFeatures{
			SFINow: 150, SFI72hMean: 145, Hp30Lag1: 2, Hp3012hMax: 3, Hp3024hMean: 2,
			BzSouth3h: 1, Vsw3h: 420, DstNow: -10, Dst24hMin: -20,
		},
	}
	before, err := m.Predict(req)
	if err != nil {
		t.Fatalf("Predict before mutate failed: %v", err)
	}
	clear(w)
	clear(om)
	after, err := m.Predict(req)
	if err != nil {
		t.Fatalf("Predict after mutate failed: %v", err)
	}
	if before != after {
		t.Fatalf("caller slice mutation affected Model predictions: before=%+v after=%+v", before, after)
	}
}

func TestLoadFromBytesRejectsCorruptAndMalformed(t *testing.T) {
	validMask := make([]byte, OceanMaskBytes)
	base := buildSyntheticWeights(t, ActivationReLU, 4000.0, []int{NumFeatures, 4, 2}, nil, nil)
	if _, err := LoadFromBytes(base, validMask); err != nil {
		t.Fatalf("baseline synthetic model should load: %v", err)
	}

	reCRC := func(b []byte) []byte {
		cp := append([]byte(nil), b...)
		crc := crc32.ChecksumIEEE(cp[:len(cp)-4])
		binary.LittleEndian.PutUint32(cp[len(cp)-4:], crc)
		return cp
	}

	tests := []struct {
		name    string
		weights []byte
		mask    []byte
	}{
		{name: "short ocean mask", weights: base, mask: make([]byte, OceanMaskBytes-1)},
		{name: "truncated weights", weights: base[:39], mask: validMask},
		{name: "oversized weights", weights: make([]byte, maxBinaryBytes+1), mask: validMask},
		{name: "bad crc", weights: func() []byte {
			cp := append([]byte(nil), base...)
			cp[len(cp)-1] ^= 0xff
			return cp
		}(), mask: validMask},
		{name: "bad magic", weights: func() []byte {
			cp := append([]byte(nil), base...)
			cp[0] = 'X'
			return reCRC(cp)
		}(), mask: validMask},
		{name: "bad format_version", weights: func() []byte {
			cp := append([]byte(nil), base...)
			binary.LittleEndian.PutUint32(cp[4:8], 3)
			return reCRC(cp)
		}(), mask: validMask},
		{name: "bad schema_version", weights: func() []byte {
			cp := append([]byte(nil), base...)
			binary.LittleEndian.PutUint32(cp[8:12], 5)
			return reCRC(cp)
		}(), mask: validMask},
		{name: "bad activation", weights: func() []byte {
			cp := append([]byte(nil), base...)
			binary.LittleEndian.PutUint32(cp[12:16], 9)
			return reCRC(cp)
		}(), mask: validMask},
		{name: "zero sigma_floor", weights: buildSyntheticWeightsRaw(0.0, 4000.0, -120.0, []int{NumFeatures, 4, 2}), mask: validMask},
		{name: "nan lp_min_dist_km", weights: buildSyntheticWeightsRaw(2.0, float32(math.NaN()), -120.0, []int{NumFeatures, 4, 2}), mask: validMask},
		{name: "neg lp_min_dist_km", weights: buildSyntheticWeightsRaw(2.0, -1.0, -120.0, []int{NumFeatures, 4, 2}), mask: validMask},
		{name: "inf mu_floor", weights: buildSyntheticWeightsRaw(2.0, 4000.0, float32(math.Inf(-1)), []int{NumFeatures, 4, 2}), mask: validMask},
		{name: "bad num_features", weights: func() []byte {
			cp := append([]byte(nil), base...)
			binary.LittleEndian.PutUint32(cp[28:32], 30)
			return reCRC(cp)
		}(), mask: validMask},
		{name: "zero layers", weights: func() []byte {
			cp := append([]byte(nil), base...)
			binary.LittleEndian.PutUint32(cp[32:36], 0)
			return reCRC(cp)
		}(), mask: validMask},
		{name: "too many layers", weights: func() []byte {
			cp := append([]byte(nil), base...)
			binary.LittleEndian.PutUint32(cp[32:36], maxLayers+1)
			return reCRC(cp)
		}(), mask: validMask},
		{name: "hidden dim exceeds maxLayerDim", weights: buildSyntheticWeightsRaw(2.0, 4000.0, -120.0, []int{NumFeatures, maxLayerDim + 1, 2}), mask: validMask},
		{name: "zero hidden dim", weights: func() []byte {
			cp := append([]byte(nil), base...)
			binary.LittleEndian.PutUint32(cp[40:44], 0)
			return reCRC(cp)
		}(), mask: validMask},
		{name: "wrong output dim", weights: buildSyntheticWeightsRaw(2.0, 4000.0, -120.0, []int{NumFeatures, 4, 3}), mask: validMask},
		{name: "trailing byte", weights: func() []byte {
			cp := append([]byte(nil), base[:len(base)-4]...)
			cp = append(cp, 0, 0, 0, 0, 0)
			return reCRC(cp)
		}(), mask: validMask},
		{name: "zero feature_std", weights: func() []byte {
			cp := append([]byte(nil), base...)
			stdOffset := headerSizeBytes + 3*4 + NumFeatures*4
			binary.LittleEndian.PutUint32(cp[stdOffset:stdOffset+4], math.Float32bits(0))
			return reCRC(cp)
		}(), mask: validMask},
		{name: "sub-floor feature_std", weights: func() []byte {
			cp := append([]byte(nil), base...)
			stdOffset := headerSizeBytes + 3*4 + NumFeatures*4
			binary.LittleEndian.PutUint32(cp[stdOffset:stdOffset+4], math.Float32bits(1e-7))
			return reCRC(cp)
		}(), mask: validMask},
		{name: "nan feature_mean", weights: func() []byte {
			cp := append([]byte(nil), base...)
			meanOffset := headerSizeBytes + 3*4
			binary.LittleEndian.PutUint32(cp[meanOffset:meanOffset+4], math.Float32bits(float32(math.NaN())))
			return reCRC(cp)
		}(), mask: validMask},
		{name: "nan weight", weights: func() []byte {
			cp := append([]byte(nil), base...)
			wOffset := headerSizeBytes + 3*4 + 2*NumFeatures*4
			binary.LittleEndian.PutUint32(cp[wOffset:wOffset+4], math.Float32bits(float32(math.NaN())))
			return reCRC(cp)
		}(), mask: validMask},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := LoadFromBytes(tc.weights, tc.mask); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestPredictRejectsNonFiniteOverflow(t *testing.T) {
	mask := make([]byte, OceanMaskBytes)
	// Extreme finite weights that overflow float32 into +Inf/NaN during SP+LP combine.
	hugeW0 := make([]float32, 1*NumFeatures)
	for i := range hugeW0 {
		hugeW0[i] = 1e20
	}
	hugeW1 := []float32{1e20, 1e20}
	raw := buildSyntheticWeights(t, ActivationReLU, 4000.0, []int{NumFeatures, 1, 2}, [][]float32{hugeW0, hugeW1}, [][]float32{{1e20}, {0, 0}})
	m, err := LoadFromBytes(raw, mask)
	if err != nil {
		t.Fatalf("LoadFromBytes failed: %v", err)
	}
	req := PathRequest{
		TXLat: 40.7, TXLon: -74.0, RXLat: 51.5, RXLon: -0.1,
		FreqMHz: 14.095,
		Time:    time.Date(2025, 3, 21, 14, 0, 0, 0, time.UTC),
		SpaceWeatherFeatures: SpaceWeatherFeatures{
			SFINow: 150, SFI72hMean: 150, Hp30Lag1: 2, Hp3012hMax: 2, Hp3024hMean: 2,
			BzSouth3h: 0, Vsw3h: 400, DstNow: 0, Dst24hMin: 0,
		},
	}
	if _, err := m.Predict(req); !errors.Is(err, ErrNonFinitePrediction) {
		t.Fatalf("expected ErrNonFinitePrediction, got %v", err)
	}
}

func TestSyntheticActivationsSiLUAndGELU(t *testing.T) {
	mask := make([]byte, OceanMaskBytes)
	req := PathRequest{
		TXLat: 40.0, TXLon: -75.0, RXLat: 42.0, RXLon: -71.0,
		FreqMHz: 14.095,
		Time:    time.Date(2025, 3, 21, 12, 0, 0, 0, time.UTC),
		SpaceWeatherFeatures: SpaceWeatherFeatures{
			SFINow: 150, SFI72hMean: 150, Hp30Lag1: 1, Hp3012hMax: 1, Hp3024hMean: 1,
			BzSouth3h: 0, Vsw3h: 400, DstNow: 0, Dst24hMin: 0,
		},
	}

	// Single hidden unit receiving constant bias b1 (all layer-0 weights = 0),
	// mapped with weight 1.0 into output 0 (mu residual) and output 1 (raw sigma).
	const b1 = float32(1.5)
	layerWeights := [][]float32{
		make([]float32, 1*NumFeatures),
		{1.0, 1.0},
	}
	layerBiases := [][]float32{
		{b1},
		{0.0, 0.0},
	}

	geom := computeGreatCircleSPLP(req.TXLat, req.TXLon, req.RXLat, req.RXLon)
	dSP := float32(earthRadiusKM * geom.deltaSP)
	spPrior := float64(skipPriorDB(dSP))

	cases := []struct {
		name string
		act  uint32
		want float64
	}{
		{
			name: "SiLU",
			act:  ActivationSiLU,
			want: float64(b1) / (1.0 + math.Exp(-float64(b1))),
		},
		{
			name: "GELU",
			act:  ActivationGELU,
			want: 0.5 * float64(b1) * (1.0 + math.Erf(float64(b1)/math.Sqrt2)),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := buildSyntheticWeights(t, tc.act, 4000.0, []int{NumFeatures, 1, 2}, layerWeights, layerBiases)
			m, err := LoadFromBytes(raw, mask)
			if err != nil {
				t.Fatalf("LoadFromBytes(%s) failed: %v", tc.name, err)
			}
			pred, err := m.Predict(req)
			if err != nil {
				t.Fatalf("Predict(%s) failed: %v", tc.name, err)
			}
			muRaw := spPrior + tc.want
			wantMu := -120.0 + math.Max(muRaw+120.0, 0) + math.Log1p(math.Exp(-math.Abs(muRaw+120.0)))
			wantSig := math.Max(tc.want, 0) + math.Log1p(math.Exp(-math.Abs(tc.want))) + 2.0
			if math.Abs(pred.SnrSP-wantMu) > 1e-4 {
				t.Fatalf("%s SnrSP=%v, want %v", tc.name, pred.SnrSP, wantMu)
			}
			if math.Abs(pred.SigmaSP-wantSig) > 1e-4 {
				t.Fatalf("%s SigmaSP=%v, want %v", tc.name, pred.SigmaSP, wantSig)
			}
		})
	}
}

func FuzzLoadFromBytes(f *testing.F) {
	f.Add(embeddedWeights[:min(len(embeddedWeights), 256)])
	f.Add(buildSyntheticWeightsRaw(2.0, 4000.0, -120.0, []int{NumFeatures, 2, 2}))
	f.Add([]byte("HFPR"))
	f.Add([]byte{})

	mask := make([]byte, OceanMaskBytes)
	req := PathRequest{
		TXLat: 40.7, TXLon: -74.0, RXLat: 51.5, RXLon: -0.1,
		FreqMHz: 14.095,
		Time:    time.Date(2025, 3, 21, 14, 0, 0, 0, time.UTC),
		SpaceWeatherFeatures: SpaceWeatherFeatures{
			SFINow: 150, SFI72hMean: 150, Hp30Lag1: 2, Hp3012hMax: 2, Hp3024hMean: 2,
			BzSouth3h: 0, Vsw3h: 400, DstNow: 0, Dst24hMin: 0,
		},
	}

	checkLoaded := func(t *testing.T, m *Model) {
		t.Helper()
		pred, err := m.Predict(req)
		if err != nil {
			if !errors.Is(err, ErrNonFinitePrediction) {
				t.Fatalf("loaded model failed valid Predict with unexpected error: %v", err)
			}
			return
		}
		if !isFinite64(pred.SnrTotal) || !isFinite64(pred.SigmaTotal) || pred.SigmaTotal <= 0 ||
			!isFinite64(pred.SnrSP) || !isFinite64(pred.SigmaSP) ||
			!isFinite64(pred.SnrLP) || !isFinite64(pred.SigmaLP) || !isFinite64(pred.WeightLP) {
			t.Fatalf("loaded model produced non-finite or invalid prediction: %+v", pred)
		}
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if m, err := LoadFromBytes(data, mask); err == nil {
			checkLoaded(t, m)
		}
		// Also test with recomputed valid CRC-32 so fuzzing reaches header/layer validation.
		if len(data) >= minBinaryBytes && len(data) <= maxBinaryBytes {
			withCRC := append([]byte(nil), data...)
			crc := crc32.ChecksumIEEE(withCRC[:len(withCRC)-4])
			binary.LittleEndian.PutUint32(withCRC[len(withCRC)-4:], crc)
			if m, err := LoadFromBytes(withCRC, mask); err == nil {
				checkLoaded(t, m)
			}
		}
	})
}

func buildSyntheticWeights(
	t *testing.T,
	activation uint32,
	lpMinDist float32,
	dims []int,
	weights [][]float32,
	biases [][]float32,
) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString(binaryMagic)
	writeU32 := func(v uint32) {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], v)
		buf.Write(b[:])
	}
	writeF32 := func(v float32) {
		writeU32(math.Float32bits(v))
	}
	writeU32(FormatVersion)
	writeU32(FeatureSchemaVersion)
	writeU32(activation)
	writeF32(2.0)
	writeF32(lpMinDist)
	writeF32(-120.0)
	writeU32(NumFeatures)
	writeU32(uint32(len(dims) - 1))
	for _, d := range dims {
		writeU32(uint32(d))
	}
	for range NumFeatures {
		writeF32(0)
	}
	for range NumFeatures {
		writeF32(1)
	}
	for l := range len(dims) - 1 {
		inDim, outDim := dims[l], dims[l+1]
		wCount := outDim * inDim
		for j := range wCount {
			var v float32
			if weights != nil && l < len(weights) && j < len(weights[l]) {
				v = weights[l][j]
			}
			writeF32(v)
		}
		for j := range outDim {
			var v float32
			if biases != nil && l < len(biases) && j < len(biases[l]) {
				v = biases[l][j]
			}
			writeF32(v)
		}
	}
	crc := crc32.ChecksumIEEE(buf.Bytes())
	writeU32(crc)
	return buf.Bytes()
}

func buildSyntheticWeightsRaw(
	sigmaFloor, lpMinDist, muFloor float32,
	dims []int,
) []byte {
	var buf bytes.Buffer
	buf.WriteString(binaryMagic)
	writeU32 := func(v uint32) {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], v)
		buf.Write(b[:])
	}
	writeF32 := func(v float32) {
		writeU32(math.Float32bits(v))
	}
	writeU32(FormatVersion)
	writeU32(FeatureSchemaVersion)
	writeU32(ActivationReLU)
	writeF32(sigmaFloor)
	writeF32(lpMinDist)
	writeF32(muFloor)
	writeU32(NumFeatures)
	writeU32(uint32(len(dims) - 1))
	for _, d := range dims {
		writeU32(uint32(d))
	}
	for range NumFeatures {
		writeF32(0)
	}
	for range NumFeatures {
		writeF32(1)
	}
	for l := range len(dims) - 1 {
		wCount := dims[l+1] * dims[l]
		for range wCount {
			writeF32(0)
		}
		for range dims[l+1] {
			writeF32(0)
		}
	}
	crc := crc32.ChecksumIEEE(buf.Bytes())
	writeU32(crc)
	return buf.Bytes()
}
