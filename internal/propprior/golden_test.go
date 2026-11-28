package propprior

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type goldenFile struct {
	FormatVersion        uint32      `json:"format_version"`
	FeatureSchemaVersion uint32      `json:"feature_schema_version"`
	MuFloor              float64     `json:"mu_floor"`
	OceanMaskCRC32       string      `json:"ocean_mask_crc32"`
	Count                int         `json:"count"`
	Rows                 []goldenRow `json:"rows"`
}

type goldenRow struct {
	Raw struct {
		TXLat       float64 `json:"tx_lat"`
		TXLon       float64 `json:"tx_lon"`
		RXLat       float64 `json:"rx_lat"`
		RXLon       float64 `json:"rx_lon"`
		FreqMHz     float64 `json:"freq_mhz"`
		UnixSec     int64   `json:"unix_sec"`
		SFINow      float64 `json:"sfi_now"`
		SFI72hMean  float64 `json:"sfi_72h_mean"`
		Hp30Lag1    float64 `json:"hp30_lag1"`
		Hp3012hMax  float64 `json:"hp30_12h_max"`
		Hp3024hMean float64 `json:"hp30_24h_mean"`
		BzSouth3h   float64 `json:"bz_south_3h"`
		Vsw3h       float64 `json:"vsw_3h"`
		DstNow      float64 `json:"dst_now"`
		Dst24hMin   float64 `json:"dst_24h_min"`
	} `json:"raw"`
	DSPKM       float64   `json:"d_sp_km"`
	DLPKM       float64   `json:"d_lp_km"`
	XSP         []float64 `json:"X_sp"`
	XLP         []float64 `json:"X_lp"`
	LPEvaluated bool      `json:"lp_evaluated"`
	SnrSP       float64   `json:"snr_sp"`
	SigmaSP     float64   `json:"sigma_sp"`
	SnrLP       *float64  `json:"snr_lp"`
	SigmaLP     *float64  `json:"sigma_lp"`
	SnrTotal    float64   `json:"snr_total"`
	SigmaTotal  float64   `json:"sigma_total"`
}

func TestGoldenParity1000Vectors(t *testing.T) {
	model, err := LoadEmbedded()
	if err != nil {
		t.Fatalf("LoadEmbedded failed: %v", err)
	}

	rawBytes, err := os.ReadFile(filepath.Join("testdata", "golden_test.json"))
	if err != nil {
		t.Fatalf("read golden_test.json failed: %v", err)
	}
	var doc goldenFile
	if err := json.Unmarshal(rawBytes, &doc); err != nil {
		t.Fatalf("unmarshal golden_test.json failed: %v", err)
	}
	if doc.FormatVersion != FormatVersion || doc.FeatureSchemaVersion != FeatureSchemaVersion {
		t.Fatalf("golden version mismatch: format=%d schema=%d", doc.FormatVersion, doc.FeatureSchemaVersion)
	}
	if doc.Count != 1000 || len(doc.Rows) != 1000 {
		t.Fatalf("expected 1000 golden rows, got count=%d len=%d", doc.Count, len(doc.Rows))
	}

	var maxFeatDiff, maxOutDiff float64
	for i := range doc.Rows {
		row := &doc.Rows[i]
		req := PathRequest{
			TXLat:   row.Raw.TXLat,
			TXLon:   row.Raw.TXLon,
			RXLat:   row.Raw.RXLat,
			RXLon:   row.Raw.RXLon,
			FreqMHz: row.Raw.FreqMHz,
			Time:    time.Unix(row.Raw.UnixSec, 0).UTC(),
			SpaceWeatherFeatures: SpaceWeatherFeatures{
				SFINow:      row.Raw.SFINow,
				SFI72hMean:  row.Raw.SFI72hMean,
				Hp30Lag1:    row.Raw.Hp30Lag1,
				Hp3012hMax:  row.Raw.Hp3012hMax,
				Hp3024hMean: row.Raw.Hp3024hMean,
				BzSouth3h:   row.Raw.BzSouth3h,
				Vsw3h:       row.Raw.Vsw3h,
				DstNow:      row.Raw.DstNow,
				Dst24hMin:   row.Raw.Dst24hMin,
			},
		}

		// Verify all 31 features on both SP and LP arcs plus arc distances.
		geom := computeGreatCircleSPLP(req.TXLat, req.TXLon, req.RXLat, req.RXLon)
		solar := computeSolarContext(req.Time)
		logFreq := math.Log(req.FreqMHz)
		var xSP, xLP [NumFeatures]float32
		buildArcFeatures(&xSP, &model.oceanMask, geom.p1, geom.l1, geom.deltaSP, geom.azSP, logFreq, solar, req.SpaceWeatherFeatures)
		buildArcFeatures(&xLP, &model.oceanMask, geom.p1, geom.l1, geom.deltaLP, geom.azLP, logFreq, solar, req.SpaceWeatherFeatures)

		dSP := float64(float32(earthRadiusKM * geom.deltaSP))
		dLP := float64(float32(earthRadiusKM * geom.deltaLP))
		if math.Abs(dSP-row.DSPKM) > 1e-2 || math.Abs(dLP-row.DLPKM) > 1e-2 {
			t.Fatalf("row %d distance mismatch: got SP=%v LP=%v, want SP=%v LP=%v", i, dSP, dLP, row.DSPKM, row.DLPKM)
		}
		for f := range NumFeatures {
			diffSP := math.Abs(float64(xSP[f]) - row.XSP[f])
			diffLP := math.Abs(float64(xLP[f]) - row.XLP[f])
			if diffSP > maxFeatDiff {
				maxFeatDiff = diffSP
			}
			if diffLP > maxFeatDiff {
				maxFeatDiff = diffLP
			}
			if diffSP > 5e-5 {
				t.Fatalf("row %d X_sp[%d] diff=%g (got=%v want=%v)", i, f, diffSP, xSP[f], row.XSP[f])
			}
			if diffLP > 5e-5 {
				t.Fatalf("row %d X_lp[%d] diff=%g (got=%v want=%v)", i, f, diffLP, xLP[f], row.XLP[f])
			}
		}

		pred, err := model.Predict(req)
		if err != nil {
			t.Fatalf("row %d Predict failed: %v", i, err)
		}
		if pred.LPEvaluated != row.LPEvaluated {
			t.Fatalf("row %d LPEvaluated=%v, want %v (d_sp=%v)", i, pred.LPEvaluated, row.LPEvaluated, dSP)
		}

		checkOut := func(name string, got, want float64) {
			d := math.Abs(got - want)
			if d > maxOutDiff {
				maxOutDiff = d
			}
			if d >= 1e-4 {
				t.Fatalf("row %d %s diff=%g >= 1e-4 (got=%v want=%v)", i, name, d, got, want)
			}
		}
		checkOut("snr_sp", pred.SnrSP, row.SnrSP)
		checkOut("sigma_sp", pred.SigmaSP, row.SigmaSP)
		checkOut("snr_total", pred.SnrTotal, row.SnrTotal)
		checkOut("sigma_total", pred.SigmaTotal, row.SigmaTotal)

		if row.LPEvaluated {
			if row.SnrLP == nil || row.SigmaLP == nil {
				t.Fatalf("row %d expected non-nil LP golden outputs", i)
			}
			checkOut("snr_lp", pred.SnrLP, *row.SnrLP)
			checkOut("sigma_lp", pred.SigmaLP, *row.SigmaLP)
			if pred.WeightLP < 0 || pred.WeightLP > 1 {
				t.Fatalf("row %d WeightLP=%v out of [0,1]", i, pred.WeightLP)
			}
			wantSigComb := (1.0-pred.WeightLP)*pred.SigmaSP + pred.WeightLP*pred.SigmaLP
			if math.Abs(pred.SigmaTotal-wantSigComb) > 1e-5 {
				t.Fatalf("row %d WeightLP/SigmaTotal mismatch: got=%v want=%v", i, pred.SigmaTotal, wantSigComb)
			}
		} else {
			if pred.SnrLP != 0 || pred.SigmaLP != 0 || pred.WeightLP != 0 {
				t.Fatalf("row %d !LPEvaluated must zero LP fields, got snrLP=%v sigmaLP=%v weightLP=%v", i, pred.SnrLP, pred.SigmaLP, pred.WeightLP)
			}
		}

		// Exact 0.0 TX<->RX reciprocity check on every vector.
		swapped := req
		swapped.TXLat, swapped.TXLon, swapped.RXLat, swapped.RXLon = req.RXLat, req.RXLon, req.TXLat, req.TXLon
		predSwap, err := model.Predict(swapped)
		if err != nil {
			t.Fatalf("row %d swapped Predict failed: %v", i, err)
		}
		if pred != predSwap {
			t.Fatalf("row %d TX<->RX reciprocity broken: %+v != %+v", i, pred, predSwap)
		}
	}

	t.Logf("1000-vector golden parity passed: max_feat_diff=%.3e, max_out_diff=%.3e dB", maxFeatDiff, maxOutDiff)
}

func TestLPGateExactBoundary(t *testing.T) {
	model, err := LoadEmbedded()
	if err != nil {
		t.Fatalf("LoadEmbedded failed: %v", err)
	}
	baseReq := PathRequest{
		TXLat: 0, TXLon: 0, RXLat: 0,
		FreqMHz: 14.095,
		Time:    time.Date(2025, 3, 21, 14, 0, 0, 0, time.UTC),
		SpaceWeatherFeatures: SpaceWeatherFeatures{
			SFINow: 150, SFI72hMean: 150, Hp30Lag1: 1, Hp3012hMax: 1, Hp3024hMean: 1,
			BzSouth3h: 0, Vsw3h: 400, DstNow: 0, Dst24hMin: 0,
		},
	}

	// Along equator, 1 km = 180 / (pi * 6371) degrees.
	degPerKM := 180.0 / (math.Pi * earthRadiusKM)

	below := baseReq
	below.RXLon = 3999.99 * degPerKM
	predBelow, err := model.Predict(below)
	if err != nil {
		t.Fatalf("Predict below gate failed: %v", err)
	}
	if predBelow.LPEvaluated {
		t.Fatalf("expected LPEvaluated=false at 3999.99 km")
	}

	above := baseReq
	above.RXLon = 4000.01 * degPerKM
	predAbove, err := model.Predict(above)
	if err != nil {
		t.Fatalf("Predict above gate failed: %v", err)
	}
	if !predAbove.LPEvaluated {
		t.Fatalf("expected LPEvaluated=true at 4000.01 km")
	}

	// Exact equality check: d_sp_km == lp_min_dist_km must evaluate LP (>=).
	exactGeom := computeGreatCircleSPLP(below.TXLat, below.TXLon, below.RXLat, below.RXLon)
	exactDist := float32(earthRadiusKM * exactGeom.deltaSP)
	exactRaw := buildSyntheticWeights(t, ActivationReLU, exactDist, []int{NumFeatures, 2, 2}, nil, nil)
	exactModel, err := LoadFromBytes(exactRaw, embeddedOceanMask)
	if err != nil {
		t.Fatalf("LoadFromBytes exact LP gate failed: %v", err)
	}
	predExact, err := exactModel.Predict(below)
	if err != nil {
		t.Fatalf("Predict exact gate failed: %v", err)
	}
	if !predExact.LPEvaluated {
		t.Fatalf("expected LPEvaluated=true when d_sp_km == lp_min_dist_km (%v)", exactDist)
	}
}

func TestOceanMaskNegativeModuloBoundaries(t *testing.T) {
	model, err := LoadEmbedded()
	if err != nil {
		t.Fatalf("LoadEmbedded failed: %v", err)
	}
	// Verify known points matching hf-prop-lab/tests/test_geometry_and_model.py.
	known := []struct {
		name  string
		lat   float64
		lon   float64
		ocean bool
	}{
		{"North Atlantic", 40.0, -40.0, true},
		{"Kansas", 39.0, -98.0, false},
		{"Central Pacific", 0.0, -150.0, true},
		{"Sahara", 23.0, 10.0, false},
	}
	for _, tc := range known {
		got := lookupOceanMaskBit(&model.oceanMask, tc.lat, tc.lon)
		if got != tc.ocean {
			t.Fatalf("%s (%v,%v): got ocean=%v, want %v", tc.name, tc.lat, tc.lon, got, tc.ocean)
		}
	}

	// Defensive negative-modulo and boundary coordinates must never panic and must
	// wrap cleanly across the antimeridian and poles.
	edgeCoords := [][2]float64{
		{-90.0, -180.00000000000003},
		{-90.0, -180.0},
		{-90.0, -1e-17},
		{90.0, 180.0},
		{90.0, 180.00000000000003},
		{0.0, -180.0 - 1e-15},
		{0.0, -360.0},
	}
	for _, c := range edgeCoords {
		_ = lookupOceanMaskBit(&model.oceanMask, c[0], c[1])
	}
}

func TestPredictionRel(t *testing.T) {
	p := Prediction{SnrTotal: -10.0, SigmaTotal: 10.0}
	if got := p.Rel(-10.0); math.Abs(got-0.5) > 1e-12 {
		t.Fatalf("Rel(mu)=%v, want 0.5", got)
	}
	if got := p.Rel(-20.0); math.Abs(got-0.8413447460685429) > 1e-12 {
		t.Fatalf("Rel(mu-sigma)=%v, want ~0.8413447", got)
	}
	if got := p.Rel(0.0); math.Abs(got-0.15865525393145707) > 1e-12 {
		t.Fatalf("Rel(mu+sigma)=%v, want ~0.1586553", got)
	}
	if !(p.Rel(-30) > p.Rel(-20) && p.Rel(-20) > p.Rel(-10) && p.Rel(-10) > p.Rel(0)) {
		t.Fatalf("Rel must be strictly decreasing with threshold")
	}

	// Deep closed-band tail where 0.5*(1-erf) cancels to 0.0, but erfc stays accurate.
	closed := Prediction{SnrTotal: -120.0, SigmaTotal: 10.0}
	tail := closed.Rel(-29.0)
	if tail <= 0 || math.IsNaN(tail) || tail > 1e-15 {
		t.Fatalf("expected tiny positive non-zero tail probability, got %e", tail)
	}

	// Inf/NaN thresholds and invalid Prediction parameters.
	if p.Rel(math.Inf(-1)) != 1.0 {
		t.Fatalf("Rel(-Inf) must be 1.0")
	}
	if p.Rel(math.Inf(1)) != 0.0 {
		t.Fatalf("Rel(+Inf) must be 0.0")
	}
	if p.Rel(math.NaN()) != 0.0 {
		t.Fatalf("Rel(NaN) must be 0.0")
	}
	invalid := []Prediction{
		{SnrTotal: 0, SigmaTotal: 0},
		{SnrTotal: 0, SigmaTotal: -5},
		{SnrTotal: math.NaN(), SigmaTotal: 5},
		{SnrTotal: 0, SigmaTotal: math.Inf(1)},
	}
	for _, bad := range invalid {
		if bad.Rel(-20) != 0 {
			t.Fatalf("expected 0 for invalid prediction %+v", bad)
		}
	}
}

func TestPredictInvalidInputs(t *testing.T) {
	model, err := LoadEmbedded()
	if err != nil {
		t.Fatalf("LoadEmbedded failed: %v", err)
	}
	valid := PathRequest{
		TXLat: 40.7, TXLon: -74.0, RXLat: 51.5, RXLon: -0.1,
		FreqMHz: 14.095,
		Time:    time.Date(2025, 3, 21, 14, 0, 0, 0, time.UTC),
		SpaceWeatherFeatures: SpaceWeatherFeatures{
			SFINow: 150, SFI72hMean: 150, Hp30Lag1: 1, Hp3012hMax: 1, Hp3024hMean: 1,
			BzSouth3h: 0, Vsw3h: 400, DstNow: 0, Dst24hMin: 0,
		},
	}

	var nilModel *Model
	if _, err := nilModel.Predict(valid); !errors.Is(err, ErrNilModel) {
		t.Fatalf("nil model error=%v, want ErrNilModel", err)
	}

	tests := []struct {
		name   string
		mutate func(*PathRequest)
		want   error
	}{
		{"lat > 90", func(r *PathRequest) { r.TXLat = 90.1 }, ErrInvalidCoordinates},
		{"lat < -90", func(r *PathRequest) { r.RXLat = -90.1 }, ErrInvalidCoordinates},
		{"lon > 180", func(r *PathRequest) { r.TXLon = 180.1 }, ErrInvalidCoordinates},
		{"lon < -180", func(r *PathRequest) { r.RXLon = -180.1 }, ErrInvalidCoordinates},
		{"nan lat", func(r *PathRequest) { r.TXLat = math.NaN() }, ErrInvalidCoordinates},
		{"zero freq", func(r *PathRequest) { r.FreqMHz = 0 }, ErrInvalidFrequency},
		{"neg freq", func(r *PathRequest) { r.FreqMHz = -7.0 }, ErrInvalidFrequency},
		{"inf freq", func(r *PathRequest) { r.FreqMHz = math.Inf(1) }, ErrInvalidFrequency},
		{"zero time", func(r *PathRequest) { r.Time = time.Time{} }, ErrZeroTime},
		{"nan sfi", func(r *PathRequest) { r.SFINow = math.NaN() }, ErrInvalidSpaceWeather},
		{"inf dst", func(r *PathRequest) { r.Dst24hMin = math.Inf(-1) }, ErrInvalidSpaceWeather},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := valid
			tc.mutate(&req)
			if _, err := model.Predict(req); !errors.Is(err, tc.want) {
				t.Fatalf("%s: err=%v, want %v", tc.name, err, tc.want)
			}
		})
	}
}

func TestPredictZeroAllocs(t *testing.T) {
	if raceDetectorEnabled {
		t.Skip("alloc count check skipped under -race")
	}
	model, err := LoadEmbedded()
	if err != nil {
		t.Fatalf("LoadEmbedded failed: %v", err)
	}
	spReq := PathRequest{
		TXLat: 40.7, TXLon: -74.0, RXLat: 41.9, RXLon: -87.6,
		FreqMHz: 7.038,
		Time:    time.Date(2025, 3, 21, 14, 0, 0, 0, time.UTC),
		SpaceWeatherFeatures: SpaceWeatherFeatures{
			SFINow: 150, SFI72hMean: 148, Hp30Lag1: 2, Hp3012hMax: 3, Hp3024hMean: 2,
			BzSouth3h: 1, Vsw3h: 420, DstNow: -10, Dst24hMin: -20,
		},
	}
	lpReq := spReq
	lpReq.RXLat = -33.9
	lpReq.RXLon = 151.2
	lpReq.FreqMHz = 14.095

	spAllocs := testing.AllocsPerRun(100, func() {
		p, predErr := model.Predict(spReq)
		if predErr != nil || p.LPEvaluated {
			t.Fatalf("unexpected SP result: pred=%+v err=%v", p, predErr)
		}
		benchPredSink = p
	})
	if spAllocs != 0 {
		t.Fatalf("SP-only Predict allocated %v allocs/op, want 0", spAllocs)
	}

	lpAllocs := testing.AllocsPerRun(100, func() {
		p, predErr := model.Predict(lpReq)
		if predErr != nil || !p.LPEvaluated {
			t.Fatalf("unexpected SP+LP result: pred=%+v err=%v", p, predErr)
		}
		benchPredSink = p
	})
	if lpAllocs != 0 {
		t.Fatalf("SP+LP Predict allocated %v allocs/op, want 0", lpAllocs)
	}
}

var benchPredSink Prediction

func BenchmarkPredictSPOnly(b *testing.B) {
	model, err := LoadEmbedded()
	if err != nil {
		b.Fatalf("LoadEmbedded failed: %v", err)
	}
	req := PathRequest{
		TXLat: 40.7, TXLon: -74.0, RXLat: 41.9, RXLon: -87.6,
		FreqMHz: 7.038,
		Time:    time.Date(2025, 3, 21, 14, 0, 0, 0, time.UTC),
		SpaceWeatherFeatures: SpaceWeatherFeatures{
			SFINow: 150, SFI72hMean: 148, Hp30Lag1: 2, Hp3012hMax: 3, Hp3024hMean: 2,
			BzSouth3h: 1, Vsw3h: 420, DstNow: -10, Dst24hMin: -20,
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	var p Prediction
	for range b.N {
		p, err = model.Predict(req)
		if err != nil {
			b.Fatal(err)
		}
	}
	benchPredSink = p
}

func BenchmarkPredictSPLP(b *testing.B) {
	model, err := LoadEmbedded()
	if err != nil {
		b.Fatalf("LoadEmbedded failed: %v", err)
	}
	req := PathRequest{
		TXLat: 40.7, TXLon: -74.0, RXLat: -33.9, RXLon: 151.2,
		FreqMHz: 14.095,
		Time:    time.Date(2025, 3, 21, 21, 0, 0, 0, time.UTC),
		SpaceWeatherFeatures: SpaceWeatherFeatures{
			SFINow: 150, SFI72hMean: 148, Hp30Lag1: 2, Hp3012hMax: 3, Hp3024hMean: 2,
			BzSouth3h: 1, Vsw3h: 420, DstNow: -10, Dst24hMin: -20,
		},
	}
	b.ReportAllocs()
	b.ResetTimer()
	var p Prediction
	for range b.N {
		p, err = model.Predict(req)
		if err != nil {
			b.Fatal(err)
		}
	}
	benchPredSink = p
}
