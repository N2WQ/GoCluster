// File role: Zero-allocation v4 MLP forward pass, SP/LP RF power-sum combiner,
// input validation, and threshold reliability calculation.
// Crawler notes: Hot-path inference entry point (Model.Predict). Uses stack
// scratch buffers bounded by maxLayerDim and unrolled scalar dot products.
package propprior

import (
	"errors"
	"math"
	"time"
)

const (
	skipPriorMinDistKM float32 = 100.0
	skipPriorRefDistKM float32 = 1000.0
	alphaDB            float32 = float32(math.Ln10 / 10.0)
)

var (
	// ErrNilModel is returned when Predict is called on a nil Model receiver.
	ErrNilModel = errors.New("propprior: nil model")
	// ErrInvalidCoordinates is returned when TX or RX coordinates are non-finite or out of bounds.
	ErrInvalidCoordinates = errors.New("propprior: invalid TX/RX coordinates")
	// ErrInvalidFrequency is returned when FreqMHz is non-finite or <= 0.
	ErrInvalidFrequency = errors.New("propprior: frequency must be finite and > 0")
	// ErrZeroTime is returned when PathRequest.Time is zero.
	ErrZeroTime = errors.New("propprior: observation time must be non-zero")
	// ErrInvalidSpaceWeather is returned when any space-weather feature is NaN or Inf.
	ErrInvalidSpaceWeather = errors.New("propprior: space-weather inputs must be finite")
	// ErrNonFinitePrediction is returned when extreme weights overflow float32 arithmetic.
	ErrNonFinitePrediction = errors.New("propprior: model produced non-finite prediction")
)

// SpaceWeatherFeatures holds the 9 causal space-weather inputs required by the
// v4 feature builder. geometry.go derives SFIDelta = SFINow - SFI72hMean and
// DstRecovery = max(0, DstNow - Dst24hMin).
type SpaceWeatherFeatures struct {
	SFINow      float64
	SFI72hMean  float64
	Hp30Lag1    float64
	Hp3012hMax  float64
	Hp3024hMean float64
	BzSouth3h   float64
	Vsw3h       float64
	DstNow      float64
	Dst24hMin   float64
}

// PathRequest specifies one HF propagation prior query.
type PathRequest struct {
	TXLat   float64
	TXLon   float64
	RXLat   float64
	RXLon   float64
	FreqMHz float64
	Time    time.Time
	SpaceWeatherFeatures
}

// Prediction holds the short-path, optional long-path, and RF power-sum combined
// SNR distribution parameters (dB, 100W WSPR reference scale). When LPEvaluated
// is false (d_sp < lp_min_dist_km), SnrLP, SigmaLP, and WeightLP are zero and
// SnrTotal/SigmaTotal equal SnrSP/SigmaSP.
type Prediction struct {
	LPEvaluated bool
	SnrSP       float64
	SigmaSP     float64
	SnrLP       float64
	SigmaLP     float64
	SnrTotal    float64
	SigmaTotal  float64
	WeightLP    float64
}

// Rel returns the predicted probability P(SNR >= thresholdDB) under the normal
// distribution N(SnrTotal, SigmaTotal^2), using math.Erfc to avoid tail
// cancellation on closed paths. Returns 0 for invalid Prediction parameters or
// NaN thresholds.
func (p Prediction) Rel(thresholdDB float64) float64 {
	if !isFinite64(p.SnrTotal) || !isFinite64(p.SigmaTotal) || p.SigmaTotal <= 0 || math.IsNaN(thresholdDB) {
		return 0
	}
	if math.IsInf(thresholdDB, -1) {
		return 1
	}
	if math.IsInf(thresholdDB, 1) {
		return 0
	}
	return 0.5 * math.Erfc((thresholdDB-p.SnrTotal)/(p.SigmaTotal*math.Sqrt2))
}

// Predict validates req and runs the zero-allocation v4 SP (and optional LP)
// propagation prior inference. TX and RX endpoints are canonically ordered so
// Predict(A->B) == Predict(B->A) identically.
func (m *Model) Predict(req PathRequest) (Prediction, error) {
	if m == nil {
		return Prediction{}, ErrNilModel
	}
	if err := validatePathRequest(&req); err != nil {
		return Prediction{}, err
	}

	geom := computeGreatCircleSPLP(req.TXLat, req.TXLon, req.RXLat, req.RXLon)
	solar := computeSolarContext(req.Time)
	logFreq := math.Log(req.FreqMHz)

	var xSP [NumFeatures]float32
	buildArcFeatures(&xSP, &m.oceanMask, geom.p1, geom.l1, geom.deltaSP, geom.azSP, logFreq, solar, req.SpaceWeatherFeatures)
	dSPKM := float32(earthRadiusKM * geom.deltaSP)
	snrSP, sigmaSP := m.forwardArc(&xSP, dSPKM)

	if dSPKM < m.lpMinDistKM {
		if !isFinite32(snrSP) || !isFinite32(sigmaSP) || sigmaSP <= 0 {
			return Prediction{}, ErrNonFinitePrediction
		}
		return Prediction{
			LPEvaluated: false,
			SnrSP:       float64(snrSP),
			SigmaSP:     float64(sigmaSP),
			SnrTotal:    float64(snrSP),
			SigmaTotal:  float64(sigmaSP),
		}, nil
	}

	var xLP [NumFeatures]float32
	buildArcFeatures(&xLP, &m.oceanMask, geom.p1, geom.l1, geom.deltaLP, geom.azLP, logFreq, solar, req.SpaceWeatherFeatures)
	dLPKM := float32(earthRadiusKM * geom.deltaLP)
	snrLP, sigmaLP := m.forwardArc(&xLP, dLPKM)

	snrTotal, sigmaTotal, weightLP := combineSPLP(snrSP, sigmaSP, snrLP, sigmaLP)
	if !isFinite32(snrSP) || !isFinite32(sigmaSP) || sigmaSP <= 0 ||
		!isFinite32(snrLP) || !isFinite32(sigmaLP) || sigmaLP <= 0 ||
		!isFinite32(snrTotal) || !isFinite32(sigmaTotal) || sigmaTotal <= 0 ||
		!isFinite32(weightLP) {
		return Prediction{}, ErrNonFinitePrediction
	}
	return Prediction{
		LPEvaluated: true,
		SnrSP:       float64(snrSP),
		SigmaSP:     float64(sigmaSP),
		SnrLP:       float64(snrLP),
		SigmaLP:     float64(sigmaLP),
		SnrTotal:    float64(snrTotal),
		SigmaTotal:  float64(sigmaTotal),
		WeightLP:    float64(weightLP),
	}, nil
}

func validatePathRequest(req *PathRequest) error {
	if !isFinite64(req.TXLat) || !isFinite64(req.TXLon) || !isFinite64(req.RXLat) || !isFinite64(req.RXLon) {
		return ErrInvalidCoordinates
	}
	if req.TXLat < -90 || req.TXLat > 90 || req.RXLat < -90 || req.RXLat > 90 {
		return ErrInvalidCoordinates
	}
	if req.TXLon < -180 || req.TXLon > 180 || req.RXLon < -180 || req.RXLon > 180 {
		return ErrInvalidCoordinates
	}
	if !isFinite64(req.FreqMHz) || req.FreqMHz <= 0 {
		return ErrInvalidFrequency
	}
	if req.Time.IsZero() {
		return ErrZeroTime
	}
	sw := &req.SpaceWeatherFeatures
	if !isFinite64(sw.SFINow) || !isFinite64(sw.SFI72hMean) ||
		!isFinite64(sw.Hp30Lag1) || !isFinite64(sw.Hp3012hMax) || !isFinite64(sw.Hp3024hMean) ||
		!isFinite64(sw.BzSouth3h) || !isFinite64(sw.Vsw3h) ||
		!isFinite64(sw.DstNow) || !isFinite64(sw.Dst24hMin) {
		return ErrInvalidSpaceWeather
	}
	return nil
}

func (m *Model) forwardArc(xRaw *[NumFeatures]float32, dArcKM float32) (float32, float32) {
	var bufA, bufB [maxLayerDim]float32
	for i := range NumFeatures {
		bufA[i] = (xRaw[i] - m.featureMean[i]) / m.featureStd[i]
	}

	inBuf := &bufA
	outBuf := &bufB
	lastLayer := len(m.layers) - 1
	for idx := range m.layers {
		layer := &m.layers[idx]
		evalDenseLayer(outBuf, inBuf, layer, idx < lastLayer, m.activation)
		inBuf, outBuf = outBuf, inBuf
	}

	muRaw := skipPriorDB(dArcKM) + inBuf[0]
	mu := m.muFloor + safeSoftplus32(muRaw-m.muFloor)
	sigma := safeSoftplus32(inBuf[1]) + m.sigmaFloor
	return mu, sigma
}

func evalDenseLayer(dst, src *[maxLayerDim]float32, layer *denseLayer, applyAct bool, activation uint32) {
	inDim := layer.inDim
	outDim := layer.outDim
	weights := layer.weights
	biases := layer.biases
	inSlice := src[:inDim]

	for o := range outDim {
		row := weights[o*inDim : (o+1)*inDim]
		var s0, s1, s2, s3 float64
		j := 0
		for ; j+3 < inDim; j += 4 {
			s0 += float64(row[j]) * float64(inSlice[j])
			s1 += float64(row[j+1]) * float64(inSlice[j+1])
			s2 += float64(row[j+2]) * float64(inSlice[j+2])
			s3 += float64(row[j+3]) * float64(inSlice[j+3])
		}
		sum := (s0 + s1) + (s2 + s3)
		for ; j < inDim; j++ {
			sum += float64(row[j]) * float64(inSlice[j])
		}
		v := float32(sum + float64(biases[o]))
		if applyAct {
			v = applyActivation32(v, activation)
		}
		dst[o] = v
	}
}

func applyActivation32(x float32, activation uint32) float32 {
	switch activation {
	case ActivationReLU:
		if x < 0 {
			return 0
		}
		return x
	case ActivationSiLU:
		if x >= 0 {
			return x / (1.0 + float32(math.Exp(float64(-x))))
		}
		e := float32(math.Exp(float64(x)))
		return x * e / (1.0 + e)
	case ActivationGELU:
		const invSqrt2 = 1.0 / math.Sqrt2
		return float32(0.5 * float64(x) * (1.0 + math.Erf(float64(x)*invSqrt2)))
	default:
		return x
	}
}

func skipPriorDB(dArcKM float32) float32 {
	if dArcKM < skipPriorMinDistKM {
		dArcKM = skipPriorMinDistKM
	}
	return float32(-20.0 * math.Log10(float64(dArcKM)/float64(skipPriorRefDistKM)))
}

func safeSoftplus32(x float32) float32 {
	absX := x
	if absX < 0 {
		absX = -absX
	}
	posX := x
	if posX < 0 {
		posX = 0
	}
	return posX + float32(math.Log1p(math.Exp(float64(-absX))))
}

func combineSPLP(snrSP, sigmaSP, snrLP, sigmaLP float32) (snrTotal, sigmaTotal, weightLP float32) {
	mMax, mMin := snrSP, snrLP
	if mMax < mMin {
		mMax, mMin = mMin, mMax
	}
	snrTotal = mMax + (1.0/alphaDB)*float32(math.Log1p(math.Exp(float64(alphaDB*(mMin-mMax)))))

	diff := alphaDB * (snrSP - snrLP)
	var wSP float32
	if diff >= 0 {
		wSP = 1.0 / (1.0 + float32(math.Exp(float64(-diff))))
	} else {
		e := float32(math.Exp(float64(diff)))
		wSP = e / (1.0 + e)
	}
	weightLP = 1.0 - wSP
	sigmaTotal = wSP*sigmaSP + weightLP*sigmaLP
	return snrTotal, sigmaTotal, weightLP
}

func isFinite64(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}
