// Package propprior implements a pure-Go, zero-allocation HF propagation prior
// inference engine and causal space-weather state model matching the frozen
// hf-prop-lab v4 binary contract.
//
// File role: Embedded binary asset storage, HFPR v4 header/CRC-32 parser, and
// immutable Model construction.
// Crawler notes: Entry point for loading the propagation prior weights and
// 0.5-degree ocean bitmask. Production binaries embed only assets/*.bin;
// testdata/golden_test.json is test-only.
package propprior

import (
	_ "embed"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
)

const (
	// FormatVersion is the required binary container version for HFPR weights.
	FormatVersion uint32 = 4
	// FeatureSchemaVersion is the required 31-feature input schema version.
	FeatureSchemaVersion uint32 = 4
	// NumFeatures is the exact v4 feature vector length.
	NumFeatures = 31

	// OceanMaskRows is the latitude cell count (0.5-degree resolution, -90..+90).
	OceanMaskRows = 360
	// OceanMaskCols is the longitude cell count (0.5-degree resolution, -180..+180).
	OceanMaskCols = 720
	// OceanMaskBytes is the packed LSB-first bitmask size (360 * 720 / 8).
	OceanMaskBytes = (OceanMaskRows * OceanMaskCols) / 8

	// ActivationReLU selects ReLU hidden activations in HFPR v4 binaries.
	ActivationReLU uint32 = 0
	// ActivationSiLU selects SiLU hidden activations in HFPR v4 binaries.
	ActivationSiLU uint32 = 1
	// ActivationGELU selects exact erf-based GELU hidden activations in HFPR v4 binaries.
	ActivationGELU uint32 = 2

	// EmbeddedWeightsCRC32 is the pinned payload IEEE CRC-32 of the shipped v4.3 weights.
	EmbeddedWeightsCRC32 uint32 = 0x857c390f
	// EmbeddedOceanMaskCRC32 is the pinned IEEE CRC-32 of the shipped 0.5-degree ocean mask.
	EmbeddedOceanMaskCRC32 uint32 = 0xc380b37e

	binaryMagic     = "HFPR"
	headerSizeBytes = 4 + 32
	crcSizeBytes    = 4
	minBinaryBytes  = headerSizeBytes + crcSizeBytes
	maxBinaryBytes  = 1 << 20 // 1 MiB defensive upper bound on weight binaries.
	maxLayers       = 16
	maxLayerDim     = 256
	featureStdFloor = float32(1e-6) // Matches hf-prop-lab/src/config.py FEATURE_STD_FLOOR.
)

var (
	//go:embed assets/prior_weights.bin
	embeddedWeights []byte

	//go:embed assets/ocean_mask_0p5deg.bin
	embeddedOceanMask []byte
)

// denseLayer stores one row-major [outDim x inDim] linear layer and its bias vector.
type denseLayer struct {
	inDim   int
	outDim  int
	weights []float32
	biases  []float32
}

// Model holds immutable propagation-prior MLP weights, feature standardization
// vectors, and the 0.5-degree ocean bitmask. A loaded Model is read-only and
// safe for lock-free concurrent Predict calls.
type Model struct {
	formatVersion        uint32
	featureSchemaVersion uint32
	activation           uint32
	sigmaFloor           float32
	lpMinDistKM          float32
	muFloor              float32
	layerDims            []int
	featureMean          [NumFeatures]float32
	featureStd           [NumFeatures]float32
	layers               []denseLayer
	oceanMask            [OceanMaskBytes]byte
}

// LoadEmbedded validates the pinned production asset checksums and constructs
// a read-only Model from the embedded v4.3 binary assets.
func LoadEmbedded() (*Model, error) {
	if len(embeddedWeights) < minBinaryBytes {
		return nil, fmt.Errorf("propprior: embedded weights truncated (%d bytes)", len(embeddedWeights))
	}
	payloadCRC := crc32.ChecksumIEEE(embeddedWeights[:len(embeddedWeights)-crcSizeBytes])
	if payloadCRC != EmbeddedWeightsCRC32 {
		return nil, fmt.Errorf("propprior: embedded weights CRC-32 mismatch: got 0x%08x, want 0x%08x", payloadCRC, EmbeddedWeightsCRC32)
	}
	maskCRC := crc32.ChecksumIEEE(embeddedOceanMask)
	if maskCRC != EmbeddedOceanMaskCRC32 {
		return nil, fmt.Errorf("propprior: embedded ocean mask CRC-32 mismatch: got 0x%08x, want 0x%08x", maskCRC, EmbeddedOceanMaskCRC32)
	}
	return LoadFromBytes(embeddedWeights, embeddedOceanMask)
}

// LoadFromBytes parses and validates an HFPR v4 weights binary and a 32,400-byte
// ocean bitmask into an owned immutable Model. Caller slices are never retained.
func LoadFromBytes(weights, oceanMask []byte) (*Model, error) {
	if len(oceanMask) != OceanMaskBytes {
		return nil, fmt.Errorf("propprior: ocean mask must be %d bytes, got %d", OceanMaskBytes, len(oceanMask))
	}
	if len(weights) < minBinaryBytes {
		return nil, fmt.Errorf("propprior: weights binary too small (%d bytes)", len(weights))
	}
	if len(weights) > maxBinaryBytes {
		return nil, fmt.Errorf("propprior: weights binary exceeds %d byte limit (%d bytes)", maxBinaryBytes, len(weights))
	}

	payload := weights[:len(weights)-crcSizeBytes]
	expectedCRC := binary.LittleEndian.Uint32(weights[len(weights)-crcSizeBytes:])
	if actualCRC := crc32.ChecksumIEEE(payload); actualCRC != expectedCRC {
		return nil, fmt.Errorf("propprior: weights CRC-32 mismatch: expected 0x%08x, got 0x%08x", expectedCRC, actualCRC)
	}
	if string(payload[:4]) != binaryMagic {
		return nil, fmt.Errorf("propprior: invalid magic %q", string(payload[:4]))
	}

	m, numLayers, err := parseHeader(payload[4:headerSizeBytes])
	if err != nil {
		return nil, err
	}
	offset := headerSizeBytes

	dimsBytes := (numLayers + 1) * 4
	if offset+dimsBytes > len(payload) {
		return nil, errors.New("propprior: truncated layer dimensions")
	}
	layerDims, expectedPayloadBytes, err := parseLayerDims(payload[offset:offset+dimsBytes], numLayers)
	if err != nil {
		return nil, err
	}
	if expectedPayloadBytes != int64(len(payload)) {
		return nil, fmt.Errorf("propprior: payload length mismatch: got %d bytes, expected %d", len(payload), expectedPayloadBytes)
	}
	offset += dimsBytes
	m.layerDims = layerDims

	offset, err = parseScaler(payload, offset, &m.featureMean, &m.featureStd)
	if err != nil {
		return nil, err
	}

	layers, err := parseLayers(payload, offset, layerDims)
	if err != nil {
		return nil, err
	}
	m.layers = layers
	copy(m.oceanMask[:], oceanMask)
	return m, nil
}

// FormatVersion returns the parsed HFPR format version.
func (m *Model) FormatVersion() uint32 {
	if m == nil {
		return 0
	}
	return m.formatVersion
}

// FeatureSchemaVersion returns the parsed feature schema version.
func (m *Model) FeatureSchemaVersion() uint32 {
	if m == nil {
		return 0
	}
	return m.featureSchemaVersion
}

// LPMinDistKM returns the short-path distance threshold (km) above which long-path
// inference is evaluated.
func (m *Model) LPMinDistKM() float32 {
	if m == nil {
		return 0
	}
	return m.lpMinDistKM
}

// MuFloor returns the smooth lower SNR floor (dB) loaded from the weights header.
func (m *Model) MuFloor() float32 {
	if m == nil {
		return 0
	}
	return m.muFloor
}

// SigmaFloor returns the lower uncertainty floor (dB) loaded from the weights header.
func (m *Model) SigmaFloor() float32 {
	if m == nil {
		return 0
	}
	return m.sigmaFloor
}

func parseHeader(hdr []byte) (*Model, int, error) {
	fmtVer := binary.LittleEndian.Uint32(hdr[0:4])
	schemaVer := binary.LittleEndian.Uint32(hdr[4:8])
	activation := binary.LittleEndian.Uint32(hdr[8:12])
	sigmaFloor := math.Float32frombits(binary.LittleEndian.Uint32(hdr[12:16]))
	lpMinDistKM := math.Float32frombits(binary.LittleEndian.Uint32(hdr[16:20]))
	muFloor := math.Float32frombits(binary.LittleEndian.Uint32(hdr[20:24]))
	numFeatures := binary.LittleEndian.Uint32(hdr[24:28])
	numLayers := binary.LittleEndian.Uint32(hdr[28:32])

	if fmtVer != FormatVersion {
		return nil, 0, fmt.Errorf("propprior: unsupported format_version %d", fmtVer)
	}
	if schemaVer != FeatureSchemaVersion {
		return nil, 0, fmt.Errorf("propprior: unsupported feature_schema_version %d", schemaVer)
	}
	if activation != ActivationReLU && activation != ActivationSiLU && activation != ActivationGELU {
		return nil, 0, fmt.Errorf("propprior: unsupported activation %d", activation)
	}
	if !isFinite32(sigmaFloor) || sigmaFloor <= 0 {
		return nil, 0, fmt.Errorf("propprior: invalid sigma_floor %v", sigmaFloor)
	}
	if !isFinite32(lpMinDistKM) || lpMinDistKM < 0 {
		return nil, 0, fmt.Errorf("propprior: invalid lp_min_dist_km %v", lpMinDistKM)
	}
	if !isFinite32(muFloor) {
		return nil, 0, fmt.Errorf("propprior: invalid mu_floor %v", muFloor)
	}
	if numFeatures != NumFeatures {
		return nil, 0, fmt.Errorf("propprior: expected %d features, got %d", NumFeatures, numFeatures)
	}
	if numLayers == 0 || numLayers > maxLayers {
		return nil, 0, fmt.Errorf("propprior: layer count %d out of bounds [1,%d]", numLayers, maxLayers)
	}

	return &Model{
		formatVersion:        fmtVer,
		featureSchemaVersion: schemaVer,
		activation:           activation,
		sigmaFloor:           sigmaFloor,
		lpMinDistKM:          lpMinDistKM,
		muFloor:              muFloor,
	}, int(numLayers), nil
}

func parseLayerDims(raw []byte, numLayers int) ([]int, int64, error) {
	dims := make([]int, numLayers+1)
	for i := range dims {
		d := binary.LittleEndian.Uint32(raw[i*4 : (i+1)*4])
		if d == 0 || d > maxLayerDim {
			return nil, 0, fmt.Errorf("propprior: layer_dims[%d]=%d out of bounds [1,%d]", i, d, maxLayerDim)
		}
		dims[i] = int(d)
	}
	if dims[0] != NumFeatures {
		return nil, 0, fmt.Errorf("propprior: layer_dims[0]=%d must equal %d", dims[0], NumFeatures)
	}
	if dims[numLayers] != 2 {
		return nil, 0, fmt.Errorf("propprior: output layer dim %d must equal 2", dims[numLayers])
	}

	// Compute exact expected payload bytes in int64 before allocating any weight buffers.
	expected := int64(headerSizeBytes) + int64(numLayers+1)*4 + int64(NumFeatures)*2*4
	for i := range numLayers {
		inDim := int64(dims[i])
		outDim := int64(dims[i+1])
		expected += (outDim*inDim + outDim) * 4
	}
	return dims, expected, nil
}

func parseScaler(payload []byte, offset int, mean, std *[NumFeatures]float32) (int, error) {
	for i := range NumFeatures {
		v := math.Float32frombits(binary.LittleEndian.Uint32(payload[offset : offset+4]))
		if !isFinite32(v) {
			return 0, fmt.Errorf("propprior: non-finite feature_mean[%d]", i)
		}
		mean[i] = v
		offset += 4
	}
	for i := range NumFeatures {
		v := math.Float32frombits(binary.LittleEndian.Uint32(payload[offset : offset+4]))
		if !isFinite32(v) || v < featureStdFloor {
			return 0, fmt.Errorf("propprior: invalid feature_std[%d]=%v", i, v)
		}
		std[i] = v
		offset += 4
	}
	return offset, nil
}

func parseLayers(payload []byte, offset int, layerDims []int) ([]denseLayer, error) {
	numLayers := len(layerDims) - 1
	layers := make([]denseLayer, numLayers)
	for i := range numLayers {
		inDim := layerDims[i]
		outDim := layerDims[i+1]
		wCount := outDim * inDim
		w := make([]float32, wCount)
		for j := range wCount {
			v := math.Float32frombits(binary.LittleEndian.Uint32(payload[offset : offset+4]))
			if !isFinite32(v) {
				return nil, fmt.Errorf("propprior: non-finite weight at layer %d index %d", i, j)
			}
			w[j] = v
			offset += 4
		}
		b := make([]float32, outDim)
		for j := range outDim {
			v := math.Float32frombits(binary.LittleEndian.Uint32(payload[offset : offset+4]))
			if !isFinite32(v) {
				return nil, fmt.Errorf("propprior: non-finite bias at layer %d index %d", i, j)
			}
			b[j] = v
			offset += 4
		}
		layers[i] = denseLayer{
			inDim:   inDim,
			outDim:  outDim,
			weights: w,
			biases:  b,
		}
	}
	return layers, nil
}

func isFinite32(v float32) bool {
	f := float64(v)
	return !math.IsNaN(f) && !math.IsInf(f, 0)
}
