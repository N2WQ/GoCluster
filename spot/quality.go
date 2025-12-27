package spot

import (
	"strings"
	"sync"
)

// callFreqKey scopes a call to a frequency bin.
type callFreqKey struct {
	Call string
	Bin  int
}

// CallQualityStore tracks per-call, per-bin quality scores used as anchors.
type CallQualityStore struct {
	mu   sync.Mutex
	data map[callFreqKey]int
}

// Purpose: Construct an empty call quality store.
// Key aspects: Initializes the internal map.
// Upstream: global callQuality initialization.
// Downstream: map allocation.
// NewCallQualityStore constructs an empty quality store.
func NewCallQualityStore() *CallQualityStore {
	return &CallQualityStore{
		data: make(map[callFreqKey]int),
	}
}

// Purpose: Convert a frequency into a bin index.
// Key aspects: Uses bin size and returns -1 for global priors.
// Upstream: CallQualityStore methods.
// Downstream: integer math.
// freqBinHz returns the integer bin for a given frequency in Hz and bin size.
func freqBinHz(freqHz float64, binSizeHz int) int {
	if binSizeHz <= 0 {
		binSizeHz = 1000
	}
	if freqHz <= 0 {
		return -1 // sentinel for global priors that apply to all bins
	}
	return int(freqHz) / binSizeHz
}

// Purpose: Retrieve the quality score for a call/bin.
// Key aspects: Falls back to global prior bin (-1).
// Upstream: correction logic and IsGood.
// Downstream: freqBinHz and map access under lock.
// Get returns the quality score for a call in the given bin.
func (s *CallQualityStore) Get(call string, freqHz float64, binSizeHz int) int {
	if s == nil {
		return 0
	}
	call = strings.ToUpper(strings.TrimSpace(call))
	if call == "" {
		return 0
	}
	bin := freqBinHz(freqHz, binSizeHz)
	key := callFreqKey{Call: call, Bin: bin}
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.data[key]; ok {
		return v
	}
	// Fallback to a global prior (bin -1) when present.
	if bin != -1 {
		if v, ok := s.data[callFreqKey{Call: call, Bin: -1}]; ok {
			return v
		}
	}
	return 0
}

// Purpose: Adjust a call/bin quality score by delta.
// Key aspects: Normalizes call and skips zero deltas.
// Upstream: correction anchors and priors loading.
// Downstream: freqBinHz and map mutation under lock.
// Add adjusts the quality score for a call/bin by delta.
func (s *CallQualityStore) Add(call string, freqHz float64, binSizeHz int, delta int) {
	if s == nil {
		return
	}
	call = strings.ToUpper(strings.TrimSpace(call))
	if call == "" || delta == 0 {
		return
	}
	key := callFreqKey{Call: call, Bin: freqBinHz(freqHz, binSizeHz)}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = s.data[key] + delta
}

// Purpose: Determine whether a call meets the quality threshold.
// Key aspects: Uses config bin size and threshold.
// Upstream: call correction decision logic.
// Downstream: Get.
// IsGood reports whether the call meets the configured quality threshold in the bin.
func (s *CallQualityStore) IsGood(call string, freqHz float64, cfg *CorrectionSettings) bool {
	if cfg == nil {
		return false
	}
	return s.Get(call, freqHz, cfg.QualityBinHz) >= cfg.QualityGoodThreshold
}

// callQuality is the shared store used by correction anchors.
var callQuality = NewCallQualityStore()
