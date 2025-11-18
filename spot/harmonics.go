package spot

import (
	"math"
	"strings"
	"sync"
	"time"
)

// HarmonicSettings controls how harmonic detection behaves.
type HarmonicSettings struct {
	Enabled              bool
	RecencyWindow        time.Duration
	MaxHarmonicMultiple  int
	FrequencyToleranceHz float64
	MinReportDelta       int
}

// harmonicEntry stores a recently seen "fundamental" spot for comparison.
type harmonicEntry struct {
	frequency float64
	report    int
	at        time.Time
}

// HarmonicDetector tracks recent fundamentals per DX call and decides whether
// a new spot is likely a harmonic that should be dropped.
type HarmonicDetector struct {
	settings HarmonicSettings
	mu       sync.Mutex
	entries  map[string][]harmonicEntry
}

// NewHarmonicDetector creates a detector with the provided settings.
func NewHarmonicDetector(settings HarmonicSettings) *HarmonicDetector {
	return &HarmonicDetector{
		settings: settings,
		entries:  make(map[string][]harmonicEntry),
	}
}

// ShouldDrop returns true if the given spot appears to be a harmonic of a lower
// frequency fundamental. The second return value is the fundamental frequency
// that triggered the drop (in kHz) for logging purposes.
func (hd *HarmonicDetector) ShouldDrop(s *Spot, now time.Time) (bool, float64) {
	if hd == nil || !hd.settings.Enabled || s == nil {
		return false, 0
	}
	if !IsCallCorrectionCandidate(s.Mode) {
		return false, 0
	}

	call := strings.ToUpper(strings.TrimSpace(s.DXCall))
	if call == "" {
		return false, 0
	}

	hd.mu.Lock()
	defer hd.mu.Unlock()

	hd.prune(call, now)
	if fundamental := hd.detectHarmonic(call, s); fundamental > 0 {
		return true, fundamental
	}

	hd.entries[call] = append(hd.entries[call], harmonicEntry{
		frequency: s.Frequency,
		report:    s.Report,
		at:        s.Time,
	})
	return false, 0
}

func (hd *HarmonicDetector) detectHarmonic(call string, s *Spot) float64 {
	candidates := hd.entries[call]
	if len(candidates) == 0 {
		return 0
	}

	minDelta := hd.settings.MinReportDelta
	toleranceKHz := hd.settings.FrequencyToleranceHz / 1000.0

	var fundamental float64
	for _, entry := range candidates {
		if entry.frequency <= 0 || s.Frequency <= entry.frequency {
			continue
		}
		if minDelta > 0 && (entry.report-s.Report) < minDelta {
			continue
		}
		for mult := 2; mult <= hd.settings.MaxHarmonicMultiple; mult++ {
			expected := entry.frequency * float64(mult)
			if math.Abs(expected-s.Frequency) <= toleranceKHz {
				if entry.at.IsZero() || s.Time.Sub(entry.at) <= hd.settings.RecencyWindow {
					fundamental = entry.frequency
				}
			}
			if fundamental > 0 {
				break
			}
		}
		if fundamental > 0 {
			break
		}
	}
	return fundamental
}

func (hd *HarmonicDetector) prune(call string, now time.Time) {
	window := hd.settings.RecencyWindow
	slice := hd.entries[call]
	if len(slice) == 0 {
		return
	}
	cutoff := now.Add(-window)
	dst := slice[:0]
	for _, entry := range slice {
		if entry.at.After(cutoff) {
			dst = append(dst, entry)
		}
	}
	if len(dst) == 0 {
		delete(hd.entries, call)
		return
	}
	hd.entries[call] = dst
}
