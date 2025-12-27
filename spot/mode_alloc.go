package spot

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// modeAllocation mirrors the YAML schema for band/mode allocations.
type modeAllocation struct {
	Band      string  `yaml:"band"`
	LowerKHz  float64 `yaml:"lower_khz"`
	CWEndKHz  float64 `yaml:"cw_end_khz"`
	UpperKHz  float64 `yaml:"upper_khz"`
	VoiceMode string  `yaml:"voice_mode"`
}

type modeAllocTable struct {
	Bands []modeAllocation `yaml:"bands"`
}

var (
	modeAllocOnce sync.Once
	modeAlloc     []modeAllocation
)

const modeAllocPath = "data/config/mode_allocations.yaml"

func loadModeAllocations() {
	// Purpose: Load the mode allocation table from YAML once.
	// Key aspects: Tries local and parent paths; logs warnings on failure.
	// Upstream: GuessModeFromAlloc.
	// Downstream: os.ReadFile and yaml.Unmarshal.
	modeAllocOnce.Do(func() {
		paths := []string{modeAllocPath, filepath.Join("..", modeAllocPath)}
		for _, path := range paths {
			data, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			var table modeAllocTable
			if err := yaml.Unmarshal(data, &table); err != nil {
				log.Printf("Warning: unable to parse mode allocations (%s): %v", path, err)
				return
			}
			modeAlloc = table.Bands
			return
		}
		log.Printf("Warning: unable to load mode allocations from %s (or parent): file not found", modeAllocPath)
	})
}

// Purpose: Map a frequency to a mode using the allocation table.
// Key aspects: Applies CW sub-band boundary and voice mode selection.
// Upstream: FinalizeMode and mode inference fallback.
// Downstream: loadModeAllocations and strings.TrimSpace.
// GuessModeFromAlloc returns the allocated mode for the given frequency based on the YAML table.
func GuessModeFromAlloc(freqKHz float64) string {
	loadModeAllocations()
	for _, b := range modeAlloc {
		if freqKHz >= b.LowerKHz && freqKHz <= b.UpperKHz {
			if b.CWEndKHz > 0 && freqKHz <= b.CWEndKHz {
				return "CW"
			}
			if strings.TrimSpace(b.VoiceMode) != "" {
				return strings.ToUpper(strings.TrimSpace(b.VoiceMode))
			}
		}
	}
	return ""
}

// Purpose: Normalize voice modes (SSB) to USB/LSB by frequency.
// Key aspects: USB above 10 MHz, LSB below.
// Upstream: comment parsing and FinalizeMode.
// Downstream: strings.ToUpper.
// NormalizeVoiceMode maps generic SSB to LSB/USB depending on frequency.
func NormalizeVoiceMode(mode string, freqKHz float64) string {
	upper := strings.ToUpper(strings.TrimSpace(mode))
	if upper == "SSB" {
		if freqKHz >= 10000 {
			return "USB"
		}
		return "LSB"
	}
	return upper
}

// Purpose: Finalize mode selection using explicit mode, allocations, and defaults.
// Key aspects: Prefers explicit mode, then allocation, then USB/CW fallback.
// Upstream: ModeAssigner fallback and callers needing a final mode.
// Downstream: NormalizeVoiceMode and GuessModeFromAlloc.
// FinalizeMode harmonizes mode selection using explicit mode, allocations, and sensible defaults.
func FinalizeMode(mode string, freq float64) string {
	mode = NormalizeVoiceMode(mode, freq)
	if mode != "" {
		return mode
	}
	if alloc := GuessModeFromAlloc(freq); alloc != "" {
		return NormalizeVoiceMode(alloc, freq)
	}
	if freq >= 10000 {
		return "USB"
	}
	return "CW"
}
