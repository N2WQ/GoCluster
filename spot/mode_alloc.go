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
