package spot

import (
	"fmt"
	"hash/fnv"
	"strings"
	"time"
)

// SourceType identifies where a spot came from
type SourceType string

const (
	SourceManual      SourceType = "MANUAL"      // User-posted spot
	SourceRBN         SourceType = "RBN"         // Reverse Beacon Network
	SourceFT8         SourceType = "FT8"         // FT8 via MQTT
	SourceFT4         SourceType = "FT4"         // FT4 via MQTT
	SourcePSKReporter SourceType = "PSKREPORTER" // PSKReporter via MQTT
	SourceUpstream    SourceType = "UPSTREAM"    // From another cluster
)

// Spot represents a DX spot in canonical form
type Spot struct {
	ID         uint64     // Unique spot ID (monotonic counter)
	DXCall     string     // Station being spotted (e.g., "LZ5VV")
	DECall     string     // Station reporting the spot (e.g., "W1ABC")
	DELocation string     // Location of reporting station (e.g., "FN42")
	Frequency  float64    // Frequency in kHz (e.g., 14074.5)
	Band       string     // Band (e.g., "20m")
	Mode       string     // Mode (e.g., "CW", "SSB", "FT8")
	Report     int        // Signal report in dB (SNR for digital modes, signal strength for CW)
	Time       time.Time  // When the spot was created
	Comment    string     // User comment or additional info
	SourceType SourceType // Where this spot came from
	SourceNode string     // Originating node/cluster
	TTL        uint8      // Time-to-live for loop prevention
	Tags       []string   // Additional metadata tags
}

// NewSpot creates a new spot with sensible defaults
func NewSpot(dxCall, deCall string, freq float64, mode string) *Spot {
	return &Spot{
		DXCall:     strings.ToUpper(dxCall),
		DECall:     strings.ToUpper(deCall),
		Frequency:  freq,
		Mode:       strings.ToUpper(mode),
		Band:       FreqToBand(freq),
		Time:       time.Now().UTC(),
		SourceType: SourceManual,
		TTL:        5, // Default hop count
		Tags:       make([]string, 0),
		Report:     0, // 0 means no report available
	}
}

// Hash32 returns a 32-bit hash for deduplication
// Hash includes: DXCall|DECall|FreqKHz|TimeMinute
// This allows different spotters to report the same DX station
func (s *Spot) Hash32() uint32 {
	// Normalize components
	dxCall := strings.ToUpper(strings.TrimSpace(s.DXCall))
	deCall := strings.ToUpper(strings.TrimSpace(s.DECall))
	freqKHz := int64(s.Frequency) // Truncate to 1 kHz resolution
	timeMin := s.Time.Truncate(time.Minute).Unix()

	// Create composite string WITH spotter
	composite := fmt.Sprintf("%s|%s|%d|%d", dxCall, deCall, freqKHz, timeMin)

	// Hash it
	h := fnv.New32a()
	h.Write([]byte(composite))
	return h.Sum32()
}

// FormatDXCluster formats the spot in standard DX cluster format with exact column positions
//
// Column positions (0-indexed):
//
//	0-5:    "DX de " (6 characters)
//	6-?:    Spotter callsign with colon (variable length)
//	?-24:   Spaces and frequency (frequency right-aligned, ENDS at position 24)
//	25-26:  Two spaces
//	27-34:  DX callsign (8 characters, left-aligned)
//	35-39:  Five spaces
//	40+:    Mode + signal report + comment
//	75-79:  Time in HHMMZ format (exactly 5 characters)
//
// Signal report formatting:
//
//	CW, RTTY: No sign (e.g., "CW 23")
//	FT8, FT4: With sign (e.g., "FT8 -5" or "FT8 +12")
//
// Example output:
//
//	"DX de RN4WA:      14022.1  HF300LOS     CW 35 22 WPM CQ                0612Z"
//	"DX de W3LPL:       7009.5  K1ABC        FT8 -5 JO93fn42>HM68jp36       0615Z"
func (s *Spot) FormatDXCluster() string {
	// Format time as HHMMZ UTC (exactly 5 characters)
	timeStr := s.Time.UTC().Format("1504Z")

	// Build comment section: mode + signal report + comment
	var commentSection string
	if s.Report != 0 {
		// Format signal report based on mode
		var reportStr string
		mode := strings.ToUpper(s.Mode)

		// For CW and RTTY: no sign (signal strength is always positive)
		// For FT8, FT4 and other digital modes: include sign (SNR can be negative)
		if mode == "CW" || mode == "RTTY" {
			reportStr = fmt.Sprintf("%d", s.Report) // No sign for CW/RTTY
		} else {
			// FT8, FT4, and other digital modes use SNR with sign
			reportStr = fmt.Sprintf("%+d", s.Report) // Always show sign for digital modes
		}

		// Mode with signal report and comment
		if s.Comment != "" {
			commentSection = fmt.Sprintf("%s %s %s", s.Mode, reportStr, s.Comment)
		} else {
			commentSection = fmt.Sprintf("%s %s", s.Mode, reportStr)
		}
	} else {
		// No report available, just mode and comment
		if s.Comment != "" {
			commentSection = fmt.Sprintf("%s %s", s.Mode, s.Comment)
		} else {
			commentSection = s.Mode
		}
	}

	// CRITICAL: Build the line so frequency ALWAYS ends at position 24
	// 1. Start with "DX de " + spotter + ":"
	prefix := "DX de " + s.DECall + ":"

	// 2. Format the frequency as a string
	freqStr := fmt.Sprintf("%.1f", s.Frequency)

	// 3. Calculate how many spaces we need between spotter and frequency
	// Frequency must end at position 24, so total width to position 25 is 25 characters
	// We need: len(prefix) + spaces + len(freqStr) = 25
	totalWidthToFreqEnd := 25
	spacesNeeded := totalWidthToFreqEnd - len(prefix) - len(freqStr)
	if spacesNeeded < 1 {
		spacesNeeded = 1 // Minimum 1 space
	}

	// 4. Build the left part:
	//    - Prefix (DX de CALL:)
	//    - Variable spaces
	//    - Frequency (ends at position 24)
	//    - Two spaces (positions 25-26)
	//    - DX callsign (8 chars, positions 27-34)
	//    - Five spaces (positions 35-39)
	//    - Comment section starts at position 40
	leftPart := fmt.Sprintf("%s%s%s  %-8s     %s",
		prefix,                            // "DX de CALL:"
		strings.Repeat(" ", spacesNeeded), // Variable spaces to align frequency
		freqStr,                           // Frequency (ends at position 24)
		s.DXCall,                          // DX callsign, 8 chars left-aligned (positions 27-34)
		commentSection)                    // Mode + report + comment starting at position 40

	// Pad the left part to exactly 75 characters so time starts at column 75
	// If leftPart is already >= 75 chars, truncate it to 75
	if len(leftPart) > 75 {
		leftPart = leftPart[:75]
	}
	paddedLeft := fmt.Sprintf("%-75s", leftPart)

	// Add time at columns 75-79
	return paddedLeft + timeStr
}

// FreqToBand converts a frequency in kHz to a band string
func FreqToBand(freq float64) string {
	for _, band := range bandTable {
		if freq >= band.Min && freq <= band.Max {
			return band.Name
		}
	}
	return "???"
}

// IsValid performs basic validation on the spot
func (s *Spot) IsValid() bool {
	// Must have callsigns
	if s.DXCall == "" || s.DECall == "" {
		return false
	}

	// Frequency must be within a supported band range
	minFreq, maxFreq := FrequencyBounds()
	if s.Frequency < minFreq || s.Frequency > maxFreq {
		return false
	}

	// Mode must be set
	if s.Mode == "" {
		return false
	}

	return true
}

// String returns a human-readable representation
func (s *Spot) String() string {
	return fmt.Sprintf("[%s] %s spotted %s on %.1f kHz (%s %s) - %s",
		s.Time.Format("15:04:05"),
		s.DECall,
		s.DXCall,
		s.Frequency,
		s.Band,
		s.Mode,
		s.Comment)
}
