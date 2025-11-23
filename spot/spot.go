package spot

import (
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
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
	ID         uint64       // Unique spot ID (monotonic counter)
	DXCall     string       // Station being spotted (e.g., "LZ5VV")
	DECall     string       // Station reporting the spot (e.g., "W1ABC")
	Frequency  float64      // Frequency in kHz (e.g., 14074.5)
	Band       string       // Band (e.g., "20m")
	Mode       string       // Mode (e.g., "CW", "SSB", "FT8")
	Report     int          // Signal report in dB (SNR for digital modes, signal strength for CW)
	Time       time.Time    // When the spot was created
	Comment    string       // User comment or additional info
	SourceType SourceType   // Where this spot came from
	SourceNode string       // Originating node/cluster
	TTL        uint8        // Time-to-live for loop prevention
	IsHuman    bool         // Whether the spot originated from a human operator
	IsBeacon   bool         // True when DX call ends with /B (beacon identifiers)
	DXMetadata CallMetadata // Metadata for the DX station
	DEMetadata CallMetadata // Metadata for the spotter station
	Confidence string       // Consensus confidence label (e.g., "75%" or "?")
	formatted  string
	formatOnce sync.Once // ensures FormatDXCluster builds expensive string only once per spot
}

// CallMetadata stores geographic metadata for a callsign
type CallMetadata struct {
	Continent string
	Country   string
	CQZone    int
	Grid      string
	ITUZone   int
}

// NewSpot creates a new spot with sensible defaults
func NewSpot(dxCall, deCall string, freq float64, mode string) *Spot {
	spot := &Spot{
		DXCall:     strings.ToUpper(dxCall),
		DECall:     strings.ToUpper(deCall),
		Frequency:  freq,
		Mode:       strings.ToUpper(mode),
		Band:       FreqToBand(freq),
		Time:       time.Now().UTC(),
		SourceType: SourceManual,
		TTL:        5, // Default hop count
		Report:     0, // 0 means no report available
		IsHuman:    true,
	}
	spot.RefreshBeaconFlag()
	return spot
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
//	69-73:  Time in HHMMZ format (exactly 5 characters)
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
	s.formatOnce.Do(func() {
		// Format time as HHMMZ UTC (exactly 5 characters)
		timeStr := s.Time.UTC().Format("1504Z")

		commentPayload := s.formatZoneGridComment()
		// Build comment section: mode + signal report + comment
		var commentSection string
		if s.Report != 0 {
			// Format signal report based on mode
			var reportStr string
			mode := strings.ToUpper(s.Mode)

			// For CW and RTTY: no sign (signal strength is always positive)
			// For FT8, FT4 and other digital modes: include sign (SNR can be negative)
			if mode == "CW" || mode == "RTTY" {
				reportStr = fmt.Sprintf("%d dB", s.Report)
			} else {
				// FT8, FT4, and other digital modes use SNR with sign
				reportStr = fmt.Sprintf("%+d", s.Report) // Always show sign for digital modes
			}

			// Mode with signal report and CQ zone/grid annotation
			commentSection = fmt.Sprintf("%s %s %s", s.Mode, reportStr, commentPayload)
		} else {
			// No report available, just mode and CQ zone/grid annotation
			commentSection = fmt.Sprintf("%s %s", s.Mode, commentPayload)
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

		// 4. Build the left part up through the DX callsign field
		leftPart := fmt.Sprintf("%s%s%s  %-8s",
			prefix,                            // "DX de CALL:"
			strings.Repeat(" ", spacesNeeded), // Variable spaces to align frequency
			freqStr,                           // Frequency (ends at position 24)
			s.DXCall,                          // DX callsign, 8 chars left-aligned (positions 27-34)
		)

		// 5. Insert enough spaces so the comment starts at column 40
		spacesToComment := 40 - len(leftPart)
		if spacesToComment < 1 {
			spacesToComment = 1
		}
		leftPart += strings.Repeat(" ", spacesToComment)
		leftPart += commentSection

		confLabel := strings.TrimSpace(s.Confidence)
		const (
			timeColumn     = 69 // position where the timestamp begins
			minGapToSymbol = 2  // minimum spaces between comment and confidence symbol
		)

		if confLabel == "" {
			// Pad/truncate so the time begins exactly at column timeColumn.
			if len(leftPart) > timeColumn {
				leftPart = leftPart[:timeColumn]
			} else if len(leftPart) < timeColumn {
				leftPart += strings.Repeat(" ", timeColumn-len(leftPart))
			}
			s.formatted = leftPart + timeStr
			return
		}

		confWidth := len(confLabel)
		maxConfWidth := timeColumn - minGapToSymbol
		if maxConfWidth < 1 {
			maxConfWidth = 1
		}
		if confWidth > maxConfWidth {
			confWidth = maxConfWidth
			confLabel = confLabel[:confWidth]
		}
		maxCommentLen := timeColumn - minGapToSymbol - confWidth
		if maxCommentLen < 0 {
			maxCommentLen = 0
		}
		if len(leftPart) > maxCommentLen {
			leftPart = leftPart[:maxCommentLen]
		}
		gapLen := timeColumn - confWidth - len(leftPart)
		if gapLen < minGapToSymbol {
			gapLen = minGapToSymbol
		}

		s.formatted = leftPart + strings.Repeat(" ", gapLen) + confLabel + " " + timeStr
	})

	return s.formatted
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

func (s *Spot) formatZoneGridComment() string {
	return fmt.Sprintf("CQ %s %s",
		formatCQZoneLabel(s.DXMetadata.CQZone),
		formatGridLabel(s.DXMetadata.Grid))
}

func formatCQZoneLabel(zone int) string {
	if zone <= 0 {
		return "??"
	}
	return fmt.Sprintf("%02d", zone)
}

func formatGridLabel(grid string) string {
	grid = strings.TrimSpace(strings.ToUpper(grid))
	if grid == "" {
		return "????"
	}
	if len(grid) > 4 {
		grid = grid[:4]
	}
	return grid
}
