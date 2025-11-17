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

// FormatDXCluster formats the spot in standard DX cluster format with fixed column positions
// Format follows the traditional layout used by DXSpider and other clusters:
//
//	Columns 0-5:   "DX de "
//	Columns 6-13:  Spotter callsign with colon (e.g., "K0RC:")
//	Columns 14-21: Frequency right-aligned (8 chars wide, includes decimal)
//	Columns 22-23: Two spaces
//	Columns 24-36: DX callsign left-aligned (13 chars wide)
//	Columns 37+:   Mode, report, comment, and time
//
// Example: "DX de K0RC:       7218.0  NG7X         FT8 -5 0259Z"
func (s *Spot) FormatDXCluster() string {
	// Format time as HHMMZ UTC
	timeStr := s.Time.UTC().Format("1504Z")

	// Build the spotter field: "DX de CALLSIGN:"
	// The spotter callsign + colon should be left-aligned in an 8-character field
	spotterField := fmt.Sprintf("%-8s", s.DECall+":")

	// Build the frequency field: right-aligned, 8 characters wide
	// Format: "  7218.0" or " 14074.0"
	freqField := fmt.Sprintf("%8.1f", s.Frequency)

	// Build the DX callsign field: left-aligned, 13 characters wide
	// Format: "NG7X         " or "5V7RU        "
	dxCallField := fmt.Sprintf("%-13s", s.DXCall)

	// Build the info part: mode and optional report
	var info string
	if s.Report != 0 {
		// Include report with sign: "FT8 -5" or "CW +23"
		info = fmt.Sprintf("%s %+d", s.Mode, s.Report)
	} else {
		// No report available, just mode
		info = s.Mode
	}

	// Assemble the complete spot line
	// Format: DX de <spotter> <freq>  <dxcall> <mode> <report> <time>
	return fmt.Sprintf("DX de %s%s  %s %s %s",
		spotterField,
		freqField,
		dxCallField,
		info,
		timeStr)
}

// FreqToBand converts a frequency in kHz to a band string
func FreqToBand(freq float64) string {
	switch {
	case freq >= 1800 && freq <= 2000:
		return "160m"
	case freq >= 3500 && freq <= 4000:
		return "80m"
	case freq >= 5330 && freq <= 5405:
		return "60m"
	case freq >= 7000 && freq <= 7300:
		return "40m"
	case freq >= 10100 && freq <= 10150:
		return "30m"
	case freq >= 14000 && freq <= 14350:
		return "20m"
	case freq >= 18068 && freq <= 18168:
		return "17m"
	case freq >= 21000 && freq <= 21450:
		return "15m"
	case freq >= 24890 && freq <= 24990:
		return "12m"
	case freq >= 28000 && freq <= 29700:
		return "10m"
	case freq >= 50000 && freq <= 54000:
		return "6m"
	case freq >= 144000 && freq <= 148000:
		return "2m"
	default:
		return "???"
	}
}

// IsValid performs basic validation on the spot
func (s *Spot) IsValid() bool {
	// Must have callsigns
	if s.DXCall == "" || s.DECall == "" {
		return false
	}

	// Frequency must be reasonable (1.8 MHz to 148 MHz)
	if s.Frequency < 1800 || s.Frequency > 148000 {
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
