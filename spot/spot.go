// Package spot defines the canonical spot data model and related utilities.
//
// A "spot" in amateur radio represents an observation of a station on the air.
// This package provides the core Spot type that unifies spots from multiple sources
// (RBN, PSKReporter, manual entry, upstream clusters) into a single format.
//
// Key Features:
//   - Unified data model for all spot sources
//   - Frequency-to-band conversion (160m - 2m)
//   - 32-bit hashing for deduplication (includes DXCall, DECall, Frequency, Time)
//   - DX Cluster format output (standard telnet protocol)
//   - Spot validation
//   - Source tracking (RBN, FT8, PSKReporter, etc.)
//
// The Spot structure is the fundamental data type that flows through the entire
// system: from sources → deduplicator → ring buffer → telnet clients.
package spot

import (
	"fmt"
	"hash/fnv"
	"strings"
	"time"
)

// SourceType identifies the origin of a spot.
// This allows clients to filter spots based on their source and helps with
// debugging and statistics tracking.
type SourceType string

const (
	SourceManual      SourceType = "MANUAL"      // User-posted spot via telnet command
	SourceRBN         SourceType = "RBN"         // Reverse Beacon Network (automated CW/RTTY skimmers)
	SourceFT8         SourceType = "FT8"         // FT8 digital mode via PSKReporter/MQTT
	SourceFT4         SourceType = "FT4"         // FT4 digital mode via PSKReporter/MQTT
	SourcePSKReporter SourceType = "PSKREPORTER" // PSKReporter service (digital modes)
	SourceUpstream    SourceType = "UPSTREAM"    // Received from another DX cluster server
)

// Spot represents a DX spot in canonical form.
// This is the core data structure that flows through the entire system.
//
// All spot sources (RBN, PSKReporter, manual entry, upstream clusters) are
// normalized into this format to enable unified processing, deduplication,
// filtering, and storage.
//
// The spot includes:
//   - Station information: DXCall (spotted station), DECall (spotter)
//   - Frequency/band/mode details
//   - Timestamp and location
//   - Source tracking and metadata
//   - TTL for distributed cluster networks (prevents routing loops)
type Spot struct {
	ID         uint64     // Unique spot ID assigned by ring buffer (monotonic counter)
	DXCall     string     // Callsign of station being spotted (e.g., "LZ5VV")
	DECall     string     // Callsign of station reporting the spot (e.g., "W1ABC")
	DELocation string     // Maidenhead grid locator of spotter (e.g., "FN42")
	Frequency  float64    // Frequency in kHz (e.g., 14074.5 for 20m FT8)
	Band       string     // Amateur radio band (e.g., "20m", "40m", "160m")
	Mode       string     // Operating mode (e.g., "CW", "SSB", "FT8", "RTTY")
	Time       time.Time  // Timestamp when the spot was created (UTC)
	Comment    string     // Additional information (signal reports, WPM, etc.)
	SourceType SourceType // Origin of this spot (RBN, PSKReporter, manual, etc.)
	SourceNode string     // Originating cluster node (for distributed networks)
	TTL        uint8      // Time-to-live hop count (prevents routing loops in cluster networks)
	Tags       []string   // Extensible metadata tags for future features
}

// NewSpot creates a new spot with sensible defaults for manual entry.
//
// Parameters:
//   - dxCall: Callsign of station being spotted
//   - deCall: Callsign of station reporting the spot
//   - freq: Frequency in kHz
//   - mode: Operating mode (e.g., "CW", "SSB", "FT8")
//
// Returns a spot with:
//   - Callsigns normalized to uppercase
//   - Band automatically determined from frequency
//   - Current UTC time
//   - SourceType set to SourceManual
//   - TTL set to 5 (standard for cluster networks)
//   - Empty tags array initialized
//
// This constructor is primarily used for manually posted spots and testing.
// Automated sources (RBN, PSKReporter) typically construct spots directly.
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

// Hash32 returns a 32-bit hash for deduplication purposes.
//
// The hash is calculated from:
//   - DXCall (spotted callsign, normalized)
//   - DECall (spotter callsign, normalized)
//   - Frequency (truncated to 1 kHz resolution)
//   - Time (truncated to 1 minute resolution)
//
// This hash design has important characteristics:
//  1. INCLUDES the spotter (DECall) - same DX station spotted by different stations
//     will have different hashes. This is intentional.
//  2. Truncates frequency to 1 kHz - small frequency variations won't create duplicates
//  3. Truncates time to 1 minute - spots within the same minute are considered similar
//
// The FNV-1a hash algorithm is used for speed and good distribution.
//
// Returns: 32-bit hash value suitable for deduplication lookups
func (s *Spot) Hash32() uint32 {
	// Normalize callsigns: uppercase and trim whitespace
	dxCall := strings.ToUpper(strings.TrimSpace(s.DXCall))
	deCall := strings.ToUpper(strings.TrimSpace(s.DECall))

	// Truncate frequency to 1 kHz resolution
	// Example: 14074.5 and 14074.8 both become 14074
	freqKHz := int64(s.Frequency)

	// Truncate time to minute resolution (ignore seconds)
	// Example: 14:35:23 and 14:35:47 both become 14:35:00
	timeMin := s.Time.Truncate(time.Minute).Unix()

	// Create composite string with pipe delimiters
	// Format: "DXCall|DECall|FreqKHz|TimeMinute"
	// Example: "LZ5VV|W1ABC|14074|1700000000"
	composite := fmt.Sprintf("%s|%s|%d|%d", dxCall, deCall, freqKHz, timeMin)

	// Compute FNV-1a 32-bit hash
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
	// Format time as HHMMZ UTC (e.g., "1504Z" for 15:04 UTC)
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

// FreqToBand converts a frequency in kHz to an amateur radio band designation.
//
// Parameter:
//   - freq: Frequency in kHz (e.g., 14074.5)
//
// Returns:
//   - Band string (e.g., "20m", "40m", "160m")
//   - "???" if the frequency doesn't fall within a recognized amateur band
//
// Supported bands (160m through 2m):
//   - 160m: 1800-2000 kHz
//   - 80m:  3500-4000 kHz
//   - 60m:  5330-5405 kHz (relatively new band)
//   - 40m:  7000-7300 kHz
//   - 30m:  10100-10150 kHz (WARC band)
//   - 20m:  14000-14350 kHz (most popular DX band)
//   - 17m:  18068-18168 kHz (WARC band)
//   - 15m:  21000-21450 kHz
//   - 12m:  24890-24990 kHz (WARC band)
//   - 10m:  28000-29700 kHz
//   - 6m:   50000-54000 kHz (VHF)
//   - 2m:   144000-148000 kHz (VHF)
//
// Note: Ranges are approximate and include common amateur allocations.
// Regional variations exist.
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
		return "30m" // WARC band (no contests)
	case freq >= 14000 && freq <= 14350:
		return "20m"
	case freq >= 18068 && freq <= 18168:
		return "17m" // WARC band
	case freq >= 21000 && freq <= 21450:
		return "15m"
	case freq >= 24890 && freq <= 24990:
		return "12m" // WARC band
	case freq >= 28000 && freq <= 29700:
		return "10m"
	case freq >= 50000 && freq <= 54000:
		return "6m" // VHF
	case freq >= 144000 && freq <= 148000:
		return "2m" // VHF
	default:
		return "???" // Unknown or out-of-band frequency
	}
}

// IsValid performs basic validation on the spot to ensure it has minimum required fields.
//
// Validation checks:
//  1. DXCall (spotted station) must not be empty
//  2. DECall (spotter) must not be empty
//  3. Frequency must be in amateur radio range (1.8 MHz to 148 MHz / 1800-148000 kHz)
//  4. Mode must not be empty
//
// Returns:
//   - true if the spot passes all validation checks
//   - false if any validation fails
//
// Invalid spots should be rejected by the system to prevent malformed data
// from entering the ring buffer or being broadcast to clients.
func (s *Spot) IsValid() bool {
	// Must have callsigns for both spotted station and spotter
	if s.DXCall == "" || s.DECall == "" {
		return false
	}

	// Frequency must be within amateur radio spectrum (1.8 MHz to 148 MHz)
	// This covers HF through 2m VHF
	if s.Frequency < 1800 || s.Frequency > 148000 {
		return false
	}

	// Mode must be specified
	if s.Mode == "" {
		return false
	}

	return true
}

// String returns a human-readable representation of the spot for logging and debugging.
//
// Format: [HH:MM:SS] <spotter> spotted <spotted> on <freq> kHz (<band> <mode>) - <comment>
// Example: [15:04:23] W1ABC spotted LZ5VV on 14074.5 kHz (20m FT8) - -12dB
//
// This format is more verbose than FormatDXCluster() and is intended for:
//   - Debug logging
//   - System logs
//   - Development/testing output
//
// For client display, use FormatDXCluster() instead.
//
// Returns: Human-readable string representation
func (s *Spot) String() string {
	return fmt.Sprintf("[%s] %s spotted %s on %.1f kHz (%s %s) - %s",
		s.Time.Format("15:04:05"), // Local time format HH:MM:SS
		s.DECall,
		s.DXCall,
		s.Frequency,
		s.Band,
		s.Mode,
		s.Comment)
}
