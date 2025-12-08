// Package spot defines the canonical spot structure and helpers used across the
// cluster pipeline: creation, formatting, hashing for dedup, and basic
// validation/mapping to bands.
package spot

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/zeebo/xxh3"
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

	// Normalized/cache fields to avoid repeated string ops on hot paths.
	ModeNorm        string
	BandNorm        string
	DXCallNorm      string
	DECallNorm      string
	DXContinentNorm string
	DEContinentNorm string
	DXGridNorm      string
	DEGridNorm      string
	DXGrid2         string
	DEGrid2         string
}

// CallMetadata stores geographic metadata for a callsign
type CallMetadata struct {
	Continent string
	Country   string
	CQZone    int
	Grid      string
	ITUZone   int
	ADIF      int // ADIF/DXCC country code from CTY lookup
}

// NewSpot creates a new spot with sensible defaults
func NewSpot(dxCall, deCall string, freq float64, mode string) *Spot {
	freq = roundFrequencyTo100Hz(freq)
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
	spot.EnsureNormalized()
	spot.RefreshBeaconFlag()
	return spot
}

// roundFrequencyTo100Hz normalizes a kHz value to the nearest 100 Hz (0.1 kHz).
func roundFrequencyTo100Hz(freqKHz float64) float64 {
	// Use half-up rounding to avoid banker's rounding surprises at .x5 boundaries.
	return math.Floor(freqKHz*10+0.5) / 10
}

// Hash32 returns a 32-bit hash for deduplication using a fixed-layout,
// zero-allocation buffer. The hash covers:
//   - Time truncated to the minute (Unix seconds)
//   - Frequency truncated to whole kHz
//   - DE and DX calls normalized, uppercased, fixed-width 12 bytes each
//
// Little-endian encoding keeps the byte order deterministic across platforms.
func (s *Spot) Hash32() uint32 {
	s.EnsureNormalized()
	var buf [36]byte
	// Time (bytes 0-7): Unix seconds, truncated to the minute.
	t := s.Time.Truncate(time.Minute).Unix()
	binary.LittleEndian.PutUint64(buf[0:8], uint64(t))
	// Frequency (bytes 8-11): whole kHz.
	freq := uint32(s.Frequency)
	binary.LittleEndian.PutUint32(buf[8:12], freq)
	// DE and DX calls (bytes 12-23, 24-35).
	writeFixedNormalizedCall(buf[12:24], s.DECallNorm)
	writeFixedNormalizedCall(buf[24:36], s.DXCallNorm)
	// Use xxh3 for speed; fold to 32 bits for existing dedup map.
	return uint32(xxh3.Hash(buf[:]))
}

// writeFixedNormalizedCall assumes call is already normalized/uppercased and fits into ASCII bytes.
func writeFixedNormalizedCall(dst []byte, call string) {
	const maxLen = 12
	n := 0
	for i := 0; i < len(call) && n < maxLen; i++ {
		dst[n] = call[i]
		n++
	}
	for n < maxLen {
		dst[n] = 0
		n++
	}
}

// Fixed layout constants for DX cluster formatting. Column numbers are 0-based.
const (
	freqFieldWidthToEnd = 25 // frequency should end at column 24, so total width is 25
	dxCallFieldWidth    = 8  // DX callsign is padded to at least 8 characters
	commentColumn       = 40 // comment (mode + report + payload) starts at column 40
	timeColumnStart     = 71 // timestamp begins at column 71
	minGapToSymbol      = 2  // minimum spaces before confidence symbol
)

type stringBuilder struct {
	buf []byte
}

func (sb *stringBuilder) Grow(n int) {
	if n <= 0 {
		return
	}
	if cap(sb.buf)-len(sb.buf) >= n {
		return
	}
	newBuf := make([]byte, len(sb.buf), len(sb.buf)+n)
	copy(newBuf, sb.buf)
	sb.buf = newBuf
}

func (sb *stringBuilder) AppendString(s string) {
	sb.buf = append(sb.buf, s...)
}

func (sb *stringBuilder) AppendByte(b byte) {
	sb.buf = append(sb.buf, b)
}

// EnsureNormalized populates cached normalized fields to avoid repeated string operations on hot paths.
func (s *Spot) EnsureNormalized() {
	if s.ModeNorm == "" && s.Mode != "" {
		s.ModeNorm = strings.ToUpper(strings.TrimSpace(s.Mode))
	}
	if s.BandNorm == "" && s.Band != "" {
		s.BandNorm = strings.ToUpper(strings.TrimSpace(s.Band))
	}
	if s.DXCallNorm == "" && s.DXCall != "" {
		s.DXCallNorm = NormalizeCallsign(s.DXCall)
	}
	if s.DECallNorm == "" && s.DECall != "" {
		s.DECallNorm = NormalizeCallsign(s.DECall)
	}
	if s.DXContinentNorm == "" && s.DXMetadata.Continent != "" {
		s.DXContinentNorm = strings.ToUpper(strings.TrimSpace(s.DXMetadata.Continent))
	}
	if s.DEContinentNorm == "" && s.DEMetadata.Continent != "" {
		s.DEContinentNorm = strings.ToUpper(strings.TrimSpace(s.DEMetadata.Continent))
	}
	if s.DXGridNorm == "" && s.DXMetadata.Grid != "" {
		s.DXGridNorm = strings.ToUpper(strings.TrimSpace(s.DXMetadata.Grid))
	}
	if s.DEGridNorm == "" && s.DEMetadata.Grid != "" {
		s.DEGridNorm = strings.ToUpper(strings.TrimSpace(s.DEMetadata.Grid))
	}
	if s.DXGrid2 == "" && len(s.DXGridNorm) >= 2 {
		s.DXGrid2 = s.DXGridNorm[:2]
	}
	if s.DEGrid2 == "" && len(s.DEGridNorm) >= 2 {
		s.DEGrid2 = s.DEGridNorm[:2]
	}
}

func (sb *stringBuilder) Len() int {
	return len(sb.buf)
}

func (sb *stringBuilder) Truncate(n int) {
	if n < 0 {
		n = 0
	}
	if n > len(sb.buf) {
		return
	}
	sb.buf = sb.buf[:n]
}

func (sb *stringBuilder) String() string {
	return string(sb.buf)
}

const spaceChunk = "                " // 16 spaces, used to pad without extra allocations

func writeSpaces(b *stringBuilder, count int) {
	for count > 0 {
		if count > len(spaceChunk) {
			b.AppendString(spaceChunk)
			count -= len(spaceChunk)
			continue
		}
		b.AppendString(spaceChunk[:count])
		break
	}
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
		// Pre-size a buffer to avoid multiple allocations while preserving the
		// exact column layout described above.
		timeStr := s.Time.UTC().Format("1504Z")
		freqStr := strconv.FormatFloat(s.Frequency, 'f', 1, 64)
		commentPayload := s.formatZoneGridComment()
		prefix := "DX de " + s.DECall + ":"

		spacesToFreq := freqFieldWidthToEnd - len(prefix) - len(freqStr)
		if spacesToFreq < 1 {
			spacesToFreq = 1
		}

		// Estimate final length to reduce builder growth.
		estimatedLen := timeColumnStart + len(timeStr) + len(s.Confidence) + 4
		if estimatedLen < len(prefix) {
			estimatedLen = len(prefix)
		}
		var b stringBuilder
		if estimatedLen < 80 {
			estimatedLen = 80 // typical DX cluster line length; helps avoid Grow reallocations
		}
		b.Grow(estimatedLen)

		b.AppendString(prefix)
		writeSpaces(&b, spacesToFreq)
		b.AppendString(freqStr)
		b.AppendString("  ")
		b.AppendString(s.DXCall)
		if pad := dxCallFieldWidth - len(s.DXCall); pad > 0 {
			writeSpaces(&b, pad)
		}

		spacesToComment := commentColumn - b.Len()
		if spacesToComment < 1 {
			spacesToComment = 1
		}
		writeSpaces(&b, spacesToComment)

		// Build comment section: Mode + signal report + CQ zone/grid annotation.
		b.AppendString(s.Mode)
		b.AppendByte(' ')
		if strings.EqualFold(s.Mode, "CW") || strings.EqualFold(s.Mode, "RTTY") {
			b.AppendString(strconv.Itoa(s.Report))
			b.AppendString(" dB")
		} else {
			if s.Report >= 0 {
				b.AppendByte('+')
			}
			b.AppendString(strconv.Itoa(s.Report))
		}
		b.AppendByte(' ')
		b.AppendString(commentPayload)

		confLabel := strings.TrimSpace(s.Confidence)
		if confLabel == "" {
			// Pad/truncate so the time begins exactly at timeColumnStart.
			if b.Len() > timeColumnStart {
				b.Truncate(timeColumnStart)
			} else if b.Len() < timeColumnStart {
				writeSpaces(&b, timeColumnStart-b.Len())
			}
			b.AppendString(timeStr)
			s.formatted = b.String()
			return
		}

		confWidth := len(confLabel)
		maxConfWidth := timeColumnStart - minGapToSymbol
		if maxConfWidth < 1 {
			maxConfWidth = 1
		}
		if confWidth > maxConfWidth {
			confLabel = confLabel[:maxConfWidth]
			confWidth = len(confLabel)
		}

		maxCommentLen := timeColumnStart - minGapToSymbol - confWidth
		if maxCommentLen < 0 {
			maxCommentLen = 0
		}
		if b.Len() > maxCommentLen {
			b.Truncate(maxCommentLen)
		}

		// Ensure the timestamp begins exactly at timeColumnStart: comment + gap + conf + space + time.
		gapLen := timeColumnStart - confWidth - 1 - b.Len()
		if gapLen < minGapToSymbol {
			gapLen = minGapToSymbol
		}

		writeSpaces(&b, gapLen)
		b.AppendString(confLabel)
		b.AppendByte(' ')
		b.AppendString(timeStr)

		s.formatted = b.String()
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
	zone := formatCQZoneLabel(s.DXMetadata.CQZone)
	grid := formatGridLabel(s.DXMetadata.Grid)
	if grid == "" {
		return fmt.Sprintf("CQ %s", zone)
	}
	return fmt.Sprintf("CQ %s %s", zone, grid)
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
		return ""
	}
	if len(grid) > 4 {
		grid = grid[:4]
	}
	return grid
}
