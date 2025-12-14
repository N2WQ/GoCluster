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
	HasReport  bool         // Whether Report is present (distinguishes real 0 dB from "unknown")
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
		Report:     0, // Meaningful only when HasReport is true
		HasReport:  false,
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

// Fixed layout constants for DX cluster formatting.
//
// IMPORTANT:
//   - All external documentation (README, user-facing specs) uses 1-based columns
//     where column 1 is the 'D' in "DX de ".
//   - The constants below are internal 0-based byte offsets (string indices).
//
// The layout is intentionally fixed-width so telnet clients can treat the line
// as a stable "record":
//   - `Spot.FormatDXCluster` returns exactly 78 characters (no CRLF).
//   - The telnet layer appends '\n' and `telnet.Client.Send` normalizes it to
//     CRLF so clients receive 80 bytes per spot line (78 chars + CRLF).
const (
	freqFieldWidthToEnd = 25 // frequency ends at internal index 24 (1-based column 25)
	dxCallFieldWidth    = 8  // DX callsign is padded to at least 8 characters
	commentColumn       = 39 // mode starts at internal index 39 (1-based column 40)
	timeColumnStart     = 73 // time starts at internal index 73 (1-based column 74)
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

// FormatDXCluster formats the spot as a fixed-width DX-cluster line.
//
// The returned string is always exactly 78 characters (no CRLF). Telnet output
// appends '\n' and `telnet.Client.Send` normalizes to CRLF, so clients see 80
// bytes on the wire (78 chars + CRLF).
//
// Column numbering below is 1-based (column 1 is the 'D' in "DX de "):
//
//	1-6:   "DX de "
//	7-?:   Spotter callsign with ":" suffix
//	25:    Frequency ends at column 25 (right-aligned within the left padding)
//	28-?:  DX callsign (left-aligned; padded to 8 chars when shorter)
//	40:    Mode starts at column 40
//	67-70: DX grid (4 chars; blank if unknown)
//	72:    Confidence glyph (1 char; blank if unknown)
//	74-78: Time in HHMMZ (Z ends at column 78)
//
// Anything between Mode and the fixed tail is treated as a free-form comment
// and is truncated so it can never push the grid/confidence/time columns.
//
// Report formatting:
//   - Only rendered when HasReport is true.
//   - CW/RTTY: no '+' prefix (e.g., "CW 23 dB")
//   - All other modes: '+' is shown for non-negative values (e.g., "FT8 +12 dB")
func (s *Spot) FormatDXCluster() string {
	s.formatOnce.Do(func() {
		// Pre-size a buffer to avoid multiple allocations while preserving the
		// exact column layout described above.
		timeStr := s.Time.UTC().Format("1504Z")
		freqStr := strconv.FormatFloat(s.Frequency, 'f', 1, 64)
		commentPayload := s.formatZoneGridComment()

		const (
			// tailStartIdx is the internal byte index where the fixed right-side
			// tail begins (1-based column 67).
			tailStartIdx = 66
			gridWidth    = 4
		)

		// Keep the left side stable by truncating overly-long spotter calls so
		// frequency and subsequent fields stay aligned.
		deCall := s.DECall
		maxPrefixLen := freqFieldWidthToEnd - len(freqStr) // prefix + spacesToFreq
		maxDELen := maxPrefixLen - len("DX de ") - len(":")
		if maxDELen < 1 {
			maxDELen = 1
		}
		if len(deCall) > maxDELen {
			deCall = deCall[:maxDELen]
		}
		prefix := "DX de " + deCall + ":"

		spacesToFreq := freqFieldWidthToEnd - len(prefix) - len(freqStr)
		if spacesToFreq < 0 {
			spacesToFreq = 0
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

		// DX callsigns longer than the available space are truncated to keep the
		// mode anchor fixed at column 40.
		dxCall := s.DXCall
		maxDXLen := commentColumn - b.Len()
		if maxDXLen < 0 {
			maxDXLen = 0
		}
		if len(dxCall) > maxDXLen {
			dxCall = dxCall[:maxDXLen]
		}
		b.AppendString(dxCall)
		if pad := dxCallFieldWidth - len(dxCall); pad > 0 {
			writeSpaces(&b, pad)
		}

		spacesToComment := commentColumn - b.Len()
		if spacesToComment < 0 {
			spacesToComment = 0
		}
		writeSpaces(&b, spacesToComment)

		// Build comment section: Mode + signal report (when present) + optional comment.
		b.AppendString(s.Mode)
		if s.HasReport {
			b.AppendByte(' ')
			if strings.EqualFold(s.Mode, "CW") || strings.EqualFold(s.Mode, "RTTY") {
				b.AppendString(strconv.Itoa(s.Report))
			} else {
				if s.Report >= 0 {
					b.AppendByte('+')
				}
				b.AppendString(strconv.Itoa(s.Report))
			}
			b.AppendString(" dB")
		}
		if trimmed := strings.TrimSpace(commentPayload); trimmed != "" {
			// Reserve one column before the fixed tail so grid/confidence/time are
			// always visually separated from the comment payload.
			remaining := (tailStartIdx - 1) - b.Len()
			if remaining > 1 {
				b.AppendByte(' ')
				remaining--
				if remaining > 0 {
					if len(trimmed) > remaining {
						trimmed = trimmed[:remaining]
					}
					b.AppendString(trimmed)
				}
			}
		}

		// Fixed tail layout uses the following 1-based columns:
		// - Grid: 67-70
		// - Space: 71
		// - Confidence: 72
		// - Space: 73
		// - Time: 74-78 (HHMMZ; 'Z' at column 78)

		gridLabel := formatGridLabel(s.DXMetadata.Grid)
		if gridLabel == "" {
			gridLabel = strings.Repeat(" ", gridWidth)
		} else if len(gridLabel) < gridWidth {
			gridLabel = gridLabel + strings.Repeat(" ", gridWidth-len(gridLabel))
		} else if len(gridLabel) > gridWidth {
			gridLabel = gridLabel[:gridWidth]
		}

		confLabel := " "
		if trimmed := strings.TrimSpace(s.Confidence); trimmed != "" {
			confLabel = trimmed[:1]
		}

		// Ensure the last comment column (1-based col 66, 0-based idx 65) is a
		// space separator before the fixed tail starts at column 67.
		separatorIdx := tailStartIdx - 1
		if b.Len() > separatorIdx {
			b.Truncate(separatorIdx)
		} else if b.Len() < separatorIdx {
			writeSpaces(&b, separatorIdx-b.Len())
		}
		b.AppendByte(' ') // col 66

		b.AppendString(gridLabel) // cols 67-70
		b.AppendByte(' ')         // col 71
		b.AppendString(confLabel) // col 72 (or space)
		b.AppendByte(' ')         // col 73
		b.AppendString(timeStr)   // cols 74-78 (Z ends at 78)

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
	return sanitizeDXClusterComment(s.Comment)
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

func sanitizeDXClusterComment(comment string) string {
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return ""
	}

	// Fast path: keep the common case allocation-free when the comment is already
	// plain ASCII without control characters.
	needsSanitize := false
	for i := 0; i < len(comment); i++ {
		b := comment[i]
		if b >= 0x20 && b <= 0x7e {
			continue
		}
		needsSanitize = true
		break
	}
	if !needsSanitize {
		return comment
	}

	// Telnet DX-cluster output is column-oriented and traditionally ASCII. Any
	// control characters (including tabs/newlines) can break alignment by either
	// expanding to multiple columns or moving the cursor. To keep the fixed tail
	// (grid/confidence/time) stable, normalize to printable ASCII and collapse
	// all whitespace runs to a single space.
	var b strings.Builder
	b.Grow(len(comment))
	lastSpace := false
	for _, r := range comment {
		if r == '\t' || r == '\n' || r == '\r' || r == '\v' || r == '\f' {
			if !lastSpace {
				b.WriteByte(' ')
				lastSpace = true
			}
			continue
		}
		if r == ' ' {
			if !lastSpace {
				b.WriteByte(' ')
				lastSpace = true
			}
			continue
		}
		if r < 0x20 || r == 0x7f {
			continue
		}
		if r <= 0x7e {
			b.WriteByte(byte(r))
			lastSpace = false
			continue
		}
		// Replace any non-ASCII rune with '?' so the output remains fixed-width
		// across telnet clients with different encodings.
		b.WriteByte('?')
		lastSpace = false
	}
	return strings.TrimSpace(b.String())
}
