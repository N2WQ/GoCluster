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
	"sync/atomic"
	"time"
	"unicode/utf8"

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
	SourceUpstream    SourceType = "UPSTREAM"    // From an upstream/human telnet feed
	SourcePeer        SourceType = "PEER"        // From DXSpider peer sessions
)

// Spot represents a DX spot in canonical form
type Spot struct {
	ID            uint64       // Unique spot ID (monotonic counter)
	DXCall        string       // Station being spotted (normalized callsign, portable suffix stripped)
	DECall        string       // Station reporting the spot (normalized callsign, portable suffix stripped)
	Frequency     float64      // Frequency in kHz (e.g., 14074.5)
	Band          string       // Band (e.g., "20m")
	Mode          string       // Mode (e.g., "CW", "USB", "FT8")
	Report        int          // Signal report in dB (SNR for digital modes, signal strength for CW)
	Time          time.Time    // When the spot was created
	Comment       string       // User comment or additional info
	SourceType    SourceType   // Where this spot came from
	SourceNode    string       // Originating node/cluster
	SpotterIP     string       // Spotter IP address for PC61 frames (optional)
	TTL           uint8        // Time-to-live for loop prevention
	IsHuman       bool         // Whether the spot originated from a human operator
	IsTestSpotter bool         // True when the spotter callsign is a CTY-valid TEST identifier.
	IsBeacon      bool         // True when DX call ends with /B (beacon identifiers)
	HasReport     bool         // Whether Report is present (distinguishes real 0 dB from "unknown")
	DXMetadata    CallMetadata // Metadata for the DX station
	DEMetadata    CallMetadata // Metadata for the spotter station
	Confidence    string       // Consensus confidence label (e.g., "75%" or "?")
	formatted     string
	formatOnce    sync.Once // ensures FormatDXCluster builds expensive string only once per spot

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
	DXCellID        uint16 // H3 res-2 proxy ID for path reliability (0 when unknown)
	DECellID        uint16 // H3 res-2 proxy ID for path reliability (0 when unknown)
	// Broadcast-only overrides (telnet/archive view); canonical calls remain raw.
	DECallStripped     string
	DECallNormStripped string
}

// pskVariantMap collapses PSK variants to a canonical family while preserving
// the original token for display/logging.
var pskVariantMap = map[string]string{
	"PSK":    "PSK",
	"PSK31":  "PSK",
	"PSK63":  "PSK",
	"PSK125": "PSK",
}

// CanonicalPSKMode returns the canonical PSK family label alongside the
// original token. When mode is not a PSK variant, it returns the input as both
// canonical and variant with isPSK=false.
func CanonicalPSKMode(mode string) (canonical string, variant string, isPSK bool) {
	upper := normalizeUpperASCIITrim(mode)
	return canonicalPSKModeUpper(upper)
}

// CallMetadata stores geographic metadata for a callsign
type CallMetadata struct {
	Continent   string
	Country     string
	CQZone      int
	Grid        string
	GridDerived bool
	ITUZone     int
	ADIF        int // ADIF/DXCC country code from CTY lookup
}

// NewSpot constructs a new spot with normalized defaults.
// Key aspects: Normalizes calls/mode, rounds frequency, sets defaults.
// Upstream: All spot creation paths (parsers and tests).
// Downstream: EnsureNormalized and RefreshBeaconFlag.
// NewSpot creates a new spot with sensible defaults
func NewSpot(dxCall, deCall string, freq float64, mode string) *Spot {
	freq = roundFrequencyTo10Hz(freq)
	mode = NormalizeVoiceMode(mode, freq)
	modeNorm := mode
	if canonical, _, ok := canonicalPSKModeUpper(mode); ok {
		modeNorm = canonical
	}
	band := FreqToBand(freq)
	dxNorm := NormalizeCallsign(dxCall)
	deNorm := NormalizeCallsign(deCall)
	spot := &Spot{
		DXCall:     dxNorm,
		DECall:     deNorm,
		Frequency:  freq,
		Mode:       mode,
		Band:       band,
		Time:       time.Now().UTC(),
		SourceType: SourceManual,
		TTL:        5, // Default hop count
		Report:     0, // Meaningful only when HasReport is true
		HasReport:  false,
		IsHuman:    true,
		ModeNorm:   modeNorm,
		BandNorm:   band,
		DXCallNorm: dxNorm,
		DECallNorm: deNorm,
		DXCellID:   0,
		DECellID:   0,
	}
	spot.EnsureNormalized()
	spot.RefreshBeaconFlag()
	return spot
}

// NewSpotNormalized constructs a new spot using pre-normalized callsigns.
// Key aspects: Assumes NormalizeCallsign already ran on DX/DE calls.
// Upstream: Ingest paths that normalize once.
// Downstream: EnsureNormalized and RefreshBeaconFlag.
// NewSpotNormalized builds a spot without re-normalizing DX/DE calls.
func NewSpotNormalized(dxCallNorm, deCallNorm string, freq float64, mode string) *Spot {
	freq = roundFrequencyTo10Hz(freq)
	mode = NormalizeVoiceMode(mode, freq)
	modeNorm := mode
	if canonical, _, ok := canonicalPSKModeUpper(mode); ok {
		modeNorm = canonical
	}
	band := FreqToBand(freq)
	dxCall := strings.TrimSpace(dxCallNorm)
	deCall := strings.TrimSpace(deCallNorm)
	spot := &Spot{
		DXCall:     dxCall,
		DECall:     deCall,
		Frequency:  freq,
		Mode:       mode,
		Band:       band,
		Time:       time.Now().UTC(),
		SourceType: SourceManual,
		TTL:        5, // Default hop count
		Report:     0, // Meaningful only when HasReport is true
		HasReport:  false,
		IsHuman:    true,
		ModeNorm:   modeNorm,
		BandNorm:   band,
		DXCallNorm: dxCall,
		DECallNorm: deCall,
		DXCellID:   0,
		DECellID:   0,
	}
	spot.EnsureNormalized()
	spot.RefreshBeaconFlag()
	return spot
}

// Purpose: Round a kHz frequency to the nearest 10 Hz (0.01 kHz).
// Key aspects: Uses half-up rounding to avoid banker rounding.
// Upstream: NewSpot and frequency normalization.
// Downstream: math.Floor.
// roundFrequencyTo10Hz normalizes a kHz value to the nearest 10 Hz (0.01 kHz).
func roundFrequencyTo10Hz(freqKHz float64) float64 {
	// Use half-up rounding to avoid banker's rounding surprises at .x5 boundaries.
	return math.Floor(freqKHz*100+0.5) / 100
}

// Hash32 computes a 32-bit dedupe hash for the spot.
// Key aspects: Uses fixed-size buffer with time/freq/calls to avoid allocations.
// Upstream: deduplication (primary).
// Downstream: writeFixedNormalizedCall and xxh3.Hash.
// Hash32 returns a 32-bit hash for deduplication using a fixed-layout,
// zero-allocation buffer. The hash covers:
//   - Time truncated to the minute (Unix seconds)
//   - Frequency truncated to whole kHz
//   - DE and DX calls normalized, uppercased, fixed-width 15 bytes each
//
// Little-endian encoding keeps the byte order deterministic across platforms.
func (s *Spot) Hash32() uint32 {
	s.EnsureNormalized()
	var buf [42]byte
	// Time (bytes 0-7): Unix seconds, truncated to the minute.
	t := s.Time.Truncate(time.Minute).Unix()
	binary.LittleEndian.PutUint64(buf[0:8], uint64(t))
	// Frequency (bytes 8-11): whole kHz.
	freq := uint32(s.Frequency)
	binary.LittleEndian.PutUint32(buf[8:12], freq)
	// DE and DX calls (bytes 12-26, 27-41).
	writeFixedNormalizedCall(buf[12:27], s.DECallNorm)
	writeFixedNormalizedCall(buf[27:42], s.DXCallNorm)
	// Use xxh3 for speed; fold to 32 bits for existing dedup map.
	return uint32(xxh3.Hash(buf[:]))
}

// Purpose: Write a normalized callsign into a fixed-width buffer.
// Key aspects: Pads/truncates to 15 bytes with zero fill.
// Upstream: Spot.Hash32.
// Downstream: None (byte copy only).
// writeFixedNormalizedCall assumes call is already normalized/uppercased and fits into ASCII bytes.
func writeFixedNormalizedCall(dst []byte, call string) {
	const maxLen = 15
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
//   - `Spot.FormatDXCluster` returns exactly the configured line length (no CRLF).
//   - The telnet layer appends '\n' and `telnet.Client.Send` normalizes it to
//     CRLF so clients receive (line length + CRLF) bytes per spot line.
const (
	freqFieldWidthToEnd = 25 // frequency ends at internal index 24 (1-based column 25)
	dxCallFieldWidth    = 8  // DX callsign is padded to at least 8 characters
	commentColumn       = 39 // mode starts at internal index 39 (1-based column 40)
	minGapToSymbol      = 2  // minimum spaces before confidence symbol
)

const dxDisplayMaxLen = 10

const (
	dxClusterDefaultLineLength = 78
	dxClusterMinLineLength     = 65
	dxClusterTailLength        = 15
)

type dxClusterLayout struct {
	lineLength   int
	tailStartIdx int
	glyphIdx     int
	gridIdx      int
	confIdx      int
	timeIdx      int
}

var dxClusterLayoutValue atomic.Value

func init() {
	layout, err := newDXClusterLayout(dxClusterDefaultLineLength)
	if err != nil {
		panic(err)
	}
	dxClusterLayoutValue.Store(layout)
}

// DXClusterLayout exposes the current DX cluster line layout in 1-based columns.
type DXClusterLayout struct {
	LineLength       int
	TailStartColumn  int
	GlyphColumn      int
	GridColumn       int
	ConfidenceColumn int
	TimeColumn       int
}

// SetDXClusterLineLength sets the DX cluster line length for formatting.
// Length excludes CRLF and must be >= dxClusterMinLineLength.
func SetDXClusterLineLength(lineLength int) error {
	layout, err := newDXClusterLayout(lineLength)
	if err != nil {
		return err
	}
	dxClusterLayoutValue.Store(layout)
	return nil
}

// CurrentDXClusterLayout reports the active layout using 1-based columns.
func CurrentDXClusterLayout() DXClusterLayout {
	layout := currentDXClusterLayout()
	return DXClusterLayout{
		LineLength:       layout.lineLength,
		TailStartColumn:  layout.tailStartIdx + 1,
		GlyphColumn:      layout.glyphIdx + 1,
		GridColumn:       layout.gridIdx + 1,
		ConfidenceColumn: layout.confIdx + 1,
		TimeColumn:       layout.timeIdx + 1,
	}
}

func newDXClusterLayout(lineLength int) (dxClusterLayout, error) {
	if lineLength <= 0 {
		lineLength = dxClusterDefaultLineLength
	}
	if lineLength < dxClusterMinLineLength {
		return dxClusterLayout{}, fmt.Errorf("dx cluster line length %d is below minimum %d", lineLength, dxClusterMinLineLength)
	}
	tailStartIdx := lineLength - dxClusterTailLength
	return dxClusterLayout{
		lineLength:   lineLength,
		tailStartIdx: tailStartIdx,
		glyphIdx:     tailStartIdx + 1,
		gridIdx:      tailStartIdx + 3,
		confIdx:      tailStartIdx + 8,
		timeIdx:      tailStartIdx + 10,
	}, nil
}

func currentDXClusterLayout() dxClusterLayout {
	if value := dxClusterLayoutValue.Load(); value != nil {
		if layout, ok := value.(dxClusterLayout); ok {
			return layout
		}
	}
	layout, err := newDXClusterLayout(dxClusterDefaultLineLength)
	if err == nil {
		return layout
	}
	fallback, fallbackErr := newDXClusterLayout(dxClusterMinLineLength)
	if fallbackErr == nil {
		return fallback
	}
	return dxClusterLayout{
		lineLength:   dxClusterMinLineLength,
		tailStartIdx: dxClusterMinLineLength - dxClusterTailLength,
		glyphIdx:     dxClusterMinLineLength - dxClusterTailLength + 1,
		gridIdx:      dxClusterMinLineLength - dxClusterTailLength + 3,
		confIdx:      dxClusterMinLineLength - dxClusterTailLength + 8,
		timeIdx:      dxClusterMinLineLength - dxClusterTailLength + 10,
	}
}

type stringBuilder struct {
	buf []byte
}

// Grow ensures the stringBuilder has capacity for n more bytes.
// Key aspects: Grows underlying buffer without shrinking.
// Upstream: FormatDXCluster and helpers.
// Downstream: make/copy for buffer growth.
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

// AppendString appends a string to the builder buffer.
// Key aspects: Appends raw bytes without extra allocations.
// Upstream: FormatDXCluster and helpers.
// Downstream: slice append.
func (sb *stringBuilder) AppendString(s string) {
	sb.buf = append(sb.buf, s...)
}

// AppendByte appends a single byte to the builder buffer.
// Key aspects: Lightweight wrapper over slice append.
// Upstream: FormatDXCluster and helpers.
// Downstream: slice append.
func (sb *stringBuilder) AppendByte(b byte) {
	sb.buf = append(sb.buf, b)
}

// EnsureNormalized populates cached normalized fields on the Spot.
// Key aspects: Uppercases/normalizes mode, band, calls, and grid prefixes.
// Upstream: Multiple hot paths (dedup, format, filters).
// Downstream: NormalizeCallsign and string operations.
// EnsureNormalized populates cached normalized fields to avoid repeated string operations on hot paths.
func (s *Spot) EnsureNormalized() {
	if s.ModeNorm == "" && s.Mode != "" {
		modeUpper := normalizeUpperASCIITrim(s.Mode)
		if canonical, _, ok := canonicalPSKModeUpper(modeUpper); ok {
			modeUpper = canonical
		}
		s.ModeNorm = modeUpper
	}
	if s.BandNorm == "" && s.Band != "" {
		// Normalize band to canonical lowercase (e.g., "20m") so allowlists match.
		s.BandNorm = NormalizeBand(s.Band)
	}
	if s.DXCallNorm == "" && s.DXCall != "" {
		s.DXCallNorm = NormalizeCallsign(s.DXCall)
	}
	if s.DECallNorm == "" && s.DECall != "" {
		s.DECallNorm = NormalizeCallsign(s.DECall)
	}
	if s.DECallNormStripped == "" && s.DECallStripped != "" {
		s.DECallNormStripped = NormalizeCallsign(s.DECallStripped)
	}
	if s.DXContinentNorm == "" && s.DXMetadata.Continent != "" {
		s.DXContinentNorm = normalizeUpperASCIITrim(s.DXMetadata.Continent)
	}
	if s.DEContinentNorm == "" && s.DEMetadata.Continent != "" {
		s.DEContinentNorm = normalizeUpperASCIITrim(s.DEMetadata.Continent)
	}
	if s.DXGridNorm == "" && s.DXMetadata.Grid != "" {
		s.DXGridNorm = normalizeUpperASCIITrim(s.DXMetadata.Grid)
	}
	if s.DEGridNorm == "" && s.DEMetadata.Grid != "" {
		s.DEGridNorm = normalizeUpperASCIITrim(s.DEMetadata.Grid)
	}
	if s.DXGrid2 == "" && len(s.DXGridNorm) >= 2 {
		s.DXGrid2 = s.DXGridNorm[:2]
	}
	if s.DEGrid2 == "" && len(s.DEGridNorm) >= 2 {
		s.DEGrid2 = s.DEGridNorm[:2]
	}
}

// CloneWithComment returns a shallow copy of the spot with the comment replaced.
// The formatted cache is reset so formatting reflects the new comment.
func (s *Spot) CloneWithComment(comment string) *Spot {
	if s == nil {
		return nil
	}
	clone := &Spot{
		ID:                 s.ID,
		DXCall:             s.DXCall,
		DECall:             s.DECall,
		Frequency:          s.Frequency,
		Band:               s.Band,
		Mode:               s.Mode,
		Report:             s.Report,
		Time:               s.Time,
		Comment:            comment,
		SourceType:         s.SourceType,
		SourceNode:         s.SourceNode,
		SpotterIP:          s.SpotterIP,
		TTL:                s.TTL,
		IsHuman:            s.IsHuman,
		IsTestSpotter:      s.IsTestSpotter,
		IsBeacon:           s.IsBeacon,
		HasReport:          s.HasReport,
		DXMetadata:         s.DXMetadata,
		DEMetadata:         s.DEMetadata,
		Confidence:         s.Confidence,
		ModeNorm:           s.ModeNorm,
		BandNorm:           s.BandNorm,
		DXCallNorm:         s.DXCallNorm,
		DECallNorm:         s.DECallNorm,
		DXContinentNorm:    s.DXContinentNorm,
		DEContinentNorm:    s.DEContinentNorm,
		DXGridNorm:         s.DXGridNorm,
		DEGridNorm:         s.DEGridNorm,
		DXGrid2:            s.DXGrid2,
		DEGrid2:            s.DEGrid2,
		DXCellID:           s.DXCellID,
		DECellID:           s.DECellID,
		DECallStripped:     s.DECallStripped,
		DECallNormStripped: s.DECallNormStripped,
	}
	return clone
}

// InvalidateMetadataCache clears cached fields derived from DXMetadata/DEMetadata.
// Call after mutating metadata so a subsequent EnsureNormalized repopulates
// continent/grid values. Caller must ensure no concurrent readers of cached fields.
func (s *Spot) InvalidateMetadataCache() {
	if s == nil {
		return
	}
	s.DXContinentNorm = ""
	s.DEContinentNorm = ""
	s.DXGridNorm = ""
	s.DEGridNorm = ""
	s.DXGrid2 = ""
	s.DEGrid2 = ""
	s.DXCellID = 0
	s.DECellID = 0
}

// Len returns the current length of the builder buffer.
// Key aspects: Thin wrapper around len.
// Upstream: FormatDXCluster and helpers.
// Downstream: None.
func (sb *stringBuilder) Len() int {
	return len(sb.buf)
}

// Truncate truncate the builder buffer to n bytes.
// Key aspects: Clamps negative n to zero.
// Upstream: FormatDXCluster and helpers.
// Downstream: slice reslice.
func (sb *stringBuilder) Truncate(n int) {
	if n < 0 {
		n = 0
	}
	if n > len(sb.buf) {
		return
	}
	sb.buf = sb.buf[:n]
}

// String converts the builder buffer to a string.
// Key aspects: Returns a new string backed by copied bytes.
// Upstream: FormatDXCluster.
// Downstream: string conversion.
func (sb *stringBuilder) String() string {
	return string(sb.buf)
}

const spaceChunk = "                " // 16 spaces, used to pad without extra allocations

// Purpose: Append a specific count of spaces to the builder.
// Key aspects: Uses a constant chunk to minimize allocations.
// Upstream: FormatDXCluster.
// Downstream: stringBuilder.AppendString.
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

// Purpose: Truncate a normalized DX callsign for telnet display.
// Key aspects: Assumes portable suffixes were stripped during normalization.
// Upstream: FormatDXCluster (DX callsign field only).
// Downstream: None.
func displayDXCall(call string) string {
	call = strings.TrimSpace(call)
	if call == "" {
		return call
	}
	if len(call) > dxDisplayMaxLen {
		call = call[:dxDisplayMaxLen]
	}
	return call
}

// FormatDXCluster formats a spot as a fixed-width DX cluster line.
// Key aspects: Enforces column layout and caches formatted string once.
// Upstream: telnet broadcast and archive formatting.
// Downstream: formatZoneGridComment, writeSpaces, and stringBuilder helpers.
// FormatDXCluster formats the spot as a fixed-width DX-cluster line.
// Callers must ensure the spot has been normalized after any mutations.
//
// The returned string is always exactly the configured line length (no CRLF).
// Telnet output appends '\n' and `telnet.Client.Send` normalizes to CRLF.
//
// Column numbering below is 1-based (column 1 is the 'D' in "DX de "):
//
//	1-6:   "DX de "
//	7-?:   Spotter callsign with ":" suffix
//	25:    Frequency ends at column 25 (right-aligned within the left padding)
//	28-?:  DX callsign (left-aligned; padded to 8 chars when shorter; display truncates to 10)
//	40:    Mode starts at column 40
//	Tail:  [space][prop glyph][space][grid4][space][conf][space][time5], anchored to line end
//
// Anything between Mode and the fixed tail is treated as a free-form comment
// and is truncated so it can never push the grid/confidence/time columns.
//
// Report formatting:
//   - Only rendered when HasReport is true.
//   - CW/RTTY: no '+' prefix (e.g., "CW 23 dB")
//   - All other modes: '+' is shown for non-negative values (e.g., "FT8 +12 dB")
func (s *Spot) FormatDXCluster() string {
	layout := currentDXClusterLayout()
	// Purpose: Populate s.formatted once for reuse on repeat formatting.
	// Key aspects: Uses sync.Once to avoid redundant allocations.
	// Upstream: FormatDXCluster.
	// Downstream: stringBuilder helpers and s.formatZoneGridComment.
	s.formatOnce.Do(func() {
		// Pre-size a buffer to avoid multiple allocations while preserving the
		// exact column layout described above.
		timeStr := s.Time.UTC().Format("1504Z")
		freqStr := strconv.FormatFloat(s.Frequency, 'f', 2, 64)
		commentPayload := s.formatZoneGridComment()

		tailStartIdx := layout.tailStartIdx
		gridWidth := 4

		// Keep the left side stable by truncating overly-long spotter calls so
		// frequency and subsequent fields stay aligned.
		deCall := s.DECall
		if s.DECallStripped != "" {
			deCall = s.DECallStripped
		}
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
		estimatedLen := layout.lineLength
		if estimatedLen < len(prefix) {
			estimatedLen = len(prefix)
		}
		var b stringBuilder
		b.Grow(estimatedLen)

		b.AppendString(prefix)
		writeSpaces(&b, spacesToFreq)
		b.AppendString(freqStr)
		b.AppendString("  ")

		// DX callsigns longer than the available space are truncated to keep the
		// mode anchor fixed at column 40.
		dxCallRaw := s.DXCallNorm
		if dxCallRaw == "" {
			dxCallRaw = NormalizeCallsign(s.DXCall)
		}
		dxCall := displayDXCall(dxCallRaw)
		if dxCall == "" {
			dxCall = displayDXCall(s.DXCall)
		}
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

		gridLabel := formatGridLabel(s.DXMetadata.Grid, s.DXMetadata.GridDerived)
		if gridLabel == "" {
			gridLabel = strings.Repeat(" ", gridWidth)
		} else if len(gridLabel) < gridWidth {
			gridLabel = gridLabel + strings.Repeat(" ", gridWidth-len(gridLabel))
		} else if len(gridLabel) > gridWidth {
			gridLabel = gridLabel[:gridWidth]
		}

		confLabel := " "
		if trimmed := strings.TrimSpace(s.Confidence); trimmed != "" {
			confLabel = firstPrintableASCIIOrQuestion(trimmed)
		}

		// Ensure the last comment column leaves room for the fixed tail.
		if b.Len() > tailStartIdx {
			b.Truncate(tailStartIdx)
		} else if b.Len() < tailStartIdx {
			writeSpaces(&b, tailStartIdx-b.Len())
		}
		b.AppendByte(' ') // space before glyph
		b.AppendByte(' ') // glyph placeholder
		b.AppendByte(' ') // space before grid

		b.AppendString(gridLabel)
		b.AppendByte(' ')
		b.AppendString(confLabel)
		b.AppendByte(' ')
		b.AppendString(timeStr)

		s.formatted = b.String()
	})

	return s.formatted
}

// FreqToBand converts a frequency in kHz to a band string
// Purpose: Map a frequency in kHz to an amateur band label.
// Key aspects: Uses inclusive band edges and returns "unknown" when unmatched.
// Upstream: NewSpot and parsing paths.
// Downstream: None (pure mapping).
func FreqToBand(freq float64) string {
	for _, band := range bandTable {
		if freq >= band.Min && freq <= band.Max {
			return band.Name
		}
	}
	return "???"
}

// IsValid performs basic validation on the spot
// Purpose: Validate core spot fields for basic sanity.
// Key aspects: Ensures calls are valid and frequency > 0.
// Upstream: ingest validation and tests.
// Downstream: IsValidCallsign and frequency checks.
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
// Purpose: Return a concise debug string for a spot.
// Key aspects: Includes call, freq, mode, and comment.
// Upstream: logging/debug outputs.
// Downstream: fmt.Sprintf and sanitizeDXClusterComment.
func (s *Spot) String() string {
	return fmt.Sprintf("[%s] %s spotted %s on %.1f kHz (%s %s) - %s",
		s.Time.UTC().Format("15:04:05"),
		s.DECall,
		s.DXCall,
		s.Frequency,
		s.Band,
		s.Mode,
		s.Comment)
}

// Purpose: Build the comment payload with zone/grid metadata.
// Key aspects: Appends zone/grid tags when available.
// Upstream: FormatDXCluster.
// Downstream: formatCQZoneLabel and formatGridLabel.
func (s *Spot) formatZoneGridComment() string {
	return sanitizeDXClusterComment(s.Comment)
}

// Purpose: Format a grid label for display.
// Key aspects: Lowercases derived grids and truncates to 4 chars.
// Upstream: formatZoneGridComment.
// Downstream: strings.ToUpper/TrimSpace.
func formatGridLabel(grid string, derived bool) string {
	grid = strings.TrimSpace(grid)
	if grid == "" {
		return ""
	}
	if derived {
		grid = strings.ToLower(grid)
	} else {
		grid = strings.ToUpper(grid)
	}
	if len(grid) > 4 {
		grid = grid[:4]
	}
	return grid
}

// Purpose: Sanitize comment text for DX cluster line output.
// Key aspects: Strips control chars and collapses whitespace.
// Upstream: String and FormatDXCluster.
// Downstream: strings.Builder and rune filtering.
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

// Purpose: Return the first printable ASCII rune or "?" for non-ASCII.
// Key aspects: Ensures telnet output remains ASCII-only.
// Upstream: FormatDXCluster (confidence glyph).
// Downstream: None.
func firstPrintableASCIIOrQuestion(s string) string {
	if s == "" {
		return "?"
	}
	r, _ := utf8.DecodeRuneInString(s)
	if r >= 0x20 && r <= 0x7e {
		return string(r)
	}
	return "?"
}
