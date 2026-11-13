// File role: Output-only native CC11 serialization. Canonical
// spots stay untouched; fixed scratch and preflight admission bound each record
// to 65,536 bytes including CRLF before any caller-buffer write.
package telnet

import (
	"errors"
	"math"
	"strconv"
	"unicode/utf8"

	"dxcluster/cty"
	"dxcluster/spot"
)

const maxVE7CCRecordBytes = 65536

var (
	errVE7CCRecordTooLarge = errors.New("CC11 record too large")
	errVE7CCInvalidSpot    = errors.New("invalid CC11 spot")
)

type ve7ccContext struct {
	cty        *cty.CTYDatabase
	node       string
	historical bool
}

type ve7ccFormatNotes struct {
	sanitized   bool
	unavailable bool
	conflict    bool
}

type ve7ccField struct {
	text string
	raw  []byte
}

// appendVE7CCRecord excludes CRLF from its result and additional-byte budget.
// On error dst is unchanged, including unused backing-array bytes. Scratch is
// fixed-size; hostile inputs cannot force an input-sized intermediate.
func appendVE7CCRecord(dst []byte, s *spot.Spot, context ve7ccContext, maxBytes int) ([]byte, ve7ccFormatNotes, error) {
	var notes ve7ccFormatNotes
	if s == nil || s.DXCall == "" || s.DECall == "" || s.Frequency <= 0 ||
		math.IsNaN(s.Frequency) || math.IsInf(s.Frequency, 0) {
		return dst, notes, errVE7CCInvalidSpot
	}
	if maxBytes > maxVE7CCRecordBytes-2 {
		maxBytes = maxVE7CCRecordBytes - 2
	}
	if maxBytes < 19 {
		return dst, notes, errVE7CCRecordTooLarge
	}
	// CTY's portable lookup and the report taxonomy may allocate internally.
	// Bound their input before calling them, even on an already-doomed record.
	for _, text := range [...]string{s.DXCall, s.DECall, s.Mode} {
		if ve7ccTextSize(text, maxBytes, &notes) > maxBytes {
			return dst, notes, errVE7CCRecordTooLarge
		}
	}
	dx := ve7ccSupplement(s.DXMetadata, s.DXCall, context.cty, &notes)
	de := ve7ccSupplement(s.DEMetadata, s.DECall, context.cty, &notes)
	var frequency [384]byte // Any finite float64 in fixed notation fits.
	var date [64]byte
	var clock [16]byte
	var numbers [6][24]byte
	var grids [2][4]byte
	when := s.Time.UTC()
	fields := [20]ve7ccField{
		{text: "CC11"},
		{raw: strconv.AppendFloat(frequency[:0], s.Frequency, 'f', 1, 64)},
		{text: s.DXCall},
		{raw: when.AppendFormat(date[:0], "_2-Jan-2006")},
		{raw: when.AppendFormat(clock[:0], "1504Z")},
		{},
		{text: s.DECall},
		{raw: ve7ccPositive(numbers[0][:0], dx.ADIF, &notes)},
		{raw: ve7ccPositive(numbers[1][:0], de.ADIF, &notes)},
		{text: ve7ccOrigin(s, context)},
		{raw: ve7ccPositive(numbers[2][:0], dx.ITUZone, &notes)},
		{raw: ve7ccPositive(numbers[3][:0], dx.CQZone, &notes)},
		{raw: ve7ccPositive(numbers[4][:0], de.ITUZone, &notes)},
		{raw: ve7ccPositive(numbers[5][:0], de.CQZone, &notes)},
		{text: dx.State}, {text: de.State},
		{text: dx.Country}, {text: de.Country},
		{raw: ve7ccGrid(grids[0][:], dx, &notes)},
		{raw: ve7ccGrid(grids[1][:], de, &notes)},
	}
	var report [32]byte
	comment := ve7ccComment(s, report[:0])
	size := 19
	for i, field := range fields {
		if i == 5 {
			for _, part := range comment {
				size += ve7ccTextSize(part.text, maxBytes-size, &notes) + len(part.raw)
				if size > maxBytes {
					return dst, notes, errVE7CCRecordTooLarge
				}
			}
		} else {
			size += ve7ccTextSize(field.text, maxBytes-size, &notes) + len(field.raw)
		}
		if size > maxBytes {
			return dst, notes, errVE7CCRecordTooLarge
		}
	}
	for i, field := range fields {
		if i != 0 {
			dst = append(dst, '^')
		}
		if i == 5 {
			for _, part := range comment {
				dst = ve7ccAppendText(dst, part.text)
				dst = append(dst, part.raw...)
			}
		} else {
			dst = ve7ccAppendText(dst, field.text)
			dst = append(dst, field.raw...)
		}
	}
	return dst, notes, nil
}

func ve7ccOrigin(s *spot.Spot, context ve7ccContext) string {
	switch s.SourceType {
	case spot.SourcePeer:
		return s.SourceNode
	case spot.SourceManual:
		if !context.historical {
			return context.node
		}
	}
	return ""
}

// Supplement only missing descriptive fields, and only after matching the
// stored positive ADIF. A CTY refresh must not rewrite stored identity.
func ve7ccSupplement(meta spot.CallMetadata, call string, db *cty.CTYDatabase, notes *ve7ccFormatNotes) spot.CallMetadata {
	if meta.Country != "" && meta.ITUZone > 0 {
		return meta
	}
	if meta.ADIF > 0 && db != nil {
		info, ok := db.LookupCallsignPortable(call)
		if ok && info != nil && info.ADIF > 0 && info.ADIF == meta.ADIF {
			if meta.Country == "" {
				meta.Country = info.Country
			}
			if meta.ITUZone <= 0 {
				meta.ITUZone = info.ITUZone
			}
		} else if ok && info != nil && info.ADIF > 0 && info.ADIF != meta.ADIF {
			notes.conflict = true
		}
	}
	if meta.Country == "" || meta.ITUZone <= 0 {
		notes.unavailable = true
	}
	return meta
}

func ve7ccPositive(dst []byte, value int, notes *ve7ccFormatNotes) []byte {
	if value <= 0 {
		notes.unavailable = true
		return dst
	}
	return strconv.AppendInt(dst, int64(value), 10)
}

func ve7ccGrid(dst []byte, meta spot.CallMetadata, notes *ve7ccFormatNotes) []byte {
	if len(meta.Grid) < 4 {
		notes.unavailable = true
		return dst[:0]
	}
	for i := 0; i < 4; i++ {
		ch := meta.Grid[i]
		if ch >= 'a' && ch <= 'z' {
			ch -= 'a' - 'A'
		}
		if (i < 2 && (ch < 'A' || ch > 'R')) || (i >= 2 && (ch < '0' || ch > '9')) {
			notes.unavailable = true
			return dst[:0]
		}
		if meta.GridDerived && i < 2 {
			ch += 'a' - 'A'
		}
		dst[i] = ch
	}
	return dst[:4]
}

// Native comments use the canonical stored payload, never CW display metadata,
// diagnostic replacements, or the cached fixed-width text line.
func ve7ccComment(s *spot.Spot, report []byte) [4]ve7ccField {
	parts := [4]ve7ccField{{text: s.Mode}}
	if s.HasReport {
		if s.Mode != "" {
			report = append(report, ' ')
		}
		if s.Report >= 0 && spot.ReportFormatForMode(s.Mode) != spot.ReportFormatPlainDB {
			report = append(report, '+')
		}
		report = strconv.AppendInt(report, int64(s.Report), 10)
		report = append(report, " dB"...)
		parts[1].raw = report
	}
	payload := s.Comment
	if s.IsBeacon && ve7ccBlankText(payload) {
		payload = s.BeaconComment
		if ve7ccBlankText(payload) {
			payload = spot.BeaconCommentDefault
		}
	}
	if payload != "" {
		if s.Mode != "" || s.HasReport {
			parts[2].text = " "
		}
		parts[3].text = payload
	}
	return parts
}

func ve7ccBlankText(text string) bool {
	for _, ch := range text {
		if ch > ' ' && ch <= '~' {
			return false
		}
	}
	return true
}

// One ASCII byte is emitted per rune, including each invalid UTF-8 byte.
// Counting first bounds output and leaves dst entirely untouched on refusal.
func ve7ccTextSize(text string, budget int, notes *ve7ccFormatNotes) int {
	size := 0
	for _, ch := range text {
		size++
		if ch == '^' || ch < ' ' || ch > '~' {
			notes.sanitized = true
		}
		if size > budget {
			return size
		}
	}
	return size
}

func ve7ccAppendText(dst []byte, text string) []byte {
	for len(text) > 0 {
		ch, width := utf8.DecodeRuneInString(text)
		text = text[width:]
		switch {
		case ch == '^':
			dst = append(dst, '~')
		case ch < ' ' || ch > '~':
			dst = append(dst, ' ')
		default:
			dst = append(dst, byte(ch))
		}
	}
	return dst
}
