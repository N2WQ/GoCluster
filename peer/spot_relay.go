// File role: Validates original peer spots before local admission and relays
// their owned original fields independently of the mutable local Spot.
package peer

import (
	"errors"
	"math"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"dxcluster/spot"
)

func isPeerSpotFrame(kind string) bool {
	return kind == "PC11" || kind == "PC61" || kind == "PC26"
}

// validateOriginalPeerSpot is the shared admission rule, including receive-only
// links. It neither normalizes nor caches input. ParseFrame owns the sentence
// and hop grammar; this boundary checks fields before tolerant local parsers.
func validateOriginalPeerSpot(frame *Frame) (time.Time, error) {
	if frame == nil || !isPeerSpotFrame(frame.Type) || frame.Hop < 0 || frame.Hop > 99 {
		return time.Time{}, errors.New("invalid peer spot frame")
	}
	fields := frame.Fields
	want := 7
	if frame.Type == "PC61" {
		want = 8
	}
	if len(fields) != want && (frame.Type != "PC26" || len(fields) != 8) {
		return time.Time{}, errors.New("invalid peer spot field count")
	}
	if !spot.IsValidNormalizedCallsign(fields[1]) || !spot.IsValidNormalizedCallsign(fields[5]) || !spot.IsValidNormalizedCallsign(fields[6]) {
		return time.Time{}, errors.New("invalid original DX, DE or origin callsign")
	}
	if !validOriginalPeerFrequency(fields[0]) {
		return time.Time{}, errors.New("invalid original frequency")
	}
	if !validOriginalPeerComment(fields[4]) {
		return time.Time{}, errors.New("invalid or unsupported original comment")
	}
	stamp, err := originalPeerSpotTime(fields[2], fields[3])
	if err != nil {
		return time.Time{}, err
	}
	if frame.Type == "PC61" {
		ip, err := netip.ParseAddr(fields[7])
		if err != nil || ip.Zone() != "" {
			return time.Time{}, errors.New("invalid original spotter IP")
		}
	}
	if frame.Type == "PC26" && len(fields) == 8 {
		requested := fields[7]
		if requested != "" && requested != " " && requested != "*" && !spot.IsValidNormalizedCallsign(requested) {
			return time.Time{}, errors.New("invalid original merge destination")
		}
	}
	return stamp, nil
}

func validOriginalPeerFrequency(text string) bool {
	if text == "" {
		return false
	}
	dot := false
	for i := 0; i < len(text); i++ {
		if text[i] >= '0' && text[i] <= '9' {
			continue
		}
		if text[i] != '.' || dot || i == 0 || i == len(text)-1 {
			return false
		}
		dot = true
	}
	freq, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsNaN(freq) || math.IsInf(freq, 0) {
		return false
	}
	// Match existing half-up 10 Hz local rounding. Primary dedupe stores whole
	// kHz as uint32; out-of-range conversion must never admit a peer record.
	rounded := math.Floor(freq*100+0.5) / 100
	return !math.IsInf(rounded, 0) && rounded >= 0 && rounded < 1<<32
}

func validOriginalPeerComment(comment string) bool {
	if comment == "" {
		return false
	}
	for i := 0; i < len(comment); i++ {
		ch := comment[i]
		// This is a byte rule, even within UTF-8. FF is Telnet IAC: the native
		// writer cannot preserve it without transport escaping (out of scope).
		if ch <= 0x08 || (ch >= 0x0a && ch <= 0x1f) || (ch >= 0x80 && ch <= 0x9f) || ch == 0xff || ch == '^' || ch == '~' {
			return false
		}
	}
	return true
}

func originalPeerSpotTime(date, clock string) (time.Time, error) {
	if len(date) != 11 || date[2] != '-' || date[6] != '-' || len(clock) != 5 || clock[4] != 'Z' {
		return time.Time{}, errors.New("invalid original date or time syntax")
	}
	// DXSpider cldate uses %2d: days 1-9 have one leading ASCII space.
	// Admit that exact spelling alongside zero padding, not general trimming.
	if (date[0] < '0' || date[0] > '9') && (date[0] != ' ' || date[1] < '1' || date[1] > '9') {
		return time.Time{}, errors.New("invalid original day syntax")
	}
	for _, i := range [...]int{1, 7, 8, 9, 10} {
		if date[i] < '0' || date[i] > '9' {
			return time.Time{}, errors.New("invalid original date digits")
		}
	}
	for i := range 4 {
		if clock[i] < '0' || clock[i] > '9' {
			return time.Time{}, errors.New("invalid original time digits")
		}
	}
	stamp, err := time.ParseInLocation("_2-Jan-2006 1504Z", date+" "+clock, time.UTC)
	if err != nil {
		return time.Time{}, errors.New("invalid original calendar date or UTC time")
	}
	return stamp, nil
}

func (m *Manager) handlePeerSpot(frame *Frame, source *session, receivedAt time.Time) {
	stamp, err := validateOriginalPeerSpot(frame)
	if err != nil {
		m.reportBadCallParseDrop(frame, source, err)
		return
	}
	// Clone each field before local parsing: normalization caches and queued
	// Spots may outlive the frame lease and must not pin its whole raw line.
	// These <=8 compact copies also form the immutable transit payload.
	original := Frame{Type: frame.Type, Hop: frame.Hop, Fields: make([]string, len(frame.Fields))}
	for i, field := range frame.Fields {
		original.Fields[i] = strings.Clone(field)
	}
	local, err := parseSpotFromFrame(&original, "")
	if err != nil {
		m.reportBadCallParseDrop(frame, source, err)
		return
	}
	// The tolerant local parser cannot decode every admitted wire spelling.
	// Use the validated instant before identity/age checks; keep transit bytes.
	local.Time = stamp
	// The local consumer owns local after handoff and may immediately mutate
	// every field. Capture the existing key and timestamp before sending it.
	key := dxKey(&original, local)
	if !m.ingestSpot(local) || original.Hop <= 1 || !m.shouldRelayDataFrame(original.Type) {
		return
	}
	if m.dedupe.markSeen(key, receivedAt) {
		m.relayOriginalPeerSpot(&original, stamp, original.Hop-1, source)
	}
}

// relayOriginalPeerSpot retains existing fanout/gates and never reads the local
// Spot. PC11/PC61 retain their second age check; PC26 keeps merge age semantics.
// Build at most two bounded variants, then use the existing nonblocking queues.
func (m *Manager) relayOriginalPeerSpot(original *Frame, stamp time.Time, hop int, source *session) {
	if original.Type != "PC26" && m.maxAgeSeconds > 0 && time.Since(stamp) > time.Duration(m.maxAgeSeconds)*time.Second {
		return
	}
	modern := originalPeerSpotSentence(original, hop, false)
	legacy := modern
	if original.Type == "PC61" {
		legacy = originalPeerSpotSentence(original, hop, true)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, destination := range m.sessions.All() {
		if destination == source || (original.Type == "PC26" && !destination.pc9x) {
			continue
		}
		line := modern
		if !destination.pc9x {
			line = legacy
		}
		if line != "" {
			m.trySendLine(destination, line, "spot")
		}
	}
}

// Refuse each variant before allocating it. MaxPeerFrameBytes includes the ~;
// the sole writer adds its separately bounded CRLF. Empty return does not undo
// cache admission or request a retry, matching existing fanout refusal policy.
func originalPeerSpotSentence(original *Frame, hop int, legacy bool) string {
	kind, fields := original.Type, original.Fields
	if legacy && kind == "PC61" {
		kind, fields = "PC11", fields[:7]
	}
	hopText := strconv.Itoa(hop)
	length := len(kind) + len(fields) + len(hopText) + 4 // ^H + hop + ^~
	for _, field := range fields {
		length += len(field)
	}
	if length > MaxPeerFrameBytes {
		return ""
	}
	var out strings.Builder
	out.Grow(length)
	out.WriteString(kind)
	for _, field := range fields {
		out.WriteByte('^')
		out.WriteString(field)
	}
	out.WriteString("^H")
	out.WriteString(hopText)
	out.WriteString("^~")
	return out.String()
}
