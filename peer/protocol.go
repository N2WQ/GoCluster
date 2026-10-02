package peer

import (
	"dxcluster/strutil"
	"fmt"
	"strconv"
	"strings"
)

const (
	telnetIAC  = 255
	telnetDONT = 254
	telnetDO   = 253
	telnetWONT = 252
	telnetWILL = 251
	telnetSB   = 250
	telnetSE   = 240
)

// telnetParser strips telnet IAC sequences from input and returns clean payload bytes plus replies.
// Replies perform a minimal refuse-all negotiation to keep the link in character mode.
type telnetParser struct {
	state   byte
	command byte
}

// Feed strips telnet IAC sequences and emits minimal refusal replies.
// Key aspects: Filters subnegotiation payloads and replies with WONT/DONT.
// Upstream: Peer reader for native telnet mode.
// Downstream: None.
func (p *telnetParser) Feed(input []byte) (output []byte, replies [][]byte) {
	// Negotiation can end in any read boundary. The reader owns this constant
	// amount of parser state for the whole connection, including unfinished SB.
	out := make([]byte, 0, len(input))
	for _, b := range input {
		switch p.state {
		case 1: // IAC command
			switch b {
			case telnetSB:
				p.state = 3
			case telnetDO, telnetDONT, telnetWILL, telnetWONT:
				p.command, p.state = b, 2
			case telnetIAC:
				out = append(out, telnetIAC)
				p.state = 0
			default:
				p.state = 0
			}
		case 2: // command option
			switch p.command {
			case telnetDO:
				replies = append(replies, []byte{telnetIAC, telnetWONT, b})
			case telnetWILL:
				replies = append(replies, []byte{telnetIAC, telnetDONT, b})
			}
			p.state = 0
		case 3: // discard subnegotiation payload
			if b == telnetIAC {
				p.state = 4
			}
		case 4: // IAC inside subnegotiation
			p.state = 3
			if b == telnetSE {
				p.state = 0
			}
		default:
			if b == telnetIAC {
				p.state = 1
			} else {
				out = append(out, b)
			}
		}
	}
	return out, replies
}

// Frame represents a parsed PC protocol sentence.
type Frame struct {
	Type   string
	Fields []string
	Hop    int
	Raw    string
}

// ParseFrame parses a caret-delimited PC frame line into a Frame.
// Key aspects: Trims trailing "~" and extracts hop suffix.
// Upstream: Peer reader.
// Downstream: Frame payload handling.
func ParseFrame(line string) (*Frame, error) {
	raw := strings.TrimRight(line, "\r\n~")
	if len(raw) > MaxPeerFrameBytes {
		return nil, fmt.Errorf("frame exceeds %d bytes", MaxPeerFrameBytes)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty line")
	}
	if !isFrameStartAt([]byte(raw), 0) {
		return nil, fmt.Errorf("invalid PC frame header")
	}
	f := &Frame{Raw: line}
	f.Type = strutil.NormalizeUpper(raw[:4])
	payload, hop, err := splitFramePayload(f.Type, raw[5:])
	if err != nil {
		return nil, err
	}
	f.Fields = payload
	f.Hop = hop
	return f, nil
}

// splitFramePayload locates the hop suffix without a string-header allocation
// per input delimiter. Authority-bearing PC92/PC93 then enforce their bounded
// field grammar before Split: a 64 KiB run of carets cannot allocate a megabyte
// of headers in each concurrently handshaking reader.
func splitFramePayload(frameType, raw string) ([]string, int, error) {
	if frameType == "PC92" || frameType == "PC93" {
		return splitAuthorityPayload(frameType, raw)
	}
	minimum := 0
	index := strings.Count(raw, "^")
	end := len(raw)
	for end > 0 && raw[end-1] == '^' {
		end--
		index--
	}
	hop, haveSuffix, haveNumeric := 0, false, false
	for index >= minimum && end >= 0 {
		start := strings.LastIndexByte(raw[:end], '^') + 1
		value, like, ok := parseHopToken(strings.TrimSpace(raw[start:end]))
		if !like {
			break
		}
		haveSuffix = true
		if ok && !haveNumeric {
			hop, haveNumeric = value, true
		}
		end = start - 1
		index--
	}
	if !haveSuffix {
		end = len(raw)
	}
	if end < 0 {
		return nil, hop, nil
	}
	return strings.Split(raw[:end], "^"), hop, nil
}

// splitAuthorityPayload consumes the final transport hop once. Numeric stacks
// collapse only outside payload grammar: PC92 membership entries cannot start
// H, whereas K extensions are open-ended and PC93 owns two optional slots.
// Malformed hop-like payload is retained for whole-record validation; a broken
// terminal hop can never recover authority from an earlier numeric marker.
func splitAuthorityPayload(frameType, raw string) ([]string, int, error) {
	index, end := strings.Count(raw, "^"), len(raw)
	for end > 0 && raw[end-1] == '^' {
		end--
		index--
	}
	minimum := 4
	keepalive := false
	if frameType == "PC93" {
		minimum = 6
	} else {
		_, tail, _ := strings.Cut(raw, "^")
		_, tail, _ = strings.Cut(tail, "^")
		action, _, _ := strings.Cut(tail, "^")
		keepalive = action == "K"
		if keepalive {
			minimum = 6
		}
	}
	hop, found := 0, false
	if index >= minimum {
		start := strings.LastIndexByte(raw[:end], '^') + 1
		value, like, valid := parseHopToken(strings.TrimSpace(raw[start:end]))
		if like && (!valid || value > 99) {
			return nil, 0, fmt.Errorf("%s has invalid terminal hop", frameType)
		}
		if valid {
			hop, found, end, index = value, true, start-1, index-1
		}
	}
	if found && !keepalive {
		stackMinimum := minimum
		if frameType == "PC93" {
			stackMinimum = 8
		}
		for index >= stackMinimum && end >= 0 {
			start := strings.LastIndexByte(raw[:end], '^') + 1
			value, _, valid := parseHopToken(strings.TrimSpace(raw[start:end]))
			if !valid || value > 99 {
				break
			}
			end, index = start-1, index-1
		}
	}
	if !found {
		end, index = len(raw), strings.Count(raw, "^")
	}
	if frameType == "PC92" && index+1 < minimum {
		return nil, 0, fmt.Errorf("PC92 requires at least %d payload fields", minimum)
	}
	if frameType == "PC92" && index+1 > 8195 {
		return nil, 0, fmt.Errorf("PC92 exceeds 8192 records")
	}
	if frameType == "PC93" && (index+1 < 6 || index+1 > 8) {
		return nil, 0, fmt.Errorf("PC93 requires 6 to 8 payload fields")
	}
	return strings.Split(raw[:end], "^"), hop, nil
}

// Encode encodes a Frame back to wire format with optional hop override.
// Key aspects: Preserves fields and appends Hn including the meaningful H0.
// Upstream: Peer writer.
// Downstream: fmt.Sprintf.
func (f *Frame) Encode(hop int) string {
	if f == nil {
		return ""
	}
	// Authority fields have already crossed their grammar boundary. Repeating
	// suffix stripping would consume K extensions or optional PC93 metadata.
	fields := f.Fields
	if f.Type != "PC92" && f.Type != "PC93" {
		fields, _ = stripFrameHopSuffix(f.Type, fields)
	}
	out := f.Type + "^" + strings.Join(fields, "^")
	if hop >= 0 {
		out += fmt.Sprintf("^H%d^", hop)
	}
	return out
}

// Purpose: Return payload fields excluding hop marker.
// Key aspects: Preserves trailing empty fields for protocol fidelity.
// Upstream: parseSpotFromFrame and PC parsers.
// Downstream: None.
func (f *Frame) payloadFields() []string {
	if f == nil {
		return nil
	}
	// Parsed fields are already payload. In particular PC93 text can itself be
	// H123 or H9x; running generic suffix stripping again would destroy it.
	return f.Fields
}

// PayloadFields is the legacy suffix helper for unparsed field arrays. Parsed
// Frame.Fields are already payload and must never be passed through it again.
func PayloadFields(fields []string) []string {
	if len(fields) == 0 {
		return fields
	}
	out, _ := stripTrailingHopSuffix(fields)
	return out
}

// stripTrailingHopSuffix removes a trailing hop suffix sequence (e.g., H95 or
// H95,H94,H93) and returns the payload fields plus effective hop. The effective
// hop is the rightmost numeric hop token in the trailing suffix.
func stripTrailingHopSuffix(fields []string) ([]string, int) {
	return stripHopSuffix(fields, 0)
}

func stripFrameHopSuffix(frameType string, fields []string) ([]string, int) {
	minimum := 0
	if frameType == "PC93" {
		minimum = 6 // origin, timestamp, recipient, sender, via, text
	}
	return stripHopSuffix(fields, minimum)
}

func stripHopSuffix(fields []string, minimum int) ([]string, int) {
	if len(fields) == 0 {
		return fields, 0
	}
	out := make([]string, len(fields))
	copy(out, fields)

	i := len(out) - 1
	for i >= minimum && out[i] == "" {
		i--
	}
	if i < 0 {
		return out, 0
	}

	hop := 0
	haveSuffix := false
	haveNumeric := false
	for i >= minimum {
		trimmed := strings.TrimSpace(out[i])
		v, isHopLike, ok := parseHopToken(trimmed)
		if !isHopLike {
			break
		}
		haveSuffix = true
		if ok && !haveNumeric {
			hop = v
			haveNumeric = true
		}
		i--
	}
	if !haveSuffix {
		return out, 0
	}
	return out[:i+1], hop
}

// parseHopToken classifies hop-like tokens (H...) and, when numeric, returns
// their integer value.
func parseHopToken(token string) (value int, isHopLike bool, ok bool) {
	token = strings.TrimSpace(token)
	if len(token) < 2 {
		return 0, false, false
	}
	if token[0] != 'H' && token[0] != 'h' {
		return 0, false, false
	}
	if token[1] < '0' || token[1] > '9' {
		return 0, false, false
	}
	v, err := strconv.Atoi(token[1:])
	if err != nil {
		return 0, true, false
	}
	return v, true, true
}
