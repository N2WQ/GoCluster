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

// Frame represents a parsed PC protocol sentence. Fields contains payload only;
// transport hop extraction belongs to ParseFrame, not subsequent encoding.
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
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("empty line")
	}
	if !isFrameStartAt([]byte(trimmed), 0) {
		return nil, fmt.Errorf("invalid PC frame header")
	}
	f := &Frame{Raw: line}
	f.Type = strutil.NormalizeUpper(trimmed[:4])
	// Spot admission validates the original sentence, before whitespace or
	// local fallbacks can hide defects. Other PC families keep their grammar.
	if isPeerSpotFrame(f.Type) && trimmed != raw {
		return nil, fmt.Errorf("%s has outer sentence whitespace", f.Type)
	}
	if isPeerSpotFrame(f.Type) && strings.ContainsAny(raw, "\r\n") {
		return nil, fmt.Errorf("%s has an embedded transport terminator", f.Type)
	}
	raw = trimmed
	payload, hop, err := splitFramePayload(f.Type, raw[5:])
	if err != nil {
		return nil, err
	}
	if isPeerSpotFrame(f.Type) {
		// The bounded split fixes field ownership before admitting literal
		// comment tildes. Other fields and transport hops cannot contain them.
		for i, field := range payload {
			if i != 4 && strings.IndexByte(field, '~') >= 0 {
				return nil, fmt.Errorf("%s has tilde outside comment", f.Type)
			}
		}
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
	if isPeerSpotFrame(frameType) {
		return splitPeerSpotPayload(frameType, raw)
	}
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

// Spot field positions delimit transport hops. In particular an origin or
// PC26 requested callsign such as H1ABC belongs to the payload. Consume one
// closing caret, then valid hop tokens only; extra empty slots stay visible to
// the field-count check. The split allocates at most eight string headers.
func splitPeerSpotPayload(frameType, raw string) ([]string, int, error) {
	if !strings.HasSuffix(raw, "^") {
		return nil, 0, fmt.Errorf("%s requires a closing caret", frameType)
	}
	raw = raw[:len(raw)-1]
	minimum := 7
	if frameType == "PC61" {
		minimum = 8
	}
	count, end := strings.Count(raw, "^")+1, len(raw)
	hop, found := 0, false
	for count > minimum {
		start := strings.LastIndexByte(raw[:end], '^') + 1
		value, valid := parsePeerSpotHop(raw[start:end])
		if !valid {
			break
		}
		if !found {
			hop, found = value, true
		}
		end, count = start-1, count-1
	}
	if !found && frameType != "PC26" {
		return nil, 0, fmt.Errorf("%s requires a valid hop", frameType)
	}
	maximum := minimum
	if frameType == "PC26" {
		maximum = 8
	}
	if count < minimum || count > maximum {
		return nil, 0, fmt.Errorf("%s has invalid fields or hop suffix", frameType)
	}
	return strings.Split(raw[:end], "^"), hop, nil
}

func parsePeerSpotHop(token string) (int, bool) {
	if len(token) < 2 || len(token) > 3 || (token[0] != 'H' && token[0] != 'h') {
		return 0, false
	}
	value := 0
	for i := 1; i < len(token); i++ {
		if token[i] < '0' || token[i] > '9' {
			return 0, false
		}
		value = value*10 + int(token[i]-'0')
	}
	return value, true
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
	// All parsed fields are payload, including a hop-like value before a blank
	// field. A second suffix pass would erase admitted data. Keep the complete
	// encoding even if it exceeds the downstream parser/writer size limit.
	out := f.Type
	if len(f.Fields) > 0 || hop < 0 {
		out += "^" + strings.Join(f.Fields, "^")
	}
	if hop >= 0 {
		out += fmt.Sprintf("^H%d^", hop)
	} else if isPeerSpotFrame(f.Type) {
		// PC26 merge frames may have no hop. Their closing caret distinguishes
		// an omitted optional slot from an explicitly empty one.
		out += "^"
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
	if len(fields) == 0 {
		return fields, 0
	}
	out := make([]string, len(fields))
	copy(out, fields)

	i := len(out) - 1
	for i >= 0 && out[i] == "" {
		i--
	}
	if i < 0 {
		return out, 0
	}

	hop := 0
	haveSuffix := false
	haveNumeric := false
	for i >= 0 {
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
