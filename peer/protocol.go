package peer

import (
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
type telnetParser struct{}

// Purpose: Strip telnet IAC sequences and emit minimal refusal replies.
// Key aspects: Filters subnegotiation payloads and replies with WONT/DONT.
// Upstream: Peer reader for native telnet mode.
// Downstream: None.
func (p *telnetParser) Feed(input []byte) (output []byte, replies [][]byte) {
	var out []byte
	var inIAC, inSB bool
	for i := 0; i < len(input); i++ {
		b := input[i]
		if inIAC {
			switch b {
			case telnetSB:
				inSB = true
			case telnetSE:
				inSB = false
			case telnetDO:
				if i+1 < len(input) {
					replies = append(replies, []byte{telnetIAC, telnetWONT, input[i+1]})
					i++
				}
			case telnetWILL:
				if i+1 < len(input) {
					replies = append(replies, []byte{telnetIAC, telnetDONT, input[i+1]})
					i++
				}
			case telnetIAC:
				out = append(out, telnetIAC)
			}
			inIAC = false
			continue
		}
		if b == telnetIAC {
			inIAC = true
			continue
		}
		if inSB {
			continue
		}
		out = append(out, b)
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

// Purpose: Parse a caret-delimited PC frame line into a Frame.
// Key aspects: Trims trailing "~" and extracts hop suffix.
// Upstream: Peer reader.
// Downstream: Frame payload handling.
func ParseFrame(line string) (*Frame, error) {
	raw := strings.TrimSpace(line)
	if raw == "" {
		return nil, fmt.Errorf("empty line")
	}
	if strings.HasSuffix(raw, "~") {
		raw = strings.TrimSuffix(raw, "~")
	}
	parts := strings.Split(raw, "^")
	if len(parts) == 0 {
		return nil, fmt.Errorf("no parts")
	}
	f := &Frame{Raw: line}
	f.Type = strings.ToUpper(strings.TrimSpace(parts[0]))
	if len(parts) > 1 {
		f.Fields = parts[1:]
	}
	// Hop is always last non-empty token with H prefix when present.
	for i := len(parts) - 1; i >= 0; i-- {
		token := strings.TrimSpace(parts[i])
		if strings.HasPrefix(token, "H") || strings.HasPrefix(token, "h") {
			v, err := strconv.Atoi(strings.TrimPrefix(strings.TrimPrefix(token, "H"), "h"))
			if err == nil {
				f.Hop = v
			}
			break
		}
	}
	return f, nil
}

// Purpose: Encode a Frame back to wire format with optional hop override.
// Key aspects: Preserves fields and appends Hn when hop>0.
// Upstream: Peer writer.
// Downstream: fmt.Sprintf.
func (f *Frame) Encode(hop int) string {
	if f == nil {
		return ""
	}
	fields := make([]string, len(f.Fields))
	copy(fields, f.Fields)
	out := f.Type + "^" + strings.Join(fields, "^")
	if hop > 0 {
		if !strings.HasSuffix(out, "^") {
			out += "^"
		}
		out += fmt.Sprintf("H%d^", hop)
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
	fields := make([]string, len(f.Fields))
	copy(fields, f.Fields)
	// strip hop marker if present at end
	if len(fields) > 0 {
		last := strings.TrimSpace(fields[len(fields)-1])
		if strings.HasPrefix(last, "H") || strings.HasPrefix(last, "h") {
			fields = fields[:len(fields)-1]
		}
	}
	return fields
}
