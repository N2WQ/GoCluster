package pskreporter

import "strings"

// pskrPayload carries only the PSKReporter MQTT fields used by the cluster.
// Byte slices point at the worker-owned payload and must not escape handlePayload.
type pskrPayload struct {
	frequency int64
	mode      []byte
	modeText  string
	report    int
	hasReport bool
	timestamp int64

	senderCall      []byte
	senderCallText  string
	senderLocator   []byte
	senderGridText  string
	receiverCall    []byte
	receiverText    string
	receiverLocator []byte
	receiverGrid    string
}

func pskrPayloadFromMessage(msg *PSKRMessage) pskrPayload {
	if msg == nil {
		return pskrPayload{}
	}
	payload := pskrPayload{
		frequency:      msg.Frequency,
		modeText:       msg.Mode,
		timestamp:      msg.Timestamp,
		senderCallText: msg.SenderCall,
		senderGridText: msg.SenderLocator,
		receiverText:   msg.ReceiverCall,
		receiverGrid:   msg.ReceiverLocator,
	}
	if msg.Report != nil {
		payload.report = *msg.Report
		payload.hasReport = true
	}
	return payload
}

func (p *pskrPayload) senderCallString() string {
	return p.payloadString(p.senderCall, p.senderCallText)
}

func (p *pskrPayload) receiverCallString() string {
	return p.payloadString(p.receiverCall, p.receiverText)
}

func (p *pskrPayload) modeString() string {
	return p.payloadString(p.mode, p.modeText)
}

func (p *pskrPayload) senderGridString() string {
	return p.payloadUpperString(p.senderLocator, p.senderGridText)
}

func (p *pskrPayload) receiverGridString() string {
	return p.payloadUpperString(p.receiverLocator, p.receiverGrid)
}

func (p *pskrPayload) payloadString(raw []byte, fallback string) string {
	if fallback != "" || len(raw) == 0 {
		return fallback
	}
	return string(raw)
}

func (p *pskrPayload) payloadUpperString(raw []byte, fallback string) string {
	if fallback != "" {
		return strings.ToUpper(strings.TrimSpace(fallback))
	}
	if len(raw) == 0 {
		return ""
	}
	start, end := trimASCII(raw)
	if start >= end {
		return ""
	}
	needsUpper := false
	for i := start; i < end; i++ {
		if raw[i] >= 'a' && raw[i] <= 'z' {
			needsUpper = true
			break
		}
	}
	if !needsUpper {
		return string(raw[start:end])
	}
	buf := make([]byte, end-start)
	for i := start; i < end; i++ {
		c := raw[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		buf[i-start] = c
	}
	return string(buf)
}

func (p *pskrPayload) modeInfo() (pskModeInfo, bool) {
	if p.modeText != "" {
		return parseModeInfo(p.modeText)
	}
	return parseModeInfoBytes(p.mode)
}

func parseModeInfoBytes(raw []byte) (pskModeInfo, bool) {
	start, end := trimASCII(raw)
	if start >= end {
		return pskModeInfo{}, false
	}
	token := raw[start:end]
	switch {
	case equalFoldASCIIBytes(token, "CW"):
		return pskModeInfo{canonical: "CW", variant: "CW"}, true
	case equalFoldASCIIBytes(token, "RTTY"):
		return pskModeInfo{canonical: "RTTY", variant: "RTTY"}, true
	case equalFoldASCIIBytes(token, "FT8"):
		return pskModeInfo{canonical: "FT8", variant: "FT8"}, true
	case equalFoldASCIIBytes(token, "FT4"):
		return pskModeInfo{canonical: "FT4", variant: "FT4"}, true
	case equalFoldASCIIBytes(token, "FT2"):
		return pskModeInfo{canonical: "FT2", variant: "FT2"}, true
	case equalFoldASCIIBytes(token, "MSK144"):
		return pskModeInfo{canonical: "MSK144", variant: "MSK144"}, true
	case equalFoldASCIIBytes(token, "PSK"):
		return pskModeInfo{canonical: "PSK", variant: "PSK", isPSK: true}, true
	case equalFoldASCIIBytes(token, "PSK31"):
		return pskModeInfo{canonical: "PSK", variant: "PSK31", isPSK: true}, true
	case equalFoldASCIIBytes(token, "PSK63"):
		return pskModeInfo{canonical: "PSK", variant: "PSK63", isPSK: true}, true
	case equalFoldASCIIBytes(token, "PSK125"):
		return pskModeInfo{canonical: "PSK", variant: "PSK125", isPSK: true}, true
	case equalFoldASCIIBytes(token, "WSPR"):
		return pskModeInfo{canonical: "WSPR", variant: "WSPR"}, true
	default:
		return parseModeInfo(string(token))
	}
}

func equalFoldASCIIBytes(raw []byte, want string) bool {
	if len(raw) != len(want) {
		return false
	}
	for i := 0; i < len(raw); i++ {
		c := raw[i]
		if c >= 'a' && c <= 'z' {
			c -= 'a' - 'A'
		}
		w := want[i]
		if w >= 'a' && w <= 'z' {
			w -= 'a' - 'A'
		}
		if c != w {
			return false
		}
	}
	return true
}

func equalASCIIBytes(raw []byte, want string) bool {
	if len(raw) != len(want) {
		return false
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] != want[i] {
			return false
		}
	}
	return true
}

func isPSKRKnownKeyFold(raw []byte) bool {
	switch {
	case equalFoldASCIIBytes(raw, "sq"):
		return true
	case equalFoldASCIIBytes(raw, "f"):
		return true
	case equalFoldASCIIBytes(raw, "md"):
		return true
	case equalFoldASCIIBytes(raw, "rp"):
		return true
	case equalFoldASCIIBytes(raw, "t"):
		return true
	case equalFoldASCIIBytes(raw, "sc"):
		return true
	case equalFoldASCIIBytes(raw, "sl"):
		return true
	case equalFoldASCIIBytes(raw, "rc"):
		return true
	case equalFoldASCIIBytes(raw, "rl"):
		return true
	case equalFoldASCIIBytes(raw, "sa"):
		return true
	case equalFoldASCIIBytes(raw, "ra"):
		return true
	case equalFoldASCIIBytes(raw, "b"):
		return true
	default:
		return false
	}
}

func trimASCII(raw []byte) (int, int) {
	start := 0
	end := len(raw)
	for start < end && raw[start] <= ' ' {
		start++
	}
	for end > start && raw[end-1] <= ' ' {
		end--
	}
	return start, end
}

func parsePSKRPayloadCompat(payload []byte) (pskrPayload, error) {
	var pskrMsg PSKRMessage
	if err := jsonFast.Unmarshal(payload, &pskrMsg); err != nil {
		if errCompat := jsonCompat.Unmarshal(payload, &pskrMsg); errCompat != nil {
			return pskrPayload{}, errCompat
		}
	}
	return pskrPayloadFromMessage(&pskrMsg), nil
}

func parsePSKRPayloadFast(payload []byte) (pskrPayload, bool) {
	parser := pskrJSONParser{payload: payload}
	return parser.parse()
}

type pskrJSONParser struct {
	payload []byte
	pos     int
}

func (p *pskrJSONParser) parse() (pskrPayload, bool) {
	var out pskrPayload
	p.skipWhitespace()
	if !p.consume('{') {
		return pskrPayload{}, false
	}
	p.skipWhitespace()
	if p.consume('}') {
		p.skipWhitespace()
		return out, p.pos == len(p.payload)
	}
	for {
		key, ok := p.readSimpleString()
		if !ok {
			return pskrPayload{}, false
		}
		p.skipWhitespace()
		if !p.consume(':') {
			return pskrPayload{}, false
		}
		p.skipWhitespace()
		switch {
		case equalASCIIBytes(key, "sq"):
			if _, ok := p.readJSONInt64(); !ok {
				return pskrPayload{}, false
			}
		case equalASCIIBytes(key, "f"):
			v, ok := p.readJSONInt64()
			if !ok {
				return pskrPayload{}, false
			}
			out.frequency = v
		case equalASCIIBytes(key, "md"):
			v, ok := p.readSimpleString()
			if !ok {
				return pskrPayload{}, false
			}
			out.mode = v
			out.modeText = ""
		case equalASCIIBytes(key, "rp"):
			v, ok := p.readJSONInt64()
			if !ok {
				return pskrPayload{}, false
			}
			out.report = int(v)
			out.hasReport = true
		case equalASCIIBytes(key, "t"):
			v, ok := p.readJSONInt64()
			if !ok {
				return pskrPayload{}, false
			}
			out.timestamp = v
		case equalASCIIBytes(key, "sc"):
			v, ok := p.readSimpleString()
			if !ok {
				return pskrPayload{}, false
			}
			out.senderCall = v
			out.senderCallText = ""
		case equalASCIIBytes(key, "sl"):
			v, ok := p.readSimpleString()
			if !ok {
				return pskrPayload{}, false
			}
			out.senderLocator = v
			out.senderGridText = ""
		case equalASCIIBytes(key, "rc"):
			v, ok := p.readSimpleString()
			if !ok {
				return pskrPayload{}, false
			}
			out.receiverCall = v
			out.receiverText = ""
		case equalASCIIBytes(key, "rl"):
			v, ok := p.readSimpleString()
			if !ok {
				return pskrPayload{}, false
			}
			out.receiverLocator = v
			out.receiverGrid = ""
		case equalASCIIBytes(key, "sa"):
			if _, ok := p.readJSONInt64(); !ok {
				return pskrPayload{}, false
			}
		case equalASCIIBytes(key, "ra"):
			if _, ok := p.readJSONInt64(); !ok {
				return pskrPayload{}, false
			}
		case equalASCIIBytes(key, "b"):
			if _, ok := p.readSimpleString(); !ok {
				return pskrPayload{}, false
			}
		default:
			if isPSKRKnownKeyFold(key) {
				return pskrPayload{}, false
			}
			if !p.skipValue() {
				return pskrPayload{}, false
			}
		}
		p.skipWhitespace()
		if p.consume(',') {
			p.skipWhitespace()
			continue
		}
		if p.consume('}') {
			p.skipWhitespace()
			return out, p.pos == len(p.payload)
		}
		return pskrPayload{}, false
	}
}

func (p *pskrJSONParser) skipWhitespace() {
	for p.pos < len(p.payload) {
		switch p.payload[p.pos] {
		case ' ', '\n', '\r', '\t':
			p.pos++
		default:
			return
		}
	}
}

func (p *pskrJSONParser) consume(c byte) bool {
	if p.pos >= len(p.payload) || p.payload[p.pos] != c {
		return false
	}
	p.pos++
	return true
}

func (p *pskrJSONParser) readSimpleString() ([]byte, bool) {
	if !p.consume('"') {
		return nil, false
	}
	start := p.pos
	for p.pos < len(p.payload) {
		c := p.payload[p.pos]
		if c == '"' {
			value := p.payload[start:p.pos]
			p.pos++
			return value, true
		}
		if c == '\\' || c < 0x20 || c >= 0x80 {
			return nil, false
		}
		p.pos++
	}
	return nil, false
}

func (p *pskrJSONParser) readJSONInt64() (int64, bool) {
	negative := false
	if p.pos < len(p.payload) && p.payload[p.pos] == '-' {
		negative = true
		p.pos++
	}
	if p.pos >= len(p.payload) || p.payload[p.pos] < '0' || p.payload[p.pos] > '9' {
		return 0, false
	}
	var unsigned uint64
	if p.payload[p.pos] == '0' {
		p.pos++
	} else {
		for p.pos < len(p.payload) && p.payload[p.pos] >= '0' && p.payload[p.pos] <= '9' {
			digit := uint64(p.payload[p.pos] - '0')
			if unsigned > (^uint64(0)-digit)/10 {
				return 0, false
			}
			unsigned = unsigned*10 + digit
			p.pos++
		}
	}
	if p.pos < len(p.payload) {
		switch p.payload[p.pos] {
		case '.', 'e', 'E':
			return 0, false
		}
	}
	if negative {
		if unsigned > uint64(1)<<63 {
			return 0, false
		}
		if unsigned == uint64(1)<<63 {
			return -1 << 63, true
		}
		return -int64(unsigned), true
	}
	if unsigned > ^uint64(0)>>1 {
		return 0, false
	}
	return int64(unsigned), true
}

func (p *pskrJSONParser) skipValue() bool {
	if p.pos >= len(p.payload) {
		return false
	}
	switch p.payload[p.pos] {
	case '"':
		return p.skipString()
	case '{':
		return p.skipComposite('{', '}')
	case '[':
		return p.skipComposite('[', ']')
	case 't':
		return p.consumeLiteral("true")
	case 'f':
		return p.consumeLiteral("false")
	case 'n':
		return p.consumeLiteral("null")
	default:
		if p.payload[p.pos] == '-' || (p.payload[p.pos] >= '0' && p.payload[p.pos] <= '9') {
			_, ok := p.readJSONNumber()
			return ok
		}
		return false
	}
}

func (p *pskrJSONParser) skipString() bool {
	if !p.consume('"') {
		return false
	}
	for p.pos < len(p.payload) {
		c := p.payload[p.pos]
		p.pos++
		if c == '"' {
			return true
		}
		if c == '\\' {
			if p.pos >= len(p.payload) {
				return false
			}
			esc := p.payload[p.pos]
			p.pos++
			if esc == 'u' {
				for i := 0; i < 4; i++ {
					if p.pos >= len(p.payload) || !isJSONHex(p.payload[p.pos]) {
						return false
					}
					p.pos++
				}
			}
			continue
		}
		if c < 0x20 {
			return false
		}
	}
	return false
}

func (p *pskrJSONParser) skipComposite(open, close byte) bool {
	if !p.consume(open) {
		return false
	}
	depth := 1
	for p.pos < len(p.payload) {
		c := p.payload[p.pos]
		switch c {
		case '"':
			if !p.skipString() {
				return false
			}
			continue
		case open:
			depth++
		case close:
			depth--
			if depth == 0 {
				p.pos++
				return true
			}
		}
		p.pos++
	}
	return false
}

func (p *pskrJSONParser) consumeLiteral(lit string) bool {
	if len(p.payload)-p.pos < len(lit) {
		return false
	}
	for i := 0; i < len(lit); i++ {
		if p.payload[p.pos+i] != lit[i] {
			return false
		}
	}
	p.pos += len(lit)
	return true
}

func (p *pskrJSONParser) readJSONNumber() ([]byte, bool) {
	start := p.pos
	if p.pos < len(p.payload) && p.payload[p.pos] == '-' {
		p.pos++
	}
	if p.pos >= len(p.payload) || p.payload[p.pos] < '0' || p.payload[p.pos] > '9' {
		return nil, false
	}
	if p.payload[p.pos] == '0' {
		p.pos++
	} else {
		for p.pos < len(p.payload) && p.payload[p.pos] >= '0' && p.payload[p.pos] <= '9' {
			p.pos++
		}
	}
	if p.pos < len(p.payload) && p.payload[p.pos] == '.' {
		p.pos++
		if p.pos >= len(p.payload) || p.payload[p.pos] < '0' || p.payload[p.pos] > '9' {
			return nil, false
		}
		for p.pos < len(p.payload) && p.payload[p.pos] >= '0' && p.payload[p.pos] <= '9' {
			p.pos++
		}
	}
	if p.pos < len(p.payload) && (p.payload[p.pos] == 'e' || p.payload[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.payload) && (p.payload[p.pos] == '+' || p.payload[p.pos] == '-') {
			p.pos++
		}
		if p.pos >= len(p.payload) || p.payload[p.pos] < '0' || p.payload[p.pos] > '9' {
			return nil, false
		}
		for p.pos < len(p.payload) && p.payload[p.pos] >= '0' && p.payload[p.pos] <= '9' {
			p.pos++
		}
	}
	return p.payload[start:p.pos], true
}

func isJSONHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
