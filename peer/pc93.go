package peer

import (
	"fmt"
	"strings"

	"dxcluster/spot"
)

type pc93Message struct {
	NodeCall  string
	Timestamp string
	To        string
	From      string
	Via       string
	Text      string
	Onode     string
	IP        string
	Hop       int
}

// Purpose: Parse a PC93 frame into a structured message.
// Key aspects: Requires minimum fields; decodes escaped text.
// Upstream: Peer session reader for announcements/talk.
// Downstream: decodePC93Text.
func parsePC93(frame *Frame) (pc93Message, bool) {
	if frame == nil {
		return pc93Message{}, false
	}
	fields := frame.payloadFields()
	if len(fields) < 6 {
		return pc93Message{}, false
	}
	msg := pc93Message{
		NodeCall:  strings.TrimSpace(fields[0]),
		Timestamp: strings.TrimSpace(fields[1]),
		To:        strings.TrimSpace(fields[2]),
		From:      strings.TrimSpace(fields[3]),
		Via:       strings.TrimSpace(fields[4]),
		Text:      decodePC93Text(strings.TrimSpace(fields[5])),
		Hop:       frame.Hop,
	}
	if len(fields) >= 7 {
		msg.Onode = strings.TrimSpace(fields[6])
	}
	if len(fields) >= 8 {
		msg.IP = strings.TrimSpace(fields[7])
	}
	return msg, true
}

// Purpose: Decode escaped caret sequences in PC93 text.
// Key aspects: Only handles %5E/%5e for caret.
// Upstream: parsePC93.
// Downstream: strings.ReplaceAll.
func decodePC93Text(text string) string {
	if text == "" {
		return text
	}
	text = strings.ReplaceAll(text, "%5E", "^")
	text = strings.ReplaceAll(text, "%5e", "^")
	return text
}

// Purpose: Determine whether a PC93 message targets a user or broadcast.
// Key aspects: Treats ALL/WX/SYSOP/#rooms as broadcasts.
// Upstream: Peer session routing.
// Downstream: spot.IsValidCallsign.
func pc93Target(msg pc93Message) (target string, broadcast bool) {
	to := strings.TrimSpace(msg.To)
	if to == "" {
		return "", true
	}
	upper := strings.ToUpper(to)
	switch upper {
	case "*", "ALL", "WX", "SYSOP":
		return "", true
	}
	if strings.HasPrefix(upper, "#") {
		return "", true
	}
	if spot.IsValidCallsign(upper) {
		return upper, false
	}
	return "", true
}

// Purpose: Format a PC93 message for telnet display.
// Key aspects: Normalizes To/From labels and drops empty text.
// Upstream: Telnet broadcast of announcements.
// Downstream: fmt.Sprintf.
func formatPC93Line(msg pc93Message) string {
	to := strings.TrimSpace(msg.To)
	if strings.EqualFold(to, "*") || strings.EqualFold(to, "ALL") || to == "" {
		to = "ALL"
	}
	from := strings.TrimSpace(msg.From)
	if from == "" {
		from = strings.TrimSpace(msg.NodeCall)
	}
	if from == "" {
		return ""
	}
	text := strings.TrimSpace(msg.Text)
	if text == "" {
		return ""
	}
	return fmt.Sprintf("To %s de %s: %s", to, from, text)
}
