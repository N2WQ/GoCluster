package peer

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"dxcluster/spot"
)

// parseSpotFromFrame converts PC61/PC11 into a Spot.
func parseSpotFromFrame(frame *Frame, fallbackOrigin string) (*spot.Spot, error) {
	if frame == nil {
		return nil, fmt.Errorf("nil frame")
	}
	fields := frame.payloadFields()
	switch frame.Type {
	case "PC11":
		return parsePC11(fields, frame.Hop, fallbackOrigin)
	case "PC61":
		return parsePC61(fields, frame.Hop, fallbackOrigin)
	case "PC26":
		return parsePC26(fields, frame.Hop, fallbackOrigin)
	default:
		return nil, fmt.Errorf("unsupported frame type: %s", frame.Type)
	}
}

func parsePC11(fields []string, hop int, fallbackOrigin string) (*spot.Spot, error) {
	if len(fields) < 7 {
		return nil, fmt.Errorf("pc11: insufficient fields")
	}
	freq, err := strconv.ParseFloat(strings.TrimSpace(fields[0]), 64)
	if err != nil {
		return nil, fmt.Errorf("pc11: freq parse: %w", err)
	}
	dx := strings.TrimSpace(fields[1])
	date := strings.TrimSpace(fields[2])
	timeStr := strings.TrimSpace(fields[3])
	comment := fields[4]
	spotter := strings.TrimSpace(fields[5])
	origin := strings.TrimSpace(fields[6])
	if origin == "" {
		origin = fallbackOrigin
	}
	if !spot.IsValidCallsign(dx) {
		return nil, fmt.Errorf("pc11: invalid DX callsign")
	}
	if !spot.IsValidCallsign(spotter) {
		return nil, fmt.Errorf("pc11: invalid DE callsign")
	}
	ts := parsePCDateTime(date, timeStr)
	parsed := spot.ParseSpotComment(comment, freq)
	// PC11 timestamps are authoritative; ignore any comment time tokens.
	s := spot.NewSpot(dx, spotter, freq, parsed.Mode)
	s.Time = ts
	s.Comment = parsed.Comment
	s.SourceType = spot.SourcePeer
	s.SourceNode = origin
	s.Report = parsed.Report
	s.HasReport = parsed.HasReport
	spot.ApplySourceHumanFlag(s)
	if hop > 0 {
		s.TTL = uint8(hop)
	}
	s.RefreshBeaconFlag()
	return s, nil
}

func parsePC61(fields []string, hop int, fallbackOrigin string) (*spot.Spot, error) {
	if len(fields) < 8 {
		return nil, fmt.Errorf("pc61: insufficient fields")
	}
	freq, err := strconv.ParseFloat(strings.TrimSpace(fields[0]), 64)
	if err != nil {
		return nil, fmt.Errorf("pc61: freq parse: %w", err)
	}
	dx := strings.TrimSpace(fields[1])
	date := strings.TrimSpace(fields[2])
	timeStr := strings.TrimSpace(fields[3])
	comment := fields[4]
	spotter := strings.TrimSpace(fields[5])
	origin := strings.TrimSpace(fields[6])
	if origin == "" {
		origin = fallbackOrigin
	}
	if !spot.IsValidCallsign(dx) {
		return nil, fmt.Errorf("pc61: invalid DX callsign")
	}
	if !spot.IsValidCallsign(spotter) {
		return nil, fmt.Errorf("pc61: invalid DE callsign")
	}
	spotterIP := strings.TrimSpace(fields[7])
	ts := parsePCDateTime(date, timeStr)
	parsed := spot.ParseSpotComment(comment, freq)
	// PC61 timestamps are authoritative; ignore any comment time tokens.
	s := spot.NewSpot(dx, spotter, freq, parsed.Mode)
	s.Time = ts
	s.Comment = parsed.Comment
	s.SourceType = spot.SourcePeer
	s.SourceNode = origin
	s.SpotterIP = spotterIP
	s.Report = parsed.Report
	s.HasReport = parsed.HasReport
	spot.ApplySourceHumanFlag(s)
	if hop > 0 {
		s.TTL = uint8(hop)
	}
	s.RefreshBeaconFlag()
	return s, nil
}

// parsePC26 handles merge DX spots (same layout as PC11 with an optional empty slot before hop).
func parsePC26(fields []string, hop int, fallbackOrigin string) (*spot.Spot, error) {
	// PC26: freq, dx, date, time, comment, spotter, origin, [empty], hop
	if len(fields) < 7 {
		return nil, fmt.Errorf("pc26: insufficient fields")
	}
	// Drop legacy placeholder slot if present (index 7).
	if len(fields) >= 8 && strings.TrimSpace(fields[7]) == "" {
		fields = append([]string{}, fields[:7]...)
	}
	return parsePC11(fields, hop, fallbackOrigin)
}

func parsePCDateTime(dateStr, timeStr string) time.Time {
	if dateStr == "" || timeStr == "" {
		return time.Now().UTC()
	}
	combined := fmt.Sprintf("%s %s", dateStr, timeStr)
	if ts, err := time.ParseInLocation("02-Jan-2006 1504Z", combined, time.UTC); err == nil {
		return ts
	}
	return time.Now().UTC()
}

// Comment parsing is centralized in spot.ParseSpotComment.
