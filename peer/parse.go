package peer

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"dxcluster/spot"
)

var modeTokens = []string{
	"CW", "RTTY", "FT8", "FT4", "MSK144", "MSK", "USB", "LSB", "SSB", "AM", "FM",
}

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
	ts := parsePCDateTime(date, timeStr)
	mode, report, hasReport, cleaned := parseCommentModeReport(comment, freq)
	s := spot.NewSpot(dx, spotter, freq, mode)
	s.Time = ts
	s.Comment = cleaned
	s.SourceType = spot.SourceUpstream
	s.SourceNode = origin
	s.Report = report
	s.HasReport = hasReport
	// Heuristic: treat spots without an SNR/report as human-originated.
	s.IsHuman = !hasReport
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
	// fields[7] user IP present but not stored in Spot; could be logged later.
	ts := parsePCDateTime(date, timeStr)
	mode, report, hasReport, cleaned := parseCommentModeReport(comment, freq)
	s := spot.NewSpot(dx, spotter, freq, mode)
	s.Time = ts
	s.Comment = cleaned
	s.SourceType = spot.SourceUpstream
	s.SourceNode = origin
	s.Report = report
	s.HasReport = hasReport
	s.IsHuman = !hasReport
	if hop > 0 {
		s.TTL = uint8(hop)
	}
	s.RefreshBeaconFlag()
	return s, nil
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

func parseCommentModeReport(comment string, freq float64) (string, int, bool, string) {
	comment = strings.ReplaceAll(comment, "^", " ")
	comment = strings.ReplaceAll(comment, "\r", " ")
	comment = strings.ReplaceAll(comment, "\n", " ")
	fields := strings.Fields(comment)
	if len(fields) == 0 {
		return spot.FinalizeMode("", freq), 0, false, ""
	}

	var mode string
	var report int
	var hasReport bool
	cleaned := make([]string, 0, len(fields))

	skipNext := false
	for i, tok := range fields {
		if skipNext {
			skipNext = false
			continue
		}
		upper := strings.ToUpper(tok)
		if mode == "" {
			if m := matchModeToken(upper); m != "" {
				mode = m
				continue
			}
		}
		if !hasReport {
			if v, ok := parseInlineDB(upper); ok {
				report = v
				hasReport = true
				continue
			}
			if v, ok := parseSignedInt(tok); ok {
				if i+1 < len(fields) && strings.EqualFold(fields[i+1], "DB") {
					report = v
					hasReport = true
					skipNext = true
					continue
				}
			}
		}
		cleaned = append(cleaned, tok)
	}
	mode = spot.FinalizeMode(mode, freq)
	return mode, report, hasReport, strings.Join(cleaned, " ")
}

func matchModeToken(token string) string {
	for _, m := range modeTokens {
		if token == m {
			return m
		}
	}
	if token == "MSK" {
		return "MSK144"
	}
	return ""
}

func parseSignedInt(tok string) (int, bool) {
	if tok == "" {
		return 0, false
	}
	if tok[0] == '+' {
		tok = tok[1:]
	}
	v, err := strconv.Atoi(tok)
	if err != nil {
		return 0, false
	}
	return v, true
}

func parseInlineDB(tok string) (int, bool) {
	if len(tok) < 3 {
		return 0, false
	}
	lower := strings.ToLower(tok)
	if !strings.HasSuffix(lower, "db") {
		return 0, false
	}
	val := tok[:len(tok)-2]
	return parseSignedInt(val)
}

// guessMode defers to the shared mode allocation logic so peering matches human spot ingestion.
func guessMode(freq float64) string {
	return spot.FinalizeMode("", freq)
}
