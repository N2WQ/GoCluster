package peer

import (
	"fmt"
	"strings"
)

// Purpose: Parse PC23/PC73 frames into a WWVEvent.
// Key aspects: Maps frame fields by type and fills origin/logger fields.
// Upstream: Peer session reader.
// Downstream: Frame.payloadFields.
func parseWWV(frame *Frame) (WWVEvent, bool) {
	if frame == nil {
		return WWVEvent{}, false
	}
	fields := frame.payloadFields()
	if len(fields) < 6 {
		return WWVEvent{}, false
	}
	ev := WWVEvent{
		Kind:  frame.Type,
		Date:  fields[0],
		Hour:  fields[1],
		SFI:   fields[2],
		A:     fields[3],
		K:     fields[4],
		Extra: fields[5:],
		Hop:   frame.Hop,
	}

	switch strings.ToUpper(strings.TrimSpace(frame.Type)) {
	case "PC23":
		if len(fields) >= 6 {
			ev.Forecast = strings.TrimSpace(fields[5])
		}
		if len(fields) >= 7 {
			ev.Logger = strings.TrimSpace(fields[6])
		}
		if len(fields) >= 8 {
			ev.Origin = strings.TrimSpace(fields[7])
		}
	case "PC73":
		if len(fields) >= 6 {
			ev.ExpK = strings.TrimSpace(fields[5])
		}
		if len(fields) >= 7 {
			ev.R = strings.TrimSpace(fields[6])
		}
		if len(fields) >= 8 {
			ev.SA = strings.TrimSpace(fields[7])
		}
		if len(fields) >= 9 {
			ev.GMF = strings.TrimSpace(fields[8])
		}
		if len(fields) >= 10 {
			ev.Aurora = strings.TrimSpace(fields[9])
		}
		if len(fields) >= 11 {
			ev.Logger = strings.TrimSpace(fields[10])
		}
		if len(fields) >= 12 {
			ev.Origin = strings.TrimSpace(fields[11])
		}
	}

	if ev.Origin == "" && len(fields) > 0 {
		ev.Origin = strings.TrimSpace(fields[len(fields)-1])
	}
	return ev, true
}

// Purpose: Format a WWVEvent into a human-readable bulletin line.
// Key aspects: Distinguishes WWV (PC23) vs WCY (PC73) and drops empty payloads.
// Upstream: Telnet broadcast of WWV/WCY bulletins.
// Downstream: fmt.Sprintf.
func formatWWVLine(ev WWVEvent) (kind string, line string) {
	kind = "WWV"
	if strings.EqualFold(ev.Kind, "PC73") {
		kind = "WCY"
	}

	logger := strings.TrimSpace(ev.Logger)
	if logger == "" {
		logger = strings.TrimSpace(ev.Origin)
	}
	if logger == "" {
		return kind, ""
	}

	hour := strings.TrimSpace(ev.Hour)
	hourToken := ""
	if hour != "" {
		hourToken = fmt.Sprintf("<%s>", hour)
	}

	if kind == "WWV" {
		parts := []string{}
		if ev.SFI != "" {
			parts = append(parts, "SFI="+ev.SFI)
		}
		if ev.A != "" {
			parts = append(parts, "A="+ev.A)
		}
		if ev.K != "" {
			parts = append(parts, "K="+ev.K)
		}
		if ev.Forecast != "" {
			parts = append(parts, "Forecast="+ev.Forecast)
		}
		if len(parts) == 0 {
			return kind, ""
		}
		if hourToken != "" {
			return kind, fmt.Sprintf("%s de %s %s : %s", kind, logger, hourToken, strings.Join(parts, " "))
		}
		return kind, fmt.Sprintf("%s de %s : %s", kind, logger, strings.Join(parts, " "))
	}

	parts := []string{}
	if ev.K != "" {
		parts = append(parts, "K="+ev.K)
	}
	if ev.ExpK != "" {
		parts = append(parts, "expK="+ev.ExpK)
	}
	if ev.A != "" {
		parts = append(parts, "A="+ev.A)
	}
	if ev.R != "" {
		parts = append(parts, "R="+ev.R)
	}
	if ev.SFI != "" {
		parts = append(parts, "SFI="+ev.SFI)
	}
	if ev.SA != "" {
		parts = append(parts, "SA="+ev.SA)
	}
	if ev.GMF != "" {
		parts = append(parts, "GMF="+ev.GMF)
	}
	if ev.Aurora != "" {
		parts = append(parts, "Au="+ev.Aurora)
	}
	if len(parts) == 0 {
		return kind, ""
	}
	if hourToken != "" {
		return kind, fmt.Sprintf("%s de %s %s : %s", kind, logger, hourToken, strings.Join(parts, " "))
	}
	return kind, fmt.Sprintf("%s de %s : %s", kind, logger, strings.Join(parts, " "))
}
