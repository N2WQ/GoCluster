package peer

import (
	"strings"
)

func parseWWV(frame *Frame) (WWVEvent, bool) {
	if frame == nil {
		return WWVEvent{}, false
	}
	fields := frame.payloadFields()
	if len(fields) < 6 {
		return WWVEvent{}, false
	}
	ev := WWVEvent{
		Kind:   frame.Type,
		Date:   fields[0],
		Hour:   fields[1],
		SFI:    fields[2],
		A:      fields[3],
		K:      fields[4],
		Extra:  fields[5:],
		Origin: "",
		Hop:    frame.Hop,
	}
	if len(fields) > 6 {
		ev.Origin = strings.TrimSpace(fields[len(fields)-1])
	}
	return ev, true
}
