package commands

import (
	"fmt"
	"strings"
)

// ParseBulletinCommand recognizes the two bulletin readbacks without claiming
// other SHOW commands. A recognized head with extra arguments returns usage.
func ParseBulletinCommand(input string) (kind string, full bool, handled bool, errText string) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return "", false, false, ""
	}
	var head string
	var args []string
	slashTail := false
	if strings.EqualFold(parts[0], "SHOW") || strings.EqualFold(parts[0], "SH") {
		if len(parts) < 2 {
			return "", false, false, ""
		}
		head, args = parts[1], parts[2:]
	} else {
		verb, rest, found := strings.Cut(parts[0], "/")
		if !found || (!strings.EqualFold(verb, "SHOW") && !strings.EqualFold(verb, "SH")) {
			return "", false, false, ""
		}
		head, _, slashTail = strings.Cut(rest, "/")
		args = parts[1:]
	}
	switch {
	case strings.EqualFold(head, "WWV"):
		kind = "WWV"
	case strings.EqualFold(head, "WCY"):
		kind = "WCY"
	default:
		return "", false, false, ""
	}
	if slashTail {
		return kind, false, true, fmt.Sprintf("Usage: SHOW %s [FULL]\n", kind)
	}
	if len(args) == 0 {
		return kind, false, true, ""
	}
	if len(args) == 1 && strings.EqualFold(args[0], "FULL") {
		return kind, true, true, ""
	}
	return kind, false, true, fmt.Sprintf("Usage: SHOW %s [FULL]\n", kind)
}
