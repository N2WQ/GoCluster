// File role: Routes usage questions for existing command heads and aliases.
// The finite HELP catalog is request-owned metadata, never executable dispatch.
// Whole-token, longest-head matching preserves narrow topics and rejects prefix
// collisions. Historical CC mode-shortcut guidance follows the loaded taxonomy.
package commands

import (
	"strings"

	"dxcluster/spot"
	"dxcluster/strutil"
)

func installHelpRoutingAliases(catalog *helpCatalog, dialect string) {
	for alias, topic := range map[string]string{
		"H": "HELP", "SH": "SHOW", "SH BUILD": "SHOW BUILD", "SH OWN": "SHOW OWN",
		"SH DXCC": "SHOW DXCC", "SH MYDX": "SHOW MYDX", "REJECT NEARBY": "NEARBY", "PASS NEARBY": "NEARBY",
		"SHOW/FILTER COMMENT": "SHOW FILTER COMMENT", "SH/FILTER COMMENT": "SHOW FILTER COMMENT",
	} {
		catalog.aliases[alias] = topic
	}
	filterTopic := "SHOW FILTER"
	if dialect == "cc" {
		filterTopic = "SHOW/FILTER"
		catalog.aliases["SHOW FILTER"] = filterTopic
		catalog.aliases["SHOW DX"] = "SHOW/DX"
		catalog.aliases["SH DX"] = "SHOW/DX"
	}
	catalog.aliases["SHOW/FILTER"] = filterTopic
	catalog.aliases["SH/FILTER"] = filterTopic
}

func helpHeadMatches(topic, head string) bool {
	return topic == head || (len(topic) > len(head) && topic[len(head)] == ' ' && strings.HasPrefix(topic, head))
}

func (catalog helpCatalog) normalizeTopic(dialect, topic string) string {
	upper := strings.Join(strings.Fields(strutil.NormalizeUpper(topic)), " ")
	matched := ""
	consider := func(head string) {
		// SHOW's subcommands have their own verified heads. A broad parent
		// match would conceal unsupported names such as SH FILTER or DXCCX.
		if (head == "SHOW" || head == "SH") && upper != head {
			return
		}
		if len(head) > len(matched) && helpHeadMatches(upper, head) {
			matched = head
		}
	}
	for head := range catalog.entries {
		consider(head)
	}
	for head := range catalog.aliases {
		consider(head)
	}
	if matched != "" {
		return matched
	}
	if dialect == "cc" {
		// Fixed controls win before historical mode-shortcut guidance.
		// HELP discards argument tails; execution remains owned by telnet.
		command, _, _ := strings.Cut(upper, " ")
		if strings.HasPrefix(command, "SET/NO") && spot.IsCCShortcutMode(strings.TrimPrefix(command, "SET/NO")) {
			return "SET/NO<MODE>"
		}
		if strings.HasPrefix(command, "SET/") && !strings.Contains(command, "/NO") && spot.IsCCShortcutMode(strings.TrimPrefix(command, "SET/")) {
			return "SET/<MODE>"
		}
	}
	return upper
}
