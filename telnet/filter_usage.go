// File role: Bounded discovery for malformed filter command headers.
package telnet

func filterUsageMsg(verb string) string {
	return "Usage: " + verb + " RULE <0..9> <expression>\nType HELP RULE for fields and examples.\n"
}
