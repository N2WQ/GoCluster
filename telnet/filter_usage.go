// File role: Gives bounded, action-aware discovery for existing filter owners.
// Detailed syntax and loaded vocabularies remain in HELP. These fixed replies
// add no command aliases or preference effects and fit within nine output rows.
package telnet

const (
	filterTypesUsage = "Types: BAND, MODE, MINSNR, SOURCE, EVENT, COMMENT, DXCALL, DECALL,\n" +
		"       CONFIDENCE, PATH, DXGRID2, DEGRID2, DXCONT, DECONT, DXZONE, DEZONE,\n" +
		"       DXDXCC, DEDXCC, DXSTATE, DESTATE, DXBM, NEARBY,\n" +
		"       BEACON, WWV, WCY, ANNOUNCE, SELF, TOXIC\n"
	passFilterExtras   = "PASS also supports NOFILTER; PC93 aliases ANNOUNCE.\n"
	rejectFilterExtras = "PC93 aliases ANNOUNCE.\n"
	unknownPassTypeMsg = "Unknown filter type.\n" + filterTypesUsage +
		"COMMENT uses PASS/REJECT COMMENT in either dialect.\n" +
		passFilterExtras + "Type HELP PASS for usage.\n"
	unknownRejectTypeMsg = "Unknown filter type.\n" + filterTypesUsage +
		"COMMENT uses PASS/REJECT COMMENT in either dialect.\n" +
		rejectFilterExtras + "Type HELP REJECT for usage.\n"
)

func filterUsageMsg(verb string) string {
	clear, ending := "ALL", "PASS supports NOFILTER. Type HELP PASS for complete syntax and examples.\n"
	if verb == "REJECT" {
		clear, ending = "NONE", "Type HELP REJECT for complete syntax and examples.\n"
	}
	return "Usage: " + verb + " <type> ...\n" + filterTypesUsage +
		"COMMENT uses PASS/REJECT COMMENT <literal phrase>.\n" +
		"MINSNR takes modes and <integer dB|" + clear + ">.\n" +
		"NEARBY takes ON|OFF with either verb; PC93 aliases ANNOUNCE.\n" + ending
}
