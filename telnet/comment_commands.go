// File role: Literal comment rules share the existing configuration transaction.
// Parse only command words; the remainder is one phrase, never a split list.
package telnet

import (
	"fmt"
	"slices"
	"strings"

	"dxcluster/filter"
)

const commentCommandUsage = "Usage: PASS|REJECT COMMENT <phrase>\nREMOVE PASS|REJECT COMMENT <phrase>\nRESET FILTER COMMENT [PASS|REJECT]\nPhrases contain 1-64 printable ASCII bytes; maximum 32 per list.\n"

// takeCommentWord consumes a command word without rebuilding its literal tail.
func takeCommentWord(line string) (word, rest string) {
	line = strings.TrimLeft(line, " ")
	end := strings.IndexByte(line, ' ')
	if end < 0 {
		return strings.ToUpper(line), ""
	}
	return strings.ToUpper(line[:end]), line[end:]
}

type commentFilterCommand struct {
	list   string
	remove bool
	reset  bool
	phrase string
}

func parseCommentFilterCommand(line string) (commentFilterCommand, bool, bool) {
	var cmd commentFilterCommand
	verb, rest := takeCommentWord(line)
	switch verb {
	case "PASS", "REJECT":
		cmd.list = verb
	case "REMOVE":
		cmd.remove = true
		cmd.list, rest = takeCommentWord(rest)
	case "RESET":
		word, tail := takeCommentWord(rest)
		if word != "FILTER" {
			return cmd, false, false
		}
		rest, cmd.reset = tail, true
	default:
		return cmd, false, false
	}
	domain, tail := takeCommentWord(rest)
	if domain != "COMMENT" {
		return cmd, false, false
	}
	if cmd.reset {
		cmd.list = strings.ToUpper(strings.Trim(tail, " "))
		return cmd, true, cmd.list == "" || cmd.list == "PASS" || cmd.list == "REJECT"
	}
	cmd.phrase = strings.Trim(tail, " ")
	return cmd, true, (cmd.list == "PASS" || cmd.list == "REJECT") && filter.ValidCommentPhrase(cmd.phrase)
}

func handleCommentFilterCommand(c *Client, line string) (string, bool, bool) {
	cmd, handled, valid := parseCommentFilterCommand(line)
	if !handled {
		return "", false, false
	}
	if !valid {
		return commentCommandUsage, true, false
	}
	var err error
	changed := false
	c.updateFilter(func(f *filter.Filter) { changed, err = applyCommentFilterCommand(f, cmd) })
	if err != nil {
		return "Invalid COMMENT filter: " + err.Error() + "\n" + commentCommandUsage, true, false
	}
	if cmd.reset {
		return "Comment rules cleared" + optionalCommentList(cmd.list) + "\n", true, changed
	}
	if cmd.remove {
		return "Comment phrase removed from " + cmd.list + "\n", true, changed
	}
	return "Comment phrase added to " + cmd.list + "\n", true, changed
}

func optionalCommentList(list string) string {
	if list == "" {
		return ""
	}
	return " (" + list + ")"
}

// Apply validates destination capacity before detaching or moving any entry.
// Replacing slices relinquishes removed strings; no historical cache survives.
func applyCommentFilterCommand(f *filter.Filter, cmd commentFilterCommand) (bool, error) {
	if cmd.reset {
		changed := false
		if cmd.list != "REJECT" {
			changed, f.Comments = len(f.Comments) != 0, nil
		}
		if cmd.list != "PASS" {
			changed = changed || len(f.BlockComments) != 0
			f.BlockComments = nil
		}
		return changed, nil
	}
	if err := filter.ConfigurationFromFilter(f, filter.SettingsConfiguration{}).ValidateCommentRules(); err != nil {
		return false, err
	}
	target, opposite := &f.Comments, &f.BlockComments
	if cmd.list == "REJECT" {
		target, opposite = opposite, target
	}
	has := slices.ContainsFunc(*target, func(value string) bool { return strings.EqualFold(value, cmd.phrase) })
	if cmd.remove {
		if !has {
			return false, nil
		}
		*target = withoutCommentPhrase(*target, cmd.phrase)
		return true, nil
	}
	if !has && len(*target) >= filter.MaxCommentPhrases {
		return false, fmt.Errorf("maximum %d phrases per list", filter.MaxCommentPhrases)
	}
	moved := slices.ContainsFunc(*opposite, func(value string) bool { return strings.EqualFold(value, cmd.phrase) })
	if !has {
		// The command ceiling may exceed the phrase ceiling. Retain only the
		// admitted phrase bytes, rather than its complete input backing string.
		*target = append(slices.Clone(*target), strings.Clone(cmd.phrase))
	}
	if moved {
		*opposite = withoutCommentPhrase(*opposite, cmd.phrase)
	}
	return !has || moved, nil
}

func withoutCommentPhrase(values []string, phrase string) []string {
	next := make([]string, 0, len(values))
	for _, value := range values {
		if !strings.EqualFold(value, phrase) {
			next = append(next, value)
		}
	}
	return next
}
