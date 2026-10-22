// File role: Command-word parsing that preserves the literal expression tail.
package telnet

import (
	"strings"
)

// takeCommentWord consumes a command word without rebuilding its literal tail.
func takeCommentWord(line string) (word, rest string) {
	line = strings.TrimLeft(line, " ")
	end := strings.IndexByte(line, ' ')
	if end < 0 {
		return strings.ToUpper(line), ""
	}
	return strings.ToUpper(line[:end]), line[end:]
}
