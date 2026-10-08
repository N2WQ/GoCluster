// File role: Frames bounded machine commands and uploads while retaining
// terminal error intent when an incomplete upload cannot be safely resumed.
package telnet

import (
	"fmt"
	"strings"
)

// machineCommand is a bounded header, separate from the case-sensitive YAML body.
// Verb and Resource are normalized; RequestID remains exactly as supplied.
type machineCommand struct {
	Verb          string
	Resource      string
	RequestID     string
	SchemaVersion int
}

// machineInputError retains recognized machine intent when ingress fails before
// a line boundary. Such a failure is terminal: unread bytes may be upload data
// or the remainder of a header and must never become ordinary commands.
type machineInputError struct {
	Verb     string
	Err      error
	Terminal bool
}

func (e *machineInputError) Error() string { return e.Err.Error() }
func (e *machineInputError) Unwrap() error { return e.Err }

func (c *Client) readCommandLine(maxLen int) (string, error) {
	return c.readInputLine(maxLen, "command", true, true, true, true, true)
}

// readInputLine preserves the established human/login editing and echo rules.
// Only recognized machine headers retain their original case, so request IDs
// survive dispatch without broadening the ordinary command character safe list.
func (c *Client) readInputLine(maxLen int, context string, allowComma, allowWildcard, allowConfidence, allowDot, machineAware bool) (string, error) {
	if maxLen <= 0 {
		maxLen = defaultCommandLineLimit
	}
	if context == "" {
		context = "command"
	}
	var line []byte
	fail := func(err error) (string, error) {
		if machineAware {
			if verb := machineIntent(line); verb != "" {
				return "", &machineInputError{Verb: verb, Err: err, Terminal: true}
			}
		}
		return "", err
	}
	for {
		b, err := c.reader.ReadByte()
		if err != nil {
			return fail(err)
		}
		if c.skipNextEOL {
			c.skipNextEOL = false
			if b == '\n' || b == 0x00 {
				continue
			}
		}
		if b == IAC {
			if err := c.consumeIACSequence(); err != nil {
				return fail(err)
			}
			continue
		}
		if b == '\n' || b == '\r' {
			if c.echoInput {
				if err := c.writeRaw([]byte("\r\n")); err != nil {
					return fail(err)
				}
			}
			c.skipNextEOL = b == '\r'
			break
		}
		if edited, err := c.editInputLine(&line, b); edited {
			if err != nil {
				return fail(err)
			}
			continue
		}
		allowed := ""
		if len(line) >= maxLen || !isAllowedInputByte(b, allowComma, allowWildcard, allowConfidence, allowDot) {
			allowed = allowedCharacterList(allowComma, allowWildcard, allowConfidence, allowDot)
		}
		if len(line) >= maxLen {
			c.logRejectedInput(context, fmt.Sprintf("exceeded %d-byte limit", maxLen))
			return fail(newInputTooLongError(context, maxLen, allowed))
		}
		if allowed != "" {
			c.logRejectedInput(context, fmt.Sprintf("forbidden byte 0x%02X", b))
			return fail(newInputInvalidCharError(context, maxLen, allowed, b))
		}
		normalized := uppercaseInputByte(b)
		if c.echoInput {
			echo := normalized
			if machineAware && machineIntent(line) != "" && machineArgumentsStarted(line) {
				echo = b
			}
			if err := c.writeRaw([]byte{echo}); err != nil {
				return fail(err)
			}
		}
		if machineAware {
			line = append(line, b)
		} else {
			line = append(line, normalized)
		}
	}
	if machineAware && machineIntent(line) == "" {
		for i := range line {
			line[i] = uppercaseInputByte(line[i])
		}
	}
	return string(line), nil
}

func (c *Client) editInputLine(line *[]byte, b byte) (bool, error) {
	erased := 0
	switch b {
	case 0x08, 0x7f:
		if len(*line) > 0 {
			erased = 1
		}
	case 0x15:
		erased = len(*line)
	case 0x17:
		erased = wordEraseCount(*line)
	default:
		return false, nil
	}
	*line = (*line)[:len(*line)-erased]
	return true, c.echoErase(erased)
}

func uppercaseInputByte(b byte) byte {
	if b >= 'a' && b <= 'z' {
		return b - ('a' - 'A')
	}
	return b
}

func machineArgumentsStarted(line []byte) bool {
	wordStarted := false
	for _, b := range line {
		if b == ' ' {
			if wordStarted {
				return true
			}
		} else {
			wordStarted = true
		}
	}
	return false
}

// machineIntent recognizes the reserved leading verb before requiring YAML or
// a resource. It is also used on early ingress errors, including PUT_ and EOF
// immediately after PUT, when waiting for a token separator would lose intent.
func machineIntent(line []byte) string {
	start := 0
	for start < len(line) && line[start] == ' ' {
		start++
	}
	end := start
	for end < len(line) && line[end] != ' ' {
		end++
	}
	switch {
	case asciiWordEqual(line[start:end], "GET"):
		return "GET"
	case asciiWordEqual(line[start:end], "PUT"):
		return "PUT"
	case asciiWordEqual(line[start:end], "PATCH"):
		return "PATCH"
	case asciiWordEqual(line[start:end], "VALIDATE"):
		return "VALIDATE"
	default:
		return ""
	}
}

func asciiWordEqual(input []byte, word string) bool {
	if len(input) != len(word) {
		return false
	}
	for i := range input {
		if uppercaseInputByte(input[i]) != word[i] {
			return false
		}
	}
	return true
}

func parseMachineHeader(line string) (machineCommand, bool, error) {
	cmd := machineCommand{Verb: machineIntent([]byte(line))}
	if cmd.Verb == "" {
		return cmd, false, nil
	}
	args := strings.Fields(line)
	if len(args) < 3 || !strings.EqualFold(args[1], "YAML") {
		return cmd, true, fmt.Errorf("expected %s YAML <resource>", cmd.Verb)
	}
	cmd.Resource = strings.ToUpper(args[2])
	if !machineResourceAllowed(cmd.Verb, cmd.Resource) {
		return cmd, true, fmt.Errorf("unsupported resource for %s YAML", cmd.Verb)
	}
	if cmd.Verb != "GET" {
		if len(args) != 3 {
			return cmd, true, fmt.Errorf("%s YAML accepts no additional header arguments", cmd.Verb)
		}
		return cmd, true, nil
	}
	remaining := args[3:]
	if len(remaining) >= 2 && strings.EqualFold(remaining[0], "SCHEMA") {
		if remaining[1] != "2" && remaining[1] != "3" {
			return cmd, true, fmt.Errorf("GET YAML SCHEMA requires version 2 or 3")
		}
		if remaining[1] == "3" {
			cmd.SchemaVersion = 3
		} else {
			cmd.SchemaVersion = 2
		}
		remaining = remaining[2:]
	}
	if len(remaining) != 0 {
		if len(remaining) != 2 || !strings.EqualFold(remaining[0], "ID") || !validMachineRequestID(remaining[1]) {
			return cmd, true, fmt.Errorf("GET YAML permits [SCHEMA 2|3] [ID <1-32 ASCII letters, digits or hyphens>]")
		}
		cmd.RequestID = remaining[1]
	}
	return cmd, true, nil
}

func machineResourceAllowed(verb, resource string) bool {
	if verb == "VALIDATE" {
		return resource == "CONFIG"
	}
	if resource == "FILTER" || resource == "SETTINGS" || resource == "CONFIG" {
		return true
	}
	return verb == "GET" && resource == "CAPABILITIES"
}

func validMachineRequestID(id string) bool {
	if len(id) < 1 || len(id) > 32 {
		return false
	}
	for i := range id {
		b := id[i]
		alphanumeric := (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
		if !alphanumeric && b != '-' {
			return false
		}
	}
	return true
}
