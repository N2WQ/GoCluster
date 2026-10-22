// File role: Frames bounded machine commands and uploads while retaining
// terminal error intent when an incomplete upload cannot be safely resumed.
package telnet

import (
	"errors"
	"fmt"
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
	return c.readBooleanCommandLine(maxLen)
}

// readInputLine preserves the established human/login editing and echo rules.
// Only recognized machine headers retain their original case. Human commands
// admit taxonomy underscores after login; login and recognized machine headers
// retain their existing character restrictions.
func (c *Client) readInputLine(maxLen int, context string, allowComma, allowWildcard, allowConfidence, allowDot, machineAware bool) (string, error) {
	return c.readInputLineWithRuleSyntax(maxLen, context, allowComma, allowWildcard, allowConfidence, allowDot, machineAware, false)
}

func (c *Client) readInputLineWithRuleSyntax(maxLen int, context string, allowComma, allowWildcard, allowConfidence, allowDot, machineAware, ruleSyntax bool) (string, error) {
	if maxLen <= 0 {
		maxLen = defaultCommandLineLimit
	}
	if context == "" {
		context = "command"
	}
	var line []byte
	progress := false
	var wireStart uint64
	if c.inputWire != nil {
		wireStart = c.inputWire.total
	}
	if c.inputExposure != nil {
		c.inputExposure.enabled = true
		defer func() { c.inputExposure.enabled = false }()
	}
	fail := func(err error) (string, error) {
		if machineAware {
			if verb := machineIntent(line); verb != "" {
				return "", &machineInputError{Verb: verb, Err: err, Terminal: true}
			}
		}
		var validationErr *InputValidationError
		if errors.As(err, &validationErr) {
			return c.rejectHumanInput(err)
		}
		wireProgress := c.inputWire != nil && (c.inputWire.total != wireStart || c.inputWire.state != 0)
		if progress || wireProgress {
			return c.terminalHumanInput(err)
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
				if c.inputWire != nil {
					wireStart = c.inputWire.total // Completes the previous CR, not a partial new line.
				}
				continue
			}
		}
		if b == IAC {
			progress = true
			if c.decodedInput() {
				continue // Already decoded literal IAC; do not consume following data.
			}
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
		progress = true // Editing can erase the bytes, but not physical progress.
		if edited, err := c.editInputLine(&line, b); edited {
			if err != nil {
				return fail(err)
			}
			continue
		}
		allowed := ""
		commentByte := machineAware && b >= 32 && b <= 126 && commentArgumentsStarted(line)
		ruleArguments := false
		if machineAware && ruleSyntax {
			_, ruleArguments = booleanRuleExpressionStart(line)
		}
		ruleByte := ruleArguments && b >= 32 && b <= 126
		humanUnderscore := machineAware && b == '_' && machineIntent(line) == ""
		if len(line) >= maxLen || (!commentByte && !ruleByte && !humanUnderscore && !isAllowedInputByte(b, allowComma, allowWildcard, allowConfidence, allowDot)) {
			allowed = allowedCharacterList(allowComma, allowWildcard, allowConfidence, allowDot)
			if machineAware && machineIntent(line) == "" {
				allowed += ", '_'"
			}
			if ruleArguments {
				allowed = "printable ASCII within a RULE expression"
			}
		}
		if len(line) >= maxLen {
			c.logRejectedInput(context, fmt.Sprintf("exceeded %d-byte limit", maxLen))
			inputErr := newInputTooLongError(context, maxLen, allowed)
			if ruleArguments {
				inputErr.guidance = "No changes applied."
			}
			return fail(inputErr)
		}
		if allowed != "" {
			c.logRejectedInput(context, fmt.Sprintf("forbidden byte 0x%02X", b))
			inputErr := newInputInvalidCharError(context, maxLen, allowed, b)
			if ruleSyntax && booleanInputNeedsHint(line, b) {
				inputErr.guidance = "Use PASS RULE <0-9> <expression> or REJECT RULE <0-9> <expression>. No changes applied."
			} else if ruleArguments {
				inputErr.guidance = "No changes applied."
			}
			return fail(inputErr)
		}
		normalized := uppercaseInputByte(b)
		if c.echoInput {
			echo := normalized
			if ruleArguments || machineAware && machineIntent(line) != "" && machineArgumentsStarted(line) {
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
		end := len(line)
		if ruleSyntax {
			if start, ok := booleanRuleExpressionStart(line); ok {
				end = start
			}
		}
		for i := 0; i < end; i++ {
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
