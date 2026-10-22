package telnet

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func FuzzBooleanInput(f *testing.F) {
	for _, body := range []string{`MODE=CW`, `COMMENT="MiXeD ""quote"""`, "MODE=CW\x15SHOW OWN", "MODE=\x01BAD", strings.Repeat("X", 129), string([]byte{IAC})} {
		f.Add([]byte(body), false)
		f.Add([]byte(body), true)
	}
	f.Fuzz(func(t *testing.T, body []byte, ziutek bool) {
		if len(body) > 1024 {
			return
		}
		body = bytes.Clone(body)
		for i := range body {
			if body[i] == '\r' || body[i] == '\n' {
				body[i] = ' '
			}
		}
		// This oracle requires an application-data newline after the payload.
		// Escape literal IAC so it cannot consume that newline as a telnet verb.
		body = bytes.ReplaceAll(body, []byte{IAC}, []byte{IAC, IAC})
		input := append([]byte("PASS RULE 0 "), body...)
		input = append(input, []byte("\nSHOW OWN\n")...)
		client, conn := newHumanRecoveryClient(t, input, ziutek)
		line, err := client.readBooleanCommandLine(128)
		var machine *machineInputError
		if errors.As(err, &machine) {
			if line != "" || !machine.Terminal {
				t.Fatal("machine framing failure became a resumable human line")
			}
			return
		}
		var terminal *humanInputError
		if errors.As(err, &terminal) {
			if line != "" || !conn.closed.Load() {
				t.Fatal("unsafe recovery retained a session")
			}
			return
		}
		if err != nil {
			var validation *InputValidationError
			if line != "" || !errors.As(err, &validation) {
				t.Fatalf("unexpected first-line failure: %v", err)
			}
		} else {
			if len(line) > 128 {
				t.Fatal("accepted overlong line")
			}
			for _, b := range []byte(line) {
				if b < 32 || b > 126 {
					t.Fatal("accepted non-printable command")
				}
			}
		}
		if next, err := client.readBooleanCommandLine(128); err != nil || next != "SHOW OWN" {
			t.Fatalf("physical-line tail changed the next command: %q, %v", next, err)
		}
	})
}
