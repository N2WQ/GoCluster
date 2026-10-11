package telnet

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// The literal alphabet is independent of the production ingress predicate and
// HELP renderer. Both actual codecs must agree with the operator's contract.
func TestDXInputCommentAlphabet(t *testing.T) {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 /#@-.,*?_"
	for _, ziutek := range []bool{false, true} {
		for _, prefix := range []string{"DX 14030 W6ABC CW ", "DX W6ABC 14030 CW "} {
			for b := byte(32); b <= 126; b++ {
				t.Run(fmt.Sprintf("ziutek=%v/%s/%02x", ziutek, prefix, b), func(t *testing.T) {
					command := prefix + "A" + string(b) + "B"
					c, conn := newHumanRecoveryClient(t, []byte(command+"\r\nSHOW OWN\r\n"), ziutek)
					got, err := c.readCommandLine(128)
					if strings.ContainsRune(alphabet, rune(b)) {
						if err != nil || got != strings.ToUpper(command) {
							t.Fatalf("documented byte %02x: command=%q err=%v", b, got, err)
						}
					} else {
						assertDXInputRefused(t, got, err, inputErrorInvalidChar)
					}
					if conn.closed.Load() {
						t.Fatal("safe physical boundary unexpectedly closed the session")
					}
					if next, err := c.readCommandLine(128); err != nil || next != "SHOW OWN" {
						t.Fatalf("following command=%q err=%v", next, err)
					}
				})
			}
			for _, suffix := range []string{"caf\u00e9", "\u03a9", "\u00a0"} {
				c, _ := newHumanRecoveryClient(t, []byte(prefix+suffix+"\r\nSHOW OWN\r\n"), ziutek)
				got, err := c.readCommandLine(128)
				assertDXInputRefused(t, got, err, inputErrorInvalidChar)
				if next, err := c.readCommandLine(128); err != nil || next != "SHOW OWN" {
					t.Fatalf("non-ASCII recovery: next=%q err=%v", next, err)
				}
			}
		}
	}
}

func assertDXInputRefused(t *testing.T, command string, err error, kind inputErrorKind) {
	t.Helper()
	var validation *InputValidationError
	var terminal *humanInputError
	if command != "" || !errors.As(err, &validation) || validation.kind != kind || errors.As(err, &terminal) {
		t.Fatalf("refused command=%q err=%v; want recoverable %s", command, err, kind)
	}
}

func TestDXInputWholeEditedCommandLimit(t *testing.T) {
	if defaultCommandLineLimit != 128 {
		t.Fatalf("HELP's default command limit is stale: %d", defaultCommandLineLimit)
	}
	for _, ziutek := range []bool{false, true} {
		for _, limit := range []int{0, 32, 128} {
			bound := limit
			if bound == 0 {
				bound = 128
			}
			for _, extra := range []int{0, 1} {
				t.Run(fmt.Sprintf("ziutek=%v/limit=%d/extra=%d", ziutek, limit, extra), func(t *testing.T) {
					const prefix = "DX 14030 W6ABC CW "
					command := prefix + strings.Repeat("X", bound+extra-len(prefix))
					// Edit below the bound: wire bytes exceed the final edited
					// length without attempting an already oversized prefix.
					input := prefix + "Z\b" + command[len(prefix):] + "\r\nSHOW OWN\r\n"
					c, _ := newHumanRecoveryClient(t, []byte(input), ziutek)
					got, err := c.readCommandLine(limit)
					if extra == 0 {
						if err != nil || got != command || len(got) != bound {
							t.Fatalf("exact whole-command boundary: got=%q err=%v", got, err)
						}
					} else {
						assertDXInputRefused(t, got, err, inputErrorTooLong)
					}
					if next, err := c.readCommandLine(limit); err != nil || next != "SHOW OWN" {
						t.Fatalf("length recovery: next=%q err=%v", next, err)
					}
				})
			}
		}
	}
}
