package telnet

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestBooleanInputPrefixScope(t *testing.T) {
	for _, test := range []struct {
		line string
		ok   bool
	}{
		{"PASS RULE 0 ", true}, {"reject rule 9 ", true},
		{"  PASS  RULE  12   ", true}, {"PASS RULE bad MODE=CW", true},
		{"PASS RULE -1 ", true}, {"PASS RULE 0", false},
		{"PASS RULE ", false}, {"PASS RULES 0 ", false},
		{"PASS BAND ", false}, {"REJECT COMMENT ", false},
		{"RESET FILTER RULE ", false}, {"GET YAML FILTER ", false},
	} {
		start, ok := booleanRuleExpressionStart([]byte(test.line))
		if ok != test.ok || ok && (start < 0 || start > len(test.line)) {
			t.Fatalf("%q offset=%d ok=%v, want %v", test.line, start, ok, test.ok)
		}
	}
	line := []byte(`pass rule 0 COMMENT="POTA * ?"`)
	start, ok := booleanRuleExpressionStart(line)
	if !ok || string(line[start:]) != `COMMENT="POTA * ?"` {
		t.Fatal("prefix discarded part of the literal expression")
	}
	if allocations := testing.AllocsPerRun(100, func() { booleanRuleExpressionStart(line) }); allocations != 0 {
		t.Fatalf("per-byte prefix scan allocated %g objects", allocations)
	}
}

func TestBooleanInputLiteralCaseAndEditing(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		for _, test := range []struct {
			input, want string
		}{
			{`pass rule 1 (band=[40m,30m] and mode=cw) or comment="MiXeD ""quote""; <>() [] * ?"` + "\r\n",
				`PASS RULE 1 (band=[40m,30m] and mode=cw) or comment="MiXeD ""quote""; <>() [] * ?"`},
			{"  reject  rule  12   mode=cw\n", "  REJECT  RULE  12   mode=cw"},
			{"PASS RULE -1 mode=cw\n", "PASS RULE -1 mode=cw"},
			{"PASS RULE 1 COMMENT=\"PotaX\b\"\n", `PASS RULE 1 COMMENT="Pota"`},
			{"PASS RULE 1 COMMENT=\"Pota\"\x15show own\n", "SHOW OWN"},
			{"pass comment Mixed: literal!\n", "PASS COMMENT MIXED: LITERAL!"},
			{"GET YAML FILTER SCHEMA 5 ID MiXeD-1\n", "GET YAML FILTER SCHEMA 5 ID MiXeD-1"},
			{"pass rule 1 COMMENT=\"Po" + string([]byte{IAC, SB, 31, '\n', IAC, SE}) + "Ta\"\n",
				`PASS RULE 1 COMMENT="PoTa"`},
			{"pass rule 1 MODE=" + string([]byte{IAC, WILL, 1}) + "cw\n", "PASS RULE 1 MODE=cw"},
		} {
			input := []byte(test.input + "SHOW OWN\n")
			client, _ := newHumanRecoveryClient(t, input, ziutek)
			if got, err := client.readBooleanCommandLine(256); err != nil || got != test.want {
				t.Fatalf("ziutek=%v input=%q got=%q err=%v want=%q", ziutek, test.input, got, err, test.want)
			}
			if next, err := client.readBooleanCommandLine(256); err != nil || next != "SHOW OWN" {
				t.Fatalf("following line changed: %q, %v", next, err)
			}
			if _, err := client.readBooleanCommandLine(256); !errors.Is(err, io.EOF) {
				t.Fatalf("following command was not consumed exactly once: %v", err)
			}
		}
	}
}

func TestBooleanInputEchoPreservesLiteralCase(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		client, _ := newHumanRecoveryClient(t, []byte("pass rule 1 comment=\"MiXeD\"\r\n"), ziutek)
		var echo bytes.Buffer
		client.writer, client.echoInput = bufio.NewWriter(&echo), true
		got, err := client.readBooleanCommandLine(128)
		want := `PASS RULE 1 comment="MiXeD"`
		if err != nil || got != want || echo.String() != want+"\r\n" {
			t.Fatalf("ziutek=%v line=%q echo=%q err=%v", ziutek, got, echo.String(), err)
		}
	}
}

func TestBooleanInputRejectedPhysicalLineRecovery(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		for _, tail := range []string{
			"PASS BAND=40M AND MODE=CW\n",
			"PASS RULE 1 \b=MODECW\n",
			"PASS RULE 1 \x17=MODECW\n",
			"PASS RULE 1 COMMENT=\"POTA\"\x15SHOW OWN=bad\n",
			"PASS RULE 1 COMMENT=\"x\x01y\" REJECT RULE 0 MODE=CW\n",
			"PASS RULE 1 COMMENT=\"café\"\n",
			"PASS RULE 1 " + strings.Repeat("A", 129) + "\r\n",
		} {
			client, conn := newHumanRecoveryClient(t, []byte(tail+"PASS RULE 0 MODE=CW\n"), ziutek)
			line, err := client.readBooleanCommandLine(128)
			var validation *InputValidationError
			var terminal *humanInputError
			if line != "" || !errors.As(err, &validation) || errors.As(err, &terminal) || conn.closed.Load() {
				t.Fatalf("ziutek=%v tail=%q line=%q err=%v", ziutek, tail, line, err)
			}
			if next, err := client.readBooleanCommandLine(128); err != nil || next != "PASS RULE 0 MODE=CW" {
				t.Fatalf("tail dispatched or next line lost: %q, %v", next, err)
			}
			if _, err := client.readBooleanCommandLine(128); !errors.Is(err, io.EOF) {
				t.Fatalf("following line consumed more than once: %v", err)
			}
		}
	}
}

func TestBooleanInputGuidanceSurvivesFormatting(t *testing.T) {
	client, _ := newHumanRecoveryClient(t, []byte("PASS BAND=40M\n"), false)
	_, err := client.readBooleanCommandLine(128)
	var validation *InputValidationError
	if !errors.As(err, &validation) {
		t.Fatal("expected recoverable input validation error", err)
	}
	server := &Server{inputInvalidMsg: "Invalid command.\n"}
	message := server.formatInputValidationMessage(validation)
	if !strings.Contains(message, "PASS RULE <0-9>") || !strings.HasSuffix(message, "No changes applied.\n") {
		t.Fatalf("runtime formatter discarded rule guidance: %q", message)
	}
}

func TestBooleanInputActivationPreservesMachineHeaderBoundaries(t *testing.T) {
	active, _ := newHumanRecoveryClient(t, []byte("PASS RULE 0 MODE=CW\n"), false)
	if line, err := active.readCommandLine(128); err != nil || line != "PASS RULE 0 MODE=CW" {
		t.Fatalf("active command reader did not admit Boolean syntax: %q, %v", line, err)
	}
	raw, _ := newHumanRecoveryClient(t, []byte("PASS RULE 0 MODE=CW\n"), false)
	if _, err := raw.ReadLine(128, "command", true, true, true, true); err == nil {
		t.Fatal("Boolean activation broadened the low-level reader")
	}
	for _, ziutek := range []bool{false, true} {
		for _, header := range []string{"GET YAML FILTER ID bad_id\n", "PUT_ YAML CONFIG\n", "GET YAML FILTER (=)\n"} {
			client, _ := newHumanRecoveryClient(t, []byte(header), ziutek)
			line, err := client.readCommandLine(128)
			var machine *machineInputError
			if line != "" || !errors.As(err, &machine) || !machine.Terminal {
				t.Fatalf("machine framing changed: line=%q error=%v", line, err)
			}
		}
	}
}

func TestBooleanInputPartialFailureCannotBecomeIdle(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		for _, input := range []string{`PASS RULE 1 COMMENT="x`, "PASS RULE 1 MODE=CW\x15"} {
			client, conn := newHumanRecoveryClient(t, nil, ziutek)
			conn.input = &humanTimeoutReader{data: []byte(input)}
			line, err := client.readBooleanCommandLine(128)
			var terminal *humanInputError
			if line != "" || !errors.As(err, &terminal) || !conn.closed.Load() {
				t.Fatalf("edited/partial read became retryable: ziutek=%v line=%q err=%v", ziutek, line, err)
			}
		}
		client, conn := newHumanRecoveryClient(t, nil, ziutek)
		conn.input = &humanTimeoutReader{}
		_, err := client.readBooleanCommandLine(128)
		var terminal *humanInputError
		if !isTimeoutErr(err) || errors.As(err, &terminal) || conn.closed.Load() {
			t.Fatalf("empty idle timeout became terminal: %v", err)
		}
	}
}
