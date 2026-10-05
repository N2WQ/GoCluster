package telnet

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
)

func TestMachineCommandLinePreservesIdentifierCase(t *testing.T) {
	var echoed bytes.Buffer
	c := &Client{reader: bufio.NewReader(strings.NewReader("get yaml settings id noise-Ab1\r\n")), writer: bufio.NewWriter(&echoed), echoInput: true}
	line, err := c.readCommandLine(128)
	if err != nil {
		t.Fatal(err)
	}
	if line != "get yaml settings id noise-Ab1" {
		t.Fatalf("header case changed: %q", line)
	}
	cmd, handled, err := parseMachineHeader(line)
	if err != nil || !handled || cmd != (machineCommand{Verb: "GET", Resource: "SETTINGS", RequestID: "noise-Ab1"}) {
		t.Fatalf("unexpected parsed header: %+v, handled=%v, err=%v", cmd, handled, err)
	}
	if !strings.HasSuffix(echoed.String(), "noise-Ab1\r\n") {
		t.Fatalf("identifier echo changed: %q", echoed.String())
	}
}

func TestMachineAwareReaderPreservesHumanEditing(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  string
		echo  string
	}{
		{"shox\bw filter\n", "SHOW FILTER", "SHOX\b \bW FILTER\r\n"},
		{"show dx\x17filter\n", "SHOW FILTER", "SHOW DX\b \b\b \bFILTER\r\n"},
		{"put\x15show settings\n", "SHOW SETTINGS", "PUT\b \b\b \b\b \bSHOW SETTINGS\r\n"},
		{"getting dx\n", "GETTING DX", "GETTING DX\r\n"},
	} {
		t.Run(tt.want, func(t *testing.T) {
			var echoed bytes.Buffer
			c := &Client{reader: bufio.NewReader(strings.NewReader(tt.input)), writer: bufio.NewWriter(&echoed), echoInput: true}
			got, err := c.readCommandLine(128)
			if err != nil || got != tt.want || echoed.String() != tt.echo {
				t.Fatalf("line=%q echo=%q err=%v; want line=%q echo=%q", got, echoed.String(), err, tt.want, tt.echo)
			}
		})
	}
}

func TestMachineInputErrorsRetainEarlyIntent(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input string
		limit int
		verb  string
	}{
		{"forbidden_before_yaml", "PUT_ YAML CONFIG\n---\nRESUME\n...\n", 128, "PUT"},
		{"forbidden_resource", "patch yaml config:\n---\nRESUME\n...\n", 128, "PATCH"},
		{"get_forbidden_id", "GET YAML SETTINGS ID noise_1\nRESUME\n", 128, "GET"},
		{"overlong", "VALIDATE YAML CONFIG EXTRA\n---\nRESUME\n...\n", 20, "VALIDATE"},
		{"eof_after_verb", "PUT", 128, "PUT"},
		{"eof_during_header", "GET YAML SET", 128, "GET"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{reader: bufio.NewReader(strings.NewReader(tt.input))}
			_, err := c.readCommandLine(tt.limit)
			var machineErr *machineInputError
			if !errors.As(err, &machineErr) || machineErr.Verb != tt.verb || !machineErr.Terminal {
				t.Fatalf("lost terminal intent: %T %v", err, err)
			}
			if tt.name == "eof_after_verb" && !errors.Is(err, io.EOF) {
				t.Fatalf("EOF cause lost: %v", err)
			}
			if strings.HasPrefix(tt.name, "forbidden") {
				var validationErr *InputValidationError
				if !errors.As(err, &validationErr) {
					t.Fatalf("validation cause lost: %v", err)
				}
			}
		})
	}
}

func TestMachineHeaderReadTimeoutRetainsIntent(t *testing.T) {
	c := &Client{reader: bufio.NewReader(io.MultiReader(strings.NewReader("PUT YAML CON"), machineErrorReader{err: os.ErrDeadlineExceeded}))}
	_, err := c.readCommandLine(128)
	var machineErr *machineInputError
	if !errors.As(err, &machineErr) || machineErr.Verb != "PUT" || !machineErr.Terminal || !isTimeoutErr(err) {
		t.Fatalf("timeout lost machine intent: %T %v", err, err)
	}
}

type machineErrorReader struct{ err error }

func (r machineErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestParseMachineHeader(t *testing.T) {
	for _, tt := range []struct {
		input   string
		command machineCommand
		handled bool
		bad     bool
	}{
		{"GET YAML FILTER", machineCommand{Verb: "GET", Resource: "FILTER"}, true, false},
		{"gEt YaMl CoNfIg iD x-Ab9", machineCommand{Verb: "GET", Resource: "CONFIG", RequestID: "x-Ab9"}, true, false},
		{"GET YAML CAPABILITIES ID " + strings.Repeat("a", 32), machineCommand{Verb: "GET", Resource: "CAPABILITIES", RequestID: strings.Repeat("a", 32)}, true, false},
		{"PUT YAML SETTINGS", machineCommand{Verb: "PUT", Resource: "SETTINGS"}, true, false},
		{"PATCH YAML FILTER", machineCommand{Verb: "PATCH", Resource: "FILTER"}, true, false},
		{"VALIDATE YAML CONFIG", machineCommand{Verb: "VALIDATE", Resource: "CONFIG"}, true, false},
		{"GET YAML SETTINGS ID " + strings.Repeat("a", 33), machineCommand{Verb: "GET", Resource: "SETTINGS"}, true, true},
		{"GET YAML SETTINGS ID noise_1", machineCommand{Verb: "GET", Resource: "SETTINGS"}, true, true},
		{"GET YAML SETTINGS ID", machineCommand{Verb: "GET", Resource: "SETTINGS"}, true, true},
		{"GET YAML SETTINGS EXTRA", machineCommand{Verb: "GET", Resource: "SETTINGS"}, true, true},
		{"PUT YAML CONFIG EXTRA", machineCommand{Verb: "PUT", Resource: "CONFIG"}, true, true},
		{"PATCH YAML CAPABILITIES", machineCommand{Verb: "PATCH", Resource: "CAPABILITIES"}, true, true},
		{"VALIDATE YAML FILTER", machineCommand{Verb: "VALIDATE", Resource: "FILTER"}, true, true},
		{"PUT CONFIG", machineCommand{Verb: "PUT"}, true, true},
		{"PUT", machineCommand{Verb: "PUT"}, true, true},
		{"SHOW FILTER YAML", machineCommand{}, false, false},
		{"GETTING DX", machineCommand{}, false, false},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, handled, err := parseMachineHeader(tt.input)
			if got != tt.command || handled != tt.handled || (err != nil) != tt.bad {
				t.Fatalf("got %+v handled=%v err=%v; want %+v handled=%v bad=%v", got, handled, err, tt.command, tt.handled, tt.bad)
			}
		})
	}
}

func TestMalformedMachineHeaderKeepsTailOutsideHeader(t *testing.T) {
	for _, verb := range []string{"GET", "PUT", "PATCH", "VALIDATE"} {
		c := &Client{reader: bufio.NewReader(strings.NewReader(verb + " YAML CONFIG EXTRA\n---\nRESUME\n...\n"))}
		line, err := c.readCommandLine(128)
		if err != nil {
			t.Fatal(err)
		}
		cmd, handled, err := parseMachineHeader(line)
		if !handled || err == nil || cmd.Verb != verb {
			t.Fatalf("malformed header unrecognized: %+v %v", cmd, err)
		}
		// Session dispatch uses this distinction: a rejected upload is terminal;
		// a fully consumed bad GET may recover. The header never absorbs the tail.
		if verb != "GET" {
			c.interrupt()
		}
		tail, err := io.ReadAll(c.reader)
		if err != nil || string(tail) != "---\nRESUME\n...\n" {
			t.Fatalf("unexpected tail: %q, err=%v", tail, err)
		}
	}
}

func FuzzMachineHeader(f *testing.F) {
	for _, header := range []string{"GET YAML SETTINGS ID noise-1", "PUT YAML CONFIG EXTRA", "PUT_ YAML CONFIG", "VALIDATE YAML CONFIG", "GETTING DX", "GET YAML FILTER ID " + strings.Repeat("a", 33)} {
		f.Add(header)
	}
	f.Fuzz(func(t *testing.T, header string) {
		if len(header) > 256 {
			t.Skip()
		}
		cmd, handled, err := parseMachineHeader(header)
		if !handled || err != nil {
			return
		}
		if cmd.Verb != "GET" && cmd.RequestID != "" {
			t.Fatal("upload header admitted a request ID")
		}
		if len(cmd.RequestID) > 32 {
			t.Fatal("oversized identifier admitted")
		}
		for _, b := range []byte(cmd.RequestID) {
			alphanumeric := (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
			if !alphanumeric && b != '-' {
				t.Fatal("invalid identifier admitted")
			}
		}
		if cmd.Verb == "VALIDATE" && cmd.Resource != "CONFIG" {
			t.Fatal("VALIDATE admitted an unsupported resource")
		}
	})
}
