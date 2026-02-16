package telnet

import (
	"bufio"
	"bytes"
	"testing"
)

func TestReadLineBackspaceAndDelete(t *testing.T) {
	t.Run("backspace", func(t *testing.T) {
		input := "AB\bC\n"
		got, echoed := readLineWithEcho(t, input, true)
		if got != "AC" {
			t.Fatalf("expected line %q, got %q", "AC", got)
		}
		expectEcho := "AB" + "\b \b" + "C" + "\r\n"
		if echoed != expectEcho {
			t.Fatalf("expected echo %q, got %q", expectEcho, echoed)
		}
	})

	t.Run("delete", func(t *testing.T) {
		input := "AB" + string([]byte{0x7f}) + "C\n"
		got, echoed := readLineWithEcho(t, input, true)
		if got != "AC" {
			t.Fatalf("expected line %q, got %q", "AC", got)
		}
		expectEcho := "AB" + "\b \b" + "C" + "\r\n"
		if echoed != expectEcho {
			t.Fatalf("expected echo %q, got %q", expectEcho, echoed)
		}
	})
}

func TestReadLineCtrlUErasesLine(t *testing.T) {
	input := "HELLO" + string([]byte{0x15}) + "OK\n"
	got, echoed := readLineWithEcho(t, input, true)
	if got != "OK" {
		t.Fatalf("expected line %q, got %q", "OK", got)
	}
	expectEcho := "HELLO" + stringsRepeat("\b \b", 5) + "OK" + "\r\n"
	if echoed != expectEcho {
		t.Fatalf("expected echo %q, got %q", expectEcho, echoed)
	}
}

func TestReadLineCtrlWErasesWord(t *testing.T) {
	input := "SHOW FILTER" + string([]byte{0x17}) + "DX\n"
	got, echoed := readLineWithEcho(t, input, true)
	if got != "SHOW DX" {
		t.Fatalf("expected line %q, got %q", "SHOW DX", got)
	}
	expectEcho := "SHOW FILTER" + stringsRepeat("\b \b", 6) + "DX" + "\r\n"
	if echoed != expectEcho {
		t.Fatalf("expected echo %q, got %q", expectEcho, echoed)
	}
}

func TestReadLineNoEchoSkipsEraseOutput(t *testing.T) {
	input := "HELLO" + string([]byte{0x08}) + "X\n"
	got, echoed := readLineWithEcho(t, input, false)
	if got != "HELLX" {
		t.Fatalf("expected line %q, got %q", "HELLX", got)
	}
	if echoed != "" {
		t.Fatalf("expected no echo output, got %q", echoed)
	}
}

func TestReadLineUppercasesInput(t *testing.T) {
	input := "dx de lz1v\n"
	got, echoed := readLineWithEcho(t, input, true)
	if got != "DX DE LZ1V" {
		t.Fatalf("expected uppercased line %q, got %q", "DX DE LZ1V", got)
	}
	if echoed != "DX DE LZ1V\r\n" {
		t.Fatalf("expected uppercased echo %q, got %q", "DX DE LZ1V\r\n", echoed)
	}
}

func TestReadLineStripsIACNegotiation(t *testing.T) {
	input := []byte{IAC, WILL, 1, 'S', '5', '3', 'Z', 'O', '\n'}
	got, echoed := readLineWithEchoBytes(t, input, false)
	if got != "S53ZO" {
		t.Fatalf("expected line %q, got %q", "S53ZO", got)
	}
	if echoed != "" {
		t.Fatalf("expected no echo output, got %q", echoed)
	}
}

func TestReadLineStripsIACSubnegotiation(t *testing.T) {
	input := []byte{'S', '5', IAC, SB, 31, 0, 80, IAC, SE, '3', 'Z', 'O', '\n'}
	got, _ := readLineWithEchoBytes(t, input, false)
	if got != "S53ZO" {
		t.Fatalf("expected line %q, got %q", "S53ZO", got)
	}
}

func TestReadLineIgnoresEscapedIAC(t *testing.T) {
	input := []byte{'S', '5', '3', IAC, IAC, 'Z', 'O', '\n'}
	got, _ := readLineWithEchoBytes(t, input, false)
	if got != "S53ZO" {
		t.Fatalf("expected line %q, got %q", "S53ZO", got)
	}
}

func TestReadLineHandlesCRLFAndCRNUL(t *testing.T) {
	t.Run("crlf", func(t *testing.T) {
		input := []byte{'S', '5', '3', 'Z', 'O', '\r', '\n', 'S', '5', '3', 'M', '\n'}
		lines := readLinesWithEchoBytes(t, input, false, 2)
		if lines[0] != "S53ZO" || lines[1] != "S53M" {
			t.Fatalf("expected lines %q and %q, got %q", "S53ZO", "S53M", lines)
		}
	})

	t.Run("crnul", func(t *testing.T) {
		input := []byte{'S', '5', '3', 'Z', 'O', '\r', 0x00, 'S', '5', '3', 'M', '\n'}
		lines := readLinesWithEchoBytes(t, input, false, 2)
		if lines[0] != "S53ZO" || lines[1] != "S53M" {
			t.Fatalf("expected lines %q and %q, got %q", "S53ZO", "S53M", lines)
		}
	})
}

func TestReadLineCRThenDataStartsNextLine(t *testing.T) {
	input := []byte{'A', '\r', 'B', '\n'}
	lines := readLinesWithEchoBytes(t, input, false, 2)
	if lines[0] != "A" || lines[1] != "B" {
		t.Fatalf("expected lines %q and %q, got %q", "A", "B", lines)
	}
}

func readLineWithEcho(t *testing.T, input string, echo bool) (string, string) {
	t.Helper()
	return readLineWithEchoBytes(t, []byte(input), echo)
}

func readLineWithEchoBytes(t *testing.T, input []byte, echo bool) (string, string) {
	t.Helper()
	reader := bufio.NewReader(bytes.NewBuffer(input))
	var out bytes.Buffer
	writer := bufio.NewWriter(&out)
	client := &Client{
		reader:    reader,
		writer:    writer,
		echoInput: echo,
	}
	line, err := client.ReadLine(128, "command", true, true, true, true)
	if err != nil {
		t.Fatalf("ReadLine failed: %v", err)
	}
	return line, out.String()
}

func readLinesWithEchoBytes(t *testing.T, input []byte, echo bool, count int) []string {
	t.Helper()
	reader := bufio.NewReader(bytes.NewBuffer(input))
	var writer bytes.Buffer
	client := &Client{
		reader:    reader,
		writer:    bufio.NewWriter(&writer),
		echoInput: echo,
	}
	lines := make([]string, 0, count)
	for i := 0; i < count; i++ {
		line, err := client.ReadLine(128, "command", true, true, true, true)
		if err != nil {
			t.Fatalf("ReadLine failed: %v", err)
		}
		lines = append(lines, line)
	}
	return lines
}

func stringsRepeat(s string, count int) string {
	if count <= 0 {
		return ""
	}
	var b bytes.Buffer
	for i := 0; i < count; i++ {
		b.WriteString(s)
	}
	return b.String()
}
