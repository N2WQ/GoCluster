package telnet

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"dxcluster/commands"
	"dxcluster/spot"
)

func TestDXCommentBoundary(t *testing.T) {
	for _, tc := range []struct {
		prefix string
		want   bool
	}{
		{"DX 14074 K1ABC ", true},
		{"DX K1ABC 14074 ", true},
		{"  dx   14074   K1ABC   FT8 ", true},
		{"DX invalid invalid ", true}, // Dispatch, not the reader, validates arguments.
		{"DX 14074 K1ABC", false},
		{"DX 14074 ", false},
		{"DX   ", false},
		{"DX", false},
		{"DXMORE 14074 K1ABC ", false},
		{"SHOW DX 14074 K1ABC ", false},
		{"GET FILTER DX 14074 K1ABC ", false},
		{"", false},
	} {
		if got := dxCommentArgumentsStarted([]byte(tc.prefix)); got != tc.want {
			t.Errorf("prefix %q: boundary=%v, want %v", tc.prefix, got, tc.want)
		}
	}
	line := []byte("DX 14074 K1ABC FT8 ")
	if count := testing.AllocsPerRun(100, func() { dxCommentArgumentsStarted(line) }); count != 0 {
		t.Fatalf("per-byte DX comment boundary allocated: %v", count)
	}
}

func TestDXPositiveReportThroughInputAndDispatch(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		for _, input := range []string{
			"DX 14074 K1ABC FT8 +02",
			"DX K1ABC 14074 FT8 +02",
			"  dx   14074   K1ABC   ft8 +02",
		} {
			t.Run(fmt.Sprintf("ziutek=%v/%s", ziutek, input), func(t *testing.T) {
				c, _ := newHumanRecoveryClient(t, []byte(input+"\r\nSHOW OWN\r\n"), ziutek)
				line, err := c.readCommandLine(128)
				if err != nil {
					t.Fatalf("read positive manual report: %v", err)
				}
				queue := make(chan *spot.Spot, 1)
				processor := commands.NewProcessor(nil, nil, queue, nil, nil, nil)
				if response := processor.ProcessCommandForClient(line, "W1AW", "127.0.0.1", nil, "go"); response != "Spot queued.\n" {
					t.Fatalf("dispatch response=%q", response)
				}
				select {
				case got := <-queue:
					if got.DXCall != "K1ABC" || got.Mode != "FT8" || !got.HasReport || got.Report != 2 || got.Comment != "" {
						t.Fatalf("manual +02 report did not reach the spot queue: %+v", got)
					}
				default:
					t.Fatal("successful command did not enqueue a spot")
				}
				if next, err := c.readCommandLine(128); err != nil || next != "SHOW OWN" {
					t.Fatalf("following command=%q err=%v", next, err)
				}
			})
		}
	}
}

func TestDXPlusContextAndEditingRefusals(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		for _, input := range []string{
			"+DX 14074 K1ABC FT8 -02",
			"DX +14074 K1ABC FT8 -02",
			"DX 14074 K1+ABC FT8 -02",
			"DX K1ABC +14074 FT8 -02",
			"DX 14074 K1ABC+",
			"DX 14074 +",
			"DXMORE 14074 K1ABC FT8 +02",
			"SHOW DX +02",
			"DX 14074 K1ABC \b+",
			"DX 14074 K1ABC \x17+",
			"DX 14074 K1ABC FT8 +02\x15+",
		} {
			t.Run(fmt.Sprintf("ziutek=%v/%q", ziutek, input), func(t *testing.T) {
				c, conn := newHumanRecoveryClient(t, []byte(input+"\r\nSHOW OWN\r\n"), ziutek)
				line, err := c.readCommandLine(128)
				assertDXInputRefused(t, line, err, inputErrorInvalidChar)
				var validation *InputValidationError
				if !errors.As(err, &validation) || strings.Contains(validation.allowed, "'+'") {
					t.Fatalf("outside-comment refusal advertised contextual '+': %v", err)
				}
				if conn.closed.Load() {
					t.Fatal("recoverable invalid command closed the session")
				}
				if next, err := c.readCommandLine(128); err != nil || next != "SHOW OWN" {
					t.Fatalf("following command=%q err=%v", next, err)
				}
			})
		}
	}
}

func TestDXPlusKeepsLoginAndMachineRestrictions(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		c, _ := newHumanRecoveryClient(t, []byte("DX 14074 K1ABC FT8 +02\r\nW1AW\r\n"), ziutek)
		line, err := c.readInputLine(128, "callsign", false, false, false, false, false)
		assertDXInputRefused(t, line, err, inputErrorInvalidChar)
		var validation *InputValidationError
		if !errors.As(err, &validation) || strings.Contains(validation.allowed, "'+'") {
			t.Fatalf("login advertised contextual '+': %v", err)
		}
		if next, err := c.readInputLine(32, "callsign", false, false, false, false, false); err != nil || next != "W1AW" {
			t.Fatalf("following login=%q err=%v", next, err)
		}

		c, _ = newHumanRecoveryClient(t, []byte("GET FILTER DX+\r\n"), ziutek)
		line, err = c.readCommandLine(128)
		var machine *machineInputError
		if line != "" || !errors.As(err, &machine) || !machine.Terminal || machine.Verb != "GET" {
			t.Fatalf("machine header gained human '+' permission: command=%q err=%v", line, err)
		}
	}
}

func TestDXPlusEditingRestoresCommentPermission(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		input := "DX 14074 K1ABC \b FT8 +02\b3\r\n"
		c, _ := newHumanRecoveryClient(t, []byte(input), ziutek)
		if line, err := c.readCommandLine(128); err != nil || line != "DX 14074 K1ABC FT8 +03" {
			t.Fatalf("ziutek=%v edited DX comment=%q err=%v", ziutek, line, err)
		}
	}
}

func TestDXCommentRefusalListsContextualPlus(t *testing.T) {
	for _, ziutek := range []bool{false, true} {
		c, _ := newHumanRecoveryClient(t, []byte("DX 14074 K1ABC FT8 +02!\r\nSHOW OWN\r\n"), ziutek)
		line, err := c.readCommandLine(128)
		assertDXInputRefused(t, line, err, inputErrorInvalidChar)
		var validation *InputValidationError
		if !errors.As(err, &validation) || !strings.Contains(validation.allowed, "'+'") {
			t.Fatalf("DX comment refusal omitted allowed '+': %v", err)
		}
		if next, err := c.readCommandLine(128); err != nil || next != "SHOW OWN" {
			t.Fatalf("following command=%q err=%v", next, err)
		}
	}
}

func FuzzDXCommentBoundary(f *testing.F) {
	for _, input := range []string{"DX 14074 K1ABC ", "DX K1ABC 14074 FT8 +02", "DX 14074 K1ABC", "DXMORE 14074 K1ABC ", "  dx   14074   K1ABC "} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 256 {
			return
		}
		// The independent oracle uses split fields and a terminal delimiter,
		// rather than the production borrowed-word walker.
		parts := strings.Split(input, " ")
		words := make([]string, 0, len(parts))
		for _, part := range parts {
			if part != "" {
				words = append(words, part)
			}
		}
		want := len(words) >= 3 && strings.EqualFold(words[0], "DX") &&
			(len(words) > 3 || strings.HasSuffix(input, " "))
		if got := dxCommentArgumentsStarted([]byte(input)); got != want {
			t.Fatalf("prefix=%q boundary=%v, independent oracle=%v", input, got, want)
		}
	})
}
