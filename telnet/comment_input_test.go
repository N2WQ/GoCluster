package telnet

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
)

func TestCommentInputPunctuationBoundariesAndEditing(t *testing.T) {
	for _, tc := range []struct {
		input, want string
		invalid     bool
	}{
		{"pass comment up  5: please!\n", "PASS COMMENT UP  5: PLEASE!", false},
		{"remove reject comment \"*?,x\"\n", "REMOVE REJECT COMMENT \"*?,X\"", false},
		{"show dx K1ABC 20 comment a:b!\n", "SHOW DX K1ABC 20 COMMENT A:B!", false},
		{"show dx K1ABC 20 band 20,40 mode cw ft8 comment a:b!\n", "SHOW DX K1ABC 20 BAND 20,40 MODE CW FT8 COMMENT A:B!", false},
		{"sh/dx mode cw band 20 comment a+b=c\n", "SH/DX MODE CW BAND 20 COMMENT A+B=C", false},
		{"DX 28201 K1ABC CW TABCDEF01-0 up-5?\n", "DX 28201 K1ABC CW TABCDEF01-0 UP-5?", false},
		{"DX K1ABC 21201 FT8 TABCDEF01-1 up-5?\n", "DX K1ABC 21201 FT8 TABCDEF01-1 UP-5?", false},
		{"DX 28201 K1ABC CW TABCDEF01-0 up:5!\n", "", true},
		{"show dx band 20 mode cw comment :\x15pass band :\n", "", true},
		{"show dx band 20 mode cw :\n", "", true},
		{"sh/dx 20 comment a+b=c\n", "SH/DX 20 COMMENT A+B=C", false},
		{"pass band :\n", "", true}, {"show dx next comment :\n", "", true},
		{"pass comment: phrase\n", "", true}, {"pass comment café\n", "", true},
		{"pass comment a\x01\n", "", true},
		{"pass comment :\x15pass band :\n", "", true},
		{"pass comment word!\x17two: words\n", "PASS COMMENT TWO: WORDS", false},
	} {
		t.Run(tc.input, func(t *testing.T) {
			var echo bytes.Buffer
			c := &Client{reader: bufio.NewReader(strings.NewReader(tc.input)), writer: bufio.NewWriter(&echo), echoInput: true}
			line, err := c.readCommandLine(128)
			if (err != nil) != tc.invalid || (!tc.invalid && line != tc.want) {
				t.Fatalf("line=%q err=%v want=%q invalid=%v", line, err, tc.want, tc.invalid)
			}
		})
	}
	c := &Client{reader: bufio.NewReader(strings.NewReader("PASS COMMENT " + strings.Repeat("x", 116) + "\n"))}
	if _, err := c.readCommandLine(128); err == nil {
		t.Fatal("command byte limit bypassed")
	}
}

func FuzzCommentInput(f *testing.F) {
	for _, seed := range []string{"pass comment a:b!\n", "pass band :\n", "show dx 1 comment a  b\n", "show dx band 20,40 mode cw ft8 comment a:b!\n", "pass comment x\x15show filter\n"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 256 {
			return
		}
		c := &Client{reader: bufio.NewReader(strings.NewReader(input)), writer: bufio.NewWriter(&bytes.Buffer{})}
		line, err := c.readCommandLine(128)
		if err == nil {
			if len(line) > 128 {
				t.Fatal("unbounded accepted command")
			}
			for _, b := range []byte(line) {
				if b < 32 || b > 126 {
					t.Fatalf("accepted non-printable ASCII %q", line)
				}
			}
		}
	})
}

func TestCommentInputHistoryPrefixAllocations(t *testing.T) {
	line := []byte("SHOW DX K1ABC 20 BAND 20,40 MODE CW FT8 UNKNOWN COMMENT ")
	if !commentArgumentsStarted(line) {
		t.Fatal("combined selection phrase marker not recognized")
	}
	if count := testing.AllocsPerRun(100, func() { commentArgumentsStarted(line) }); count != 0 {
		t.Fatalf("per-byte prefix scan allocated: %v", count)
	}
}
