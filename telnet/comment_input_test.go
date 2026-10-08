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
	for _, seed := range []string{"pass comment a:b!\n", "pass band :\n", "show dx 1 comment a  b\n", "pass comment x\x15show filter\n"} {
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
