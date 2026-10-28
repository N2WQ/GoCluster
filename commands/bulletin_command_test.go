package commands

import (
	"strings"
	"testing"
	"unicode"
)

func TestParseBulletinCommand(t *testing.T) {
	cases := []struct {
		input, kind, err string
		full, handled    bool
	}{
		{"SHOW WWV", "WWV", "", false, true},
		{" sh\t wcy  ", "WCY", "", false, true},
		{"SHOW wwv fUlL", "WWV", "", true, true},
		{"SH WCY FULL", "WCY", "", true, true},
		{"SHOW/WWV", "WWV", "", false, true},
		{"SH/WWV FULL", "WWV", "", true, true},
		{"show/wcy", "WCY", "", false, true},
		{"sh/wcy FuLl", "WCY", "", true, true},
		{"SH/WWV/10", "WWV", "Usage: SHOW WWV [FULL]\n", false, true},
		{"SH/WWV 10", "WWV", "Usage: SHOW WWV [FULL]\n", false, true},
		{"SHOW WWV 10", "WWV", "Usage: SHOW WWV [FULL]\n", false, true},
		{"SHOW/WCY/99", "WCY", "Usage: SHOW WCY [FULL]\n", false, true},
		{"SH/WCY/", "WCY", "Usage: SHOW WCY [FULL]\n", false, true},
		{"SH/WWV/FULL", "WWV", "Usage: SHOW WWV [FULL]\n", false, true},
		{"SHOW/WCY FULL extra", "WCY", "Usage: SHOW WCY [FULL]\n", false, true},
		{"SHOW WWV BAD", "WWV", "Usage: SHOW WWV [FULL]\n", false, true},
		{"SH WCY FULL extra", "WCY", "Usage: SHOW WCY [FULL]\n", false, true},
		{"SHOW WWV FULL FULL", "WWV", "Usage: SHOW WWV [FULL]\n", false, true},
		{"SHOW", "", "", false, false},
		{"SHOW WWVX", "", "", false, false},
		{"SH/WWVX", "", "", false, false},
		{"SHOW/WCYX FULL", "", "", false, false},
		{"HELP SHOW WWV", "", "", false, false},
		{"", "", "", false, false},
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			kind, full, handled, errText := ParseBulletinCommand(tc.input)
			if kind != tc.kind || full != tc.full || handled != tc.handled || errText != tc.err {
				t.Fatalf("got (%q, %t, %t, %q), want (%q, %t, %t, %q)", kind, full, handled, errText, tc.kind, tc.full, tc.handled, tc.err)
			}
		})
	}
}

// FuzzParseBulletinCommand checks the public grammar and routing boundary for
// arbitrary input, including malformed UTF-8 and unrecognized SHOW heads.
func FuzzParseBulletinCommand(f *testing.F) {
	for _, seed := range []string{
		"SHOW WWV", "sh wcy FULL", "SHOW WWV FULL EXTRA", "SHOW WWVX",
		"SHOW/WWV", "SH/WCY FULL", "SH/WWV/10", "SH/WWVX", "SHOW WWV\x00FULL", "", string([]byte{0xff, ' ', 'W'}),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 8192 {
			return
		}
		kind, full, handled, errText := ParseBulletinCommand(input)
		// Tokenize the reference grammar independently of the parser's
		// strings.Fields call, including Unicode whitespace and invalid bytes.
		words := strings.FieldsFunc(input, unicode.IsSpace)
		var command, head string
		var args []string
		extraSlash := false
		if len(words) > 0 {
			slashParts := strings.Split(words[0], "/")
			if len(slashParts) > 1 {
				command, head = slashParts[0], slashParts[1]
				extraSlash = len(slashParts) > 2
				args = words[1:]
			} else if len(words) >= 2 {
				command, head, args = words[0], words[1], words[2:]
			}
		}
		isShow := strings.EqualFold(command, "SHOW") || strings.EqualFold(command, "SH")
		wantKind := ""
		if isShow && strings.EqualFold(head, "WWV") {
			wantKind = "WWV"
		} else if isShow && strings.EqualFold(head, "WCY") {
			wantKind = "WCY"
		}
		if wantKind == "" {
			if handled || kind != "" || full || errText != "" {
				t.Fatalf("claimed unrelated command %q: (%q, %t, %t, %q)", input, kind, full, handled, errText)
			}
			return
		}
		if !handled || kind != wantKind {
			t.Fatalf("recognized bulletin head %q returned (%q, %t)", input, kind, handled)
		}
		switch {
		case !extraSlash && len(args) == 0:
			if full || errText != "" {
				t.Fatalf("plain request %q was not accepted", input)
			}
		case !extraSlash && len(args) == 1 && strings.EqualFold(args[0], "FULL"):
			if !full || errText != "" {
				t.Fatalf("FULL request %q was not accepted", input)
			}
		default:
			if full || errText != "Usage: SHOW "+wantKind+" [FULL]\n" {
				t.Fatalf("invalid arguments %q returned full=%t usage=%q", input, full, errText)
			}
		}
	})
}

func TestBulletinHelpRouting(t *testing.T) {
	p := NewProcessor(nil, nil, nil, nil, nil, nil)
	for _, dialect := range []string{"go", "cc"} {
		for _, kind := range []string{"WWV", "WCY"} {
			for _, topic := range []string{"SHOW " + kind, "SH " + kind, "SHOW " + kind + " FULL", "SHOW/" + kind, "SH/" + kind, "SH/" + kind + " FULL"} {
				got := p.ProcessCommandForClient("HELP "+topic, "N2WQ", "", nil, dialect)
				for _, want := range []string{"SHOW " + kind, "[FULL]", "24 hours", "filters", "latest five"} {
					if !strings.Contains(got, want) {
						t.Fatalf("%s HELP %s lacks %q: %s", dialect, topic, want, got)
					}
				}
			}
		}
		got := p.ProcessCommandForClient("HELP SHOW WWVX", "N2WQ", "", nil, dialect)
		if !strings.Contains(got, "Unknown help topic") {
			t.Fatalf("%s accepted near-miss HELP topic: %s", dialect, got)
		}
	}
}
