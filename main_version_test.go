package main

import "testing"

func TestShortRevision(t *testing.T) {
	if got := shortRevision("1234567890abcdef"); got != "1234567890ab" {
		t.Fatalf("shortRevision truncation mismatch: got %q", got)
	}
	if got := shortRevision("1234"); got != "1234" {
		t.Fatalf("shortRevision short value mismatch: got %q", got)
	}
}

func TestShouldPrintVersion(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want bool
	}{
		{name: "long", args: []string{"--version"}, want: true},
		{name: "short", args: []string{"-version"}, want: true},
		{name: "word", args: []string{"version"}, want: true},
		{name: "mixed", args: []string{"run", "--version"}, want: true},
		{name: "none", args: []string{"run"}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldPrintVersion(tc.args); got != tc.want {
				t.Fatalf("shouldPrintVersion(%v) = %v, want %v", tc.args, got, tc.want)
			}
		})
	}
}
