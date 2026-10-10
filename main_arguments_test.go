package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestRootExecutableHelper(t *testing.T) {
	mode := os.Getenv("DXC_ROOT_ARGUMENT_HELPER")
	if mode != "run" && mode != "closed-output" {
		t.Skip("actual-main subprocess only")
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("helper missing argv separator")
	}
	os.Args = append([]string{"gocluster"}, os.Args[separator+1:]...)
	Version, ReleaseTag, Commit, BuildTime = "261010", "261010r9abc", "123456789abc", "2026-10-10T12:00:00Z"
	if mode == "closed-output" {
		if err := os.Stdout.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "helper could not close output:", err)
			os.Exit(91)
		}
	}
	main()
	os.Exit(0)
}

func TestRootExecutableArguments(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
		code int
	}{
		{"short-help", []string{"-h"}, "help", 0},
		{"go-help", []string{"-help"}, "help", 0},
		{"long-help", []string{"--help"}, "help", 0},
		{"repeated-help", []string{"-h", "--help", "-help"}, "help", 0},
		{"long-version", []string{"--version"}, "version", 0},
		{"go-version", []string{"-version"}, "version", 0},
		{"word-version", []string{"version"}, "version", 0},
		{"unknown-then-version", []string{"--unknown", "--version"}, "version", 0},
		{"help-then-version", []string{"--help", "version"}, "version", 0},
		{"version-first", []string{"-version", "unexpected"}, "version", 0},
		{"normalized-version", []string{"--help", "bad", "  VeRsIoN  "}, "version", 0},
		{"unknown", []string{"--unknown"}, "unknown", 2},
		{"unknown-after-help", []string{"--help", "--unknown"}, "unknown", 2},
		{"unknown-before-help", []string{"bad", "--help"}, "unknown", 2},
		{"positional-config", []string{"config.yaml"}, "unknown", 2},
		{"empty-argument", []string{""}, "unknown", 2},
		{"help-value", []string{"--help=true"}, "unknown", 2},
		{"unicode-positional", []string{"☃"}, "unknown", 2},
		{"control-argument", []string{"\r\n"}, "unknown", 2},
		{"normal-startup", nil, "startup", 1},
		{"closed-help-output", []string{"--help"}, "output-error", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			args := append([]string{"-test.run=^TestRootExecutableHelper$", "--"}, tc.args...)
			cmd := exec.CommandContext(ctx, exe, args...)
			cmd.Dir = dir // Excludes the relative public config fallback too.
			mode := "run"
			if tc.want == "output-error" {
				mode = "closed-output"
			}
			cmd.Env = append(os.Environ(), "DXC_ROOT_ARGUMENT_HELPER="+mode, "DXC_CONFIG_PATH="+filepath.Join(dir, "missing-config"))
			cmd.WaitDelay = 2 * time.Second
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err = cmd.Run()
			if ctx.Err() != nil {
				t.Fatalf("actual main did not finish: %v", ctx.Err())
			}
			code := 0
			if err != nil {
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) {
					t.Fatalf("helper failed to launch/join: %v", err)
				}
				code = exitErr.ExitCode()
			}
			out, diag := stdout.String(), stderr.String()
			if code != tc.code {
				t.Fatalf("exit=%d want=%d stdout=%q stderr=%q", code, tc.code, out, diag)
			}
			switch tc.want {
			case "help":
				for _, phrase := range []string{"Usage: gocluster", "-h", "-help", "--help", "--version", "data/config", "DXC_CONFIG_PATH"} {
					if !strings.Contains(out, phrase) {
						t.Fatalf("help missing %q: %q", phrase, out)
					}
				}
				if diag != "" || strings.Contains(out, "Product version:") {
					t.Fatalf("unexpected help streams: %q / %q", out, diag)
				}
			case "version":
				if diag != "" || !strings.HasPrefix(out, "Product version: 261010\n") || !strings.Contains(out, "Commit:          123456789abc\n") || strings.Contains(out, "Usage:") {
					t.Fatalf("unexpected version streams: %q / %q", out, diag)
				}
			case "unknown":
				if out != "" || !strings.Contains(diag, "Unrecognized argument") || !strings.Contains(diag, "Usage: gocluster") {
					t.Fatalf("unknown streams: %q / %q", out, diag)
				}
			case "startup":
				if !strings.Contains(diag, "Startup failed:") || !strings.Contains(diag, "config path") || !strings.Contains(diag, "missing-config") {
					t.Fatalf("fixture did not exercise ordinary config startup failure: %q / %q", out, diag)
				}
			case "output-error":
				if out != "" || !strings.Contains(diag, "Unable to write help:") {
					t.Fatalf("child did not exercise help output failure: %q / %q", out, diag)
				}
			}
			if tc.want != "startup" && (strings.Contains(out+diag, "Startup failed:") || strings.Contains(out+diag, "unable to load config")) {
				t.Fatalf("non-startup invocation entered config: %q / %q", out, diag)
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("actual main created files in isolated cwd: %v %v", entries, err)
			}
		})
	}
}

func FuzzUnknownRootArgument(f *testing.F) {
	for _, seed := range []string{"", "--help", "--version", "\r\n", "☃", "\x00"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		// Every generated value is unambiguously positional. Valid surrounding
		// help flags must neither hide it nor change its bytes.
		value := "value:" + input
		got, unknown := unknownRootArgument([]string{"-h", "--help", value, "-help"})
		if !unknown || got != value {
			t.Fatalf("first unknown changed or concealed: %q %t", got, unknown)
		}
	})
}
