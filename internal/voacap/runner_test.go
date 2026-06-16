package voacap

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestRunnerSuccessRemovesStaleOutputAndReadsNewOutput(t *testing.T) {
	r := testRunner(t, "success")
	outputPath := filepath.Join(r.RunDir, "sample.out")
	if err := os.WriteFile(outputPath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("write stale output: %v", err)
	}

	result, err := r.Run(context.Background(), RunRequest{
		Deck:       []byte("deck"),
		OutputName: "sample.out",
		Timeout:    5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0", result.ExitCode)
	}
	if got := string(result.Output); !strings.Contains(got, "fake voacap output") {
		t.Fatalf("output = %q, want fake output", got)
	}
	if result.OutputPath != outputPath {
		t.Fatalf("OutputPath = %q, want %q", result.OutputPath, outputPath)
	}
	onDisk, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output left on disk: %v", err)
	}
	if string(onDisk) != string(result.Output) || strings.Contains(string(onDisk), "stale") {
		t.Fatalf("on-disk output = %q, want fresh output bytes %q", onDisk, result.Output)
	}
}

func TestRunnerRemovesOutputAfterReadWhenRequested(t *testing.T) {
	r := testRunner(t, "success")
	outputPath := filepath.Join(r.RunDir, "cleanup.out")

	result, err := r.Run(context.Background(), RunRequest{
		Deck:                  []byte("deck"),
		OutputName:            "cleanup.out",
		Timeout:               5 * time.Second,
		RemoveOutputAfterRead: true,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got := string(result.Output); !strings.Contains(got, "fake voacap output") {
		t.Fatalf("output = %q, want fake output", got)
	}
	if result.OutputPath != outputPath {
		t.Fatalf("OutputPath = %q, want %q", result.OutputPath, outputPath)
	}
	if _, err := os.Stat(outputPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("output file should be removed after read, stat err=%v", err)
	}
}

func TestRunnerRejectsUnsafeOutputName(t *testing.T) {
	r := testRunner(t, "success")
	for _, name := range []string{`..\escape.out`, "..", "."} {
		_, err := r.Run(context.Background(), RunRequest{
			Deck:       []byte("deck"),
			OutputName: name,
			Timeout:    5 * time.Second,
		})
		if err == nil {
			t.Fatalf("Run returned nil error for unsafe output name %q", name)
		}
	}
}

func TestRunnerReportsNonzeroExit(t *testing.T) {
	r := testRunner(t, "exit2")
	result, err := r.Run(context.Background(), RunRequest{
		Deck:       []byte("deck"),
		OutputName: "fail.out",
		Timeout:    5 * time.Second,
	})
	if err == nil {
		t.Fatal("Run returned nil error for nonzero exit")
	}
	if result.ExitCode != 2 {
		t.Fatalf("ExitCode = %d, want 2", result.ExitCode)
	}
}

func TestRunnerReportsMissingOutput(t *testing.T) {
	r := testRunner(t, "missing")
	_, err := r.Run(context.Background(), RunRequest{
		Deck:       []byte("deck"),
		OutputName: "missing.out",
		Timeout:    5 * time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "not created") {
		t.Fatalf("Run error = %v, want missing output", err)
	}
}

func TestRunnerReportsTimeout(t *testing.T) {
	r := testRunner(t, "sleep")
	_, err := r.Run(context.Background(), RunRequest{
		Deck:       []byte("deck"),
		OutputName: "timeout.out",
		Timeout:    100 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("Run error = %v, want timeout", err)
	}
}

func TestRunnerReportsOversizedOutput(t *testing.T) {
	r := testRunner(t, "success")
	r.OutputLimit = 4
	_, err := r.Run(context.Background(), RunRequest{
		Deck:       []byte("deck"),
		OutputName: "large.out",
		Timeout:    5 * time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("Run error = %v, want output limit error", err)
	}
}

func TestRunnerWaitsForLockUntilContextExpires(t *testing.T) {
	r := testRunner(t, "success")
	r.LockRetryDelay = 10 * time.Millisecond
	if err := os.WriteFile(r.LockFile, []byte("held"), 0o644); err != nil {
		t.Fatalf("write lock file: %v", err)
	}
	defer os.Remove(r.LockFile)

	_, err := r.Run(context.Background(), RunRequest{
		Deck:       []byte("deck"),
		OutputName: "locked.out",
		Timeout:    50 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "wait for VOACAP lock") {
		t.Fatalf("Run error = %v, want lock timeout", err)
	}
}

func testRunner(t *testing.T, behavior string) Runner {
	t.Helper()
	home := t.TempDir()
	binDir := filepath.Join(home, "bin_win")
	runDir := filepath.Join(home, "run")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatalf("mkdir run: %v", err)
	}
	engine := buildFakeEngine(t, home)
	return Runner{
		Home:           home,
		EnginePath:     engine,
		RunDir:         runDir,
		InputName:      DefaultVOACAPInputName,
		OutputLimit:    defaultVOACAPOutputLimit,
		LockFile:       filepath.Join(runDir, defaultVOACAPLockFileName),
		LockRetryDelay: 10 * time.Millisecond,
		ExtraEnv:       []string{"FAKE_VOACAP_BEHAVIOR=" + behavior},
		allowAnyEngine: true,
	}
}

func buildFakeEngine(t *testing.T, home string) string {
	t.Helper()
	source := filepath.Join(home, "fakeengine.go")
	binary := filepath.Join(home, "bin_win", "fakeengine")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	if err := os.WriteFile(source, []byte(fakeEngineSource), 0o644); err != nil {
		t.Fatalf("write fake engine source: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-o", binary, source)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build fake engine: %v\n%s", err, out)
	}
	return binary
}

const fakeEngineSource = `package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintf(os.Stderr, "unexpected args: %v", os.Args)
		os.Exit(3)
	}
	home := os.Args[2]
	inputName := os.Args[3]
	outputName := os.Args[4]
	input, err := os.ReadFile(filepath.Join(home, "run", inputName))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(4)
	}
	switch os.Getenv("FAKE_VOACAP_BEHAVIOR") {
	case "exit2":
		fmt.Fprintln(os.Stderr, "forced failure")
		os.Exit(2)
	case "missing":
		return
	case "sleep":
		time.Sleep(5 * time.Second)
	default:
		output := append([]byte("fake voacap output\n"), input...)
		if err := os.WriteFile(filepath.Join(home, "run", outputName), output, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(5)
		}
	}
}
`
