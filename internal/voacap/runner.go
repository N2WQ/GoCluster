package voacap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	DefaultVOACAPHome         = `C:\itshfbc`
	DefaultVOACAPInputName    = "voacapx.dat"
	defaultVOACAPOutputLimit  = 4 << 20
	defaultVOACAPLockFileName = ".gocluster-voacap.lock"
)

var voacapRunMu sync.Mutex

// Runner invokes the installed VOACAP engine for one deck at a time. VOACAP's
// Windows engine uses a shared run directory and conventional input filename,
// so Run serializes access in this process and also takes a lock file for
// cooperation with other Go experiment processes.
type Runner struct {
	Home           string
	EnginePath     string
	RunDir         string
	InputName      string
	OutputLimit    int64
	LockFile       string
	LockRetryDelay time.Duration
	ExtraEnv       []string
	allowAnyEngine bool
}

type RunRequest struct {
	Deck       []byte
	OutputName string
	Timeout    time.Duration
	// RemoveOutputAfterRead deletes the .out file after Output is copied into memory.
	// Leave it false for experiment commands that intentionally keep artifacts.
	RemoveOutputAfterRead bool
}

type RunResult struct {
	OutputPath string
	Output     []byte
	Elapsed    time.Duration
	ExitCode   int
}

func NewRunner(home string) Runner {
	if strings.TrimSpace(home) == "" {
		home = DefaultVOACAPHome
	}
	return Runner{
		Home:           home,
		EnginePath:     filepath.Join(home, "bin_win", "Voacapw.exe"),
		RunDir:         filepath.Join(home, "run"),
		InputName:      DefaultVOACAPInputName,
		OutputLimit:    defaultVOACAPOutputLimit,
		LockFile:       filepath.Join(home, "run", defaultVOACAPLockFileName),
		LockRetryDelay: 100 * time.Millisecond,
	}
}

// Validate checks that the configured VOACAP engine and shared run directory
// are usable before a runtime enables background forecast work.
func (r Runner) Validate() error {
	r = r.withDefaults()
	return r.validate()
}

func (r Runner) Run(ctx context.Context, req RunRequest) (RunResult, error) {
	if ctx == nil {
		return RunResult{}, errors.New("nil context")
	}
	if req.Timeout <= 0 {
		return RunResult{}, errors.New("timeout must be positive")
	}
	if len(req.Deck) == 0 {
		return RunResult{}, errors.New("deck is empty")
	}

	r = r.withDefaults()
	if err := r.validate(); err != nil {
		return RunResult{}, err
	}
	outputName, err := cleanVOACAPFileName(req.OutputName)
	if err != nil {
		return RunResult{}, err
	}

	voacapRunMu.Lock()
	defer voacapRunMu.Unlock()

	runCtx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()

	release, err := r.acquireLock(runCtx)
	if err != nil {
		return RunResult{}, err
	}
	defer release()

	inputPath := filepath.Join(r.RunDir, r.InputName)
	outputPath := filepath.Join(r.RunDir, outputName)
	if err := os.WriteFile(inputPath, req.Deck, 0o644); err != nil {
		return RunResult{}, fmt.Errorf("write VOACAP deck: %w", err)
	}
	if err := os.Remove(outputPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return RunResult{}, fmt.Errorf("remove stale VOACAP output: %w", err)
	}

	start := time.Now()
	// #nosec G204 -- validate requires an absolute Voacapw.exe path for normal
	// runners; package tests use allowAnyEngine only with a temp-built fake engine.
	cmd := exec.CommandContext(runCtx, r.EnginePath, "silent", r.Home, r.InputName, outputName)
	cmd.Dir = filepath.Dir(r.EnginePath)
	if len(r.ExtraEnv) > 0 {
		cmd.Env = append(os.Environ(), r.ExtraEnv...)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()
	elapsed := time.Since(start)
	if runCtx.Err() == context.DeadlineExceeded {
		return RunResult{OutputPath: outputPath, Elapsed: elapsed, ExitCode: exitCode(cmd)}, fmt.Errorf("VOACAP timed out after %s", req.Timeout)
	}
	if err != nil {
		return RunResult{OutputPath: outputPath, Elapsed: elapsed, ExitCode: exitCode(cmd)}, formatRunError(err, stderr.String())
	}

	output, err := readBoundedFile(outputPath, r.OutputLimit)
	if err != nil {
		return RunResult{OutputPath: outputPath, Elapsed: elapsed, ExitCode: exitCode(cmd)}, err
	}
	if len(output) == 0 {
		return RunResult{OutputPath: outputPath, Elapsed: elapsed, ExitCode: exitCode(cmd)}, fmt.Errorf("VOACAP output is empty: %s", outputPath)
	}
	result := RunResult{
		OutputPath: outputPath,
		Output:     output,
		Elapsed:    elapsed,
		ExitCode:   exitCode(cmd),
	}
	if req.RemoveOutputAfterRead {
		if err := os.Remove(outputPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return result, fmt.Errorf("remove VOACAP output after read: %w", err)
		}
	}
	return result, nil
}

func (r Runner) withDefaults() Runner {
	if strings.TrimSpace(r.Home) == "" {
		r.Home = DefaultVOACAPHome
	}
	if strings.TrimSpace(r.EnginePath) == "" {
		r.EnginePath = filepath.Join(r.Home, "bin_win", "Voacapw.exe")
	}
	if strings.TrimSpace(r.RunDir) == "" {
		r.RunDir = filepath.Join(r.Home, "run")
	}
	if strings.TrimSpace(r.InputName) == "" {
		r.InputName = DefaultVOACAPInputName
	}
	if r.OutputLimit <= 0 {
		r.OutputLimit = defaultVOACAPOutputLimit
	}
	if strings.TrimSpace(r.LockFile) == "" {
		r.LockFile = filepath.Join(r.RunDir, defaultVOACAPLockFileName)
	}
	if r.LockRetryDelay <= 0 {
		r.LockRetryDelay = 100 * time.Millisecond
	}
	return r
}

func (r Runner) validate() error {
	if _, err := os.Stat(r.EnginePath); err != nil {
		return fmt.Errorf("VOACAP engine not available at %s: %w", r.EnginePath, err)
	}
	if !filepath.IsAbs(r.EnginePath) {
		return fmt.Errorf("VOACAP engine path must be absolute: %s", r.EnginePath)
	}
	if !r.allowAnyEngine && !strings.EqualFold(filepath.Base(r.EnginePath), "Voacapw.exe") {
		return fmt.Errorf("VOACAP engine must be Voacapw.exe: %s", r.EnginePath)
	}
	info, err := os.Stat(r.RunDir)
	if err != nil {
		return fmt.Errorf("VOACAP run directory not available at %s: %w", r.RunDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("VOACAP run directory is not a directory: %s", r.RunDir)
	}
	if _, err := cleanVOACAPFileName(r.InputName); err != nil {
		return fmt.Errorf("invalid VOACAP input name: %w", err)
	}
	return nil
}

func (r Runner) acquireLock(ctx context.Context) (func(), error) {
	for {
		file, err := os.OpenFile(r.LockFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err == nil {
			_, _ = fmt.Fprintf(file, "pid=%d\ncreated_utc=%s\n", os.Getpid(), time.Now().UTC().Format(time.RFC3339))
			_ = file.Close()
			return func() { _ = os.Remove(r.LockFile) }, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create VOACAP lock file: %w", err)
		}
		timer := time.NewTimer(r.LockRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("wait for VOACAP lock %s: %w", r.LockFile, ctx.Err())
		case <-timer.C:
		}
	}
}

func cleanVOACAPFileName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("file name is empty")
	}
	if name == "." || name == ".." {
		return "", fmt.Errorf("file name must not be %q", name)
	}
	if name != filepath.Base(name) {
		return "", fmt.Errorf("file name must not include a path: %s", name)
	}
	if strings.ContainsAny(name, `<>:"/\|?*`) {
		return "", fmt.Errorf("file name contains unsafe characters: %s", name)
	}
	return name, nil
}

func readBoundedFile(path string, limit int64) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("VOACAP output not created at %s: %w", path, err)
	}
	if info.Size() > limit {
		return nil, fmt.Errorf("VOACAP output %s exceeds %d bytes", path, limit)
	}
	return os.ReadFile(path)
}

func exitCode(cmd *exec.Cmd) int {
	if cmd == nil || cmd.ProcessState == nil {
		return -1
	}
	return cmd.ProcessState.ExitCode()
}

func formatRunError(err error, stderr string) error {
	stderr = strings.TrimSpace(stderr)
	if stderr == "" {
		return fmt.Errorf("VOACAP failed: %w", err)
	}
	return fmt.Errorf("VOACAP failed: %w: %s", err, stderr)
}
