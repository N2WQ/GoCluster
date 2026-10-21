package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/metrics"
	"strconv"
	"testing"
	"time"

	"dxcluster/config"

	"golang.org/x/sys/windows"
)

const diagChildWatchdog = 6 * time.Second

type diagChildStart struct {
	Nonce      string
	PID        uint32
	Executable string
	Directory  string
}

type diagChildResult struct {
	Nonce        string
	GOGC         string
	Address      string
	InitialGC    uint64
	LoadedGC     int
	EffectiveGC  uint64
	Config       string
	Arguments    int
	Status       int
	StdinEOF     bool
	Context      string
	EmptyPresent bool
	EmptyValue   string
}

// TestMain's private branch belongs only to the native wrapper fixture. The
// ordinary test binary keeps m.Run and the runner's unexpected-exit safeguard.
// The child exercises the real loader/tuning consumer without starting sockets,
// cluster services or diagnostics. Its watchdog bounds life after parent loss.
func TestMain(m *testing.M) {
	if os.Getenv("DXC_DIAG_WRAPPER_CHILD") != "1" {
		os.Exit(m.Run())
	}
	if err := runDiagWrapperChild(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(95)
	}
	status, err := strconv.Atoi(os.Getenv("DXC_DIAG_WRAPPER_STATUS"))
	if err != nil || status < 0 || status > 255 {
		fmt.Fprintln(os.Stderr, "invalid private diagnostic child status")
		os.Exit(95)
	}
	os.Exit(status)
}

func runDiagWrapperChild() error {
	directory := os.Getenv("DXC_DIAG_WRAPPER_PRIVATE")
	nonce := os.Getenv("DXC_DIAG_WRAPPER_NONCE")
	if !filepath.IsAbs(directory) || len(nonce) != 32 {
		return fmt.Errorf("invalid private diagnostic child ownership")
	}
	watchdog := time.AfterFunc(diagChildWatchdog, func() {
		// Separate terminal receipt avoids racing the normal result file.
		_ = diagWriteJSON(filepath.Join(directory, "watchdog.json"), struct {
			Nonce  string
			Status int
		}{nonce, 96})
		os.Exit(96)
	})
	defer watchdog.Stop()
	job, err := diagOpenJob(nonce, diagJobQuery)
	if err != nil {
		return err
	}
	memberErr := diagRequireLiveMember(windows.CurrentProcess(), job)
	if err := errors.Join(memberErr, windows.CloseHandle(job)); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	start := diagChildStart{nonce, uint32(os.Getpid()), executable, cwd}
	if os.Getenv("DXC_DIAG_WRAPPER_REJECT_START") == "1" {
		start.Nonce = "rejected-" + nonce
	}
	if err := diagWriteJSON(filepath.Join(directory, "start.json"), start); err != nil {
		return err
	}
	if err := diagAwaitRelease(directory, nonce); err != nil {
		return err
	}
	if os.Getenv("DXC_DIAG_WRAPPER_STALL") == "1" {
		select {} // The already armed watchdog is the terminal owner.
	}
	initialGC, err := diagRuntimeGC()
	if err != nil {
		return err
	}
	configDirectory := filepath.Join(directory, "config")
	cfg, err := config.Load(configDirectory)
	if err != nil {
		return fmt.Errorf("load private diagnostic configuration: %w", err)
	}
	applyGoRuntimeTuning(cfg.GoRuntime)
	effectiveGC, err := diagRuntimeGC()
	if err != nil {
		return err
	}
	status, err := strconv.Atoi(os.Getenv("DXC_DIAG_WRAPPER_STATUS"))
	if err != nil {
		return err
	}
	input := []byte{0}
	count, inputErr := os.Stdin.Read(input)
	if count != 0 || !errors.Is(inputErr, io.EOF) {
		if inputErr != nil {
			return fmt.Errorf("diagnostic child stdin is not EOF: count=%d: %w", count, inputErr)
		}
		return fmt.Errorf("diagnostic child stdin is not EOF: count=%d without an EOF error", count)
	}
	fault := os.Getenv("DXC_DIAG_WRAPPER_FAULT")
	if fault == "stdio_roundtrip" || fault == "stdio_backpressure" || fault == "launch_context" {
		if _, err := fmt.Fprintln(os.Stdout, "DIAG_STDOUT_SENTINEL "+nonce); err != nil {
			return err
		}
		if _, err := fmt.Fprintln(os.Stderr, "DIAG_STDERR_SENTINEL "+nonce); err != nil {
			return err
		}
		if fault == "stdio_backpressure" {
			for range 1024 {
				if _, err := os.Stdout.Write([]byte("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n")); err != nil {
					return err
				}
				if _, err := os.Stderr.Write([]byte("fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210\n")); err != nil {
					return err
				}
			}
		}
	}
	empty, emptyPresent := os.LookupEnv("DXC_DIAG_WRAPPER_EMPTY")
	result := diagChildResult{Nonce: nonce, GOGC: os.Getenv("GOGC"), Address: os.Getenv("DXC_PPROF_ADDR"),
		InitialGC: initialGC, LoadedGC: cfg.GoRuntime.GCPercent, EffectiveGC: effectiveGC,
		Config: cfg.LoadedFrom, Arguments: len(os.Args), Status: status, StdinEOF: true,
		Context: os.Getenv("DXC_DIAG_WRAPPER_CONTEXT"), EmptyPresent: emptyPresent, EmptyValue: empty}
	return diagWriteJSON(filepath.Join(directory, "result.json"), result)
}

func diagAwaitRelease(directory, nonce string) error {
	for {
		body, err := os.ReadFile(filepath.Join(directory, "release"))
		if err == nil {
			if string(body) != nonce {
				return fmt.Errorf("diagnostic parent release does not match child ownership")
			}
			return nil
		}
		if !os.IsNotExist(err) && !errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
			return err
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func diagRuntimeGC() (uint64, error) {
	samples := []metrics.Sample{{Name: "/gc/gogc:percent"}}
	metrics.Read(samples)
	if samples[0].Value.Kind() != metrics.KindUint64 {
		return 0, fmt.Errorf("runtime GC metric is unavailable")
	}
	return samples[0].Value.Uint64(), nil
}

func diagWriteJSON(path string, value any) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(body) > 4096 {
		return fmt.Errorf("diagnostic receipt exceeds 4096 bytes")
	}
	if err := os.WriteFile(path+".pending", body, 0o600); err != nil {
		return err
	}
	return os.Rename(path+".pending", path)
}
