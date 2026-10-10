package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

type diagJobAccounting struct {
	UserTime, KernelTime, PeriodUserTime, PeriodKernelTime           int64
	PageFaults, TotalProcesses, ActiveProcesses, TerminatedProcesses uint32
}

type diagSupervisorResult struct {
	Nonce   string
	Status  int
	Context string
	Error   string
}

// Parent-created named jobs own the supervisor before it can start PowerShell.
// No breakaway is allowed. Kernel accounting covers even a rejected/missing
// child receipt; authenticated child handles provide the stronger normal witness.
func newDiagJob(nonce string) (windows.Handle, error) {
	name := "Local\\GoClusterDiagWrapper-" + nonce
	encoded, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return 0, err
	}
	// Use the raw return's last error to reject an existing named owner.
	create := windows.NewLazySystemDLL("kernel32.dll").NewProc("CreateJobObjectW")
	value, _, callErr := create.Call(0, uintptr(unsafe.Pointer(encoded)))
	if value == 0 {
		return 0, fmt.Errorf("create diagnostic job: %w", callErr)
	}
	handle := windows.Handle(value)
	if errors.Is(callErr, windows.ERROR_ALREADY_EXISTS) {
		return 0, errors.Join(fmt.Errorf("diagnostic job name collision"), windows.CloseHandle(handle))
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	_, err = windows.SetInformationJobObject(handle, windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits)))
	if err != nil {
		return 0, errors.Join(err, windows.CloseHandle(handle))
	}
	return handle, nil
}

func diagJoinParentJob(name string) error {
	encoded, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return err
	}
	open := windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenJobObjectW")
	const assignProcess = 1
	value, _, callErr := open.Call(assignProcess, 0, uintptr(unsafe.Pointer(encoded)))
	if value == 0 {
		return fmt.Errorf("open parent diagnostic job: %w", callErr)
	}
	handle := windows.Handle(value)
	err = windows.AssignProcessToJobObject(handle, windows.CurrentProcess())
	return errors.Join(err, windows.CloseHandle(handle))
}

func diagJobSnapshot(handle windows.Handle) (diagJobAccounting, error) {
	var accounting diagJobAccounting
	err := windows.QueryInformationJobObject(handle, windows.JobObjectBasicAccountingInformation,
		uintptr(unsafe.Pointer(&accounting)), uint32(unsafe.Sizeof(accounting)), nil)
	return accounting, err
}

func diagAwaitJob(handle windows.Handle) (diagJobAccounting, error) {
	deadline := time.Now().Add(diagChildWatchdog + 2*time.Second)
	for {
		accounting, err := diagJobSnapshot(handle)
		if err != nil || accounting.ActiveProcesses == 0 {
			return accounting, err
		}
		if !time.Now().Before(deadline) {
			return accounting, fmt.Errorf("diagnostic job retains %d active processes", accounting.ActiveProcesses)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// This test-only supervisor closes launch/assignment races by joining the
// parent job before any PowerShell process exists. It owns the actual native
// -File child wait and records causal status/context separately from owner exit.
func runDiagSupervisor() (int, error) {
	private := os.Getenv("DXC_DIAG_WRAPPER_PRIVATE")
	nonce := os.Getenv("DXC_DIAG_WRAPPER_NONCE")
	if !filepath.IsAbs(private) || len(nonce) != 32 {
		return 95, fmt.Errorf("invalid private diagnostic supervisor ownership")
	}
	if err := diagJoinParentJob("Local\\GoClusterDiagWrapper-" + nonce); err != nil {
		return 95, err // Fail closed before PowerShell launch.
	}
	budget := 8 * time.Second
	if os.Getenv("DXC_DIAG_WRAPPER_STALL") == "1" {
		budget = 4 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	command := exec.CommandContext(ctx, "pwsh", "-NoProfile", "-NonInteractive", "-File", os.Getenv("DXC_DIAG_WRAPPER_SCRIPT"))
	command.Dir = filepath.Join(private, "working directory")
	command.Env = append(os.Environ(), "DXC_DIAG_WRAPPER_CHILD=1")
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	command.WaitDelay = 250 * time.Millisecond
	runErr := command.Run()
	result := diagSupervisorResult{Nonce: nonce}
	if runErr != nil {
		result.Error = runErr.Error()
		var exit *exec.ExitError
		if errors.As(runErr, &exit) {
			result.Status = exit.ExitCode()
		} else {
			result.Status = 95
		}
	}
	if ctx.Err() != nil {
		result.Context = ctx.Err().Error()
	}
	if err := diagWriteJSON(filepath.Join(private, "supervisor.json"), result); err != nil {
		return 95, err
	}
	if result.Context != "" {
		return 94, nil // Context failure is never credited as a child status.
	}
	return result.Status, nil
}
