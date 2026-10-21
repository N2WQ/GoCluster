package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

type diagJobAccounting struct {
	UserTime, KernelTime, PeriodUserTime, PeriodKernelTime           int64
	PageFaults, TotalProcesses, ActiveProcesses, TerminatedProcesses uint32
}

type diagSupervisorResult struct {
	Nonce              string
	Status             int
	Context            string
	Error              string
	Image              string
	Stage              string
	NativeJoined       bool
	NativeClosed       bool
	HandleListExcluded bool
}

type diagPowerShellOwner struct {
	Nonce  string
	Handle uint64
	Stage  string
}

const (
	diagPowerShellIdentity = "GOCLUSTER_TEST_POWERSHELL"
	diagJobAssign          = 1
	diagJobQuery           = 4
)

var diagKernel = windows.NewLazySystemDLL("kernel32.dll")

// The parent retains the kill-on-close owner through complete retirement.
// Neither the job handle nor process-ownership handles enter the stdio allowlist.
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

func diagOpenJob(nonce string, access uint32) (windows.Handle, error) {
	name := "Local\\GoClusterDiagWrapper-" + nonce
	encoded, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return 0, err
	}
	open := windows.NewLazySystemDLL("kernel32.dll").NewProc("OpenJobObjectW")
	value, _, callErr := open.Call(uintptr(access), 0, uintptr(unsafe.Pointer(encoded)))
	if value == 0 {
		return 0, fmt.Errorf("open parent diagnostic job: %w", callErr)
	}
	return windows.Handle(value), nil
}

func diagRequireMember(process, job windows.Handle) error {
	query := diagKernel.NewProc("IsProcessInJob")
	if err := query.Find(); err != nil {
		return err
	}
	var member int32
	ok, _, queryErr := query.Call(uintptr(process), uintptr(job), uintptr(unsafe.Pointer(&member)))
	if ok == 0 {
		return fmt.Errorf("diagnostic job membership query: %w", queryErr)
	}
	if member == 0 {
		return fmt.Errorf("diagnostic process is outside its intended job")
	}
	return nil
}

func diagRequireLiveMember(process, job windows.Handle) error {
	event, err := windows.WaitForSingleObject(process, 0)
	if err != nil {
		return fmt.Errorf("diagnostic process liveness query: %w", err)
	}
	if event != uint32(windows.WAIT_TIMEOUT) {
		return fmt.Errorf("diagnostic process is not live: wait=%d", event)
	}
	return diagRequireMember(process, job)
}

func diagVerifyImage(process windows.Handle, expected string) error {
	var path [32768]uint16
	length := uint32(len(path))
	if err := windows.QueryFullProcessImageName(process, 0, &path[0], &length); err != nil {
		return err
	}
	actual := windows.UTF16ToString(path[:length])
	if !diagEqualPath(actual, expected) {
		return fmt.Errorf("diagnostic process image %q, want %q", actual, expected)
	}
	return nil
}

func diagDecodeOwner(body []byte, nonce, stage string) (diagPowerShellOwner, error) {
	var owner diagPowerShellOwner
	if len(body) == 0 || len(body) > 4096 {
		return owner, fmt.Errorf("invalid diagnostic ownership receipt size")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&owner); err != nil {
		return owner, fmt.Errorf("invalid diagnostic ownership receipt: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return owner, fmt.Errorf("trailing diagnostic ownership receipt data")
	}
	// Pseudo process/thread/token handles are not transferable ownership.
	if len(nonce) != 32 || owner.Nonce != nonce || owner.Stage != stage ||
		owner.Handle == 0 || owner.Handle >= uint64(^uintptr(0))-15 {
		return owner, fmt.Errorf("unauthenticated diagnostic PowerShell ownership receipt")
	}
	return owner, nil
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
