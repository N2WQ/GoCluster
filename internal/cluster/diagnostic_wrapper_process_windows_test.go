package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

type diagInvocation struct {
	command        *exec.Cmd
	context        context.Context
	cancel         context.CancelFunc
	done           chan error
	output         diagOutput
	waited         bool
	waitErr        error
	child          windows.Handle
	joined         bool
	status         uint32
	closed         bool
	started        bool
	job            windows.Handle
	jobJoined      bool
	jobClosed      bool
	accounting     diagJobAccounting
	forcedCleanup  bool
	supervisor     windows.Handle
	powerShell     windows.Handle
	adopted        bool
	admissionErr   error
	aborted        bool
	admissionStage string
}

type diagOutput struct{ bytes.Buffer }

func (output *diagOutput) Write(value []byte) (int, error) {
	if len(value) > 256*1024-output.Len() {
		return 0, fmt.Errorf("diagnostic fixture output exceeds 256 KiB")
	}
	return output.Buffer.Write(value)
}

// One parent owns supervisor Start/Wait, its output worker, a descendant job,
// and one authenticated child handle. The supervisor joins the job before
// launching PowerShell. No cleanup targets a raw PID or an unverified image.
func startDiagInvocation(t *testing.T, fixture diagWrapperFixture, status int, stall bool) *diagInvocation {
	t.Helper()
	invocation := launchDiagInvocation(t, fixture, status, stall)
	if err := invocation.admitChild(fixture); err != nil {
		t.Fatal(err)
	}
	return invocation
}

func (invocation *diagInvocation) admitChild(fixture diagWrapperFixture) error {
	if err := invocation.checkAdmissionContext("child startup"); err != nil {
		return err
	}
	if invocation.admissionErr != nil {
		return invocation.admissionErr
	}
	start, err := invocation.awaitStart(fixture)
	if err != nil {
		return err
	}
	if start.Nonce != fixture.nonce || start.PID == 0 || start.PID == uint32(os.Getpid()) ||
		!diagEqualPath(start.Executable, fixture.executable) {
		return fmt.Errorf("unauthenticated diagnostic child receipt: %+v", start)
	}
	invocation.child, err = diagOpenOwnedProcess(start.PID, fixture.executable)
	if err != nil {
		return err
	}
	if !diagEqualPath(start.Directory, invocation.command.Dir) {
		return fmt.Errorf("diagnostic wrapper changed the caller directory: %+v", start)
	}
	if fixture.fault == "powershell_loss_before_release" {
		if err := diagPublishMarker(fixture.private, "stop-powershell", fixture.nonce); err != nil {
			return err
		}
		event, err := windows.WaitForSingleObject(invocation.powerShell, 2000)
		if err != nil {
			return fmt.Errorf("controlled PowerShell loss wait: %w", err)
		}
		if event != windows.WAIT_OBJECT_0 {
			return fmt.Errorf("controlled PowerShell loss did not reach the final admission gate: wait=%d", event)
		}
	}
	var identities [3]uint32
	for i, handle := range []windows.Handle{invocation.supervisor, invocation.powerShell, invocation.child} {
		if err := diagRequireLiveMember(handle, invocation.job); err != nil {
			return err
		}
		id, err := windows.GetProcessId(handle)
		if err != nil {
			return fmt.Errorf("diagnostic role identity query: %w", err)
		}
		if id == 0 || id == uint32(os.Getpid()) {
			return fmt.Errorf("invalid diagnostic role identity: pid=%d", id)
		}
		for _, previous := range identities[:i] {
			if id == previous {
				return fmt.Errorf("diagnostic roles share process identity %d", id)
			}
		}
		identities[i] = id
	}
	invocation.admissionStage = "child-verified"
	if fixture.fault == "cancel_before_release" {
		invocation.cancel()
	}
	if err := invocation.checkAdmissionContext("child release"); err != nil {
		return err
	}
	if err := diagPublishMarker(fixture.private, "release", fixture.nonce); err != nil {
		return err
	}
	invocation.admissionStage = "released"
	return nil
}

func launchDiagInvocation(t *testing.T, fixture diagWrapperFixture, status int, stall bool) *diagInvocation {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	invocation := &diagInvocation{context: ctx, cancel: cancel, done: make(chan error, 1)}
	var err error
	invocation.job, err = newDiagJob(fixture.nonce)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { invocation.retire(t) })
	image := os.Getenv(diagPowerShellIdentity)
	if !filepath.IsAbs(image) {
		t.Fatal("diagnostic tests require the actual PowerShell identity from scripts/test-windows.ps1")
	}
	command := exec.CommandContext(ctx, image, "-NoProfile", "-NonInteractive", "-File", fixture.supervisor)
	working := filepath.Join(fixture.private, "working directory")
	if err := os.MkdirAll(working, 0o700); err != nil {
		cancel()
		t.Fatal(err)
	}
	command.Dir = working
	command.Env = append(os.Environ(), "GOGC=123", "DXC_PPROF_ADDR=parent.invalid:6069",
		"DXC_DIAG_WRAPPER_PRIVATE="+fixture.private,
		"DXC_DIAG_WRAPPER_NONCE="+fixture.nonce, fmt.Sprintf("DXC_DIAG_WRAPPER_STATUS=%d", status),
		fmt.Sprintf("DXC_DIAG_WRAPPER_STALL=%d", diagBoolInt(stall)), "DXC_DIAG_WRAPPER_SCRIPT="+fixture.script,
		fmt.Sprintf("DXC_DIAG_WRAPPER_REJECT_START=%d", diagBoolInt(fixture.rejectStart)),
		"DXC_DIAG_WRAPPER_FAULT="+fixture.fault)
	switch fixture.fault {
	case "identity_missing":
		command.Env = append(command.Env, diagPowerShellIdentity+"=")
	case "identity_mismatch":
		command.Env = append(command.Env, diagPowerShellIdentity+"="+fixture.script)
	}
	command.Stdout, command.Stderr = &invocation.output, &invocation.output
	// A short output-pipe wait leaves a clear interval in the stalled fixture
	// between PowerShell cancellation and the child's six-second watchdog.
	command.WaitDelay = 250 * time.Millisecond
	invocation.command = command
	if err := command.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	invocation.started = true
	invocation.supervisor, err = diagOpenOwnedProcessAccess(uint32(command.Process.Pid), image,
		windows.SYNCHRONIZE|windows.PROCESS_QUERY_LIMITED_INFORMATION|windows.PROCESS_DUP_HANDLE|windows.PROCESS_TERMINATE)
	go func() { invocation.done <- command.Wait() }()
	if err != nil {
		invocation.admissionErr = err
	} else {
		invocation.admissionStage = "launched"
		invocation.admissionErr = invocation.adoptPowerShell(fixture)
	}
	if invocation.admissionErr != nil {
		invocation.admissionErr = errors.Join(invocation.admissionErr, invocation.abortJob())
	}
	return invocation
}

func (invocation *diagInvocation) adoptPowerShell(fixture diagWrapperFixture) error {
	name, stage := "powershell.json", "verified"
	if fixture.fault == "owner_loss_after_create" {
		name, stage = "created.json", "created"
	}
	path := filepath.Join(fixture.private, name)
	if fixture.fault == "cancel_before_receipt" {
		// Keep a genuine available receipt at the cancellation boundary.
		if _, err := invocation.awaitReceipt(path); err != nil {
			return err
		}
		invocation.admissionStage = "before-receipt"
		invocation.cancel()
	}
	body, err := invocation.awaitReceipt(path)
	if err != nil {
		return err
	}
	owner, err := diagDecodeOwner(body, fixture.nonce, stage)
	if err != nil {
		return err
	}
	invocation.admissionStage = "parsed"
	if fixture.fault == "cancel_after_parse" {
		invocation.cancel()
	}
	if err := invocation.checkAdmissionContext("native handle duplication"); err != nil {
		return err
	}
	source := invocation.supervisor
	if fixture.fault == "duplicate_failure" {
		source = 0
	}
	access := uint32(windows.SYNCHRONIZE | windows.PROCESS_QUERY_LIMITED_INFORMATION)
	if strings.HasPrefix(fixture.fault, "owner_loss_") {
		// The fault controller retains a native witness even if a deliberately
		// broken launcher puts its suspended process outside the target job.
		access |= windows.PROCESS_TERMINATE
	}
	sourceHandle := windows.Handle(owner.Handle)
	if fixture.fault == "source_identity" {
		sourceHandle = windows.CurrentProcess()
	}
	if err := windows.DuplicateHandle(source, sourceHandle, windows.CurrentProcess(),
		&invocation.powerShell, access, false, 0); err != nil {
		return fmt.Errorf("duplicate owned diagnostic PowerShell handle: %w", err)
	}
	invocation.admissionStage = "duplicated"
	if fixture.fault == "cancel_after_duplicate" {
		invocation.cancel()
	}
	if err := invocation.checkAdmissionContext("native handle verification"); err != nil {
		return err
	}
	if err := diagVerifyImage(invocation.powerShell, os.Getenv(diagPowerShellIdentity)); err != nil {
		return err
	}
	id, err := windows.GetProcessId(invocation.powerShell)
	if err != nil {
		return fmt.Errorf("diagnostic PowerShell identity query: %w", err)
	}
	// Supervisor and wrapper share an image but must name distinct processes.
	if id == uint32(invocation.command.Process.Pid) || id == uint32(os.Getpid()) || id == 0 {
		return fmt.Errorf("diagnostic PowerShell ownership aliases its supervisor or parent: pid=%d", id)
	}
	for _, handle := range []windows.Handle{invocation.supervisor, invocation.powerShell} {
		if err := diagRequireLiveMember(handle, invocation.job); err != nil {
			return err
		}
	}
	invocation.admissionStage = "verified"
	if strings.HasPrefix(fixture.fault, "owner_loss_") {
		if err := windows.TerminateProcess(invocation.supervisor, 98); err != nil {
			return err
		}
		return fmt.Errorf("injected supervisor loss at %s", fixture.fault)
	}
	if fixture.fault == "cancel_after_verify" {
		invocation.cancel()
	}
	if err := invocation.checkAdmissionContext("native ownership acknowledgment"); err != nil {
		return err
	}
	if err := diagPublishMarker(fixture.private, "adopted", fixture.nonce); err != nil {
		return err
	}
	invocation.adopted = true
	invocation.admissionStage = "adopted"
	return nil
}

func (invocation *diagInvocation) checkAdmissionContext(phase string) error {
	if err := invocation.context.Err(); err != nil {
		return fmt.Errorf("diagnostic admission cancellation at %s: %w", phase, err)
	}
	return nil
}

func diagPublishMarker(directory, name, nonce string) error {
	path := filepath.Join(directory, name)
	if err := os.WriteFile(path+".pending", []byte(nonce), 0o600); err != nil {
		return err
	}
	return os.Rename(path+".pending", path)
}

func (invocation *diagInvocation) awaitReceipt(path string) ([]byte, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := invocation.checkAdmissionContext(filepath.Base(path)); err != nil {
			return nil, err
		}
		body, err := diagReadReceipt(path)
		if err == nil {
			if err := invocation.checkAdmissionContext(filepath.Base(path)); err != nil {
				return nil, err
			}
			return body, nil
		}
		if !os.IsNotExist(err) && !errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
			return nil, err
		}
		select {
		case err := <-invocation.done:
			invocation.waited, invocation.waitErr = true, err
			if body, readErr := diagReadReceipt(path); readErr == nil {
				if contextErr := invocation.checkAdmissionContext(filepath.Base(path)); contextErr != nil {
					return nil, contextErr
				}
				return body, nil
			}
			if err != nil {
				return nil, fmt.Errorf("supervisor ended before diagnostic receipt %s: %w; %s", filepath.Base(path), err, invocation.outputText())
			}
			return nil, fmt.Errorf("supervisor ended without diagnostic receipt %s; %s", filepath.Base(path), invocation.outputText())
		case <-invocation.context.Done():
			return nil, fmt.Errorf("diagnostic receipt %s: %w", filepath.Base(path), invocation.context.Err())
		case <-ticker.C:
		}
	}
}

func diagReadReceipt(path string) (body []byte, resultErr error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	body, err = io.ReadAll(io.LimitReader(file, 4097))
	if err != nil {
		return nil, err
	}
	if len(body) > 4096 {
		return nil, fmt.Errorf("diagnostic receipt exceeds 4096 bytes")
	}
	return body, nil
}

func (invocation *diagInvocation) abortJob() error {
	if invocation.job == 0 || invocation.jobClosed {
		return fmt.Errorf("diagnostic abort has no owned job")
	}
	if err := windows.TerminateJobObject(invocation.job, 97); err != nil {
		return err
	}
	invocation.aborted = true
	return nil
}

func diagBoolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (invocation *diagInvocation) awaitStart(fixture diagWrapperFixture) (diagChildStart, error) {
	var start diagChildStart
	body, err := invocation.awaitReceipt(filepath.Join(fixture.private, "start.json"))
	if err != nil {
		return start, err
	}
	if err := json.Unmarshal(body, &start); err != nil {
		return start, fmt.Errorf("invalid diagnostic child receipt: %w", err)
	}
	return start, nil
}

func diagOpenOwnedProcess(pid uint32, executable string) (windows.Handle, error) {
	access := uint32(windows.SYNCHRONIZE | windows.PROCESS_QUERY_LIMITED_INFORMATION | windows.PROCESS_TERMINATE)
	return diagOpenOwnedProcessAccess(pid, executable, access)
}

func diagOpenOwnedProcessAccess(pid uint32, executable string, access uint32) (windows.Handle, error) {
	handle, err := windows.OpenProcess(access, false, pid)
	if err != nil {
		return 0, err
	}
	if err := diagVerifyImage(handle, executable); err != nil {
		return 0, errors.Join(err, windows.CloseHandle(handle))
	}
	return handle, nil
}

func diagEqualPath(a, b string) bool {
	return strings.EqualFold(filepath.Clean(a), filepath.Clean(b))
}

func (invocation *diagInvocation) outputText() string {
	if !invocation.waited {
		return "supervisor output worker is not joined"
	}
	return invocation.output.String()
}

func (invocation *diagInvocation) waitSupervisor() error {
	if !invocation.started {
		return nil
	}
	if invocation.waited {
		return invocation.waitErr
	}
	select {
	case err := <-invocation.done:
		invocation.waited, invocation.waitErr = true, err
		return err
	case <-time.After(14 * time.Second):
		return fmt.Errorf("supervisor process wait exceeded fixture bound")
	}
}

func (invocation *diagInvocation) joinChild() error {
	if invocation.child == 0 || invocation.joined {
		return nil
	}
	event, err := windows.WaitForSingleObject(invocation.child, uint32((diagChildWatchdog+2*time.Second)/time.Millisecond))
	if err != nil {
		return err
	}
	if event != windows.WAIT_OBJECT_0 {
		return fmt.Errorf("diagnostic child kernel wait incomplete: %d", event)
	}
	invocation.joined = true
	return windows.GetExitCodeProcess(invocation.child, &invocation.status)
}

// Normal completion joins the supervisor and all job members before return.
// Rejected startup receipts still have job ownership. Forced cleanup only
// targets owned handles and always fails qualification; no confirmation is
// manufactured from cancellation or a failed handle close.
func (invocation *diagInvocation) retire(t *testing.T) {
	t.Helper()
	invocation.cancel()
	waitErr := invocation.waitSupervisor()
	var exit *exec.ExitError
	if waitErr != nil && !errors.As(waitErr, &exit) {
		t.Errorf("diagnostic supervisor wait: %v", waitErr)
	}
	if invocation.started && !invocation.waited {
		t.Error("diagnostic supervisor worker was not joined")
	}
	if err := invocation.joinChild(); err != nil {
		t.Errorf("diagnostic child retirement requires forced cleanup: %v", err)
		if invocation.child != 0 && !invocation.joined {
			invocation.forcedCleanup = true
			if err := windows.TerminateProcess(invocation.child, 97); err != nil {
				t.Errorf("owned diagnostic child termination: %v", err)
			}
			if err := invocation.joinChild(); err != nil {
				t.Errorf("forced diagnostic child kernel wait: %v", err)
			}
		}
	}
	if invocation.child != 0 && !invocation.closed {
		if err := windows.CloseHandle(invocation.child); err != nil {
			t.Errorf("diagnostic child handle close: %v", err)
			diagFixtureRetirementBlocked = true
		} else {
			invocation.closed = true
		}
	}
	for _, reference := range []*windows.Handle{&invocation.powerShell, &invocation.supervisor} {
		if *reference != 0 {
			if err := windows.CloseHandle(*reference); err != nil {
				t.Errorf("diagnostic ownership reference close: %v", err)
				diagFixtureRetirementBlocked = true
			} else {
				*reference = 0
			}
		}
	}
	// Release the confirmed child's final process reference before inspecting
	// job accounting; zero active membership must follow actual handle release.
	invocation.retireJob(t)
	if invocation.started && !invocation.waited {
		// Descendant retirement can release inherited output handles that held
		// the supervisor's Wait worker. Rejoin before any stable path reuse.
		if err := invocation.waitSupervisor(); err != nil && !errors.As(err, &exit) {
			t.Errorf("diagnostic supervisor wait after descendant retirement: %v", err)
		}
		if !invocation.waited {
			t.Error("diagnostic supervisor retirement remains unconfirmed")
			diagFixtureRetirementBlocked = true
		}
	}
}

func (invocation *diagInvocation) retireJob(t *testing.T) {
	t.Helper()
	if invocation.job == 0 || invocation.jobClosed {
		return
	}
	accounting, err := diagAwaitJob(invocation.job)
	if err != nil {
		t.Errorf("diagnostic job retirement requires forced cleanup: %v", err)
		invocation.forcedCleanup = true
		if err := windows.TerminateJobObject(invocation.job, 97); err != nil {
			t.Errorf("owned diagnostic job termination: %v", err)
		}
		accounting, err = diagAwaitJob(invocation.job)
	}
	invocation.accounting = accounting
	invocation.jobJoined = err == nil && accounting.ActiveProcesses == 0
	if !invocation.jobJoined {
		t.Errorf("diagnostic job retirement remains unconfirmed: %v", err)
		diagFixtureRetirementBlocked = true
	}
	if err := windows.CloseHandle(invocation.job); err != nil {
		t.Errorf("diagnostic job handle close: %v", err)
		diagFixtureRetirementBlocked = true
	} else {
		invocation.jobClosed = true
	}
}

func diagExitCode(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("native PowerShell execution failure: %v", err)
	}
	return exit.ExitCode()
}
