package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

type diagInvocation struct {
	command       *exec.Cmd
	context       context.Context
	cancel        context.CancelFunc
	done          chan error
	output        bytes.Buffer
	waited        bool
	waitErr       error
	child         windows.Handle
	joined        bool
	status        uint32
	closed        bool
	started       bool
	job           windows.Handle
	jobJoined     bool
	jobClosed     bool
	accounting    diagJobAccounting
	forcedCleanup bool
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
	release := filepath.Join(fixture.private, "release")
	if err := os.WriteFile(release+".pending", []byte(fixture.nonce), 0o600); err != nil {
		return err
	}
	if err := os.Rename(release+".pending", release); err != nil {
		return err
	}
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
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, executable)
	working := filepath.Join(fixture.private, "working directory")
	if err := os.MkdirAll(working, 0o700); err != nil {
		cancel()
		t.Fatal(err)
	}
	command.Dir = working
	command.Env = append(os.Environ(), "GOGC=123", "DXC_PPROF_ADDR=parent.invalid:6069",
		"DXC_DIAG_WRAPPER_CHILD=supervisor", "DXC_DIAG_WRAPPER_PRIVATE="+fixture.private,
		"DXC_DIAG_WRAPPER_NONCE="+fixture.nonce, fmt.Sprintf("DXC_DIAG_WRAPPER_STATUS=%d", status),
		fmt.Sprintf("DXC_DIAG_WRAPPER_STALL=%d", diagBoolInt(stall)), "DXC_DIAG_WRAPPER_SCRIPT="+fixture.script,
		fmt.Sprintf("DXC_DIAG_WRAPPER_REJECT_START=%d", diagBoolInt(fixture.rejectStart)))
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
	go func() { invocation.done <- command.Wait() }()
	return invocation
}

func diagBoolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func (invocation *diagInvocation) awaitStart(fixture diagWrapperFixture) (diagChildStart, error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		var start diagChildStart
		body, err := os.ReadFile(filepath.Join(fixture.private, "start.json"))
		if err == nil && json.Unmarshal(body, &start) == nil {
			return start, nil
		}
		if err != nil && !os.IsNotExist(err) && !errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
			return start, err
		}
		select {
		case err := <-invocation.done:
			invocation.waited, invocation.waitErr = true, err
			if err == nil {
				err = errors.New("supervisor exited before startup receipt")
			}
			return start, fmt.Errorf("supervisor ended before diagnostic child ownership: %w; %s", err, invocation.outputText())
		case <-invocation.context.Done():
			return start, fmt.Errorf("diagnostic child startup: %w", invocation.context.Err())
		case <-ticker.C:
		}
	}
}

func diagOpenOwnedProcess(pid uint32, executable string) (windows.Handle, error) {
	access := uint32(windows.SYNCHRONIZE | windows.PROCESS_QUERY_LIMITED_INFORMATION | windows.PROCESS_TERMINATE)
	handle, err := windows.OpenProcess(access, false, pid)
	if err != nil {
		return 0, err
	}
	var path [32768]uint16
	length := uint32(len(path))
	if err := windows.QueryFullProcessImageName(handle, 0, &path[0], &length); err != nil {
		return 0, errors.Join(err, windows.CloseHandle(handle))
	}
	actual := windows.UTF16ToString(path[:length])
	if !diagEqualPath(actual, executable) {
		return 0, errors.Join(fmt.Errorf("diagnostic child image %q, want %q", actual, executable), windows.CloseHandle(handle))
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
