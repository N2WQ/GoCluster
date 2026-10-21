package cluster

import (
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

func TestDiagnosticWrapperNativeEnvironmentGCAndStatus(t *testing.T) {
	for _, test := range []struct {
		name   string
		gc     int
		status int
		wantGC uint64
	}{
		{"positive", 77, 0, 77},
		{"zero", 0, 0, 50},
		{"child-failure", 77, 23, 77},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDiagWrapperFixture(t, "native wrapper "+test.name, test.gc, true, false)
			invocation := startDiagInvocation(t, fixture, test.status, false)
			waitErr := invocation.waitSupervisor()
			contextErr := invocation.context.Err()
			invocation.retire(t)
			if contextErr != nil || diagExitCode(t, waitErr) != test.status ||
				!invocation.joined || invocation.status != uint32(test.status) || !invocation.closed {
				t.Fatalf("native status/retirement mismatch: PS=%v context=%v child=%d joined=%t closed=%t; %s",
					waitErr, contextErr, invocation.status, invocation.joined, invocation.closed, invocation.outputText())
			}
			owner := readDiagSupervisorResult(t, fixture)
			if owner.Status != test.status || owner.Context != "" || !invocation.jobJoined || !invocation.jobClosed ||
				invocation.accounting.ActiveProcesses != 0 || invocation.accounting.TotalProcesses < 3 {
				t.Fatalf("native PowerShell causal status/job retirement mismatch: %+v / %+v", owner, invocation.accounting)
			}
			result := readDiagChildResult(t, fixture)
			if result.GOGC != "50" || result.Address != "localhost:6061" || result.InitialGC != 50 ||
				result.LoadedGC != test.gc || result.EffectiveGC != test.wantGC || result.Status != test.status ||
				result.Arguments != 1 || !diagEqualPath(result.Config, filepath.Join(fixture.private, "config")) {
				t.Fatalf("actual environment/loader/runtime consumer mismatch: %+v", result)
			}
		})
	}
}

func TestDiagnosticWrapperDroppedExitControl(t *testing.T) {
	fixture := newDiagWrapperFixture(t, "dropped exit", 77, true, true)
	invocation := startDiagInvocation(t, fixture, 23, false)
	waitErr := invocation.waitSupervisor()
	contextErr := invocation.context.Err()
	invocation.retire(t)
	result := readDiagChildResult(t, fixture)
	owner := readDiagSupervisorResult(t, fixture)
	if contextErr != nil || diagExitCode(t, waitErr) != 0 || invocation.status != 23 ||
		!invocation.joined || !invocation.closed || result.Status != 23 || owner.Status != 0 || owner.Context != "" ||
		!invocation.jobJoined || !invocation.jobClosed {
		t.Fatalf("removing only propagation did not falsify the checker: PS=%v context=%v child=%d result=%+v; %s",
			waitErr, contextErr, invocation.status, result, invocation.outputText())
	}
}

func TestDiagnosticWrapperMissingExecutable(t *testing.T) {
	fixture := newDiagWrapperFixture(t, "never populated", 0, false, false)
	invocation := launchDiagInvocation(t, fixture, 0, false)
	waitErr := invocation.waitSupervisor()
	contextErr := invocation.context.Err()
	invocation.retire(t)
	output := invocation.outputText()
	owner := readDiagSupervisorResult(t, fixture)
	if contextErr != nil || diagExitCode(t, waitErr) != 1 || !strings.Contains(output, "Unable to launch gocluster:") ||
		!strings.Contains(output, "gocluster.exe") || !strings.Contains(output, fixture.root) ||
		owner.Status != 1 || owner.Context != "" || !invocation.jobJoined || !invocation.jobClosed {
		t.Fatalf("missing executable was not a causal unsuccessful launch: %v / %v / %s", waitErr, contextErr, output)
	}
	for _, name := range []string{"start.json", "result.json", "watchdog.json"} {
		if _, err := os.Stat(filepath.Join(fixture.private, name)); !os.IsNotExist(err) {
			t.Fatalf("missing executable produced unexpected child receipt %s: %v", name, err)
		}
	}
	if _, err := os.Stat(fixture.executable); !os.IsNotExist(err) {
		t.Fatalf("missing fixture acquired an executable: %v", err)
	}
}

func TestDiagnosticWrapperTimeoutRetiresChild(t *testing.T) {
	fixture := newDiagWrapperFixture(t, "stalled child", 77, true, false)
	invocation := startDiagInvocation(t, fixture, 0, true)
	event, err := windows.WaitForSingleObject(invocation.child, 0)
	if err != nil || event != uint32(windows.WAIT_TIMEOUT) {
		t.Fatalf("stalled child was not live after authenticated release: %d / %v", event, err)
	}
	waitErr := invocation.waitSupervisor()
	contextErr := invocation.context.Err()
	// The child still holding the output handles must survive this parent-only
	// cancellation; PowerShell completion is deliberately insufficient evidence.
	event, liveErr := windows.WaitForSingleObject(invocation.child, 0)
	invocation.retire(t)
	owner := readDiagSupervisorResult(t, fixture)
	if contextErr != nil || owner.Context != context.DeadlineExceeded.Error() ||
		diagExitCode(t, waitErr) != 94 || liveErr != nil ||
		event != uint32(windows.WAIT_TIMEOUT) || !invocation.joined || invocation.status != 96 || !invocation.closed ||
		!invocation.jobJoined || !invocation.jobClosed {
		t.Fatalf("parent timeout/child watchdog retirement mismatch: PS=%v context=%v before-child-wait=%d/%v child=%d joined=%t closed=%t",
			waitErr, contextErr, event, liveErr, invocation.status, invocation.joined, invocation.closed)
	}
	var watchdog struct {
		Nonce  string
		Status int
	}
	diagReadJSON(t, filepath.Join(fixture.private, "watchdog.json"), &watchdog)
	if watchdog.Nonce != fixture.nonce || watchdog.Status != 96 {
		t.Fatalf("actual child watchdog receipt mismatch: %+v", watchdog)
	}
	if _, err := os.Stat(filepath.Join(fixture.private, "result.json")); !os.IsNotExist(err) {
		t.Fatalf("stalled child unexpectedly reached the configuration result: %v", err)
	}
}

func TestDiagnosticWrapperRejectedStartupRetiresJob(t *testing.T) {
	fixture := newDiagWrapperFixture(t, "rejected startup", 77, true, false)
	fixture.rejectStart = true
	invocation := launchDiagInvocation(t, fixture, 0, false)
	admissionErr := invocation.admitChild(fixture)
	before, snapshotErr := diagJobSnapshot(invocation.job)
	invocation.retire(t)
	if admissionErr == nil || !strings.Contains(admissionErr.Error(), "unauthenticated diagnostic child receipt") ||
		snapshotErr != nil || before.ActiveProcesses < 3 || invocation.child != 0 || !invocation.waited ||
		!invocation.jobJoined || !invocation.jobClosed || invocation.accounting.ActiveProcesses != 0 ||
		invocation.accounting.TotalProcesses < 3 || invocation.forcedCleanup {
		t.Fatalf("startup rejection omitted descendant retirement: admission=%v before=%+v/%v after=%+v child=%d owner-waited=%t job-joined=%t job-closed=%t",
			admissionErr, before, snapshotErr, invocation.accounting, invocation.child, invocation.waited,
			invocation.jobJoined, invocation.jobClosed)
	}
	var watchdog struct {
		Nonce  string
		Status int
	}
	diagReadJSON(t, filepath.Join(fixture.private, "watchdog.json"), &watchdog)
	if watchdog.Nonce != fixture.nonce || watchdog.Status != 96 {
		t.Fatalf("rejected live child did not actually retire through its watchdog: %+v", watchdog)
	}
	if _, err := os.Stat(filepath.Join(fixture.private, "result.json")); !os.IsNotExist(err) {
		t.Fatalf("rejected child unexpectedly passed the release barrier: %v", err)
	}
}

func readDiagSupervisorResult(t *testing.T, fixture diagWrapperFixture) diagSupervisorResult {
	t.Helper()
	var result diagSupervisorResult
	diagReadJSON(t, filepath.Join(fixture.private, "supervisor.json"), &result)
	if result.Nonce != fixture.nonce {
		t.Fatal("PowerShell supervisor receipt does not belong to this invocation")
	}
	return result
}

func readDiagChildResult(t *testing.T, fixture diagWrapperFixture) diagChildResult {
	t.Helper()
	var result diagChildResult
	diagReadJSON(t, filepath.Join(fixture.private, "result.json"), &result)
	if result.Nonce != fixture.nonce {
		t.Fatal("normal result does not belong to this child")
	}
	return result
}

func diagReadJSON(t *testing.T, path string, value any) {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(body, value); err != nil {
		t.Fatalf("invalid diagnostic child receipt %s: %v", path, err)
	}
}

func TestDiagnosticWrapperHandleReceipt(t *testing.T) {
	const nonce = "0123456789abcdef0123456789abcdef"
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"valid", `{"Nonce":"0123456789abcdef0123456789abcdef","Handle":4,"Stage":"verified"}`, true},
		{"missing", `{}`, false},
		{"wrong_nonce", `{"Nonce":"11111111111111111111111111111111","Handle":4,"Stage":"verified"}`, false},
		{"wrong_stage", `{"Nonce":"0123456789abcdef0123456789abcdef","Handle":4,"Stage":"created"}`, false},
		{"unknown_field", `{"Nonce":"0123456789abcdef0123456789abcdef","Handle":4,"Stage":"verified","PID":123}`, false},
		{"string_handle", `{"Nonce":"0123456789abcdef0123456789abcdef","Handle":"4","Stage":"verified"}`, false},
		{"fractional_handle", `{"Nonce":"0123456789abcdef0123456789abcdef","Handle":4.5,"Stage":"verified"}`, false},
		{"negative_handle", `{"Nonce":"0123456789abcdef0123456789abcdef","Handle":-1,"Stage":"verified"}`, false},
		{"null_handle", `{"Nonce":"0123456789abcdef0123456789abcdef","Handle":0,"Stage":"verified"}`, false},
		{"overflow_handle", `{"Nonce":"0123456789abcdef0123456789abcdef","Handle":18446744073709551616,"Stage":"verified"}`, false},
		{"pseudo_process", `{"Nonce":"0123456789abcdef0123456789abcdef","Handle":18446744073709551615,"Stage":"verified"}`, false},
		{"pseudo_thread", `{"Nonce":"0123456789abcdef0123456789abcdef","Handle":18446744073709551614,"Stage":"verified"}`, false},
		{"pseudo_process_token", `{"Nonce":"0123456789abcdef0123456789abcdef","Handle":18446744073709551612,"Stage":"verified"}`, false},
		{"pseudo_thread_token", `{"Nonce":"0123456789abcdef0123456789abcdef","Handle":18446744073709551611,"Stage":"verified"}`, false},
		{"trailing_document", `{"Nonce":"0123456789abcdef0123456789abcdef","Handle":4,"Stage":"verified"} {}`, false},
		{"oversized", strings.Repeat(" ", 4097), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner, err := diagDecodeOwner([]byte(tc.body), nonce, "verified")
			if (err == nil) != tc.valid {
				t.Fatalf("ownership receipt validity: owner=%+v error=%v", owner, err)
			}
			if err == nil && (owner.Handle != 4 || owner.Nonce != nonce || owner.Stage != "verified") {
				t.Fatal("literal ownership values changed")
			}
		})
	}
}

func FuzzDiagnosticWrapperHandleReceipt(f *testing.F) {
	for _, body := range []string{
		`{"Nonce":"0123456789abcdef0123456789abcdef","Handle":4,"Stage":"verified"}`,
		`{"Nonce":"stale","Handle":4,"Stage":"verified"}`, `{}`, `null`, `{"Handle":-1}`, `{"Handle":"4"}`,
	} {
		f.Add(body)
	}
	f.Fuzz(func(t *testing.T, body string) {
		const nonce = "0123456789abcdef0123456789abcdef"
		owner, err := diagDecodeOwner([]byte(body), nonce, "verified")
		if err == nil && (len(body) == 0 || len(body) > 4096 || owner.Nonce != nonce ||
			owner.Stage != "verified" || owner.Handle == 0 || owner.Handle > uint64(^uintptr(0)) ||
			owner.Handle == uint64(^uintptr(0)) || owner.Handle == uint64(^uintptr(0))-1 ||
			owner.Handle == uint64(^uintptr(0))-3 || owner.Handle == uint64(^uintptr(0))-4) {
			t.Fatalf("unadmitted ownership metadata reached a native-call boundary: %+v", owner)
		}
	})
}

func TestDiagnosticWrapperNativeLauncher(t *testing.T) {
	for _, name := range []string{"launch_context", "stdin_eof", "stdio_roundtrip", "stdio_backpressure", "handle_list_exclusion"} {
		t.Run(name, func(t *testing.T) { diagCheckNativeConsumer(t, name) })
	}
	t.Run("stdio_held_writer", TestDiagnosticWrapperTimeoutRetiresChild)
	for _, tc := range []struct {
		name, stage, cause string
		total              uint32
		created            bool
	}{
		{"identity_missing", "identity", "missing or mismatched actual PowerShell runner identity", 0, false},
		{"identity_mismatch", "identity", "missing or mismatched actual PowerShell runner identity", 0, false},
		{"stdio_acquisition_failure", "startup", "stdio acquisition failure after two owned duplicates", 1, false},
		{"attribute_failure", "startup", "attribute failure after initialized native allocation", 1, false},
		{"job_refusal", "startup", "assign native diagnostic supervisor to parent job: Win32 0x00000006", 0, false},
		{"creation_job_refusal", "create", "create atomically contained diagnostic PowerShell: Win32 0x00000006", 1, false},
		{"create_refusal", "create", "create atomically contained diagnostic PowerShell: Win32 0x00000002", 1, false},
		{"suspended_verification_failure", "created", "verification refusal after actual image/membership checks", 2, true},
		{"membership_api_error", "created", "diagnostic job membership query: Win32 0x00000006", 2, true},
		{"resume_failure", "verified", "resume diagnostic PowerShell: Win32 0x00000006", 2, true},
		{"handle_list_leak_control", "created", "HANDLE_LIST leaked the unrelated inheritable pipe writer", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newDiagWrapperFixture(t, "native fault "+tc.name, 77, true, false)
			fixture.fault = tc.name
			invocation := launchDiagInvocation(t, fixture, 0, false)
			refusal := invocation.admitChild(fixture)
			invocation.retire(t)
			if refusal == nil || invocation.admissionStage == "released" {
				t.Fatal("native fault was accepted")
			}
			owner := readDiagSupervisorResult(t, fixture)
			if owner.Status != 95 || !strings.Contains(owner.Error, tc.cause) || owner.Context != "" ||
				owner.Stage != tc.stage || !owner.NativeClosed || invocation.accounting.TotalProcesses < tc.total ||
				(tc.total == 0 && invocation.accounting.TotalProcesses != 0) || (tc.created && !owner.NativeJoined) {
				t.Fatalf("native fault did not reach/retire its intended phase: %+v / %+v", owner, invocation.accounting)
			}
			diagAssertNoWorkload(t, fixture, invocation)
		})
	}
	t.Run("outside_target", diagCheckOutsideTarget)
	for _, name := range []string{"owner_loss_after_create", "owner_loss_before_transfer"} {
		t.Run(name, func(t *testing.T) {
			fixture := newDiagWrapperFixture(t, name, 77, true, false)
			fixture.fault = name
			invocation := launchDiagInvocation(t, fixture, 0, false)
			if invocation.admissionErr == nil || invocation.admissionStage != "verified" || invocation.powerShell == 0 {
				t.Fatalf("owner-loss controller did not reach a live native creation witness: stage=%s error=%v",
					invocation.admissionStage, invocation.admissionErr)
			}
			event, err := windows.WaitForSingleObject(invocation.powerShell, 5000)
			if err != nil || event != windows.WAIT_OBJECT_0 {
				t.Fatalf("lost launcher left its witnessed native process alive: %d / %v", event, err)
			}
			if code := diagExitCode(t, invocation.waitSupervisor()); code != 98 {
				t.Fatalf("controller did not actually retire its launch-established supervisor: %d", code)
			}
			invocation.retire(t)
			if !invocation.aborted || invocation.accounting.TotalProcesses < 2 {
				t.Fatal("owner loss was mistaken for normal qualification")
			}
			diagAssertNoWorkload(t, fixture, invocation)
		})
	}
}

func diagCheckNativeConsumer(t *testing.T, fault string) {
	t.Helper()
	name := "native consumer " + fault
	if fault == "launch_context" {
		name += " \u00e9 [quoted path]"
		t.Setenv("DXC_DIAG_WRAPPER_CONTEXT", "MiXeD \u03b1 = preserved")
		t.Setenv("DXC_DIAG_WRAPPER_EMPTY", "")
	}
	fixture := newDiagWrapperFixture(t, name, 77, true, false)
	fixture.fault = fault
	invocation := startDiagInvocation(t, fixture, 0, false)
	waitErr := invocation.waitSupervisor()
	invocation.retire(t)
	owner, child := readDiagSupervisorResult(t, fixture), readDiagChildResult(t, fixture)
	if diagExitCode(t, waitErr) != 0 || owner.Error != "" || owner.Context != "" ||
		!owner.NativeJoined || !owner.NativeClosed || !child.StdinEOF || child.Status != 0 ||
		child.GOGC != "50" || child.Address != "localhost:6061" || child.LoadedGC != 77 || child.EffectiveGC != 77 ||
		!diagEqualPath(child.Config, filepath.Join(fixture.private, "config")) {
		t.Fatalf("native consumer/fidelity failed: owner=%+v child=%+v", owner, child)
	}
	output := invocation.outputText()
	if fault == "stdio_roundtrip" || fault == "stdio_backpressure" || fault == "launch_context" {
		for _, stream := range []string{"STDOUT", "STDERR"} {
			if strings.Count(output, "DIAG_"+stream+"_SENTINEL "+fixture.nonce) != 1 {
				t.Fatalf("native %s stream did not roundtrip exactly once", stream)
			}
		}
	}
	if fault == "stdio_backpressure" {
		for _, row := range []string{
			"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			"fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
		} {
			if strings.Count(output, row) != 1024 {
				t.Fatal("pipe-capacity output was lost or duplicated")
			}
		}
	}
	if fault == "launch_context" && (child.Context != "MiXeD \u03b1 = preserved" || !child.EmptyPresent || child.EmptyValue != "") {
		t.Fatalf("native context changed Unicode or empty environment state: %+v", child)
	}
	if fault == "handle_list_exclusion" && !owner.HandleListExcluded {
		t.Fatal("unrelated inheritable handle exclusion was not independently witnessed")
	}
}

func diagAssertNoWorkload(t *testing.T, fixture diagWrapperFixture, invocation *diagInvocation) {
	t.Helper()
	for _, name := range []string{"release", "result.json"} {
		if _, err := os.Stat(filepath.Join(fixture.private, name)); !os.IsNotExist(err) {
			t.Fatalf("refused invocation published %s: %v", name, err)
		}
	}
	if !invocation.adopted {
		if _, err := os.Stat(filepath.Join(fixture.private, "adopted")); !os.IsNotExist(err) {
			t.Fatalf("refused ownership published its adoption marker: %v", err)
		}
	}
	if !invocation.waited || !invocation.jobJoined || !invocation.jobClosed || invocation.forcedCleanup ||
		invocation.accounting.ActiveProcesses != 0 || invocation.powerShell != 0 || invocation.supervisor != 0 {
		t.Fatalf("refused invocation did not prove complete retirement: %+v", invocation.accounting)
	}
}

func TestDiagnosticWrapperCanceledReceipts(t *testing.T) {
	const nonce = "0123456789abcdef0123456789abcdef"
	directory := t.TempDir()
	body := []byte(`{"Nonce":"0123456789abcdef0123456789abcdef","Handle":4,"Stage":"verified"}`)
	path := filepath.Join(directory, "powershell.json")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	invocation := &diagInvocation{context: ctx, cancel: cancel, done: make(chan error, 1)}
	if _, err := invocation.awaitReceipt(path); !errors.Is(err, context.Canceled) {
		t.Fatalf("available receipt bypassed actual cancellation: %v", err)
	}
	if err := invocation.adoptPowerShell(diagWrapperFixture{private: directory, nonce: nonce}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ownership adoption bypassed actual cancellation: %v", err)
	}
	if invocation.powerShell != 0 || invocation.adopted {
		t.Fatal("canceled ownership consumed a native handle or acknowledged adoption")
	}
	if _, err := os.Stat(filepath.Join(directory, "adopted")); !os.IsNotExist(err) {
		t.Fatalf("canceled ownership wrote an acknowledgment: %v", err)
	}
}

func diagCheckOutsideTarget(t *testing.T) {
	fixture := newDiagWrapperFixture(t, "outside target", 77, true, false)
	invocation := launchDiagInvocation(t, fixture, 0, false)
	if invocation.admissionErr != nil {
		t.Fatal(invocation.admissionErr)
	}
	other, err := newDiagJob(fixture.nonce + "-other")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := windows.CloseHandle(other); err != nil {
			t.Error(err)
		}
	}()
	if err := diagRequireLiveMember(invocation.powerShell, invocation.job); err != nil {
		t.Fatal(err)
	}
	if err := diagRequireMember(invocation.powerShell, other); err == nil || !strings.Contains(err.Error(), "outside") {
		t.Fatalf("real live outside-target witness was not established: %v", err)
	}
	refusal := func() error {
		real := invocation.job
		invocation.job = other
		defer func() { invocation.job = real }()
		return invocation.admitChild(fixture)
	}()
	if refusal == nil || !strings.Contains(refusal.Error(), "outside") {
		t.Fatalf("actual admission bypassed wrong-target membership refusal: %v", refusal)
	}
	if err := invocation.abortJob(); err != nil {
		t.Fatal(err)
	}
	invocation.retire(t)
	diagAssertNoWorkload(t, fixture, invocation)
}

func TestDiagnosticWrapperHandleTransfer(t *testing.T) {
	var replay []byte
	for _, tc := range []struct{ name, stage, cause string }{
		{"receipt_missing", "launched", "receipt"},
		{"receipt_malformed", "launched", "receipt"},
		{"receipt_wrong_nonce", "launched", "unauthenticated"},
		{"receipt_replay", "launched", "unauthenticated"},
		{"source_closed", "parsed", "duplicate"},
		{"source_identity", "duplicated", "aliases"},
		{"duplicate_failure", "parsed", "duplicate"},
		{"cancel_before_receipt", "before-receipt", "cancellation"},
		{"cancel_after_parse", "parsed", "cancellation"},
		{"cancel_after_duplicate", "duplicated", "cancellation"},
		{"cancel_after_verify", "verified", "cancellation"},
		{"cancel_before_release", "child-verified", "cancellation"},
		{"powershell_loss_before_release", "adopted", "not live"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "receipt_replay" {
				prior := newDiagWrapperFixture(t, "replay producer", 77, true, false)
				owner := startDiagInvocation(t, prior, 0, false)
				if err := owner.waitSupervisor(); err != nil {
					t.Fatal(err)
				}
				owner.retire(t)
				var err error
				replay, err = os.ReadFile(filepath.Join(prior.private, "powershell.json"))
				if err != nil {
					t.Fatal(err)
				}
			}
			fixture := newDiagWrapperFixture(t, "transfer "+tc.name, 77, true, false)
			fixture.fault = tc.name
			if tc.name == "receipt_replay" {
				if err := os.WriteFile(filepath.Join(fixture.private, "replay.json"), replay, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			invocation := launchDiagInvocation(t, fixture, 0, false)
			refusal := invocation.admitChild(fixture)
			if refusal == nil || invocation.admissionStage != tc.stage || !strings.Contains(refusal.Error(), tc.cause) {
				t.Fatalf("transfer fault missed intended epoch/cause: stage=%s refusal=%v", invocation.admissionStage, refusal)
			}
			if !invocation.aborted {
				if err := invocation.abortJob(); err != nil {
					t.Fatal(err)
				}
			}
			invocation.retire(t)
			diagAssertNoWorkload(t, fixture, invocation)
		})
	}
}

func TestDiagnosticWrapperRetirementFence(t *testing.T) {
	if os.Getenv("DXC_DIAG_WRAPPER_FENCE_CHILD") == "1" {
		invocation := &diagInvocation{job: windows.InvalidHandle}
		invocation.retireJob(t)
		if !diagFixtureRetirementBlocked {
			t.Fatal("native retirement query failure did not fence fixture writes")
		}
		fmt.Println("NATIVE_RETIREMENT_FENCE_ARMED")
		newDiagWrapperFixture(t, os.Getenv("DXC_DIAG_WRAPPER_FENCE_NAME"), 0, false, false)
		t.Fatal("fixture wrote after unconfirmed retirement")
	}
	fixture := newDiagWrapperFixture(t, "retirement fence controller", 0, false, false)
	name := "retirement blocked " + fixture.nonce
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestDiagnosticWrapperRetirementFence$", "-test.timeout=8s", "-test.v")
	command.Env = append(os.Environ(), "DXC_DIAG_WRAPPER_FENCE_CHILD=1", "DXC_DIAG_WRAPPER_FENCE_NAME="+name)
	command.WaitDelay = 250 * time.Millisecond
	output, err := command.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 || ctx.Err() != nil ||
		!strings.Contains(string(output), "NATIVE_RETIREMENT_FENCE_ARMED") ||
		!strings.Contains(string(output), "a prior diagnostic fixture did not confirm owner retirement") {
		t.Fatalf("isolated native retirement fence did not fail closed: %v; %s", err, output)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(fixture.root), name)); !os.IsNotExist(err) {
		t.Fatalf("retirement fence allowed a later fixture write: %v", err)
	}
	if diagFixtureRetirementBlocked {
		t.Fatal("isolated refusal poisoned the parent fixture owner")
	}
}
