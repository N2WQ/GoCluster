package cluster

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
