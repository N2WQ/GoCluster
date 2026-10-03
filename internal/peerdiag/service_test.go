package peerdiag

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"dxcluster/internal/logutil"
)

var helperBuildOnce sync.Once
var helperBuildError error
var helperBuildOutput []byte

func buildCompanion(t *testing.T) {
	t.Helper()
	helperBuildOnce.Do(func() {
		executable, err := os.Executable()
		if err != nil {
			helperBuildError = err
			return
		}
		cmd := exec.CommandContext(t.Context(), "go", "build", "-o", filepath.Join(filepath.Dir(executable), helperExecutable), "./cmd/peerdiag")
		cmd.Dir = filepath.Join("..", "..")
		helperBuildOutput, helperBuildError = cmd.CombinedOutput()
	})
	if helperBuildError != nil {
		t.Fatalf("build companion: %v %s", helperBuildError, helperBuildOutput)
	}
}

func TestV15HelperRealCompanionWritesAndJoins(t *testing.T) {
	buildCompanion(t)
	dir := t.TempDir()
	s := New(Options{Enabled: true, Directory: filepath.Join(dir, "peer_connections"), RetentionDays: 7, DedupeWindow: 0, OverlongPath: filepath.Join(dir, "overlong.log")})
	s.Start()
	defer s.Stop()
	s.Emit(Connection, Fields{Action: "established", Peer: "N1TEST"})
	deadline := time.Now().Add(10 * time.Second)
	for s.Snapshot().Written != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if stats := s.Snapshot(); stats.Written != 1 || stats.Generation != 1 || stats.Degraded || stats.ChargedBytes != BackingLimit {
		t.Fatalf("real companion status=%+v", stats)
	}
	data, err := os.ReadFile(logutil.DailyActivePath(s.options.Directory))
	if err != nil || !strings.Contains(string(data), "event=peer_connection action=established peer=N1TEST\n") {
		t.Fatalf("peer file=%q err=%v", data, err)
	}
	start := time.Now()
	s.Stop()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("helper Stop=%s", elapsed)
	}
	if stats := s.Snapshot(); stats.CleanupFailed || stats.ChargedBytes != 0 {
		t.Fatalf("joined status=%+v", stats)
	}
}

func TestV15HelperDisabledDailyStillWritesOverlong(t *testing.T) {
	buildCompanion(t)
	dir := t.TempDir()
	options := Options{Directory: filepath.Join(dir, "peer_connections"), OverlongPath: filepath.Join(dir, "overlong.log")}
	s := New(options)
	s.Start()
	defer s.Stop()
	if s.Emit(Diagnostic, Fields{Action: "disabled"}) {
		t.Fatal("disabled daily diagnostic admitted")
	}
	if !s.Emit(Overlong, Fields{Detail: "overlong sample"}) {
		t.Fatal("overlong sample refused with daily logging disabled")
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.Snapshot().Written == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if stats := s.Snapshot(); stats.Written != 1 || stats.DisabledEvents != 1 || !stats.Disabled {
		t.Fatalf("disabled/overlong status=%+v", stats)
	}
	if _, err := os.Stat(options.Directory); !os.IsNotExist(err) {
		t.Fatalf("disabled daily directory exists: %v", err)
	}
	data, err := os.ReadFile(options.OverlongPath)
	if err != nil || !strings.Contains(string(data), "detail=overlong_sample") {
		t.Fatalf("overlong=%q err=%v", data, err)
	}
}

func TestV15HelperInvalidLaunchReleasesReservation(t *testing.T) {
	s := New(Options{Enabled: true, Directory: strings.Repeat("x", 32769)})
	s.Start()
	defer s.Stop()
	deadline := time.Now().Add(time.Second)
	for !s.Snapshot().Degraded && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if stats := s.Snapshot(); !stats.Degraded || stats.Generation != 0 || stats.ChargedBytes != parentReservation {
		t.Fatalf("failed-launch accounting=%+v", stats)
	}
}

func TestV15HelperSinkFailureRetiresGeneration(t *testing.T) {
	buildCompanion(t)
	directory := filepath.Join(t.TempDir(), "file-instead-of-directory")
	if err := os.WriteFile(directory, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	s := New(Options{Enabled: true, Directory: directory})
	s.Start()
	defer s.Stop()
	s.Emit(Diagnostic, Fields{Action: "cannot_write"})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stats := s.Snapshot()
		if stats.Unconfirmed == 1 && stats.ChargedBytes == parentReservation {
			if stats.Written != 0 || stats.Generation != 1 || stats.CleanupFailed {
				t.Fatalf("sink failure status=%+v", stats)
			}
			data, err := os.ReadFile(directory)
			if err != nil || string(data) != "preserve" {
				t.Fatal("sink failure changed the obstructing file")
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("failed sink did not retire: %+v", s.Snapshot())
}

func TestV15HelperIPCFailureMatrix(t *testing.T) {
	for _, fault := range []string{"wrong_sequence", "bad_outcome", "oversized_keys", "no_ack", "file_partial_error"} {
		t.Run(fault, func(t *testing.T) {
			parent, helper := net.Pipe()
			defer parent.Close()
			defer helper.Close()
			s := New(Options{Enabled: true})
			t.Cleanup(s.Stop)
			s.Emit(Diagnostic, Fields{Action: "test"})
			child := &helperProcess{conn: parent, exited: make(chan struct{})}
			done := make(chan struct{})
			go func() { defer close(done); s.exchange(child) }()
			var wire [RecordBytes]byte
			if _, err := io.ReadFull(helper, wire[:]); err != nil {
				t.Fatal(err)
			}
			if fault != "no_ack" {
				var ack [ackBytes]byte
				binary.LittleEndian.PutUint64(ack[:8], binary.LittleEndian.Uint64(wire[:8]))
				binary.LittleEndian.PutUint32(ack[8:12], ackWritten)
				switch fault {
				case "wrong_sequence":
					binary.LittleEndian.PutUint64(ack[:8], 99)
				case "bad_outcome":
					binary.LittleEndian.PutUint32(ack[8:12], 99)
				case "oversized_keys":
					binary.LittleEndian.PutUint32(ack[12:16], 513)
				case "file_partial_error":
					binary.LittleEndian.PutUint32(ack[8:12], ackFailed)
				}
				if _, err := helper.Write(ack[:]); err != nil {
					t.Fatal(err)
				}
				if fault == "file_partial_error" {
					s.stopOnce.Do(func() { s.closeAdmission(); close(s.stop) })
				}
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("invalid ACK did not terminate exchange")
			}
			if stats := s.Snapshot(); stats.Unconfirmed != 1 || stats.Written != 0 {
				t.Fatalf("ACK status=%+v", stats)
			}
		})
	}
}

func TestV15PartialWriteIsUnconfirmed(t *testing.T) {
	writer, reader := net.Pipe()
	defer writer.Close()
	go func() { var prefix [7]byte; _, _ = io.ReadFull(reader, prefix[:]); _ = reader.Close() }()
	if err := writeFull(writer, []byte("a diagnostic record longer than the accepted prefix")); err == nil {
		t.Fatal("partial write silently accepted")
	}
}

func TestV15LossSummaryRequiresWriteAcknowledgement(t *testing.T) {
	s := New(Options{Enabled: true})
	t.Cleanup(s.Stop)
	s.Mailbox.mu.Lock()
	s.stats.Dropped = 4
	s.Mailbox.mu.Unlock()
	s.emitLossSummary()
	if s.reportedDropped != 0 || s.summarySequence == 0 {
		t.Fatal("enqueue falsely confirmed loss summary")
	}
	parent, helper := net.Pipe()
	defer parent.Close()
	done := make(chan struct{})
	go func() { defer close(done); s.exchange(&helperProcess{conn: parent, exited: make(chan struct{})}) }()
	var record [RecordBytes]byte
	if _, err := io.ReadFull(helper, record[:]); err != nil {
		t.Fatal(err)
	}
	_ = helper.Close() // helper could have written the file but no ACK arrived
	<-done
	if s.reportedDropped != 0 || s.summarySequence != 0 || s.Snapshot().Unconfirmed != 1 {
		t.Fatal("missing ACK advanced summary progress")
	}
	s.emitLossSummary()
	if s.summarySequence == 0 || s.summaryDropped != 4 || s.summaryUnconfirmed != 1 {
		t.Fatal("recovery did not retain summary obligation")
	}
}

// This subprocess intentionally blocks in an OS pipe write. It proves real
// process termination/Wait behavior, not merely context cancellation of a mock.
func TestV15BlockedChild(t *testing.T) {
	if os.Getenv("PEERDIAG_BLOCK_CHILD") != "1" {
		return
	}
	_, _ = os.Stderr.WriteString("ready\n")
	var data [64 << 10]byte
	for {
		if _, err := os.Stdout.Write(data[:]); err != nil {
			os.Exit(0)
		}
	}
}

func TestV15CrashBeforeACKChild(t *testing.T) {
	address := os.Getenv("PEERDIAG_CRASH_ADDRESS")
	if address == "" {
		return
	}
	dialer := net.Dialer{Timeout: time.Second}
	connection, err := dialer.DialContext(context.Background(), "tcp4", address)
	if err != nil {
		os.Exit(2)
	}
	var event [RecordBytes]byte
	if _, err = io.ReadFull(connection, event[:]); err != nil {
		os.Exit(3)
	}
	os.Exit(23) // no ACK after the real subprocess accepted a complete record
}

func TestV15ActualProcessCrashBeforeACKIsUnconfirmed(t *testing.T) {
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err = listener.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p, err := startCompanion(executable, []string{helperExecutable, "-test.run=^TestV15CrashBeforeACKChild$"}, &os.ProcAttr{Env: append(helperEnvironment(helperEnvironmentRoot()), "PEERDIAG_CRASH_ADDRESS="+listener.Addr().String()), Files: []*os.File{file, file, file}, Sys: helperProcessAttributes()})
	if err != nil {
		if p != nil {
			_ = p.release()
		}
		t.Fatal(err)
	}
	child := &helperProcess{process: p, exited: make(chan struct{})}
	go func() {
		var waitErr error
		child.waitSucceeded, waitErr = p.Wait()
		child.failedExit = waitErr != nil
		close(child.exited)
	}()
	s := New(Options{Enabled: true})
	t.Cleanup(func() { s.retire(child); s.Stop() })
	child.conn, err = listener.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	s.Emit(Diagnostic, Fields{Action: "crash"})
	s.exchange(child)
	if stats := s.Snapshot(); stats.Unconfirmed != 1 || stats.Written != 0 {
		t.Fatalf("crash ACK status=%+v", stats)
	}
}

func blockedChild(t *testing.T) *helperProcess {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, stderrWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	devnull, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	process, err := startCompanion(executable, []string{helperExecutable, "-test.run=^TestV15BlockedChild$"}, &os.ProcAttr{Env: append(helperEnvironment(helperEnvironmentRoot()), "PEERDIAG_BLOCK_CHILD=1"), Files: []*os.File{devnull, writer, stderrWriter}, Sys: helperProcessAttributes()})
	_ = devnull.Close()
	_ = stderrWriter.Close()
	if err != nil {
		if process != nil {
			_ = process.release()
		}
		t.Fatal(err)
	}
	child := &helperProcess{process: process, exited: make(chan struct{})}
	exited := child.exited
	go func() {
		var waitErr error
		child.waitSucceeded, waitErr = process.Wait()
		child.failedExit = waitErr != nil
		close(exited)
	}()
	if line, err := bufio.NewReader(stderr).ReadString('\n'); err != nil || line != "ready\n" {
		t.Fatalf("child readiness=%q %v", line, err)
	}
	t.Cleanup(func() {
		_ = process.Kill()
		_ = reader.Close()
		_ = writer.Close()
		_ = stderr.Close()
		select {
		case <-exited:
		case <-time.After(2 * time.Second):
			t.Error("child test cleanup did not join")
		}
		_ = process.release()
	})
	return child
}

func TestV15HelperBlockedWriteTerminatesAndJoins(t *testing.T) {
	child := blockedChild(t)
	s := New(Options{})
	t.Cleanup(s.Stop)
	start := time.Now()
	if !s.retire(child) {
		t.Fatal("OS termination did not join blocked child")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("blocked write join=%s", elapsed)
	}
	// Exited reports normal exit only on Unix; a successful signal termination
	// is terminal too. The Wait witness and published state prove retirement.
	if !child.waitSucceeded {
		t.Fatal("Wait did not publish terminal process state")
	}
}

func TestV15HelperFailedTerminationRetainsGeneration(t *testing.T) {
	child := blockedChild(t)
	// Keep the real Wait witness, but deliberately delay the supervisor's join
	// notification. This is the same retained-ownership path as failed Kill.
	actual := child.exited
	delayed := make(chan struct{})
	child.exited = delayed
	s := New(Options{})
	t.Cleanup(s.Stop)
	if s.retire(child) {
		t.Fatal("unconfirmed process retirement was accepted")
	}
	if stats := s.Snapshot(); !stats.CleanupFailed || !stats.Degraded || stats.ChargedBytes != BackingLimit {
		t.Fatalf("failed retirement=%+v", stats)
	}
	if s.retired != child {
		t.Fatal("failed generation ownership was released")
	}
	select {
	case <-actual:
	case <-time.After(time.Second):
		t.Fatal("real child did not exit")
	}
	close(delayed)
	if !s.awaitRetirement(child) {
		t.Fatal("late successful Wait not confirmed")
	}
	if stats := s.Snapshot(); stats.CleanupFailed || stats.ChargedBytes != parentReservation || s.retired != nil {
		t.Fatalf("late Wait did not release generation: %+v", stats)
	}
}

func TestV15FailedWaitOwnsServiceAcrossReplacement(t *testing.T) {
	child := blockedChild(t)
	s := New(Options{Enabled: true})
	if !s.retire(child) {
		t.Fatal("fixture process did not actually terminate")
	}
	actualWait := child.waitSucceeded
	// A closed notification with a failed Wait is not a termination witness.
	child.waitSucceeded, child.failedExit = false, true
	if s.confirmRetirement(child) || s.awaitRetirement(child) {
		t.Fatal("failed Wait discharged ownership")
	}
	s.Stop()
	s = nil //nolint:wastedassign // deliberately discard the external owner before GC
	runtime.GC()
	refused := New(Options{Enabled: true, Directory: "must-not-be-retained"})
	if refused != refusedEnabled || refused.events != nil || refused.options != (Options{}) {
		t.Fatal("replacement allocated or retained configuration")
	}
	if n := testing.AllocsPerRun(100, func() { New(Options{Enabled: true}) }); n != 0 {
		t.Fatalf("refused constructor allocated %v", n)
	}
	refused.Start()
	refused.Stop()
	if refused.Emit(Diagnostic, Fields{Action: "refused"}) {
		t.Fatal("refused handle admitted a record")
	}
	companionOwner.Lock()
	retained := companionOwner.service
	companionOwner.Unlock()
	if retained == nil || retained.retired != child || retained.Snapshot().ChargedBytes != BackingLimit {
		t.Fatal("discarded owner lost failed generation")
	}
	retained.Mailbox.mu.Lock()
	retained.stats.Generation = 7
	retained.stats.Written = 29
	retained.Mailbox.mu.Unlock()
	if stats := refused.Snapshot(); !stats.CleanupFailed || stats.ChargedBytes != BackingLimit || stats.Generation != 7 || stats.Written != 0 {
		t.Fatalf("replacement hid ownership or inherited producer counters: %+v", stats)
	}
	// Fault-control cleanup: the real subprocess already has a successful Wait.
	child.waitSucceeded, child.failedExit = actualWait, false
	if !retained.confirmRetirement(child) {
		t.Fatal("actual Wait control not accepted")
	}
	retained.Stop()
	next := New(Options{})
	if next.refused {
		t.Fatal("confirmed retirement did not permit next owner")
	}
	next.Stop()
	if next.events != nil || next.options != (Options{}) || next.Snapshot().ChargedBytes != 0 {
		t.Fatal("Stop retained owned queue/options reservation")
	}
	if next.Emit(Overlong, Fields{}) {
		t.Fatal("stopped nil queue admitted a record")
	}
	if _, ok := next.Next(); ok {
		t.Fatal("stopped nil queue returned a record")
	}
}

func TestV15SetupCloseFailureRetainsLease(t *testing.T) {
	file, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	s := New(Options{})
	child := &helperProcess{devnull: file}
	if s.retire(child) || !s.Snapshot().CleanupFailed {
		t.Fatal("failed setup Close released lease")
	}
	s.Stop()
	if got := New(Options{}); got != refusedDisabled {
		t.Fatal("close failure permitted a new owner")
	}
	// This fixture deliberately closed the real file successfully above; only
	// the reported second Close failed. Restore that independent witness.
	child.cleanupFailed, child.devnull = false, nil
	if !s.confirmRetirement(child) {
		t.Fatal("known close control not accepted")
	}
	s.Stop()
}

func TestV15DiagnosticStartStopRace(t *testing.T) {
	oversized := strings.Repeat("x", 32769)
	for range 20 {
		s := New(Options{Enabled: true, Directory: oversized})
		if s.refused {
			t.Fatal("prior owner not released")
		}
		var workers sync.WaitGroup
		workers.Add(2)
		go func() { defer workers.Done(); s.Start() }()
		go func() { defer workers.Done(); s.Stop() }()
		workers.Wait()
		s.Stop()
		s.Start() // closed service must never reacquire a released reservation
		if s.Snapshot().ChargedBytes != 0 || s.events != nil {
			t.Fatal("Start/Stop retained or resurrected queue")
		}
	}
}
