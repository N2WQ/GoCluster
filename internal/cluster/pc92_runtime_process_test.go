//go:build qualification

package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"dxcluster/telnet"
)

func TestQualificationCounterRoundingAndRemoteMissing(t *testing.T) {
	for _, tc := range []struct {
		ticks, hz int64
		upper     bool
		want      time.Duration
	}{
		{50000, 10_000_000, false, 5 * time.Millisecond},
		{50000, 10_000_000, true, 5*time.Millisecond + 100*time.Nanosecond},
		{1, 3_000_000, true, 667 * time.Nanosecond},
		{3_000_001, 3_000_000, false, time.Second + 333*time.Nanosecond},
	} {
		if got := qualificationTickDuration(time.Duration(tc.ticks), tc.hz, tc.upper); got != tc.want {
			t.Fatalf("counter conversion %+v=%v", tc, got)
		}
	}
	var h qualificationHistogram
	for _, ticks := range []time.Duration{49999, 50000, 50001, 249999, 250000, 250001} {
		h.observeCounter(ticks, 10_000_000)
	}
	if got := h.result(); got.Over5MS != 5 || got.Over25MS != 2 || got.UncertaintyCross5MS != 1 || got.UncertaintyCross25MS != 1 {
		t.Fatalf("QPC threshold/margin accounting: %+v", got)
	}
	o := newQualificationOracle(2, 1, 0, 1)
	o.epoch = time.Now()
	_, token, _ := o.add(true, -1, 0, 0)
	o.observed(o.clients[0], token, qualificationDXCall(0)[:10], o.epoch.Add(time.Millisecond), false)
	child := newQualificationOracleInputs(o.inputs, 1, 0, 1)
	rows, err := child.enqueueResults(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.acceptEnqueueResults(qualificationReply{Enqueue: rows}); err != nil {
		t.Fatal(err)
	}
	if result := o.results(true)[0]; result.MissingEnqueue != 1 || o.failures.Load() == 0 {
		t.Fatal("missing child observation disappeared")
	}
	rows[0].Cohorts[0].RequiredSpots++
	prior := o.failures.Load()
	_ = o.results(false)
	if o.failures.Load() == prior {
		t.Fatal("child denominator mismatch did not fail")
	}
}

func TestQualificationControlRecordBound(t *testing.T) {
	var wire bytes.Buffer
	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], qualificationRPCMaxBytes+1)
	wire.Write(header[:])
	var reply qualificationReply
	if err := qualificationReadPacket(&wire, &reply); err == nil {
		t.Fatal("oversized control record accepted")
	}
}

type qualificationMappingReceipt struct {
	Frequency, Tick   int64
	Spot              bool
	Client            int
	Required, Allowed uint64
}

func TestQualificationCrossProcessClockAndPublication(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows measurement mechanism")
	}
	now, frequency, err := qualificationCounterClock()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "inputs.bin")
	mapping, err := openQualificationSharedInputs(path, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	defer mapping.close()
	if _, err := openQualificationSharedInputs(path, 2, false); err == nil {
		t.Fatal("mapping size mismatch accepted")
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestQualificationMappedClockChild$", "-test.timeout=8s")
	cmd.Env = append(os.Environ(), "GOCLUSTER_QPC_MAPPING_TEST="+path, "GOCLUSTER_QPC_FREQUENCY="+strconv.FormatInt(frequency, 10))
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path + ".ready"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			_ = cmd.Wait()
			t.Fatal("mapped child readiness timeout")
		}
		if err := qualificationWaitContext(ctx, time.Millisecond); err != nil {
			_ = cmd.Wait()
			t.Fatal(err)
		}
	}
	in := &mapping.inputs[0]
	in.spot, in.client, in.peerRequired, in.peerAllowed = true, 7, 0x123456789abcdef0, 0xfedcba9876543210
	published := now().UnixNano()
	in.started.Store(published)
	if err := cmd.Wait(); err != nil {
		t.Fatalf("mapped child: %v %s", err, output.String())
	}
	after := now().UnixNano()
	data, err := os.ReadFile(path + ".receipt")
	if err != nil {
		t.Fatal(err)
	}
	var receipt qualificationMappingReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.Frequency != frequency || receipt.Tick < published-1 || receipt.Tick > after+1 || !receipt.Spot || receipt.Client != 7 || receipt.Required != in.peerRequired || receipt.Allowed != in.peerAllowed {
		t.Fatalf("cross-process clock/publication differs: %+v", receipt)
	}
}

func TestQualificationMappedClockChild(t *testing.T) {
	path := os.Getenv("GOCLUSTER_QPC_MAPPING_TEST")
	if path == "" {
		t.Skip("private mapping probe")
	}
	mapping, err := openQualificationSharedInputs(path, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	defer mapping.close()
	now, frequency, err := qualificationCounterClock()
	if err != nil {
		t.Fatal(err)
	}
	wantFrequency, _ := strconv.ParseInt(os.Getenv("GOCLUSTER_QPC_FREQUENCY"), 10, 64)
	if frequency != wantFrequency {
		t.Fatal("cross-process counter frequency differs")
	}
	if err := os.WriteFile(path+".ready", []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for mapping.inputs[0].started.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("mapped publication timeout")
		}
		runtime.Gosched()
	}
	in := &mapping.inputs[0]
	receipt := qualificationMappingReceipt{frequency, now().UnixNano(), in.spot, in.client, in.peerRequired, in.peerAllowed}
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".receipt", data, 0600); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkQualificationCounterEnqueueGuard(b *testing.B) {
	if runtime.GOOS != "windows" {
		b.Skip("Windows measurement mechanism")
	}
	now, frequency, err := qualificationCounterClock()
	if err != nil {
		b.Fatal(err)
	}
	o := newQualificationOracle(1, 1, 0, 1)
	o.clockNow, o.clockFrequency, o.measurementEpoch = now, frequency, now()
	o.sessionIDs[0] = 1
	_, token, _ := o.add(true, -1, 0, 0)
	guard := qualificationObservationGuard{oracle: o}
	event := telnet.QualificationEnqueue{Login: "DL1CAA", SessionID: 1, Comment: token, DXCall: qualificationDXCall(0)}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		o.clients[0].enqueue[0].Store(0)
		guard.observe(event)
	}
	if o.failures.Load() != 0 {
		b.Fatal("observer checker failed")
	}
}

func TestQualificationObservationGuardJoinsActiveAndRejectsLate(t *testing.T) {
	o := newQualificationOracle(1, 1, 0, 1)
	guard := qualificationObservationGuard{oracle: o}
	o.failureMu.Lock()
	observed := make(chan struct{})
	go func() {
		guard.observe(telnet.QualificationEnqueue{Login: "unknown", Comment: "QID0000000"})
		close(observed)
	}()
	deadline := time.Now().Add(time.Second)
	for o.failures.Load() == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if o.failures.Load() == 0 {
		o.failureMu.Unlock()
		t.Fatal("callback did not enter guard")
	}
	stopped := make(chan struct{})
	go func() { guard.stop(); close(stopped) }()
	select {
	case <-stopped:
		o.failureMu.Unlock()
		t.Fatal("stop returned before active callback")
	case <-time.After(10 * time.Millisecond):
	}
	o.failureMu.Unlock()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("guard failed to join callback")
	}
	<-observed
	guard.oracle = nil // a late loaded callback must not touch released storage
	guard.observe(telnet.QualificationEnqueue{Comment: "QID0000000"})
}
