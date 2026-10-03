//go:build qualification

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"path/filepath"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

func validQualificationWarmFixture() *qualificationWarmProfile {
	p := &qualificationWarmProfile{Count: 900, Complete: true, EpochCounterNS: 1, EpochUTCNS: 1,
		Windows: qualificationWarmWindows("fixture")}
	for i := range p.Samples {
		p.Samples[i].Second, p.Samples[i].AtNS = i+1, int64(i+1)*int64(time.Second)
	}
	for i := range p.Windows {
		w := &p.Windows[i]
		w.StartedNS, w.StoppedNS, w.Bytes = int64(w.StartSecond)*int64(time.Second), int64(w.EndSecond)*int64(time.Second), 1
	}
	return p
}

func TestPC92WarmDiagnosticProfileContract(t *testing.T) {
	p, err := runtimeQualificationProfile("warm-diagnostic")
	if err != nil || p.load != 900*time.Second || p.drain != 660*time.Second || p.peers != 16 || !p.diagnostic || p.shipped || p.full || p.burst {
		t.Fatalf("warm profile changed approved population/timing: %+v %v", p, err)
	}
	for _, name := range []string{"q1", "q2", "q3", "shipped-q1"} {
		p, err := runtimeQualificationProfile(name)
		if err != nil || p.diagnostic {
			t.Fatalf("acceptance profile affected: %+v %v", p, err)
		}
	}
}

func TestPC92WarmProfileSchedule(t *testing.T) {
	p := validQualificationWarmFixture()
	if err := p.validate(); err != nil {
		t.Fatal(err)
	}
	want := [3][2]int{{0, 20}, {60, 180}, {660, 780}}
	for i, w := range p.Windows {
		if [2]int{w.StartSecond, w.EndSecond} != want[i] {
			t.Fatalf("window %d: %+v", i, w)
		}
	}
}

func TestPC92WarmProfileRejectsIncompleteWindows(t *testing.T) {
	cases := []struct {
		name          string
		breakEvidence func(*qualificationWarmProfile)
	}{
		{"incomplete", func(p *qualificationWarmProfile) { p.Complete = false }},
		{"short", func(p *qualificationWarmProfile) { p.Count-- }},
		{"failure", func(p *qualificationWarmProfile) { p.Failure = "missing" }},
		{"clock", func(p *qualificationWarmProfile) { p.EpochCounterNS = 0 }},
		{"missing-profile", func(p *qualificationWarmProfile) { p.Windows[1].Bytes = 0 }},
		{"wrong-window", func(p *qualificationWarmProfile) { p.Windows[2].StartSecond = 650 }},
		{"late-start", func(p *qualificationWarmProfile) { p.Windows[1].StartedNS = 61 * int64(time.Second) }},
		{"early-stop", func(p *qualificationWarmProfile) { p.Windows[1].StoppedNS = 179 * int64(time.Second) }},
		{"late-stop", func(p *qualificationWarmProfile) { p.Windows[1].StoppedNS = 181 * int64(time.Second) }},
		{"missing-sample", func(p *qualificationWarmProfile) { p.Samples[42].Second = 0 }},
		{"duplicate-sample", func(p *qualificationWarmProfile) { p.Samples[42] = p.Samples[41] }},
		{"clock-regression", func(p *qualificationWarmProfile) { p.Samples[42].AtNS = -1 }},
		{"catch-up", func(p *qualificationWarmProfile) { p.Samples[42].AtNS = 44 * int64(time.Second) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := validQualificationWarmFixture()
			tc.breakEvidence(p)
			if err := p.validate(); err == nil {
				t.Fatal("accepted invalid diagnostic evidence")
			}
		})
	}
}

func TestPC92WarmProfileCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	p := validQualificationWarmFixture()
	p.Count, p.Complete = 0, false
	ready := make(chan struct{})
	report := runQualificationWarmProfile(ctx, p, nil, time.Now, ready)
	select {
	case <-ready:
	default:
		t.Fatal("canceled worker stranded startup")
	}
	if report.Warm.validate() == nil || report.Warm.Count != 0 {
		t.Fatal("canceled diagnostic became complete")
	}
}

func TestPC92WarmProfileBackingAndPacketBound(t *testing.T) {
	p := validQualificationWarmFixture()
	for i := range p.Samples {
		s := &p.Samples[i]
		s.HeapAlloc, s.HeapInuse, s.TotalAlloc, s.Mallocs = math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64
		s.Frees, s.PauseTotalNS, s.NumGC, s.Goroutines = math.MaxUint64, math.MaxUint64, math.MaxUint32, math.MaxInt32
		state := reflect.ValueOf(&s.State).Elem()
		for j := range state.NumField() {
			switch field := state.Field(j); field.Kind() {
			case reflect.Int, reflect.Int64:
				field.SetInt(math.MaxInt64)
			case reflect.Uint64:
				field.SetUint(math.MaxUint64)
			case reflect.Bool:
				field.SetBool(true)
			}
		}
	}
	for i := range p.Windows {
		p.Windows[i].Path = filepath.Join(t.TempDir(), "maximal-profile.pprof")
	}
	reply := qualificationReply{Profile: qualificationLoadProfile{Enabled: true, Warm: p}}
	var packet bytes.Buffer
	if err := qualificationWritePacket(&packet, reply); err != nil {
		t.Fatal(err)
	}
	var decoded qualificationReply
	if err := qualificationReadPacket(&packet, &decoded); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(reply)
	if err != nil {
		t.Fatal(err)
	}
	// Both report owners, bounded RPC read/write encodings and the final JSON
	// report can overlap. This conservative reservation uses full RPC limits,
	// not only the observed encoded length. CPU profiler runtime backing is
	// reported separately; this is not the protocol's 480 MiB proof.
	backing := 2*uint64(unsafe.Sizeof(*p)) + 6*qualificationRPCMaxBytes
	if backing > 32<<20 {
		t.Fatalf("warm observation backing %d exceeds 32MiB", backing)
	}
	t.Logf("warm samples=%d struct=%d maximal RPC=%d reserved report/encoding backing=%d", len(p.Samples), unsafe.Sizeof(*p), len(data), backing)
}
