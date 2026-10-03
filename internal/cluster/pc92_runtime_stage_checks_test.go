//go:build qualification

package cluster

import (
	"bytes"
	"math"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"dxcluster/internal/qualificationstage"
	"dxcluster/telnet"
)

func stageFixture(t testing.TB) (*qualificationStageTrace, telnet.QualificationStageFanout, *atomic.Int64) {
	t.Helper()
	o := newQualificationOracle(1, 2, 0, 15)
	o.measurementEpoch = time.Unix(0, 100)
	o.clockFrequency = 1_000_000_000
	clock := new(atomic.Int64)
	clock.Store(100)
	o.clockNow = func() time.Time { return time.Unix(0, clock.Load()) }
	o.sessionIDs[0], o.sessionIDs[1] = 11, 22
	f := telnet.QualificationStageFanout{Workers: 2, Mask: 3, Count: 2}
	f.Clients[0] = telnet.QualificationStageClient{SessionID: 11, Worker: 0}
	f.Clients[1] = telnet.QualificationStageClient{SessionID: 22, Worker: 1}
	s, err := newQualificationStageTrace(o, &f)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.retire)
	o.inputs[0].spot = true
	o.inputs[0].client = -1
	o.inputs[0].started.Store(1)
	return s, f, clock
}

func stageLiteral(s *qualificationStageTrace, clock *atomic.Int64) {
	for stage, tick := range []int64{110, 125, 140, 155, 170} {
		clock.Store(tick)
		s.observe(qualificationstage.Event{Stage: qualificationstage.Stage(stage), Comment: "QID0000000", Worker: -1})
	}
	for worker := range 2 {
		s.observe(qualificationstage.Event{Stage: qualificationstage.WorkerDispatched, Comment: "QID0000000", Worker: worker})
		clock.Store(190)
		s.observe(qualificationstage.Event{Stage: qualificationstage.WorkerStarted, Comment: "QID0000000", Worker: worker})
	}
}

func TestPC92StageTraceLiteralChain(t *testing.T) {
	s, f, clock := stageFixture(t)
	stageLiteral(s, clock)
	if got, ok := s.deltas(0, 0, 220, 100); !ok || got != [7]int64{10, 15, 15, 15, 15, 20, 30} {
		t.Fatalf("literal differences=%v valid=%t", got, ok)
	}
	clock.Store(220)
	for i := range 2 {
		s.oracle.clients[i].enqueueLatency[0].observeCounter(120, s.oracle.clockFrequency)
		s.oracle.clients[i].enqueueLatency[1].observeCounter(120, s.oracle.clockFrequency)
		s.enqueue(i, "QID0000000", 220)
	}
	r := s.finish(1, &f)
	if !r.Complete || r.RequiredSpots != 1 {
		t.Fatalf("literal report=%+v", r)
	}
	for segment, count := range []uint32{1, 1, 1, 1, 1, 2, 2} {
		if r.Segments[0][0][segment].Count != count || r.Segments[0][1][segment].Count != count {
			t.Fatalf("segment%d count", segment)
		}
	}
}

func TestPC92StageTraceRejectsCorruptEvidence(t *testing.T) {
	cases := []struct {
		name    string
		corrupt func(*qualificationStageTrace, *atomic.Int64)
	}{
		{"duplicate", func(s *qualificationStageTrace, c *atomic.Int64) {
			s.observe(qualificationstage.Event{Stage: qualificationstage.PrimaryReady, Comment: "QID0000000", Worker: -1})
		}},
		{"unknown", func(s *qualificationStageTrace, c *atomic.Int64) {
			s.observe(qualificationstage.Event{Stage: qualificationstage.PrimaryReady, Comment: "QID9999999", Worker: -1})
		}},
		{"malformed", func(s *qualificationStageTrace, c *atomic.Int64) {
			s.observe(qualificationstage.Event{Stage: qualificationstage.PrimaryReady, Comment: "QIDabcdefg", Worker: -1})
		}},
		{"descending", func(s *qualificationStageTrace, c *atomic.Int64) { s.rows[2].Store(101) }},
		{"future", func(s *qualificationStageTrace, c *atomic.Int64) { s.rows[2].Store(9999) }},
		{"dispatch", func(s *qualificationStageTrace, c *atomic.Int64) { s.dispatched[0].Store(1) }},
		{"worker-overflow", func(s *qualificationStageTrace, c *atomic.Int64) {
			s.observe(qualificationstage.Event{Stage: qualificationstage.WorkerStarted, Comment: "QID0000000", Worker: 32})
		}},
		{"unpublished", func(s *qualificationStageTrace, c *atomic.Int64) { s.oracle.inputs[0].started.Store(0) }},
		{"clock-add-overflow", func(s *qualificationStageTrace, c *atomic.Int64) { s.oracle.inputs[0].started.Store(math.MaxInt64) }},
		{"minute-overflow", func(s *qualificationStageTrace, c *atomic.Int64) {
			s.oracle.inputs[0].started.Store(math.MaxInt64 - 100)
		}},
		{"duration-overflow", func(s *qualificationStageTrace, c *atomic.Int64) {
			s.oracle.clockFrequency = 1_000_000
			s.enqueue(0, "QID0000000", math.MaxInt64)
		}},
		{"counter-overflow", func(s *qualificationStageTrace, c *atomic.Int64) { s.failures[1].Store(math.MaxUint64); s.fail(1) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, f, c := stageFixture(t)
			stageLiteral(s, c)
			tc.corrupt(s, c)
			c.Store(300)
			if s.finish(1, &f).Complete {
				t.Fatal("corrupt evidence qualified")
			}
		})
	}
	for missing := 0; missing < 7; missing++ {
		t.Run("missing-"+string(rune('0'+missing)), func(t *testing.T) {
			s, f, c := stageFixture(t)
			stageLiteral(s, c)
			s.rows[missing].Store(0)
			c.Store(300)
			r := s.finish(1, &f)
			if r.Complete || r.Missing == 0 {
				t.Fatal("missing stage accepted")
			}
		})
	}
}

func TestPC92StageTraceObservedFanout(t *testing.T) {
	s, f, _ := stageFixture(t)
	f.Clients[0], f.Clients[1] = f.Clients[1], f.Clients[0]
	if err := s.matchFanout(&f, false); err != nil {
		t.Fatal("snapshot iteration order is irrelevant", err)
	}
	f.Clients[0].Worker = 0
	if s.matchFanout(&f, false) == nil {
		t.Fatal("changed actual assignment accepted")
	}
}

func TestPC92StageTraceBackingAndPacketBound(t *testing.T) {
	backing, err := qualificationStageBacking(151928, 10)
	if err != nil || backing > stageLimit {
		t.Fatalf("backing=%d err=%v", backing, err)
	}
	for _, dims := range [][2]int{{151929, 10}, {151928, 20}, {0, 10}, {1, 33}} {
		if _, err := qualificationStageBacking(dims[0], dims[1]); err == nil {
			t.Fatal("oversized diagnostic admitted", dims)
		}
	}
	warm := validQualificationWarmFixture()
	for i := range warm.Samples {
		sample := &warm.Samples[i]
		sample.HeapAlloc, sample.HeapInuse, sample.TotalAlloc, sample.Mallocs = math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64
		sample.Frees, sample.PauseTotalNS, sample.NumGC, sample.Goroutines = math.MaxUint64, math.MaxUint64, math.MaxUint32, math.MaxInt32
		state := reflect.ValueOf(&sample.State).Elem()
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
	report := new(qualificationStageReport)
	for g := range 2 {
		for m := range stageCohorts {
			for j := range stageSegments {
				report.Segments[g][m][j] = qualificationLatency{Count: math.MaxUint32, P99UpperMS: math.MaxFloat64, Over5MS: math.MaxUint32, Over25MS: math.MaxUint32, UncertaintyCross5MS: math.MaxUint32, UncertaintyCross25MS: math.MaxUint32}
			}
		}
	}
	reply := qualificationReply{Profile: qualificationLoadProfile{Enabled: true, Warm: warm}, Stages: report}
	var packet bytes.Buffer
	if err := qualificationWritePacket(&packet, reply); err != nil {
		t.Fatal(err)
	}
	packetBytes := packet.Len()
	var decoded qualificationReply
	if err := qualificationReadPacket(&packet, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Stages == nil {
		t.Fatal("stage report lost")
	}
	t.Logf("backing=%d warm=%d hist=%d controller=%d report=%d packet=%d", backing, unsafe.Sizeof(*warm), unsafe.Sizeof(qualificationStageHistograms{}), unsafe.Sizeof(qualificationStageTrace{}), unsafe.Sizeof(*report), packetBytes)
}

func TestPC92StageTraceRetirement(t *testing.T) {
	s, _, _ := stageFixture(t)
	if !s.enter() {
		t.Fatal("live guard refused")
	}
	done := make(chan struct{})
	go func() { s.freeze(); close(done) }()
	for !s.closed.Load() {
		runtime.Gosched()
	}
	select {
	case <-done:
		t.Fatal("active callback not drained")
	default:
	}
	s.active.Add(-1)
	<-done
	late := s.observe
	s.retire()
	late(qualificationstage.Event{Stage: qualificationstage.PrimaryReady, Comment: "QID0000000", Worker: -1})
	if s.rows != nil || s.dispatched != nil || s.hist != nil || s.oracle != nil || s.report != nil {
		t.Fatal("retired backing retained")
	}
}

func TestPC92StageTraceObserverAllocation(t *testing.T) {
	s, f, c := stageFixture(t)
	if n := testing.AllocsPerRun(1000, func() {
		s.rows[0].Store(0)
		c.Store(110)
		s.observe(qualificationstage.Event{Stage: qualificationstage.PrimaryReady, Comment: "QID0000000", Worker: -1})
	}); n != 0 {
		t.Fatalf("per-event allocs=%v", n)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 10 {
		if _, err := newQualificationStageTrace(s.oracle, &f); err == nil {
			t.Fatal("second owner admitted")
		}
	}
	runtime.ReadMemStats(&after)
	if after.TotalAlloc-before.TotalAlloc > 4096 {
		t.Fatalf("rejected constructor allocated backing: %d", after.TotalAlloc-before.TotalAlloc)
	}
}

func TestPC92StageTraceAccountingAndLateAttribution(t *testing.T) {
	s, f, c := stageFixture(t)
	stageLiteral(s, c)
	c.Store(6_000_100)
	for i := range 2 {
		for _, m := range []int{0, 1} {
			s.oracle.clients[i].enqueueLatency[m].observeCounter(6_000_000, s.oracle.clockFrequency)
		}
		s.enqueue(i, "QID0000000", c.Load())
	}
	r := s.finish(1, &f)
	if !r.Complete {
		t.Fatal(r)
	}
	for segment := range stageSegments {
		if r.Segments[1][0][segment].Count != 2 {
			t.Fatal("late attribution denominator differs")
		}
	}
}
