package peer

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"

	"dxcluster/config"
)

func manualPeerIngress(t testing.TB) (*Manager, *session) {
	t.Helper()
	q := NewSpotIngress()
	m := &Manager{ingress: q, cfg: config.PeeringConfig{ForwardSpots: true},
		dedupe: newBoundedDedupe(time.Minute, 128, 16384), sessions: newFixedIndex[string, *session](64), parseBudget: newFrameParseBudget()}
	s := &session{identity: new(sourceIdentity), manager: m, ctx: t.Context(), id: "source", writeCh: make(chan string, 128)}
	m.sessions.Set(s.id, s)
	t.Cleanup(q.Close)
	return m, s
}

func ingressTestFrame(t testing.TB, index int) *Frame {
	t.Helper()
	f, err := ParseFrame(strings.Replace(originalSpotTestWire("PC61", "CQ"), "K1ABC", fmt.Sprintf("K1Q%03d", index), 1))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestPeerIngressOutstandingIncludesBuildersAndActive(t *testing.T) {
	m, _ := manualPeerIngress(t)
	q := m.ingress
	f := ingressTestFrame(t, 1)
	var jobs []*SpotAdmission
	for range peerIngressCount {
		j := q.reserve(m, f)
		if j == nil {
			t.Fatal("premature reservation refusal")
		}
		jobs = append(jobs, j)
	}
	if q.reserve(m, f) != nil {
		t.Fatal("builder count bypassed cap")
	}
	if n := testing.AllocsPerRun(10, func() {
		if q.reserve(m, f) != nil {
			panic("unexpected reservation")
		}
	}); n != 0 {
		t.Fatalf("refusal allocated %v", n)
	}
	jobs[0].publish()
	j := q.Take()
	if j == nil || q.reserve(m, f) != nil {
		t.Fatal("active reservation lost count credit")
	}
	j.Finish()
	for _, j := range jobs[1:] {
		j.Finish()
	}
	if count, _ := q.Usage(); count != 0 {
		t.Fatal("credit not returned")
	}
}

func TestPeerIngressFIFOReadinessAndZeroing(t *testing.T) {
	m, source := manualPeerIngress(t)
	q := m.ingress
	_, fixed := q.Usage()
	for i := 0; i < 5; i++ {
		m.HandleFrame(ingressTestFrame(t, i), source)
	}
	for i := 0; i < 5; i++ {
		select {
		case <-q.Ready():
		case <-time.After(time.Second):
			t.Fatal("queued work stranded without a later producer")
		}
		j := q.Take()
		if j == nil {
			t.Fatal("notified empty FIFO")
		}
		if got := j.TakeSpot().DXCall; got != fmt.Sprintf("K1Q%03d", i) {
			t.Fatalf("FIFO %q at %d", got, i)
		}
		j.Finish()
	}
	if count, backing := q.Usage(); count != 0 || backing != fixed {
		t.Fatalf("retained credit %d/%d", count, backing)
	}
	for i := range q.jobs {
		if q.jobs[i].owner != nil || q.jobs[i].local != nil || q.jobs[i].original.Fields != nil {
			t.Fatal("descriptor not zeroed")
		}
	}
}

func TestPeerIngressCloseJoinsBuilderAndActive(t *testing.T) {
	for _, stage := range []string{"builder", "active"} {
		t.Run(stage, func(t *testing.T) {
			m, source := manualPeerIngress(t)
			q := m.ingress
			_, fixed := q.Usage()
			minimumFixed := dedupeOracleAllocation(int(unsafe.Sizeof(*q))+8) +
				dedupeOracleAllocation(int(unsafe.Sizeof(sync.Cond{}))+8) +
				int(reflect.TypeOf(q.ctx).Elem().Size()) + 3*256 + 16 +
				2*dedupeOracleAllocation(MaxPeerFrameBytes) + 1024
			if fixed < minimumFixed {
				t.Fatalf("fixed backing/workspace undercharged %d < %d", fixed, minimumFixed)
			}
			var held *SpotAdmission
			if stage == "builder" {
				held = q.reserve(m, ingressTestFrame(t, 1))
			} else {
				m.HandleFrame(ingressTestFrame(t, 1), source)
				held = q.Take()
			}
			m.HandleFrame(ingressTestFrame(t, 2), source)
			done := make(chan struct{})
			go func() { q.Close(); close(done) }()
			select {
			case <-q.Done():
			case <-time.After(time.Second):
				t.Fatal("close did not cancel")
			}
			select {
			case <-done:
				t.Fatal("close returned with live lease")
			default:
			}
			if q.reserve(m, ingressTestFrame(t, 3)) != nil {
				t.Fatal("reservation after close")
			}
			held.Finish()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("retirement did not join")
			}
			if count, backing := q.Usage(); count != 0 || backing != fixed {
				t.Fatalf("retired backing %d/%d", count, backing)
			}
		})
	}
}

func TestPeerIngressMaximumBackingAndOwnedFields(t *testing.T) {
	for _, shape := range []string{"one_token", "many_tokens"} {
		t.Run(shape, func(t *testing.T) {
			m, source := manualPeerIngress(t)
			q := m.ingress
			_, fixed := q.Usage()
			wire := peerSpotResourceWire("PC61", shape)
			for range peerIngressCount + 1 {
				_, err := source.withParsedFrame(wire, time.Time{}, func(f *Frame) (bool, error) { m.HandleFrame(f, source); return false, nil })
				if err != nil {
					t.Fatal(err)
				}
			}
			count, backing := q.Usage()
			if count == 0 || count > 64 || backing > 8<<20 {
				t.Fatalf("unbounded jobs/backing %d/%d", count, backing)
			}
			// Independent size-class oracle inventories every retained string
			// visible in the queued local Spot as well as immutable transit data.
			// This deliberately overcounts aliases/static strings; it cannot pass
			// solely because the production reservation counter claims a bound.
			inventory := fixed
			for i := range q.jobs {
				j := &q.jobs[i]
				if j.state == ingressFree {
					continue
				}
				assertPeerSpotStringOwned(t, wire, j.original.Type, "queued type")
				for _, field := range j.original.Fields {
					assertPeerSpotStringOwned(t, wire, field, "queued field")
				}
				actual := independentPeerAdmissionBacking(j)
				if actual > j.charge {
					t.Fatalf("undercharged retained job: inventory=%d charge=%d", actual, j.charge)
				}
				inventory += actual
			}
			if inventory > backing || inventory > 8<<20 {
				t.Fatalf("independent backing inventory %d exceeds charged %d", inventory, backing)
			}
			j := q.Take()
			modern := originalPeerSpotSentence(&j.original, j.original.Hop-1, false)
			legacy := originalPeerSpotSentence(&j.original, j.original.Hop-1, true)
			if modern == "" || legacy == "" {
				t.Fatal("maximum fixture skipped active encoder work")
			}
			if dedupeOracleAllocation(len(modern))+dedupeOracleAllocation(len(legacy)) > 2*dedupeOracleAllocation(MaxPeerFrameBytes) {
				t.Fatal("active variant workspace exceeded fixed reservation")
			}
			j.Finish()
			q.Close()
			if count, backing := q.Usage(); count != 0 || backing != fixed {
				t.Fatal("maximum jobs retained after close")
			}
			if used, _ := m.parseBudget.usage(); used != 0 {
				t.Fatal("reader lease retained by queued jobs")
			}
		})
	}
}

func independentPeerAdmissionBacking(j *SpotAdmission) int {
	// Objects <16B may keep a tiny-allocation block alive. These tests reuse
	// the pre-existing independent dedupe allocator oracle, not reservation code.
	stringsBacking := func(s string) int {
		if s == "" {
			return 0
		}
		return max(16, dedupeOracleAllocation(len(s)))
	}
	n := 16 + stringsBacking(j.original.Type) + stringsBacking(j.key)
	for _, s := range j.original.Fields {
		n += stringsBacking(s)
	}
	if j.local != nil {
		n += dedupeOracleAllocation(int(unsafe.Sizeof(*j.local)) + 8)
		v := reflect.ValueOf(j.local).Elem()
		for i := 0; i < v.NumField(); i++ {
			if v.Field(i).Kind() == reflect.String {
				n += stringsBacking(v.Field(i).String())
			}
		}
	}
	return n
}

func TestPeerIngressSourceTokenDisconnectAndReplacement(t *testing.T) {
	m, source := manualPeerIngress(t)
	if unsafe.Sizeof(sourceIdentity{}) == 0 {
		t.Fatal("zero-size identity")
	}
	ctx, cancel := context.WithCancel(context.Background())
	source.ctx, source.cancel = ctx, cancel
	other := &session{identity: new(sourceIdentity), id: "other", ctx: t.Context(), writeCh: make(chan string, 8)}
	m.sessions.Set(other.id, other)
	m.HandleFrame(ingressTestFrame(t, 1), source)
	j := m.ingress.Take()
	j.TakeSpot()
	j.Relay()
	j.Finish()
	if len(source.writeCh) != 0 || len(other.writeCh) != 1 {
		t.Fatal("exact source exclusion failed")
	}
	m.HandleFrame(ingressTestFrame(t, 2), source)
	source.close()
	replacement := &session{identity: new(sourceIdentity), id: source.id, ctx: t.Context(), writeCh: make(chan string, 8)}
	m.sessions.Set(source.id, replacement)
	j = m.ingress.Take()
	j.TakeSpot()
	j.Relay()
	j.Finish()
	if len(replacement.writeCh) != 1 || len(other.writeCh) != 2 {
		t.Fatal("disconnect revoked accepted work or replacement inherited old identity")
	}
	m.HandleFrame(ingressTestFrame(t, 3), nil)
	j = m.ingress.Take()
	j.TakeSpot()
	j.Relay()
	j.Finish()
	if len(replacement.writeCh) != 2 || len(other.writeCh) != 3 {
		t.Fatal("nil source excluded destinations")
	}
}

func TestPeerIngressConcurrentReservationChurn(t *testing.T) {
	m, _ := manualPeerIngress(t)
	q := m.ingress
	_, fixed := q.Usage()
	f := ingressTestFrame(t, 1)
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			for range 50 {
				j := q.reserve(m, f)
				if j != nil {
					j.Finish()
				}
			}
		})
	}
	wg.Wait()
	q.Close()
	if n, b := q.Usage(); n != 0 || b != fixed {
		t.Fatalf("churn retained %d/%d", n, b)
	}
}

func TestPeerIngressProgrammaticFrameCannotBypassByteEnvelope(t *testing.T) {
	m, _ := manualPeerIngress(t)
	_, fixed := m.ingress.Usage()
	f := ingressTestFrame(t, 1)
	f.Fields[4] = strings.Repeat("X", MaxPeerFrameBytes+1)
	if m.ingress.reserve(m, f) != nil {
		t.Fatal("programmatic frame exceeded retained envelope")
	}
	if n, b := m.ingress.Usage(); n != 0 || b != fixed || m.ingress.Refusals() != 1 {
		t.Fatal("oversize refusal retained a reservation")
	}
}
