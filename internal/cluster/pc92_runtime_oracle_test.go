//go:build qualification

package cluster

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/telnet"
)

const qualificationHistogramBins = 512

var qualificationLatencyBounds = func() [qualificationHistogramBins]time.Duration {
	var bounds [qualificationHistogramBins]time.Duration
	i := 1 // zero has its own bucket
	for us := 100; us <= 25000; us += 100 {
		bounds[i] = time.Duration(us) * time.Microsecond
		i++
	}
	for ms := 26; ms <= 100; ms++ {
		bounds[i] = time.Duration(ms) * time.Millisecond
		i++
	}
	for ms := 110; ms <= 1000; ms += 10 {
		bounds[i] = time.Duration(ms) * time.Millisecond
		i++
	}
	for sec := 2; sec <= 64; sec++ {
		bounds[i] = time.Duration(sec) * time.Second
		i++
	}
	for sec := 66; sec <= 120; sec += 2 {
		bounds[i] = time.Duration(sec) * time.Second
		i++
	}
	for _, sec := range []int{128, 256, 512, 1024} {
		bounds[i] = time.Duration(sec) * time.Second
		i++
	}
	if i != len(bounds)-1 {
		panic("qualification histogram bound mismatch")
	}
	bounds[i] = math.MaxInt64
	return bounds
}()

// All state is allocated to profile maxima. A token index addresses immutable
// input metadata published by its atomic timestamp. Atomic bitsets distinguish
// actual arrivals from repeated callbacks without retaining traffic strings.
type qualificationInput struct {
	started                   atomic.Int64
	spot                      bool
	client                    int // -1 = every client, otherwise one private-message recipient
	peerRequired, peerAllowed uint64
}

type qualificationHistogram struct {
	bins                        [qualificationHistogramBins]atomic.Uint32
	count, over5, over25        atomic.Uint32
	marginCross5, marginCross25 atomic.Uint32
}

func (h *qualificationHistogram) observe(d time.Duration) {
	i := 0
	if d > 0 && d <= 25*time.Millisecond {
		i = int((d + 100*time.Microsecond - 1) / (100 * time.Microsecond))
	} else if d > 25*time.Millisecond {
		i = sort.Search(len(qualificationLatencyBounds), func(i int) bool { return d <= qualificationLatencyBounds[i] })
	}
	h.bins[i].Add(1)
	h.count.Add(1)
	if d > 5*time.Millisecond {
		h.over5.Add(1)
	}
	if d > 25*time.Millisecond {
		h.over25.Add(1)
	}
}

type qualificationLatency struct {
	Count                                     uint32  `json:"count"`
	P99UpperMS                                float64 `json:"p99_upper_ms"`
	Over5MS, Over25MS                         uint32
	UncertaintyCross5MS, UncertaintyCross25MS uint32
}

func (h *qualificationHistogram) result() qualificationLatency {
	r := qualificationLatency{Count: h.count.Load(), Over5MS: h.over5.Load(), Over25MS: h.over25.Load(), UncertaintyCross5MS: h.marginCross5.Load(), UncertaintyCross25MS: h.marginCross25.Load()}
	if r.Count == 0 {
		return r
	}
	rank := (uint64(r.Count)*99 + 99) / 100
	var cumulative uint64
	for i := range h.bins {
		cumulative += uint64(h.bins[i].Load())
		if cumulative >= rank {
			r.P99UpperMS = float64(qualificationLatencyBounds[i]) / float64(time.Millisecond)
			break
		}
	}
	return r
}

type qualificationRecipient struct {
	name                        string
	peer                        bool
	index                       int
	read, enqueue               []atomic.Uint64
	readLatency, enqueueLatency []qualificationHistogram // zero=overall, then input minutes
	renamed                     atomic.Uint32
	displayTruncated            atomic.Uint32
}

type qualificationOracle struct {
	epoch            time.Time
	measurementEpoch time.Time
	clockNow         func() time.Time
	clockFrequency   int64
	remoteEnqueue    []qualificationRecipientResult
	inputs           []qualificationInput
	used             int
	clients, peers   []*qualificationRecipient
	sessionIDs       []uint64
	failures         atomic.Uint64
	failureMu        sync.Mutex
	examples         []string
	closing          atomic.Bool
	// Instrumentation owns no per-event allocation beyond bounded examples.
	allocatedBytes uint64
}

func newQualificationOracle(maxInputs, clients, peers, minutes int) *qualificationOracle {
	return newQualificationOracleInputs(make([]qualificationInput, maxInputs), clients, peers, minutes)
}

func newQualificationOracleInputs(inputs []qualificationInput, clients, peers, minutes int) *qualificationOracle {
	maxInputs := len(inputs)
	o := &qualificationOracle{inputs: inputs, sessionIDs: make([]uint64, clients)}
	words := (maxInputs + 63) / 64
	for i := 0; i < clients+peers; i++ {
		r := &qualificationRecipient{read: make([]atomic.Uint64, words)}
		if i < clients {
			r.name, r.index = fmt.Sprintf("client-%d", i), i
			r.enqueue = make([]atomic.Uint64, words)
			r.readLatency, r.enqueueLatency = make([]qualificationHistogram, minutes+1), make([]qualificationHistogram, minutes+1)
			o.clients = append(o.clients, r)
		} else {
			r.name, r.index, r.peer = fmt.Sprintf("peer-%d", i-clients), i-clients, true
			o.peers = append(o.peers, r)
		}
	}
	o.allocatedBytes = uint64(maxInputs)*40 + uint64(words)*8*uint64(2*clients+peers) + uint64(clients*(minutes+1)*2)*(qualificationHistogramBins*4+20)
	return o
}

func (o *qualificationOracle) fail(format string, args ...any) {
	o.failures.Add(1)
	o.failureMu.Lock()
	defer o.failureMu.Unlock()
	if len(o.examples) < 32 {
		o.examples = append(o.examples, fmt.Sprintf(format, args...))
	}
}

func (o *qualificationOracle) add(spot bool, client int, required, allowed uint64) (int, string, error) {
	if o.used == len(o.inputs) {
		return 0, "", fmt.Errorf("declared input ledger capacity exhausted")
	}
	id := o.used
	o.used++
	in := &o.inputs[id]
	in.spot, in.client, in.peerRequired, in.peerAllowed = spot, client, required, allowed
	// Zero is reserved for unpublished entries. The original clock is monotonic
	// and remains in this table throughout every queue, correction and hold.
	in.started.Store(o.measurementNow().Sub(o.measurementBase()).Nanoseconds() + 1)
	return id, fmt.Sprintf("QID%07d", id), nil
}

func qualificationToken(text string) (int, bool) {
	i := strings.Index(text, "QID")
	if i < 0 || len(text) < i+10 {
		return 0, false
	}
	id := 0
	for j := i + 3; j < i+10; j++ {
		if text[j] < '0' || text[j] > '9' {
			return 0, false
		}
		id = id*10 + int(text[j]-'0')
	}
	if len(text) > i+10 && text[i+10] >= '0' && text[i+10] <= '9' {
		return 0, false
	}
	if strings.Contains(text[i+10:], "QID") {
		return 0, false
	}
	return id, true
}

func qualificationTokenBytes(text []byte) (int, bool) {
	i := bytes.Index(text, []byte("QID"))
	if i < 0 || len(text) < i+10 {
		return 0, false
	}
	id := 0
	for j := i + 3; j < i+10; j++ {
		if text[j] < '0' || text[j] > '9' {
			return 0, false
		}
		id = id*10 + int(text[j]-'0')
	}
	if len(text) > i+10 && text[i+10] >= '0' && text[i+10] <= '9' {
		return 0, false
	}
	if bytes.Contains(text[i+10:], []byte("QID")) {
		return 0, false
	}
	return id, true
}

func qualificationMark(bits []atomic.Uint64, id int) bool {
	mask := uint64(1) << uint(id%64)
	return bits[id/64].Or(mask)&mask == 0
}

func qualificationHas(bits []atomic.Uint64, id int) bool {
	return len(bits) > 0 && bits[id/64].Load()&(uint64(1)<<uint(id%64)) != 0
}

func (o *qualificationOracle) observed(r *qualificationRecipient, text, dx string, at time.Time, enqueue bool) {
	id, ok := qualificationToken(text)
	if !ok || id >= len(o.inputs) {
		o.fail("%s unknown/malformed token in %q", r.name, text)
		return
	}
	o.observedID(r, id, dx, at, enqueue)
}

func (o *qualificationOracle) observedID(r *qualificationRecipient, id int, dx string, at time.Time, enqueue bool) {
	if id < 0 || id >= len(o.inputs) {
		o.fail("%s unknown token %d", r.name, id)
		return
	}
	in := &o.inputs[id]
	stamp := in.started.Load()
	if stamp == 0 {
		o.fail("%s observed unsent token %d", r.name, id)
		return
	}
	allowed := in.client < 0 || in.client == r.index
	if r.peer {
		allowed = in.peerAllowed&(uint64(1)<<uint(r.index)) != 0
	}
	if !allowed || (enqueue && !in.spot) {
		o.fail("%s unexpected token %d enqueue=%t", r.name, id, enqueue)
		return
	}
	bits := r.read
	if enqueue {
		bits = r.enqueue
	}
	if !qualificationMark(bits, id) {
		o.fail("%s duplicate token %d enqueue=%t", r.name, id, enqueue)
		return
	}
	if !in.spot {
		return
	}
	expectedDX := qualificationDXCall(id)
	// The telnet presentation intentionally clips calls to ten characters.
	// Semantic correction is observed from the full identity at enqueue; peer
	// records also retain it. Display truncation must not masquerade as rename.
	if !enqueue && !r.peer && dx == expectedDX[:10] {
		r.displayTruncated.Add(1)
	}
	if (enqueue || r.peer) && dx != "" && dx != expectedDX {
		r.renamed.Add(1)
	}
	if r.peer {
		return
	}
	latency := at.Sub(o.measurementBase()) - time.Duration(stamp-1)
	if latency < 0 {
		o.fail("%s negative token latency %d", r.name, id)
		return
	}
	minute := int(qualificationTickDuration(time.Duration(stamp-1), o.clockFrequency, false)/time.Minute) + 1
	hist := r.readLatency
	if enqueue {
		hist = r.enqueueLatency
	}
	if minute >= len(hist) {
		o.fail("%s cohort overflow for token %d", r.name, id)
		return
	}
	hist[0].observeCounter(latency, o.clockFrequency)
	hist[minute].observeCounter(latency, o.clockFrequency)
}

func (o *qualificationOracle) enqueued(event telnet.QualificationEnqueue) {
	// The tagged service uses the same QPC domain as the external sender and
	// receiver. Taking this immediately on entry is conservative relative to
	// the successful queue admission that invoked the callback.
	if o.clockNow != nil {
		event.ObservedAt = o.clockNow()
	}
	if !strings.Contains(event.Comment, "QID") {
		return
	}
	login := strings.TrimSuffix(strings.TrimPrefix(event.Login, "DL"), "CAA")
	i, err := strconv.Atoi(login)
	if err != nil || i < 1 || i > len(o.clients) {
		o.fail("unknown enqueue recipient %q", event.Login)
		return
	}
	if event.SessionID != o.sessionIDs[i-1] {
		o.fail("enqueue session replaced for %s", event.Login)
		return
	}
	o.observed(o.clients[i-1], event.Comment, event.DXCall, event.ObservedAt, true)
}

type qualificationCohort struct {
	InputMinute        int
	RequiredSpots      int
	Enqueue, FirstByte qualificationLatency
}

type qualificationRecipientResult struct {
	Name                                            string
	Required, Received, MissingEnqueue, MissingRead int
	Renamed                                         uint32
	DisplayTruncated                                uint32
	Cohorts                                         []qualificationCohort `json:",omitempty"`
}

func (o *qualificationOracle) results(enforceLatency bool) []qualificationRecipientResult {
	all := append(append([]*qualificationRecipient(nil), o.clients...), o.peers...)
	out := make([]qualificationRecipientResult, 0, len(all))
	for _, r := range all {
		result := qualificationRecipientResult{Name: r.name, Renamed: r.renamed.Load(), DisplayTruncated: r.displayTruncated.Load()}
		remote := !r.peer && o.remoteEnqueue != nil
		if remote {
			result.MissingEnqueue = o.remoteEnqueue[r.index].MissingEnqueue
			result.Renamed = o.remoteEnqueue[r.index].Renamed
		}
		if !r.peer {
			result.Cohorts = make([]qualificationCohort, len(r.readLatency))
			for i := range result.Cohorts {
				result.Cohorts[i] = qualificationCohort{InputMinute: i - 1, Enqueue: r.enqueueLatency[i].result(), FirstByte: r.readLatency[i].result()}
				if remote {
					result.Cohorts[i].Enqueue = o.remoteEnqueue[r.index].Cohorts[i].Enqueue
				}
			}
		}
		for id := 0; id < o.used; id++ {
			in := &o.inputs[id]
			required := in.client < 0 || in.client == r.index
			if r.peer {
				required = in.peerRequired&(uint64(1)<<uint(r.index)) != 0
			}
			if !required {
				continue
			}
			result.Required++
			if qualificationHas(r.read, id) {
				result.Received++
			} else {
				result.MissingRead++
			}
			if !r.peer && in.spot {
				if !remote && !qualificationHas(r.enqueue, id) {
					result.MissingEnqueue++
				}
				cohort := int(qualificationTickDuration(time.Duration(in.started.Load()-1), o.clockFrequency, false)/time.Minute) + 1
				result.Cohorts[0].RequiredSpots++
				if cohort < 1 || cohort >= len(result.Cohorts) {
					o.fail("%s final cohort overflow for token %d", r.name, id)
				} else {
					result.Cohorts[cohort].RequiredSpots++
				}
			}
		}
		if result.MissingRead+result.MissingEnqueue > 0 {
			o.fail("%s missing read=%d enqueue=%d", r.name, result.MissingRead, result.MissingEnqueue)
		}
		for _, cohort := range result.Cohorts {
			if remote && o.remoteEnqueue[r.index].Cohorts[cohort.InputMinute+1].RequiredSpots != cohort.RequiredSpots {
				o.fail("%s child input cohort denominator differs", r.name)
			}
			if !enforceLatency || cohort.RequiredSpots == 0 {
				continue
			}
			// Nearest-rank p99 can have at most N-ceil(.99*N) observations above
			// threshold. Counts use the declared denominator, never survivors.
			allowed := cohort.RequiredSpots - (99*cohort.RequiredSpots+99)/100
			if int(cohort.Enqueue.Count) != cohort.RequiredSpots || int(cohort.FirstByte.Count) != cohort.RequiredSpots || int(cohort.Enqueue.Over5MS) > allowed || int(cohort.FirstByte.Over25MS) > allowed {
				o.fail("%s input minute%d violates delivery/5ms/25ms contract", r.name, cohort.InputMinute)
			}
		}
		out = append(out, result)
	}
	return out
}

// Equal-length paired letters prevent distance-one substitution families. All
// digits are before the suffix, each identity fits the 12-byte secondary key,
// and ordinary CTY/filter/correction/dedupe processing still runs unchanged.
func qualificationDXCall(id int) string {
	var call [11]byte
	copy(call[:], "DL1")
	for i := 3; i >= 0; i-- {
		letter := byte('A' + id%26)
		id /= 26
		call[3+i*2], call[4+i*2] = letter, letter
	}
	return string(call[:])
}

func TestQualificationOracleThresholdsAndMissing(t *testing.T) {
	var h qualificationHistogram
	for _, d := range []time.Duration{4999 * time.Microsecond, 5 * time.Millisecond, 5001 * time.Microsecond, 24999 * time.Microsecond, 25 * time.Millisecond, 25001 * time.Microsecond} {
		h.observe(d)
	}
	r := h.result()
	if r.Count != 6 || r.Over5MS != 4 || r.Over25MS != 1 || r.P99UpperMS != 26 {
		t.Fatalf("threshold boundaries: %+v", r)
	}
	o := newQualificationOracle(101, 1, 0, 2)
	o.epoch = time.Now()
	for i := 0; i < 100; i++ {
		_, _, _ = o.add(true, -1, 0, 0)
	}
	for i := 0; i < 99; i++ {
		in := &o.inputs[i]
		at := o.epoch.Add(time.Duration(in.started.Load()-1) + time.Millisecond)
		token := fmt.Sprintf("QID%07d", i)
		o.observed(o.clients[0], token, "renamed-call", at, true)
		o.observed(o.clients[0], token, "renamed-call", at, false)
	}
	results := o.results(true)
	if results[0].MissingRead != 1 || results[0].MissingEnqueue != 1 || o.failures.Load() == 0 {
		t.Fatal("missing slow input disappeared from denominator")
	}
}

func TestQualificationOracleTokenAndMinute(t *testing.T) {
	o := newQualificationOracle(1, 1, 0, 2)
	o.epoch = time.Now()
	_, _, _ = o.add(true, -1, 0, 0)
	o.inputs[0].started.Store(int64(59999*time.Millisecond) + 1)
	o.observed(o.clients[0], "prefix QID0000000", qualificationDXCall(0), o.epoch.Add(61*time.Second), false)
	if o.clients[0].readLatency[1].count.Load() != 1 || o.clients[0].readLatency[2].count.Load() != 0 {
		t.Fatal("late delivery moved to output minute")
	}
	o.observed(o.clients[0], "QID0000000", qualificationDXCall(0), o.epoch.Add(62*time.Second), false)
	o.observed(o.clients[0], "QID9999999", qualificationDXCall(0), o.epoch.Add(62*time.Second), false)
	if o.failures.Load() != 2 {
		t.Fatal("duplicate/unknown was not a checker failure")
	}
}

func TestQualificationOracleBoundsAndDisplay(t *testing.T) {
	o := newQualificationOracle(1, 1, 0, 1)
	o.epoch = time.Now()
	_, token, err := o.add(true, -1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := o.add(true, -1, 0, 0); err == nil {
		t.Fatal("input capacity overflow silently admitted")
	}
	for _, text := range []string{"QID00000001", "QID0000000 QID0000000", "QIDabcdefg", "QID000000", "QID+000001", "QID-000001"} {
		if _, ok := qualificationToken(text); ok {
			t.Fatalf("accepted ambiguous token %q", text)
		}
		if _, ok := qualificationTokenBytes([]byte(text)); ok {
			t.Fatalf("accepted ambiguous byte token %q", text)
		}
	}
	at := o.epoch.Add(time.Duration(o.inputs[0].started.Load()-1) + time.Millisecond)
	o.observed(o.clients[0], token, qualificationDXCall(0), at, true)
	o.observed(o.clients[0], token, qualificationDXCall(0)[:10], at, false)
	result := o.results(true)[0]
	if result.Renamed != 0 || result.DisplayTruncated != 1 || o.failures.Load() != 0 {
		t.Fatalf("display truncation changed identity accounting: %+v", result)
	}
	o.inputs[0].started.Store(int64(time.Minute) + 1)
	_ = o.results(false)
	if o.failures.Load() == 0 {
		t.Fatal("out-of-budget input cohort did not fail")
	}
}
