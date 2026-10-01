//go:build qualification

package peer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math/bits"
	"strings"
	"sync"
	"time"
)

type qualificationRelay struct {
	digest                  [32]byte
	required, seen, allowed uint64
	node, order             int
}

type qualificationConfirmed struct {
	wire  string
	order int
	at    time.Time
}

// QualificationTopology owns bounded driver fixtures, not runtime authority.
// All graph population enters authenticated sockets through send. Clock changes
// occur only during Prepare; Next and Now then use a fixed offset at real speed.
type QualificationTopology struct {
	full                          bool
	peers                         []string
	nodes, users                  int
	starts, counts                []int
	offset                        time.Duration
	stamps                        []*TimestampGenerator
	mu                            sync.Mutex
	relays                        map[string]*qualificationRelay
	unavailable                   uint64
	observed, duplicates, unknown int
	prepared                      QualificationState
	confirmed                     []qualificationConfirmed
}

type QualificationTopologyReport struct {
	Prepared                                                                    QualificationState
	Records, RequiredRelays, ObservedRelays, MissingRelays, Duplicates, Unknown int
	DriverRecordLimit                                                           int
}

func NewQualificationTopology(full bool, ingressCalls []string) (*QualificationTopology, error) {
	want := 16
	if full {
		want = 64
	}
	if len(ingressCalls) != want {
		return nil, fmt.Errorf("topology profile requires %d peers", want)
	}
	return newQualificationTopology(full, ingressCalls), nil
}

// NewQualificationTopologyForCapacity preserves full graph/freshness while
// respecting Q4B's63 established identities. Its ingress target is the actual
// reachable4096*63 pairs; Q2/Q3 still require the strict64-peer constructor.
func NewQualificationTopologyForCapacity(ingressCalls []string) (*QualificationTopology, error) {
	if len(ingressCalls) != 63 && len(ingressCalls) != 64 {
		return nil, fmt.Errorf("capacity topology requires63 or64 peers")
	}
	return newQualificationTopology(true, ingressCalls), nil
}

func newQualificationTopology(full bool, ingressCalls []string) *QualificationTopology {
	f := &QualificationTopology{full: full, peers: append([]string(nil), ingressCalls...), nodes: 2048, users: 32768, relays: make(map[string]*qualificationRelay)}
	if full {
		f.nodes, f.users = 4096, 65536
	}
	f.starts, f.counts, f.stamps = make([]int, f.nodes), make([]int, f.nodes), make([]*TimestampGenerator, f.nodes)
	f.confirmed = make([]qualificationConfirmed, f.nodes)
	edges := f.users * 2
	for i := range f.nodes {
		f.stamps[i] = NewTimestampGenerator()
		if i == 0 {
			f.counts[i] = 8000
		} else {
			f.starts[i] = f.starts[i-1] + f.counts[i-1]
			f.counts[i] = (edges - 8000) / (f.nodes - 1)
			if i <= (edges-8000)%(f.nodes-1) {
				f.counts[i]++
			}
		}
	}
	return f
}

func qualificationCall(prefix string, index int) string {
	var suffix [4]byte
	for i := 3; i >= 0; i-- {
		suffix[i] = 'A' + byte(index%26)
		index /= 26
	}
	return prefix + string(suffix[:])
}

func (f *QualificationTopology) Now() time.Time      { return time.Now().Add(f.offset) }
func (*QualificationTopology) MessageOrigin() string { return qualificationCall("M0", 0) }

func (f *QualificationTopology) members(node int) []PC92Entry {
	result := make([]PC92Entry, 0, f.counts[node])
	for i := range f.counts[node] {
		result = append(result, PC92Entry{Call: qualificationCall("U0", (f.starts[node]+i)%f.users), Flags: 1})
	}
	return result
}

func qualificationFrame(origin, stamp, action string, members []PC92Entry, hop int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "PC92^%s^%s^%s^5%s:5457:633^", origin, stamp, action, origin)
	if action == "K" {
		b.WriteString("0^0^")
	} else {
		for _, member := range members {
			fmt.Fprintf(&b, "%d%s^", member.Flags, member.Call)
		}
	}
	fmt.Fprintf(&b, "H%d^", hop)
	return b.String()
}

func qualificationStamp(now time.Time) string {
	now = now.UTC()
	return fmt.Sprintf("%d", now.Hour()*3600+now.Minute()*60+now.Second())
}

func qualificationWait(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// QualificationAuthority permits an isolated external driver to use the same
// actor observations and setup clock seam over a bounded test-only transport.
// It grants no topology, cache, freshness, session, or gate mutation authority.
type QualificationAuthority interface {
	QualificationSnapshot(context.Context) (QualificationState, error)
	QualificationSetClockOffset(context.Context, time.Duration) error
}

func qualificationAwait(ctx context.Context, m QualificationAuthority, check func(QualificationState) bool) (QualificationState, error) {
	deadline := time.Now().Add(15 * time.Second)
	for {
		s, err := m.QualificationSnapshot(ctx)
		if err != nil {
			return s, err
		}
		if s.BlockedPeers != 0 || s.ClockGated || s.PublicationGated || s.PC92Refused != 0 || s.PC93Refused != 0 {
			return s, fmt.Errorf("qualification setup refused or gated: %+v", s)
		}
		if check(s) {
			return s, nil
		}
		if time.Now().After(deadline) {
			return s, fmt.Errorf("qualification setup checkpoint timed out: %+v", s)
		}
		if err := qualificationWait(ctx, 10*time.Millisecond); err != nil {
			return s, err
		}
	}
}

// Prepare asserts an empty authority graph, populates only through ordinary
// protocol records, and verifies each retirement/renewal transition. It refuses
// a fixture environment with unexpected initial peer authority rather than
// deleting it to create headroom.
func (f *QualificationTopology) Prepare(ctx context.Context, m QualificationAuthority, send func(int, string) error) error {
	initial, err := m.QualificationSnapshot(ctx)
	if err != nil {
		return err
	}
	if initial.Nodes != 0 || initial.Freshness != 0 || initial.Established != len(f.peers) {
		return fmt.Errorf("unexpected initial authority: %+v", initial)
	}
	if f.full {
		if err := f.prepareDetached(ctx, m, send); err != nil {
			return err
		}
	}
	if err := f.populate(ctx, m, send, 0, true); err != nil {
		return err
	}
	if f.full {
		if err := f.renew(ctx, m, send, "M0", 0, 4096); err != nil {
			return err
		}
	}
	wantFresh := f.nodes
	if f.full {
		wantFresh = 16384
	}
	s, err := qualificationAwait(ctx, m, func(s QualificationState) bool {
		return s.Nodes == f.nodes && s.Users == f.users && s.Edges == f.users*2 && s.Ingress == f.nodes*len(f.peers) && s.Freshness == wantFresh
	})
	if err != nil {
		return err
	}
	if f.full && (s.MessageOrigins != 4096 || s.DetachedTopologyWatermarks != 8192) {
		return fmt.Errorf("freshness classes differ: %+v", s)
	}
	f.prepared = s
	return nil
}

func (f *QualificationTopology) populate(ctx context.Context, m QualificationAuthority, send func(int, string) error, start int, members bool) error {
	baseline, err := m.QualificationSnapshot(ctx)
	if err != nil {
		return err
	}
	for i := range f.nodes {
		var entries []PC92Entry
		if members {
			entries = f.members(i)
		}
		line := qualificationFrame(qualificationCall("N0", start+i), qualificationStamp(f.Now()), "C", entries, 1)
		if len(line) > MaxPeerFrameBytes {
			return fmt.Errorf("fixture C exceeds wire bound: %d", len(line))
		}
		copies := 1
		if members {
			copies = len(f.peers)
		}
		for peerIndex := range copies {
			if err := send(peerIndex, line+"\r\n"); err != nil {
				return err
			}
			if len(line) > 32<<10 {
				// A simultaneous64-peer copy of a64KiB C exceeds the3MiB
				// authority mailbox. Setup is not the100-record/sec load;
				// observe each large ingress copy before sending the next.
				wantIngress := baseline.Ingress + i*copies + peerIndex + 1
				if _, err := qualificationAwait(ctx, m, func(s QualificationState) bool {
					return s.Nodes == baseline.Nodes+i+1 && s.Ingress == wantIngress
				}); err != nil {
					return err
				}
			}
		}
		if (i+1)%16 == 0 || i+1 == f.nodes {
			want := baseline.Nodes + i + 1
			if _, err := qualificationAwait(ctx, m, func(s QualificationState) bool { return s.Nodes == want && s.Ingress == baseline.Ingress+(i+1)*copies }); err != nil {
				return err
			}
		}
		// Setup is paced, with at most16 graph updates in flight. Large C frames
		// and duplicate ingress observations still exercise the real read path.
		if err := qualificationWait(ctx, 10*time.Millisecond); err != nil {
			return err
		}
	}
	return nil
}

func (f *QualificationTopology) advance(ctx context.Context, m QualificationAuthority, target time.Time) error {
	f.offset = target.Sub(time.Now())
	if err := m.QualificationSetClockOffset(ctx, f.offset); err != nil {
		return err
	}
	_, err := qualificationAwait(ctx, m, func(s QualificationState) bool { return s.NextObservation.After(target) })
	return err
}

func (f *QualificationTopology) renew(ctx context.Context, m QualificationAuthority, send func(int, string) error, prefix string, start, count int) error {
	before, err := m.QualificationSnapshot(ctx)
	if err != nil {
		return err
	}
	if before.PC93Keys+count > 65536 {
		return fmt.Errorf("warmup PC93 cache count would exceed budget")
	}
	for i := range count {
		line := fmt.Sprintf("PC93^%s^%s^V0VOID^W0TEST^^warmup^H1^\r\n", qualificationCall(prefix, start+i), qualificationStamp(f.Now()))
		if err := send(0, line); err != nil {
			return err
		}
		if (i+1)%32 == 0 || i+1 == count {
			if _, err := qualificationAwait(ctx, m, func(s QualificationState) bool { return s.PC93Keys == before.PC93Keys+i+1 }); err != nil {
				return err
			}
		}
	}
	after, err := m.QualificationSnapshot(ctx)
	if err != nil {
		return err
	}
	if before.Nodes != after.Nodes || before.Users != after.Users || before.Edges != after.Edges || before.ObservationCounts != after.ObservationCounts {
		return fmt.Errorf("PC93 renewal changed topology: before=%+v after=%+v", before, after)
	}
	return nil
}

func (f *QualificationTopology) prepareDetached(ctx context.Context, m QualificationAuthority, send func(int, string) error) error {
	// Cohort A occupies a disjoint range from the final live cohort.
	if err := f.populate(ctx, m, send, 4096, false); err != nil {
		return err
	}
	s, err := m.QualificationSnapshot(ctx)
	if err != nil {
		return err
	}
	third := s.NextObservation.Add(2 * time.Hour)
	if err := f.advance(ctx, m, third.Add(-time.Hour+time.Second)); err != nil {
		return err
	}
	if err := f.renew(ctx, m, send, "N0", 4096, 4096); err != nil {
		return err
	}
	if err := f.advance(ctx, m, third.Add(-time.Minute)); err != nil {
		return err
	}
	if err := f.renew(ctx, m, send, "N0", 4096, 4096); err != nil {
		return err
	}
	if err := f.advance(ctx, m, third.Add(time.Second)); err != nil {
		return err
	}
	if _, err := qualificationAwait(ctx, m, func(s QualificationState) bool {
		return s.Nodes == 0 && s.DetachedTopologyWatermarks == 4096 && s.MessageOrigins == 0
	}); err != nil {
		return err
	}
	if err := f.populate(ctx, m, send, 8192, false); err != nil {
		return err
	}
	s, err = m.QualificationSnapshot(ctx)
	if err != nil {
		return err
	}
	third = s.NextObservation.Add(2 * time.Hour)
	second := third.Add(-time.Hour + time.Second)
	for _, target := range []time.Time{second, third.Add(-time.Minute)} {
		for f.Now().Before(target) {
			next := f.Now().Add(20 * time.Minute)
			if next.After(target) {
				next = target
			}
			if err := f.advance(ctx, m, next); err != nil {
				return err
			}
			if err := f.renew(ctx, m, send, "N0", 4096, 4096); err != nil {
				return err
			}
		}
		if err := f.renew(ctx, m, send, "N0", 8192, 4096); err != nil {
			return err
		}
	}
	if err := f.advance(ctx, m, third.Add(time.Second)); err != nil {
		return err
	}
	_, err = qualificationAwait(ctx, m, func(s QualificationState) bool {
		return s.Nodes == 0 && s.DetachedTopologyWatermarks == 8192 && s.MessageOrigins == 0
	})
	return err
}

// SetUnavailablePeers changes only the predeclared Q3 recipient oracle. It has
// no access to admission or connection state. Source peer0 always remains live.
func (f *QualificationTopology) SetUnavailablePeers(mask uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unavailable = mask &^ 1
}

// Next uses the 45D/45A/2C/8K cycle; A restores the same45 ordinary edges removed
// by D. Node0's two complete64KB snapshots exercise large-record forwarding.
// The PC92 dedupe key hashes member fields, so retained key bytes do not equal
// complete wire bytes; transport and parsed working-set bounds still apply.
func (f *QualificationTopology) Next(now time.Time, index int) (int, string, error) {
	within := index % 100
	node := 1 + ((index/100)*45+within%45)%(f.nodes-1)
	action := "D"
	var entries []PC92Entry
	switch {
	case within < 90:
		if within >= 45 {
			action = "A"
		}
		entries = []PC92Entry{{Call: qualificationCall("U0", f.starts[node]%f.users), Flags: 1}}
	case within < 92:
		node, action = 0, "C"
		entries = f.members(0)
	default:
		action = "K"
	}
	stamp, err := f.stamps[node].NextAt(now)
	if err != nil {
		return 0, "", err
	}
	origin := qualificationCall("N0", node)
	line := qualificationFrame(origin, stamp, action, entries, 2)
	key := origin + "^" + stamp + "^" + action
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.relays) >= 270000 {
		return 0, "", fmt.Errorf("PC92 driver record budget exhausted")
	}
	if f.relays[key] != nil {
		return 0, "", fmt.Errorf("duplicate PC92 fixture key %s", key)
	}
	all := ^uint64(0)
	if len(f.peers) < 64 {
		all = (uint64(1) << len(f.peers)) - 1
	}
	f.relays[key] = &qualificationRelay{digest: sha256.Sum256([]byte(strings.TrimSuffix(line, "H2^") + "H1^")), required: all &^ (f.unavailable | 1), allowed: all &^ 1, node: node, order: index + 1}
	return 0, line + "\r\n", nil
}

// Observe verifies the exact forwarded payload and hop, independent of graph
// admission counters. Unknown fixture records and duplicate outputs fail.
func (f *QualificationTopology) Observe(peerIndex int, line string) error {
	return f.ObserveBytes(peerIndex, []byte(line))
}

// ObserveBytes borrows the receiver buffer only for this call. Successful
// relay lookup/digest needs no string copy; the one latest-confirmed record per
// origin receives its own compact copy for Q3 alternate-ingress recovery.
func (f *QualificationTopology) ObserveBytes(peerIndex int, line []byte) error {
	line = bytes.TrimSpace(line)
	if !bytes.HasPrefix(line, []byte("PC92^N0")) {
		return nil
	}
	end := 5
	for range 3 {
		next := bytes.IndexByte(line[end:], '^')
		if next < 0 {
			return fmt.Errorf("malformed PC92 fixture relay")
		}
		end += next + 1
	}
	key := line[5 : end-1]
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.relays[string(key)]
	if r == nil {
		// Local publication has its distinct configured origin; it is not a
		// fixture relay. All other unknown N0 fixture identities are errors.
		if bytes.HasPrefix(line, []byte("PC92^N0CALL-1^")) {
			return nil
		}
		f.unknown++
		return fmt.Errorf("unknown PC92 fixture relay %s", key)
	}
	if peerIndex < 0 || peerIndex >= len(f.peers) {
		f.unknown++
		return fmt.Errorf("wrong PC92 relay peer %d", peerIndex)
	}
	bit := uint64(1) << peerIndex
	if r.allowed&bit == 0 || r.digest != sha256.Sum256(line) {
		f.unknown++
		return fmt.Errorf("wrong PC92 relay recipient or content for %s", key)
	}
	if r.seen&bit != 0 {
		f.duplicates++
		return fmt.Errorf("duplicate PC92 relay %s", key)
	}
	r.seen |= bit
	f.observed++
	if r.order > f.confirmed[r.node].order {
		f.confirmed[r.node] = qualificationConfirmed{wire: string(line), order: r.order, at: time.Now()}
	}
	return nil
}

// RestoreIngress replays only externally observed accepted records. Their
// hop1 copy adds alternate ingress through the ordinary duplicate path without
// racing a newer sender timestamp or generating another required relay.
func (f *QualificationTopology) RestoreIngress(ctx context.Context, m QualificationAuthority, mask uint64, send func(int, string) error) error {
	for node := range f.nodes {
		f.mu.Lock()
		confirmed := f.confirmed[node]
		f.mu.Unlock()
		if confirmed.wire == "" || time.Since(confirmed.at) >= 5*time.Minute {
			return fmt.Errorf("no recent externally confirmed record for origin%d", node)
		}
		for peer := range len(f.peers) {
			if mask&(uint64(1)<<peer) != 0 {
				if err := send(peer, confirmed.wire+"\r\n"); err != nil {
					return err
				}
			}
		}
		if err := qualificationWait(ctx, 2*time.Millisecond); err != nil {
			return err
		}
	}
	_, err := qualificationAwait(ctx, m, func(s QualificationState) bool { return s.Ingress == f.nodes*len(f.peers) })
	return err
}

func (f *QualificationTopology) Report() QualificationTopologyReport {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := QualificationTopologyReport{Prepared: f.prepared, Records: len(f.relays), ObservedRelays: f.observed, Duplicates: f.duplicates, Unknown: f.unknown, DriverRecordLimit: 270000}
	for _, entry := range f.relays {
		r.RequiredRelays += bits.OnesCount64(entry.required)
		r.MissingRelays += bits.OnesCount64(entry.required &^ entry.seen)
	}
	return r
}

func (f *QualificationTopology) Verify() error {
	r := f.Report()
	if r.MissingRelays != 0 || r.Duplicates != 0 || r.Unknown != 0 {
		return fmt.Errorf("PC92 delivery ledger: %+v", r)
	}
	return nil
}
