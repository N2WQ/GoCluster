//go:build qualification

package peer

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dxcluster/config"
	"dxcluster/spot"
)

type q5Receiver struct {
	conn                        net.Conn
	done                        chan struct{}
	spot, healthySpot, bulletin atomic.Int64
	invalid                     atomic.Bool
}

type q5Rig struct {
	m                                                *Manager
	addr                                             string
	peers                                            []*q5Receiver
	ingest, messages, bulletins                      atomic.Int64
	healthyIngest, healthyMessages, healthyBulletins atomic.Int64
}

func newQ5Rig(t *testing.T) *q5Rig {
	t.Helper()
	port := q6Port(t)
	r := &q5Rig{addr: fmt.Sprintf("127.0.0.1:%d", port)}
	cfg := config.PeeringConfig{Enabled: true, ForwardSpots: true, ListenPort: port, LocalCallsign: "N0CALL", NodeVersion: "5457", NodeBuild: "633", PC92Bitmap: 5, HopCount: 99, MaxLineLength: 65536, PC92MaxBytes: 65536, WriteQueueSize: 128, KeepaliveSeconds: 600, ConfigSeconds: 1800,
		Timeouts: config.PeeringTimeouts{LoginSeconds: 60, InitSeconds: 60, IdleSeconds: 3600}}
	for _, call := range []string{"GB7SRC", "GB7DST", "GB7HEALTH"} {
		cfg.Peers = append(cfg.Peers, config.PeeringPeer{Enabled: true, Family: config.PeeringPeerFamilyDXSpider, Direction: config.PeeringPeerDirectionInbound, PreferPC9x: true, RemoteCallsign: call})
	}
	input := make(chan *spot.Spot, 4096)
	var err error
	r.m, err = NewManager(completeProtocolTestConfig(cfg, "N0CALL"), "N0CALL", input, 600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.m.SetBuildIdentity("v7-q5", "", "local", "2026-10-01", "go1.26"); err != nil {
		t.Fatal(err)
	}
	r.m.SetAnnouncementBroadcast(func(line string) {
		r.messages.Add(1)
		if strings.Contains(line, "Q5HEALTH") {
			r.healthyMessages.Add(1)
		}
	})
	r.m.SetWWVBroadcast(func(_, line string) {
		r.bulletins.Add(1)
		if strings.Contains(line, "Q5HEALTH") {
			r.healthyBulletins.Add(1)
		}
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case s := <-input:
				r.ingest.Add(1)
				if strings.Contains(s.Comment, "Q5HEALTH") {
					r.healthyIngest.Add(1)
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	t.Cleanup(func() {
		for _, p := range r.peers {
			_ = p.conn.Close()
		}
		r.m.Stop()
		cancel()
		<-done
		for _, p := range r.peers {
			<-p.done
			if p.invalid.Load() {
				t.Error("Q5 receiver exceeded bounded framing")
			}
		}
	})
	if err := r.m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *q5Rig) connect(t *testing.T, call string) *q5Receiver {
	t.Helper()
	dialer := net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(t.Context(), "tcp", r.addr)
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReaderSize(conn, MaxPeerFrameBytes+2)
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	q6ReadUntil(t, reader, "login:")
	if _, err := io.WriteString(conn, call+"\r\n"); err != nil {
		t.Fatal(err)
	}
	q6ReadUntil(t, reader, "PC18^")
	q6ReadUntil(t, reader, "\n")
	if _, err := io.WriteString(conn, "PC18^DXSpider Version: 1.57 Build: 633 [pc9x 91]^5457^\r\nPC20^\r\n"); err != nil {
		t.Fatal(err)
	}
	gotC := false
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(line, "^C^") {
			gotC = true
		}
		if gotC && strings.Contains(line, "^A^") {
			break
		}
	}
	_ = conn.SetDeadline(time.Time{})
	p := &q5Receiver{conn: conn, done: make(chan struct{})}
	r.peers = append(r.peers, p)
	go func() {
		defer close(p.done)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if len(line) > MaxPeerFrameBytes+2 {
				p.invalid.Store(true)
				return
			}
			if strings.HasPrefix(line, "PC11^") || strings.HasPrefix(line, "PC61^") || strings.HasPrefix(line, "PC26^") {
				p.spot.Add(1)
				if strings.Contains(line, "Q5HEALTH") {
					p.healthySpot.Add(1)
				}
			}
			if strings.HasPrefix(line, "PC23^") || strings.HasPrefix(line, "PC73^") {
				p.bulletin.Add(1)
			}
		}
	}()
	return p
}

func q5Send(t *testing.T, p *q5Receiver, line string) {
	t.Helper()
	if err := p.conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(p.conn, line+"\r\n"); err != nil {
		t.Fatal(err)
	}
}

type q5Generator struct{ stamps [128]TimestampGenerator }

func (g *q5Generator) line(t *testing.T, class string, index int, healthy bool) string {
	t.Helper()
	now := time.Now().UTC()
	token, prefix := fmt.Sprintf("Q5FILL%07d", index), "F0"
	if healthy {
		token, prefix = fmt.Sprintf("Q5HEALTH%07d", index), "H0"
	}
	switch class {
	case "spot":
		return fmt.Sprintf("PC61^14020.0^%s^%s^%s^CW %s^DL1AAA^GB7SRC^192.0.2.1^H2^", qualificationCall(prefix, index), now.Format("02-Jan-2006"), now.Format("1504Z"), token)
	case "pc92", "pc93":
		stamp, err := g.stamps[index%len(g.stamps)].NextAt(now)
		if err != nil {
			t.Fatal(err)
		}
		origin := qualificationCall(prefix, index%len(g.stamps))
		if class == "pc92" {
			return qualificationFrame(origin, stamp, "K", nil, 1)
		}
		return fmt.Sprintf("PC93^%s^%s^ALL^DL1AAA^*^%s^H1^", origin, stamp, token)
	case "bulletin":
		return fmt.Sprintf("PC23^%s^%02d^100^5^2^%s^DL1AAA^GB7SRC^H2^", now.Format("02-Jan-2006"), now.Hour(), token)
	default:
		t.Fatalf("unknown Q5 class %s", class)
		return ""
	}
}

func q5Cache(r *q5Rig, class string) *dedupeCache {
	switch class {
	case "spot":
		return r.m.dedupe
	case "pc92":
		return r.m.protocol.pc92
	case "pc93":
		return r.m.protocol.pc93
	default:
		return r.m.bulletinDedupe
	}
}

func q5Key(t *testing.T, class, wire string) string {
	t.Helper()
	f, err := ParseFrame(wire)
	if err != nil {
		t.Fatal(err)
	}
	switch class {
	case "spot":
		s, err := parseSpotFromFrame(f, "GB7SRC")
		if err != nil {
			t.Fatal(err)
		}
		return dxKey(f, s)
	case "pc92":
		return pc92Key(f)
	case "pc93":
		return pc93Key(f)
	default:
		return wwvKey(f)
	}
}

func q5Await(t *testing.T, label string, duration time.Duration, predicate func() bool) {
	t.Helper()
	_, err := qualificationAwaitUntil(t.Context(), time.Now().Add(duration), 5*time.Millisecond,
		func(context.Context) (bool, error) { return predicate(), nil }, func(ready bool) bool { return ready })
	if err != nil {
		t.Fatalf("Q5 %s: %v", label, err)
	}
}

// Read-only evidence must not prune the cache it is observing. In particular,
// firstAdmission and contains are unsuitable here because they perform cleanup.
func q5StoredAdmission(cache *dedupeCache, key string) (time.Time, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	offset, ok := cache.items.Get(key)
	if !ok {
		return time.Time{}, false
	}
	return cache.epoch.Add(time.Duration(offset)), true
}

func q5CacheHasLive(cache *dedupeCache, key string) bool {
	at, ok := q5StoredAdmission(cache, key)
	return ok && time.Since(at) <= cache.ttl
}

func q5VerifyOriginalAdmission(cache *dedupeCache, key string, expected time.Time) error {
	actual, present := q5StoredAdmission(cache, key)
	if !present || !actual.Equal(expected) {
		return fmt.Errorf("original payload admission changed: present=%v actual=%s expected=%s", present, actual, expected)
	}
	return nil
}

// Expiration remains independent of retry eligibility. Observe the original
// payload's actual removal without calling a helper that prunes it. Equality
// at TTL is still live, and the existing maintenance allowance is one second.
func q5AwaitOriginalExpiry(ctx context.Context, cache *dedupeCache, key string, admitted time.Time) error {
	expires := admitted.Add(cache.ttl)
	_, err := qualificationAwaitUntil(ctx, expires.Add(time.Second), 5*time.Millisecond, func(context.Context) (bool, error) {
		actual, present := q5StoredAdmission(cache, key)
		observed := time.Now()
		if present && !actual.Equal(admitted) {
			return false, fmt.Errorf("original cache admission was renewed")
		}
		if !present && !observed.After(expires) {
			return false, fmt.Errorf("cache key disappeared before strict TTL expiry")
		}
		return !present, nil
	}, func(expired bool) bool { return expired })
	return err
}

func q5Snapshot(t *testing.T, m *Manager) QualificationState {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	s, err := m.QualificationSnapshot(ctx)
	if err != nil || ctx.Err() != nil {
		t.Fatalf("Q5 snapshot failed: %v context=%v", err, ctx.Err())
	}
	return s
}

// Four isolated managers saturate one class each, concurrently in elapsed time.
// Every other class continues through authenticated TCP and ordinary admission.
// This is a class-isolation/cleanup test, not the Q1-Q3 latency measurement.
func TestPC92QualificationQ5Isolation(t *testing.T) {
	profile := os.Getenv("GOCLUSTER_PC92_Q5_PROFILE")
	if profile != "qualification" && profile != "preflight" {
		t.Skip("opt-in Q5: scripts/pc92-q5-qualification.ps1")
	}
	for _, class := range []string{"spot", "pc92", "pc93", "bulletin"} {
		t.Run(class, func(t *testing.T) { t.Parallel(); q5IsolationClass(t, class, profile == "qualification") })
	}
}

func q5IsolationClass(t *testing.T, class string, full bool) {
	r := newQ5Rig(t)
	source := r.connect(t, "GB7SRC")
	destination := r.connect(t, "GB7DST")
	health := r.connect(t, "GB7HEALTH")
	cache := q5Cache(r, class)
	capacity := 65536
	if class == "spot" {
		capacity = 131072
	}
	if class == "bulletin" {
		capacity = 8192
	}
	cycles := 3
	if !full {
		cycles = 1
		t.Log("DIAGNOSTIC ONLY: full saturation, no600second-window qualification")
	}
	var fill, healthy q5Generator
	healthIndex := 0
	for cycle := 0; cycle < cycles; cycle++ {
		var first string
		var firstAdmission time.Time
		evidence := &retryEvidence{call: "GB7SRC"}
		if class == "pc92" {
			// Install before refusal. Retry startup and local recovery are allowed
			// while this cache remains full; actual PC92 admission still owns TTL.
			r.m.QualificationSetAdmissionObserver(evidence.observe)
		}
		started := time.Now()
		lastHealthy := started
		for base := 0; base < capacity; base += 32 {
			var batch strings.Builder
			for i := base; i < min(base+32, capacity); i++ {
				line := fill.line(t, class, cycle*capacity+i, false)
				if i == 0 {
					first = line
				}
				batch.WriteString(line)
				batch.WriteString("\r\n")
			}
			q5Send(t, source, batch.String())
			want := min(base+32, capacity)
			q5Await(t, "admitted saturation batch", 5*time.Second, func() bool { count, _, _ := cache.occupancy(); return count == want })
			if base == 0 {
				var present bool
				firstAdmission, present = q5StoredAdmission(cache, q5Key(t, class, first))
				if !present {
					t.Fatal("oldest admitted payload lacks original-age evidence")
				}
			}
			if time.Since(lastHealthy) >= time.Second {
				q5Healthy(t, r, health, destination, &healthy, class, healthIndex)
				healthIndex++
				lastHealthy = time.Now()
			}
			if err := qualificationWait(t.Context(), 10*time.Millisecond); err != nil {
				t.Fatal(err)
			}
		}
		filled := time.Now()
		count, keyBytes, refused := cache.occupancy()
		if count != capacity {
			t.Fatalf("did not reach required count: %d/%d", count, capacity)
		}
		q5Await(t, "admitted saturated-class delivery", 5*time.Second, func() bool {
			expected := int64((cycle + 1) * capacity)
			switch class {
			case "spot":
				return destination.spot.Load() == expected
			case "pc93":
				return r.messages.Load() == expected
			case "bulletin":
				return r.bulletins.Load() == expected
			default:
				return true // H1 K admission is observed through the actor/cache.
			}
		})
		ingestedBeforeProbe := r.ingest.Load()
		probe := fill.line(t, class, (cycle+1)*capacity+1, false)
		q5Send(t, source, first)
		q5Send(t, source, probe)
		q5Await(t, "new-key refusal", 5*time.Second, func() bool { _, _, n := cache.occupancy(); return n == refused+1 })
		// This same-source refusal follows the duplicate in receive order. A
		// next-oldest expiry can hide renewal of the first key from gate timing,
		// so compare the stored age itself without pruning or extending it.
		if err := q5VerifyOriginalAdmission(cache, q5Key(t, class, first), firstAdmission); err != nil {
			t.Fatal(err)
		}
		if count, _, _ := cache.occupancy(); count != capacity || !q5CacheHasLive(cache, q5Key(t, class, first)) {
			t.Fatal("overflow evicted unexpired key")
		}
		if class == "spot" {
			q5Await(t, "local ingestion during forwarding-cache saturation", time.Second, func() bool { return r.ingest.Load() == ingestedBeforeProbe+2 })
		}
		if class == "pc92" {
			q5Await(t, "authoritative source closure", 3*time.Second, func() bool {
				select {
				case <-source.done:
					return true
				default:
					return false
				}
			})
			failure, err := evidence.latest("failure")
			if err != nil || failure.Generation == 0 {
				t.Fatalf("Q5 missing failure attempt: %+v %v", failure, err)
			}
			// This fixture directly constructs a manager with zero backoff.
			// Its inherited effective delay is one second, not loader 2s/300s.
			if err := qualificationWait(t.Context(), max(0, time.Until(failure.At.Add(time.Second)))); err != nil {
				t.Fatal(err)
			}
			source = r.connect(t, "GB7SRC")
			if err := evidence.check(time.Second, time.Second, false); err != nil {
				t.Fatalf("Q5 controlled startup with full cache: %v", err)
			}
			if count, _, _ := cache.occupancy(); count != capacity {
				t.Fatal("retry startup required cache headroom or evicted unexpired entries")
			}
		}
		hold := 601 * time.Second
		if !full {
			hold = 3 * time.Second
		}
		deadline := filled.Add(hold)
		recoveryChecked := class != "pc92"
		expiryChecked := false
		firstExpiry := firstAdmission.Add(cache.ttl)
		for time.Now().Before(deadline) {
			if full && !expiryChecked && !time.Now().Before(firstExpiry.Add(-100*time.Millisecond)) {
				if err := q5AwaitOriginalExpiry(t.Context(), cache, q5Key(t, class, first), firstAdmission); err != nil {
					t.Fatalf("Q5 original key actual expiry: %v", err)
				}
				expiryChecked = true
			}
			if class == "pc92" && full && !recoveryChecked {
				reset, err := evidence.latest("healthy_reset")
				if err != nil {
					t.Fatal(err)
				}
				if reset.Generation != 0 {
					if err := evidence.check(time.Second, time.Second, true); err != nil {
						t.Fatalf("Q5 healthy retry reset: %v", err)
					}
					recoveryChecked = true
				}
			}
			q5Healthy(t, r, health, destination, &healthy, class, healthIndex)
			healthIndex++
			wait := time.Second
			if full && !expiryChecked {
				wait = min(wait, max(0, time.Until(firstExpiry.Add(-100*time.Millisecond))))
			}
			if err := qualificationWait(t.Context(), wait); err != nil {
				t.Fatal(err)
			}
		}
		if full {
			if !expiryChecked {
				t.Fatal("first original key expiry was not observed")
			}
			q5Await(t, "strict-TTL cleanup within maintenance allowance", time.Second, func() bool { count, _, _ := cache.occupancy(); return count == 0 })
			if _, present := q5StoredAdmission(cache, q5Key(t, class, first)); present {
				t.Fatal("duplicate renewed oldest payload age")
			}
			if class == "pc92" && !recoveryChecked {
				t.Fatal("healthy retry reset was not observed during the hold")
			}
		}
		stats := q5Snapshot(t, r.m)
		if stats.PC93InputRefused != 0 || class != "spot" && stats.SpotRefused != 0 || class != "pc92" && stats.PC92Refused != 0 || class != "pc93" && stats.PC93Refused != 0 || class != "bulletin" && stats.BulletinRefused != 0 {
			t.Fatalf("cross-class refusal: %+v", stats)
		}
		t.Logf("class=%s cycle=%d capacity=%d keyBytes=%d fill=%s realWindow=%s healthyChecks=%d stats=%+v", class, cycle, capacity, keyBytes, filled.Sub(started), time.Since(filled), healthIndex, stats)
		if class == "pc92" {
			r.m.QualificationSetAdmissionObserver(nil)
		}
	}
	if full {
		// Drain healthy-class keys as well: no input extends the final window.
		if err := qualificationWait(t.Context(), 601*time.Second); err != nil {
			t.Fatal(err)
		}
		stats := q5Snapshot(t, r.m)
		if stats.SpotKeys+stats.PC92Keys+stats.PC93Keys+stats.BulletinKeys != 0 {
			t.Fatalf("terminal payload cleanup incomplete: %+v", stats)
		}
	}
}

func q5Healthy(t *testing.T, r *q5Rig, source, destination *q5Receiver, g *q5Generator, saturated string, index int) {
	t.Helper()
	for _, class := range []string{"spot", "pc92", "pc93", "bulletin"} {
		if class == saturated {
			continue
		}
		beforeSpot, beforeMessage, beforeBulletin := destination.healthySpot.Load(), r.healthyMessages.Load(), r.healthyBulletins.Load()
		beforeIngest := r.healthyIngest.Load()
		line := g.line(t, class, index, true)
		q5Send(t, source, line)
		q5Await(t, "healthy "+class, time.Second, func() bool {
			switch class {
			case "spot":
				return destination.healthySpot.Load() == beforeSpot+1 && r.healthyIngest.Load() == beforeIngest+1
			case "pc93":
				return r.healthyMessages.Load() == beforeMessage+1
			case "bulletin":
				return r.healthyBulletins.Load() == beforeBulletin+1
			default:
				return q5CacheHasLive(r.m.protocol.pc92, q5Key(t, class, line))
			}
		})
	}
}
