//go:build qualification

package peer

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This observer changes only the external immutable membership provider, after
// a real retry C was admitted. It neither blocks the actor nor edits its state.
// One bounded target overlaps membership change with recovery amid all retries.
type retryMembershipQualification struct {
	mu                                       sync.Mutex
	m                                        *Manager
	provider                                 *atomic.Pointer[LocalMembership]
	after                                    *LocalMembership
	target                                   string
	removed, joined, metadata                string
	baseline, replacement                    string
	changed                                  time.Time
	armed, paired, withdrawn, added, updated bool
	err                                      error
}

func newRetryMembershipQualification(m *Manager, provider *atomic.Pointer[LocalMembership], before *LocalMembership, target string) *retryMembershipQualification {
	after := &LocalMembership{Revision: before.Revision + 1, Complete: true, RawCount: before.RawCount,
		Users: append([]LocalUser(nil), before.Users[1:]...)}
	after.Users[0].IP = "203.0.113.9"
	joined := LocalUser{SessionID: 1001, Login: qualificationCall("L0", 1000), IP: "192.0.2.2"}
	after.Users = append(after.Users, joined)
	return &retryMembershipQualification{m: m, provider: provider, after: after, target: target,
		removed:  "^1" + before.Users[0].Login,
		joined:   "^1" + joined.Login + ":" + joined.IP + "^",
		metadata: "^1" + before.Users[1].Login + ":203.0.113.9^"}
}

func (q *retryMembershipQualification) Arm() {
	q.mu.Lock()
	q.armed = true
	q.mu.Unlock()
}

func (q *retryMembershipQualification) ChangeTime() time.Time {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.changed
}

func (q *retryMembershipQualification) completeLocked() bool {
	return q.paired && q.withdrawn && q.added && q.updated
}

func (q *retryMembershipQualification) Observe(event QualificationPublication) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.armed || event.Peer != q.target || q.err != nil || q.completeLocked() {
		return
	}
	parts := strings.SplitN(event.Wire, "^", 5)
	if len(parts) != 5 || len(event.Wire) > MaxPeerFrameBytes {
		q.err = fmt.Errorf("malformed recovery membership publication to %s", q.target)
		return
	}
	payload := parts[4]
	if q.changed.IsZero() {
		if event.Action != "C" {
			return
		}
		if !strings.Contains(event.Wire, q.removed+":") {
			q.err = fmt.Errorf("retry C omitted baseline member for %s", q.target)
			return
		}
		q.baseline = strings.Clone(payload)
		q.changed = time.Now()
		q.provider.Store(q.after)
		q.m.NotifyMembershipChanged()
		return
	}
	if event.At.Before(q.changed) || event.At.After(q.changed.Add(time.Second)) {
		q.err = fmt.Errorf("retry membership admission outside one second for %s: %s", q.target, event.At.Sub(q.changed))
		return
	}
	switch event.Action {
	case "C":
		if !q.paired {
			q.err = fmt.Errorf("retry C restarted before immutable A for %s", q.target)
			return
		}
		q.replacement = strings.Clone(payload)
	case "A":
		if !q.paired {
			if payload != q.baseline {
				q.err = fmt.Errorf("retry A changed immutable baseline for %s", q.target)
				return
			}
			q.paired = true
			return
		}
		// A later complete pair may express the withdrawal. C alone cannot
		// prove convergence of membership and metadata in the real receiver.
		if payload == q.replacement && !strings.Contains(event.Wire, q.removed+":") && !strings.Contains(event.Wire, q.removed+"^") {
			q.withdrawn = true
		}
		q.added = q.added || strings.Contains(event.Wire, q.joined)
		q.updated = q.updated || strings.Contains(event.Wire, q.metadata)
	case "D":
		if !q.paired {
			q.err = fmt.Errorf("withdrawal overtook immutable recovery A for %s", q.target)
			return
		}
		q.withdrawn = q.withdrawn || strings.Contains(event.Wire, q.removed+"^") || strings.Contains(event.Wire, q.removed+":")
	}
}

func (q *retryMembershipQualification) Verify() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return q.err
	}
	if q.changed.IsZero() || !q.completeLocked() {
		return fmt.Errorf("missing retry membership evidence for %s: changed=%v pair=%v withdrawal=%v join=%v metadata=%v", q.target, !q.changed.IsZero(), q.paired, q.withdrawn, q.added, q.updated)
	}
	return nil
}

func TestPC92V14RetryMembershipOracle(t *testing.T) {
	for _, scenario := range []string{"valid", "missing", "order", "late", "baseline", "restart", "replacement"} {
		t.Run(scenario, func(t *testing.T) {
			before := &LocalMembership{Revision: 1, Complete: true, RawCount: 2, Users: []LocalUser{
				{SessionID: 1, Login: qualificationCall("L0", 0), IP: "192.0.2.1"},
				{SessionID: 2, Login: qualificationCall("L0", 1), IP: "192.0.2.1"},
			}}
			var provider atomic.Pointer[LocalMembership]
			provider.Store(before)
			q := newRetryMembershipQualification(&Manager{}, &provider, before, "P0AAAA")
			q.Arm()
			base := "5N0LOCAL:1:1^1" + before.Users[0].Login + ":192.0.2.1^1" + before.Users[1].Login + ":192.0.2.1^H10^"
			emit := func(action, payload string, at time.Time) {
				q.Observe(QualificationPublication{Peer: q.target, Action: action, Wire: "PC92^N0LOCAL^1^" + action + "^" + payload, At: at})
			}
			emit("C", base, time.Now())
			changed := q.ChangeTime()
			if changed.IsZero() || provider.Load() == before || len(provider.Load().Users) != 2 {
				t.Fatal("observer did not publish the bounded replacement provider")
			}
			at := changed.Add(time.Millisecond)
			withdrawal := "5N0LOCAL:1:1" + q.removed + "^H10^"
			if scenario == "order" {
				emit("D", withdrawal, at)
			}
			if scenario == "restart" {
				emit("C", base, at)
			}
			pair := base
			if scenario == "baseline" {
				pair = strings.ReplaceAll(base, "192.0.2.1", "192.0.2.9")
			}
			emit("A", pair, at)
			if scenario == "late" {
				at = changed.Add(time.Second + time.Nanosecond)
			}
			update := "5N0LOCAL:1:1" + q.joined + strings.TrimPrefix(q.metadata, "^") + "H10^"
			if scenario == "replacement" {
				emit("C", update, at)
			} else if scenario != "missing" {
				emit("D", withdrawal, at)
			}
			emit("A", update, at)
			err := q.Verify()
			wantSuccess := scenario == "valid" || scenario == "replacement"
			if (err == nil) != wantSuccess {
				t.Fatalf("%s: %v", scenario, err)
			}
		})
	}
}
