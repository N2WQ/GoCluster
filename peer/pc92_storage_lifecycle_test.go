package peer

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	sqlite3 "github.com/ncruces/go-sqlite3"
)

// A second driver's write lock is a real storage bottleneck. The protocol owner
// must continue accepting complete snapshots, and shutdown must cancel the
// blocked projection before it joins workers and closes storage.
func TestPC92SlowStorageDoesNotBlockAuthorityAndStop(t *testing.T) {
	m := newProtocolTestManager(t)
	store, err := openTopologyStore(filepath.Join(t.TempDir(), "slow.db")+"?_pragma=busy_timeout(10000)", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	m.topology = store
	m.cfg.Topology.PersistIntervalSeconds = 1
	control := topologyTestDB(t, store)
	held, err := control.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if _, err = held.ExecContext(t.Context(), "begin immediate"); err != nil {
		t.Fatal(err)
	}
	defer held.ExecContext(context.Background(), "rollback")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &session{id: "N1SRC", remoteCall: "N1SRC", localCall: m.localCall, manager: m, pc9x: true, ctx: ctx, cancel: cancel, priorityLineCh: make(chan string, 128), writeCh: make(chan string, 128)}
	m.sessions.Set(source.id, source)
	if err := m.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	send := func(stamp string, members string) {
		t.Helper()
		frame, err := ParseFrame(fmt.Sprintf("PC92^N2NODE^%s^C^5N2NODE^%sH1^", stamp, members))
		if err != nil {
			t.Fatal(err)
		}
		m.HandleFrame(frame, source)
	}
	gen := NewTimestampGenerator()
	stamp, err := gen.Next()
	if err != nil {
		t.Fatal(err)
	}
	send(stamp, "1K1OLD^")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stats := m.ProtocolStats()
		if stats.Nodes == 1 && stats.Users == 1 && m.protocol.projectionBytes.Load() > 0 && len(m.protocol.projection) == 0 && store.db.inOperation.Load() {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !store.db.inOperation.Load() || m.protocol.projectionBytes.Load() == 0 {
		t.Fatal("projection did not reach the externally locked database")
	}
	stamp, err = gen.Next()
	if err != nil {
		t.Fatal(err)
	}
	send(stamp, "1K2NEW^1K3NEW^")
	deadline = time.Now().Add(2 * time.Second)
	for m.ProtocolStats().Users != 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if stats := m.ProtocolStats(); stats.Nodes != 1 || stats.Users != 2 || stats.Edges != 2 {
		t.Fatalf("blocked storage prevented atomic live replacement: %+v", stats)
	}
	stopped := make(chan struct{})
	start := time.Now()
	go func() { m.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		_ = held.Close()
		t.Fatal("Stop exceeded five-second total while storage was blocked")
	}
	if charge := m.protocol.projectionBytes.Load(); charge != 0 {
		t.Fatalf("terminal projection retained%d reserved bytes", charge)
	}
	if err := store.db.run(t.Context(), func(*sqlite3.Conn) error { return nil }); err == nil {
		t.Fatal("storage remained open after joined shutdown")
	}
	t.Logf("blocked-storage Stop joined and closed storage in %s", time.Since(start))
}
