package peer

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestTopologyKSubjectNumericProjection(t *testing.T) {
	for _, tc := range kNumericReplacementCases() {
		t.Run(tc.name, func(t *testing.T) {
			p, source, _, now := controllerTestOwner(t)
			store, err := openTopologyStore(filepath.Join(t.TempDir(), "k-metadata.db"), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			p.manager.topology = store
			projectAndWrite := func(at time.Time, wantVersion, wantBuild string) {
				t.Helper()
				p.project(at)
				var snapshot graphProjection
				select {
				case snapshot = <-p.projection:
				default:
					t.Fatal("production project did not emit a snapshot")
				}
				defer p.releaseProjection(&snapshot)
				if err := store.replaceProjection(t.Context(), snapshot); err != nil {
					t.Fatal(err)
				}
				var version, build, ip, versionType, buildType string
				if err := store.db.QueryRowContext(t.Context(), `select version,build,ip,typeof(version),typeof(build) from peer_pc92_nodes where call='N2AAA'`).Scan(&version, &build, &ip, &versionType, &buildType); err != nil {
					t.Fatal(err)
				}
				// Never CAST: SQLite would turn an incorrect empty string into 0.
				if version != wantVersion || build != wantBuild || ip != "192.0.2.1" || versionType != "text" || buildType != "text" {
					t.Fatalf("stored values=%q/%q ip=%q types=%q/%q; want %s/%s", version, build, ip, versionType, buildType, wantVersion, wantBuild)
				}
				var users int
				if err := store.db.QueryRowContext(t.Context(), `select count(*) from peer_pc92_typed_edges where parent='N2AAA' and call='K1USER' and kind=0 and version='' and build=''`).Scan(&users); err != nil || users != 1 {
					t.Fatalf("K altered projected user membership: %d %v", users, err)
				}
			}
			receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^5N2AAA:5457:633:192.0.2.1^1K1USER^H1^", now)
			projectAndWrite(now, "5457", "633")
			receiveControllerWire(t, p, source, "PC92^N2AAA^43201^K^5N2AAA"+tc.suffix+"^0^0^H1^", now.Add(time.Second))
			projectAndWrite(now.Add(301*time.Second), tc.version, tc.build)
			if p.projectionBytes.Load() != 0 {
				t.Fatal("completed writes retained projection reservations")
			}
		})
	}
}

func TestPC92ProjectionRetainsNumericGenerationThroughKClear(t *testing.T) {
	p, source, _, now := controllerTestOwner(t)
	p.manager.topology = &topologyStore{}
	version, build := strings.Repeat("1", 8193), strings.Repeat("2", 4097)
	receiveControllerWire(t, p, source, "PC92^N2AAA^43200^C^5N2AAA:"+version+":"+build+"^1K1USER^H1^", now)
	p.project(now)
	if len(p.projection) != 1 {
		t.Fatal("old snapshot missing")
	}
	active := <-p.projection
	activeCharge, graphCharge := active.charge, p.graph.retainedCharge()
	receiveControllerWire(t, p, source, "PC92^N2AAA^43201^K^5N2AAA^0^0^H1^", now.Add(time.Second))
	const releasedNumeric = 9472 + 4864 - 16
	if p.graph.retainedCharge() != graphCharge-releasedNumeric || p.projectionBytes.Load() != int64(activeCharge) {
		t.Fatal("live clearing failed to release graph charge or released an active projection")
	}
	p.project(now.Add(301 * time.Second))
	if len(p.projection) != 1 {
		t.Fatal("new snapshot missing")
	}
	queued := <-p.projection
	if len(active.nodes) != 1 || len(queued.nodes) != 1 || active.nodes[0].entry.Version != version || active.nodes[0].entry.Build != build || queued.nodes[0].entry.Version != "0" || queued.nodes[0].entry.Build != "0" {
		t.Fatal("clearing corrupted immutable old metadata or omitted new zero metadata")
	}
	if activeCharge-queued.charge != releasedNumeric || p.projectionBytes.Load() != int64(activeCharge+queued.charge) || activeCharge+queued.charge > projectionAllocationLimit {
		t.Fatal("old and new generation backing escaped exact combined reservation")
	}
	if len(active.edges) != 1 || len(queued.edges) != 1 || active.edges[0] != queued.edges[0] {
		t.Fatal("numeric clearing changed projected membership")
	}
	p.releaseProjection(&active)
	if active.nodes != nil || active.edges != nil || active.charge != 0 || p.projectionBytes.Load() != int64(queued.charge) {
		t.Fatal("active release retained references or released queued backing")
	}
	p.releaseProjection(&queued)
	if queued.nodes != nil || queued.edges != nil || queued.charge != 0 || p.projectionBytes.Load() != 0 {
		t.Fatal("final release retained projection backing")
	}
}
