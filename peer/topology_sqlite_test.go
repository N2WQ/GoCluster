package peer

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sqlite3 "github.com/ncruces/go-sqlite3"
	_ "modernc.org/sqlite"
)

// Use the unchanged driver as an external content oracle. Test-only readers do
// not consume the production adapter's one-engine reservation or query methods.
func topologyTestDB(t *testing.T, store *topologyStore) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", store.db.path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestV15TopologySingleReservation(t *testing.T) {
	first := filepath.Join(t.TempDir(), "first.db")
	second := filepath.Join(t.TempDir(), "second.db")
	a, err := openTopologyStore(first, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	observer := topologyTestDB(t, a)
	if _, err = observer.ExecContext(t.Context(), "insert into peer_nodes(origin,call) values('PC19','K1KEEP')"); err != nil {
		t.Fatal(err)
	}
	if b, err := openTopologyStore(second, time.Hour); b != nil || !errors.Is(err, errTopologyReservation) {
		t.Fatalf("second engine admitted: %v %v", b, err)
	}
	var call string
	if err = observer.QueryRowContext(t.Context(), "select call from peer_nodes").Scan(&call); err != nil || call != "K1KEEP" {
		t.Fatal("refused construction changed committed data", err, call)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := openTopologyStore(second, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestV15TopologyDeadlineIncludesAdmission(t *testing.T) {
	store, err := openTopologyStore(filepath.Join(t.TempDir(), "wait.db"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	<-store.db.gate
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	called := false
	start := time.Now()
	err = store.db.run(ctx, func(*sqlite3.Conn) error { called = true; return nil })
	store.db.gate <- struct{}{}
	if !errors.Is(err, context.DeadlineExceeded) || called || time.Since(start) > time.Second {
		t.Fatalf("admission deadline violated: called%v err%v elapsed%v", called, err, time.Since(start))
	}
}

func TestV15TopologyActualOOMRetiresBeforeReplacement(t *testing.T) {
	store, err := openTopologyStore(filepath.Join(t.TempDir(), "oom.db"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	observer := topologyTestDB(t, store)
	if _, err = observer.ExecContext(t.Context(), "create table preserved(v);insert into preserved values('old')"); err != nil {
		t.Fatal(err)
	}
	old := store.db.usage.Load()
	err = store.db.transaction(t.Context(), func(tx *topologyTransaction) error {
		return tx.Exec("delete from preserved;insert into preserved values(randomblob(16777216))")
	})
	if !errors.Is(err, errTopologyBudget) {
		t.Fatalf("expected actual engine exhaustion: %v", err)
	}
	if store.db.conn != nil || old.EngineBacking.Load() != 0 {
		t.Fatal("poisoned engine retained without retirement")
	}
	var value string
	if err = observer.QueryRowContext(t.Context(), "select v from preserved").Scan(&value); err != nil || value != "old" {
		t.Fatalf("failed transaction lost committed data: %q %v", value, err)
	}
	if err = store.applyLegacy(t.Context(), &Frame{Type: "PC19"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if store.db.usage.Load() == old || store.db.legacyCommits.Load() != 1 {
		t.Fatal("replacement did not follow completed retirement")
	}
}

func TestV15TopologyTransactionStatementLifetime(t *testing.T) {
	store, err := openTopologyStore(filepath.Join(t.TempDir(), "statements.db"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	observer := topologyTestDB(t, store)
	if _, err = observer.ExecContext(t.Context(), "create table rows(k integer primary key, v text); insert into rows values(0,'saved')"); err != nil {
		t.Fatal(err)
	}
	insert := "insert into rows values(?,?)"
	err = store.db.transaction(t.Context(), func(tx *topologyTransaction) error {
		for i, value := range []string{"first", "", "third"} {
			if err := tx.Exec(insert, i+1, value); err != nil {
				return err
			}
		}
		return tx.Exec("update rows set v=? where k=?", "changed", 1)
	})
	if err != nil {
		t.Fatal(err)
	}
	var result string
	if err = observer.QueryRowContext(t.Context(), "select group_concat(k || ':' || v, '|') from (select * from rows order by k)").Scan(&result); err != nil || result != "0:saved|1:changed|2:|3:third" {
		t.Fatalf("repeated/switching bindings: %q %v", result, err)
	}
	old := store.db.usage.Load()
	for _, failure := range []string{"constraint", "prepare", "binding", "oom"} {
		t.Run(failure, func(t *testing.T) {
			err := store.db.transaction(t.Context(), func(tx *topologyTransaction) error {
				if err := tx.Exec(insert, 4, "must rollback"); err != nil {
					return err
				}
				switch failure {
				case "constraint":
					return tx.Exec(insert, 0, "duplicate")
				case "prepare":
					return tx.Exec("insert into nonexistent values(?)", 1)
				case "binding":
					return tx.Exec(insert, 5)
				default:
					return tx.Exec(insert, 5, strings.Repeat("x", 16<<20))
				}
			})
			if err == nil || (failure == "oom" && !errors.Is(err, errTopologyBudget)) {
				t.Fatalf("expected %s failure: %v", failure, err)
			}
			var count int
			if err = observer.QueryRowContext(t.Context(), "select count(*) from rows").Scan(&count); err != nil || count != 4 {
				t.Fatalf("failed transaction changed saved contents: %d %v", count, err)
			}
			if failure == "oom" {
				if store.db.conn != nil || old.EngineBacking.Load() != 0 {
					t.Fatal("active statement prevented poisoned engine retirement")
				}
			} else if store.db.usage.Load() != old || store.db.conn == nil {
				t.Fatal("ordinary SQL failure unnecessarily replaced the connection")
			}
		})
	}
	if err = store.applyLegacy(t.Context(), &Frame{Type: "PC19"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil || store.db.usage.Load().EngineBacking.Load() != 0 || topologyReservation.Load() != nil {
		t.Fatalf("statement owner survived verified close: %v", err)
	}
}
