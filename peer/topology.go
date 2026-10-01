package peer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type topologyStore struct {
	db        *sql.DB
	retention time.Duration
}

const topologyDBTimeout = 5 * time.Second

func newTopologyDBContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, topologyDBTimeout)
}

func openTopologyStore(path string, retention time.Duration) (*topologyStore, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	ctx, cancel := newTopologyDBContext(context.Background())
	defer cancel()
	if _, err := db.ExecContext(ctx, `pragma journal_mode=WAL;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensurePeerNodesSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &topologyStore{db: db, retention: retention}
	if err := ensurePC92ProjectionSchema(store); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func ensurePeerNodesSchema(db *sql.DB) error {
	schema := `
	create table if not exists peer_nodes (
		id integer primary key autoincrement,
		origin text,
		bitmap int,
		call text,
		version text,
		build text,
		ip text,
		updated_at integer
	);
	create index if not exists idx_peer_nodes_origin on peer_nodes(origin);
	`
	ctx, cancel := newTopologyDBContext(context.Background())
	defer cancel()
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return err
	}
	cols, err := fetchColumns(ctx, db, "peer_nodes")
	if err != nil {
		return err
	}
	need := []string{"origin", "bitmap", "call", "version", "build", "ip", "updated_at"}
	for _, col := range need {
		if _, ok := cols[col]; ok {
			continue
		}
		// These are fixed identifier literals, never user input. Additive migration
		// keeps old diagnostic rows intact; rollback can ignore the new columns.
		kind := "text"
		if col == "bitmap" || col == "updated_at" {
			kind = "integer"
		}
		if _, err := db.ExecContext(ctx, fmt.Sprintf("alter table peer_nodes add column %s %s", col, kind)); err != nil {
			return err
		}
	}
	return nil
}

func fetchColumns(ctx context.Context, db *sql.DB, table string) (map[string]struct{}, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("pragma table_info(%s);", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols := make(map[string]struct{})
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols[strings.ToLower(name)] = struct{}{}
	}
	return cols, rows.Err()
}

// applyLegacy retains only the latest diagnostic per legacy record family.
// Legacy traffic does not create PC92 authority or an unbounded history table.
func (t *topologyStore) applyLegacy(ctx context.Context, frame *Frame, now time.Time) {
	if t == nil || frame == nil {
		return
	}
	ctx, cancel := newTopologyDBContext(ctx)
	defer cancel()
	tx, err := t.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Peering: legacy projection: %v", err)
		return
	}
	defer rollbackTopology(tx)
	if _, err = tx.ExecContext(ctx, `delete from peer_nodes where origin=?`, frame.Type); err != nil {
		log.Printf("Peering: legacy projection: %v", err)
		return
	}
	if _, err = tx.ExecContext(ctx, `insert into peer_nodes(origin,bitmap,call,version,build,ip,updated_at) values(?,?,?,?,?,?,?)`, frame.Type, 0, "", "", "", "", now.Unix()); err != nil {
		log.Printf("Peering: legacy projection: %v", err)
		return
	}
	if err = tx.Commit(); err != nil {
		log.Printf("Peering: legacy projection: %v", err)
	}
}

func (t *topologyStore) Close() error {
	if t == nil {
		return nil
	}
	return t.db.Close()
}

func rollbackTopology(tx *sql.Tx) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		log.Printf("Peering: topology rollback failed: %v", err)
	}
}
