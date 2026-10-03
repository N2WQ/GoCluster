package peer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	sqlite3 "github.com/ncruces/go-sqlite3"
)

// Optional persistence holds diagnostics only. Its fixed reservation and direct
// serialized engine never provide routing or freshness authority after restart.
type topologyStore struct {
	db        *topologyDatabase
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
	ctx, cancel := newTopologyDBContext(context.Background())
	defer cancel()
	db, err := openTopologyDatabase(ctx, path)
	if err != nil {
		return nil, err
	}
	store := &topologyStore{db: db, retention: retention}
	if err = ensurePeerNodesSchema(db); err == nil {
		err = ensurePC92ProjectionSchema(store)
	}
	if err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return store, nil
}

func ensurePeerNodesSchema(db *topologyDatabase) error {
	ctx, cancel := newTopologyDBContext(context.Background())
	defer cancel()
	return db.run(ctx, func(conn *sqlite3.Conn) (err error) {
		if err = conn.Exec(`create table if not exists peer_nodes (
 id integer primary key autoincrement,origin text,bitmap int,call text,version text,build text,ip text,updated_at integer);
 create index if not exists idx_peer_nodes_origin on peer_nodes(origin);`); err != nil {
			return err
		}
		// Existing databases may contain extra columns or very large default
		// expressions. Read only a borrowed name and retain seven presence bits.
		need := [7]string{"origin", "bitmap", "call", "version", "build", "ip", "updated_at"}
		var present [7]bool
		stmt, _, err := conn.Prepare("pragma table_info(peer_nodes)")
		if err != nil {
			return err
		}
		for stmt.Step() {
			name := stmt.ColumnRawText(1)
			for i, col := range need {
				if bytes.EqualFold(name, []byte(col)) {
					present[i] = true
				}
			}
		}
		err = errors.Join(stmt.Err(), stmt.Close())
		if err != nil {
			return err
		}
		for i, col := range need {
			if present[i] {
				continue
			}
			kind := "text"
			if col == "bitmap" || col == "updated_at" {
				kind = "integer"
			}
			if err = conn.Exec(fmt.Sprintf("alter table peer_nodes add column %s %s", col, kind)); err != nil {
				return err
			}
		}
		return nil
	})
}

// applyLegacy retains the latest diagnostic of each legacy family. Errors are
// returned to the manager's bounded diagnostic mailbox, never logged inline.
func (t *topologyStore) applyLegacy(parent context.Context, frame *Frame, now time.Time) error {
	if t == nil || frame == nil {
		return nil
	}
	ctx, cancel := newTopologyDBContext(parent)
	defer cancel()
	err := t.db.transaction(ctx, func(tx *topologyTransaction) error {
		if err := tx.Exec(`delete from peer_nodes where origin=?`, frame.Type); err != nil {
			return err
		}
		return tx.Exec(`insert into peer_nodes(origin,bitmap,call,version,build,ip,updated_at) values(?,?,?,?,?,?,?)`, frame.Type, 0, "", "", "", "", now.Unix())
	})
	if err == nil {
		t.db.lastLegacyCommit.Store(time.Now().UnixNano())
		t.db.legacyCommits.Add(1)
	}
	return err
}

func (t *topologyStore) Close() error {
	if t == nil {
		return nil
	}
	return t.db.Close()
}
