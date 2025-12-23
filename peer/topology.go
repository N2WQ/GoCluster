package peer

import (
	"database/sql"
	"fmt"
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
	if _, err := db.Exec(`pragma journal_mode=WAL;`); err != nil {
		return nil, err
	}
	if err := ensurePeerNodesSchema(db); err != nil {
		return nil, err
	}
	return &topologyStore{db: db, retention: retention}, nil
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
	if _, err := db.Exec(schema); err != nil {
		return err
	}
	cols, err := fetchColumns(db, "peer_nodes")
	if err != nil {
		return err
	}
	need := []string{"origin", "bitmap", "call", "version", "build", "ip", "updated_at"}
	missing := false
	for _, col := range need {
		if _, ok := cols[col]; ok {
			continue
		}
		missing = true
	}
	if missing {
		if _, err := db.Exec(`drop table if exists peer_nodes;`); err != nil {
			return err
		}
		if _, err := db.Exec(schema); err != nil {
			return err
		}
	}
	return nil
}

func fetchColumns(db *sql.DB, table string) (map[string]struct{}, error) {
	rows, err := db.Query(fmt.Sprintf("pragma table_info(%s);", table))
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

func (t *topologyStore) applyPC92(frame *Frame, now time.Time) {
	if t == nil || frame == nil {
		return
	}
	fields := frame.payloadFields()
	if len(fields) < 3 {
		return
	}
	entry := strings.TrimSpace(fields[1])
	if entry == "" {
		entry = strings.TrimSpace(fields[2])
	}
	bitmap, call, version, build, ip := parsePC92Entry(entry)
	_, _ = t.db.Exec(`insert into peer_nodes(origin, bitmap, call, version, build, ip, updated_at) values(?,?,?,?,?,?,?)`,
		frame.Type, bitmap, call, version, build, ip, now.Unix())
}

func (t *topologyStore) applyLegacy(frame *Frame, now time.Time) {
	if t == nil {
		return
	}
	_, _ = t.db.Exec(`insert into peer_nodes(origin, bitmap, call, version, build, ip, updated_at) values(?,?,?,?,?,?,?)`,
		frame.Type, 0, "", "", "", "", now.Unix())
}

func (t *topologyStore) prune(now time.Time) {
	if t == nil {
		return
	}
	cutoff := now.Add(-t.retention).Unix()
	_, _ = t.db.Exec(`delete from peer_nodes where updated_at < ?`, cutoff)
}

func (t *topologyStore) Close() error {
	if t == nil {
		return nil
	}
	return t.db.Close()
}

func parsePC92Entry(entry string) (bitmap int, call, version, build, ip string) {
	// entry format: <bitmap><call>:<version>[:<build>[:<ip>]]
	parts := strings.Split(entry, ":")
	head := parts[0]
	if len(head) > 0 {
		bitmap = int(head[0] - '0')
		if len(head) > 1 {
			call = head[1:]
		}
	}
	if len(parts) > 1 {
		version = parts[1]
	}
	if len(parts) > 2 {
		build = parts[2]
	}
	if len(parts) > 3 {
		ip = parts[3]
	}
	return
}
