package peer

import (
	"context"
	"database/sql"
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
	missing := false
	for _, col := range need {
		if _, ok := cols[col]; ok {
			continue
		}
		missing = true
	}
	if missing {
		ctx, cancel := newTopologyDBContext(context.Background())
		defer cancel()
		if _, err := db.ExecContext(ctx, `drop table if exists peer_nodes;`); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, schema); err != nil {
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

func (t *topologyStore) applyPC92(ctx context.Context, frame *Frame, now time.Time) {
	if t == nil || frame == nil {
		return
	}
	fields := frame.payloadFields()
	// Expected payload fields (after "PC92^"):
	//   0: origin node
	//   1: timestamp
	//   2: record type (A/C/D/K)
	//   3+: node entries: <bitmap><call>:<version>[:<build>[:<ip>]]
	if len(fields) < 3 {
		return
	}
	origin := strings.TrimSpace(fields[0])
	if origin == "" {
		origin = frame.Type // fallback; should not happen
	}
	recordType := strings.TrimSpace(fields[2])
	entries := fields[3:]
	if len(entries) == 0 {
		return
	}
	ctx, cancel := newTopologyDBContext(ctx)
	defer cancel()
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if isHopField(entry) {
			continue
		}
		bitmap, call, version, build, ip := parsePC92Entry(entry)
		if strings.TrimSpace(call) == "" {
			continue
		}
		updatedAt := now.Unix()
		if strings.EqualFold(recordType, "D") {
			// Delete record type: remove matching origin+call rows.
			if _, err := t.db.ExecContext(ctx, `delete from peer_nodes where origin = ? and call = ?`, origin, call); err != nil {
				log.Printf("Peering: failed to delete topology row origin=%s call=%s: %v", origin, call, err)
			}
			continue
		}
		if err := t.upsertPeerNode(ctx, origin, bitmap, call, version, build, ip, updatedAt); err != nil {
			log.Printf("Peering: failed to upsert topology row origin=%s call=%s: %v", origin, call, err)
		}
	}
}

func (t *topologyStore) applyLegacy(ctx context.Context, frame *Frame, now time.Time) {
	if t == nil {
		return
	}
	ctx, cancel := newTopologyDBContext(ctx)
	defer cancel()
	if _, err := t.db.ExecContext(ctx, `insert into peer_nodes(origin, bitmap, call, version, build, ip, updated_at) values(?,?,?,?,?,?,?)`,
		frame.Type, 0, "", "", "", "", now.Unix()); err != nil {
		log.Printf("Peering: failed to record legacy topology frame %s: %v", frame.Type, err)
	}
}

func (t *topologyStore) upsertPeerNode(ctx context.Context, origin string, bitmap int, call, version, build, ip string, updatedAt int64) error {
	if t == nil {
		return nil
	}
	// Best-effort upsert: delete any existing row for origin+call, then insert fresh state.
	if _, err := t.db.ExecContext(ctx, `delete from peer_nodes where origin = ? and call = ?`, origin, call); err != nil {
		return err
	}
	if _, err := t.db.ExecContext(ctx, `insert into peer_nodes(origin, bitmap, call, version, build, ip, updated_at) values(?,?,?,?,?,?,?)`,
		origin, bitmap, call, version, build, ip, updatedAt); err != nil {
		return err
	}
	return nil
}

// isHopField returns true when the token is the trailing hop marker (e.g., H27).
func isHopField(token string) bool {
	token = strings.TrimSpace(strings.ToUpper(token))
	if !strings.HasPrefix(token, "H") || len(token) < 2 {
		return false
	}
	for i := 1; i < len(token); i++ {
		if token[i] < '0' || token[i] > '9' {
			return false
		}
	}
	return true
}

func (t *topologyStore) prune(ctx context.Context, now time.Time) {
	if t == nil {
		return
	}
	cutoff := now.Add(-t.retention).Unix()
	ctx, cancel := newTopologyDBContext(ctx)
	defer cancel()
	if _, err := t.db.ExecContext(ctx, `delete from peer_nodes where updated_at < ?`, cutoff); err != nil {
		log.Printf("Peering: failed to prune topology rows before %d: %v", cutoff, err)
	}
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
