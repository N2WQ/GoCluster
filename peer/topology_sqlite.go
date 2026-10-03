package peer

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	sqlite3 "github.com/ncruces/go-sqlite3"
)

const (
	topologyPersistenceBytes       = 16 << 20
	topologyEngineBytes            = 8 << 20
	topologyHostBytes              = 2 << 20
	topologyOpening          int32 = 1
	topologyActive           int32 = 2
	topologyRetiring         int32 = 3
)

var (
	errTopologyBudget      = errors.New("topology persistence memory budget exhausted")
	errTopologyReservation = errors.New("topology persistence memory reservation already owned")
	errTopologyClosed      = errors.New("topology persistence is closed")
	// A single reservation covers opening, active and failed retirement. This
	// strong reference also owns failures returned by a constructor; the caller
	// cannot accidentally orphan native allocations by discarding its error.
	topologyReservation        atomic.Pointer[topologyDatabase]
	topologyOpeningReservation topologyDatabase
)

// topologyDatabase has one serialized connection and no worker of its own.
// The manager's two projection workers begin deadlines before admission. Failed
// native cleanup keeps this owner and its reservation; replacement cannot run.
type topologyDatabase struct {
	gate                   chan struct{}
	conn                   *sqlite3.Conn
	program                topologyDSN
	path                   string
	closed                 bool
	poisoned               bool
	directoryOwner         topologyDirectoryOwner
	state                  atomic.Int32
	failedCleanup          atomic.Bool
	inOperation            atomic.Bool
	usage                  atomic.Pointer[sqlite3.OwnershipCounters]
	projectionCommits      atomic.Uint64
	legacyCommits          atomic.Uint64
	lastProjectionSnapshot atomic.Int64
	lastProjectionCommit   atomic.Int64
	lastLegacyCommit       atomic.Int64
}

func openTopologyDatabase(ctx context.Context, path string) (*topologyDatabase, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !topologyReservation.CompareAndSwap(nil, &topologyOpeningReservation) {
		old := topologyReservation.Load()
		// Failed constructor retirement may be retried, but an active store or
		// another cleanup attempt is never waited on or overlapped.
		if old == nil || old.state.Load() != topologyRetiring || !old.tryRetireClosed(ctx) || !topologyReservation.CompareAndSwap(nil, &topologyOpeningReservation) {
			return nil, errTopologyReservation
		}
	}
	if len(path) > topologyDSNBytes {
		topologyReservation.CompareAndSwap(&topologyOpeningReservation, nil)
		return nil, errTopologyBudget
	}
	// A direct caller may pass a small substring of a large allocation. Own an
	// exact bounded copy before retaining path-derived strings in the program.
	path = strings.Clone(path)
	db := &topologyDatabase{gate: make(chan struct{}, 1), path: path}
	db.gate <- struct{}{}
	db.state.Store(topologyOpening)
	topologyReservation.Store(db)
	program, err := parseTopologyDSN(path)
	if err == nil {
		db.program = program
		if dir := filepath.Dir(path); dir != "" && dir != "." {
			// Apply the existing VFS filename ceiling before recursive OS
			// directory work. Keep the previous directory interpretation,
			// including configured URI/query text; do not silently rewrite it.
			if len(dir) > 1024 {
				err = errTopologyBudget
			} else {
				err = db.makeTopologyDirectory(dir)
			}
		}
	}
	if err == nil {
		err = db.run(ctx, func(*sqlite3.Conn) error { return nil })
	}
	if err != nil {
		err = errors.Join(err, db.Close())
		return nil, err
	}
	return db, nil
}

func (db *topologyDatabase) tryRetireClosed(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case <-db.gate:
	default:
		return false
	}
	defer func() { db.gate <- struct{}{} }()
	if !db.closed || db.retireLocked() != nil {
		return false
	}
	db.program = topologyDSN{}
	db.path = ""
	return topologyReservation.CompareAndSwap(db, nil)
}

func (db *topologyDatabase) run(ctx context.Context, fn func(*sqlite3.Conn) error) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-db.gate:
	}
	defer func() { db.gate <- struct{}{} }()
	if err = ctx.Err(); err != nil {
		return err
	}
	if db.closed {
		return errTopologyClosed
	}
	if db.poisoned {
		if err = db.retireLocked(); err != nil {
			return err
		}
	}
	defer func() {
		if failure := recover(); failure != nil {
			if !sqlite3.IsOutOfMemoryPanic(failure) {
				panic(failure)
			}
			db.poisoned = true
			err = errTopologyBudget
		}
		if db.conn != nil && db.conn.CleanupFailed() {
			db.poisoned = true
			err = errors.Join(err, sqlite3.IOERR_CLOSE)
		}
		if errors.Is(err, sqlite3.NOMEM) || errors.Is(err, sqlite3.IOERR_NOMEM) {
			db.poisoned = true
			err = errors.Join(errTopologyBudget, err)
		}
		if db.poisoned {
			err = errors.Join(err, db.retireLocked())
		}
		if db.conn != nil {
			db.conn.SetInterrupt(context.Background())
		}
	}()
	if db.conn == nil {
		db.state.Store(topologyOpening)
		db.conn, err = sqlite3.OpenTopologyContext(sqlite3.WithMaxMemory(ctx, topologyEngineBytes), db.program.filename)
		if db.conn != nil {
			db.usage.Store(db.conn.Ownership())
		}
		if err != nil {
			db.poisoned = true
			return err
		}
		db.conn.SetInterrupt(ctx)
		// Preserve the previous topology driver's defaults. Explicit DSN
		// pragmas below retain their authority over these initial settings.
		if _, err = db.conn.Config(sqlite3.DBCONFIG_ENABLE_FKEY, false); err == nil {
			_, err = db.conn.Config(sqlite3.DBCONFIG_TRUSTED_SCHEMA, true)
		}
		if err != nil {
			db.poisoned = true
			return err
		}
		for i := 0; i < db.program.count; i++ {
			if err = db.conn.Exec("pragma " + db.program.pragmas[i].sql); err != nil {
				db.poisoned = true
				return err
			}
		}
		if err = db.conn.Exec("pragma journal_mode=WAL;"); err != nil {
			db.poisoned = true
			return err
		}
		db.state.Store(topologyActive)
	}
	db.conn.SetInterrupt(ctx)
	db.inOperation.Store(true)
	defer db.inOperation.Store(false)
	return fn(db.conn)
}

func (db *topologyDatabase) retireLocked() error {
	db.state.Store(topologyRetiring)
	if err := db.directoryOwner.Close(); err != nil {
		db.failedCleanup.Store(true)
		db.poisoned = true
		return err
	}
	if db.conn != nil {
		if err := db.conn.Retire(); err != nil {
			db.failedCleanup.Store(true)
			db.poisoned = true
			return err
		}
		db.conn = nil
	}
	db.poisoned = false
	db.failedCleanup.Store(false)
	db.state.Store(0)
	return nil
}

func (db *topologyDatabase) Close() error {
	if db == nil {
		return nil
	}
	<-db.gate // Manager joins both workers before closing their shared owner.
	defer func() { db.gate <- struct{}{} }()
	db.closed = true
	if err := db.retireLocked(); err != nil {
		return err
	}
	db.program = topologyDSN{}
	db.path = ""
	topologyReservation.CompareAndSwap(db, nil)
	return nil
}

type topologyTransaction struct {
	conn      *sqlite3.Conn
	statement *sqlite3.Stmt
	query     string
}

func (db *topologyDatabase) transaction(ctx context.Context, fn func(*topologyTransaction) error) error {
	return db.run(ctx, func(conn *sqlite3.Conn) (err error) {
		if err = conn.Exec(db.program.begin); err != nil {
			return err
		}
		committed := false
		tx := &topologyTransaction{conn: conn}
		defer func() {
			if failure := recover(); failure != nil {
				db.poisoned = true
				panic(failure)
			}
			if conn.CleanupFailed() {
				db.poisoned = true
				err = errors.Join(err, sqlite3.IOERR_CLOSE)
				return
			}
			if !committed {
				if ctx.Err() != nil || errors.Is(err, sqlite3.NOMEM) || errors.Is(err, sqlite3.IOERR_NOMEM) {
					db.poisoned = true
					return
				}
				// Finalize may return the previous Step error even though it
				// destroyed the statement. Preserve that error and still roll back.
				err = errors.Join(err, tx.close())
				if rollbackErr := conn.Exec("rollback"); rollbackErr != nil {
					db.poisoned = true
					err = errors.Join(err, rollbackErr)
				}
			}
		}()
		if err = fn(tx); err != nil {
			return err
		}
		if err = tx.close(); err != nil {
			return err
		}
		err = conn.Exec("commit")
		committed = err == nil
		return err
	})
}

// Exec streams one row at a time. Exactly one prepared statement belongs to
// the transaction; consecutive equal application-owned SQL reuses it. Switching
// SQL finalizes it first. No statement or row batch survives the transaction.
func (tx *topologyTransaction) Exec(query string, args ...any) (err error) {
	if len(args) == 0 {
		if err = tx.close(); err != nil {
			return err
		}
		return tx.conn.Exec(query)
	}
	if tx.statement == nil || tx.query != query {
		if err = tx.close(); err != nil {
			return err
		}
		var tail string
		tx.statement, tail, err = tx.conn.Prepare(query)
		if err != nil {
			return err
		}
		tx.query = query
		if tx.statement == nil || tail != "" {
			return errors.New("topology statement is empty or has trailing SQL")
		}
	}
	stmt := tx.statement
	if stmt.BindCount() != len(args) {
		return errors.New("topology statement binding mismatch")
	}
	if err = stmt.Reset(); err != nil {
		return err
	}
	if err = stmt.ClearBindings(); err != nil {
		return err
	}
	for i, value := range args {
		switch value := value.(type) {
		case string:
			err = stmt.BindText(i+1, value)
		case bool:
			err = stmt.BindBool(i+1, value)
		case int:
			err = stmt.BindInt(i+1, value)
		case int64:
			err = stmt.BindInt64(i+1, value)
		case uint8:
			err = stmt.BindInt64(i+1, int64(value))
		default:
			return fmt.Errorf("unsupported topology binding type %T", value)
		}
		if err != nil {
			return err
		}
	}
	for stmt.Step() {
	}
	return stmt.Err()
}

func (tx *topologyTransaction) close() error {
	if tx.statement == nil {
		return nil
	}
	// SQLite finalization destroys the statement even when it reports the last
	// evaluation error. A panic instead leaves ownership in Conn's fixed table,
	// and the outer operation retires that entire engine without SQL cleanup.
	err := tx.statement.Close()
	tx.statement = nil
	tx.query = ""
	return err
}

// TopologyPersistenceStats is a bounded scalar observation. HostReservedBytes
// is a reservation, not a claim that source/OS qualification has been completed.
type TopologyPersistenceStats struct {
	Enabled                                                                                                         bool
	ReservedBytes, EngineBackingBytes, EngineNativeBytes, WALViewBytes, WALShadowBytes, WALSlots, HostReservedBytes int64
	Opening, Active, Retiring                                                                                       int
	CleanupFailed, OperationActive                                                                                  bool
	ProjectionCommits, LegacyCommits                                                                                uint64
	LastProjectionSnapshotUnixNano, LastProjectionCommitUnixNano, LastLegacyCommitUnixNano                          int64
}

func (m *Manager) TopologyPersistenceStats() TopologyPersistenceStats {
	var s TopologyPersistenceStats
	if m != nil && m.topology != nil && m.topology.db != nil {
		db := m.topology.db
		s.Enabled = true
		s.ProjectionCommits = db.projectionCommits.Load()
		s.LegacyCommits = db.legacyCommits.Load()
		s.LastProjectionSnapshotUnixNano = db.lastProjectionSnapshot.Load()
		s.LastProjectionCommitUnixNano = db.lastProjectionCommit.Load()
		s.LastLegacyCommitUnixNano = db.lastLegacyCommit.Load()
	}
	// Allocation ownership is process-wide. A new disabled manager must still
	// expose an old failed retirement, without inheriting that owner's commits.
	db := topologyReservation.Load()
	if db == nil {
		return s
	}
	s.ReservedBytes = topologyPersistenceBytes
	s.HostReservedBytes = topologyHostBytes
	if db == &topologyOpeningReservation {
		s.Opening = 1
		return s
	}
	s.CleanupFailed = db.failedCleanup.Load()
	s.OperationActive = db.inOperation.Load()
	switch db.state.Load() {
	case topologyOpening:
		s.Opening = 1
	case topologyActive:
		s.Active = 1
	case topologyRetiring:
		s.Retiring = 1
	}
	if usage := db.usage.Load(); usage != nil {
		s.EngineBackingBytes = usage.EngineBacking.Load()
		s.EngineNativeBytes = usage.EngineNative.Load()
		s.WALViewBytes = usage.WALViews.Load()
		s.WALShadowBytes = usage.WALShadows.Load()
		s.WALSlots = usage.WALSlots.Load()
	}
	return s
}

func (t *topologyStore) recordProjectionCommit(snapshot time.Time) {
	t.db.lastProjectionSnapshot.Store(snapshot.UnixNano())
	t.db.lastProjectionCommit.Store(time.Now().UnixNano())
	t.db.projectionCommits.Add(1)
}
