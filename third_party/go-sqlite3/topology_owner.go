package sqlite3

import (
	"context"
	"github.com/ncruces/go-sqlite3/internal/errutil"
	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
)

// OpenTopologyContext uses only the fixed built-in engine and OS VFS. The peer
// adapter owns DSN pragma compatibility and does not enter dynamic Go extension
// or logger registries. Failure may return an unreleased owner with its error.
func OpenTopologyContext(ctx context.Context, filename string) (*Conn, error) {
	return newOwnedConn(ctx, filename, OPEN_READWRITE|OPEN_CREATE|OPEN_URI, true)
}

// OwnershipCounters are safe to sample concurrently and retain scalars only.
type OwnershipCounters = sqlite3_wrap.MemoryAccounting

// Ownership exposes the current allocation counters to the serialized topology
// owner. Obtain it before retirement; the returned scalar owner remains valid.
func (c *Conn) Ownership() *OwnershipCounters {
	if c == nil || c.wrp == nil || c.wrp.Memory == nil {
		return nil
	}
	return c.wrp.Accounting
}

// CleanupFailed is sticky until the owner is retired. It lets the serialized
// topology adapter skip SQL finalization/rollback after a native close failure.
func (c *Conn) CleanupFailed() bool {
	return c != nil && c.wrp != nil && (c.wrp.Poisoned || c.wrp.Retiring)
}

// IsOutOfMemoryPanic identifies only the pinned engine bridge's exact allocation
// sentinel. The topology boundary must re-panic every other value.
func IsOutOfMemoryPanic(value any) bool {
	err, ok := value.(errutil.ErrorString)
	return ok && err == errutil.OOMErr
}

// Retire abandons a poisoned engine without executing further SQL. Closing its
// files and mappings releases locks; uncommitted SQLite transactions remain
// uncommitted on disk. A release error leaves the wrapper reachable for retry.
// The caller owns serialization and must prevent replacement until success.
func (c *Conn) Retire() error {
	if c == nil || c.wrp == nil {
		return nil
	}
	if err := qualificationRetirement(c); err != nil {
		return err
	}
	c.handle = 0
	if err := c.wrp.Close(); err != nil {
		return err
	}
	c.wrp = nil
	c.stmts = nil
	clear(c.stmtSlots[:])
	c.arena = sqlite3_wrap.Arena{}
	return nil
}
