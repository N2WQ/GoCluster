package sqlite3_wrap

import "sync/atomic"

// MemoryAccounting contains scalars only. Observers may retain it without
// retaining an engine wrapper, connection, callbacks or another owner generation.
type MemoryAccounting struct {
	EngineBacking atomic.Int64
	EngineNative  atomic.Int64
	WALViews      atomic.Int64
	WALShadows    atomic.Int64
	WALSlots      atomic.Int64
}
