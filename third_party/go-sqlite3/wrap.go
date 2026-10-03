// Package sqlite3 wraps the C SQLite API.
package sqlite3

import (
	"context"
	"unsafe"

	sqlite3_wasm "github.com/ncruces/go-sqlite3-wasm/v6"
	"github.com/ncruces/go-sqlite3/internal/errutil"
	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
	"github.com/ncruces/go-sqlite3/vfs"
)

type configKey struct{}

// WithMaxMemory returns a derived context that configures
// each SQLite connection not to use more than max amount of memory.
func WithMaxMemory(ctx context.Context, max int64) context.Context {
	if max < 0 || max > 65536*65536 {
		panic(errutil.OOMErr)
	}
	return context.WithValue(ctx, configKey{}, max/65536)
}

type env struct{ *sqlite3_wrap.Wrapper }

func initializeWrapper(ctx context.Context, wrp *sqlite3_wrap.Wrapper) {
	mem := wrp.Memory
	mem.Max = 128 // The private topology owner reserves exactly eight MiB.
	if cfg, ok := ctx.Value(configKey{}).(int64); ok {
		mem.Max = min(cfg, mem.Max)
	}
	configureQualificationMemory(ctx, mem)
	if mem.Grow(5 /*320KB*/, mem.Max) < 0 {
		panic(errutil.OOMErr)
	}

	env := env{wrp}
	env.Module = sqlite3_wasm.New(env)
	env.X_initialize()
}

func (e env) Xgo_vfs_find(zVfsName int32) int32 {
	span := e.BorrowStringBytes(ptr_t(zVfsName), int64(len(e.Memory.Buf)))
	// Find only compares/looks up this name; it neither retains the argument
	// nor re-enters SQLite. Keep the engine-backed view inside that call so an
	// unknown SQL-generated VFS name does not allocate another large Go copy.
	name := unsafe.String(unsafe.SliceData(span), len(span))
	if vfs.Find(name) != nil {
		return 1
	}
	return 0
}
