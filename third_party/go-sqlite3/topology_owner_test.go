package sqlite3

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/ncruces/go-sqlite3/internal/errutil"
	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
)

func TestV15SQLiteZeroHandleClose(t *testing.T) {
	w := &sqlite3_wrap.Wrapper{Memory: &sqlite3_wrap.Memory{}}
	initializeWrapper(WithMaxMemory(t.Context(), 8<<20), w)
	c := &Conn{wrp: w}
	if c.handle != 0 || cap(w.Buf) != 8<<20 {
		t.Fatal("fixture must own backing without a SQL handle")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if c.wrp != nil || w.Memory != nil {
		t.Fatal("failed-open wrapper remained owned")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestV15SQLitePartialInitializationOwnership(t *testing.T) {
	for _, limit := range []int64{0, 4 << 16, 5 << 16, 8 << 20} {
		c, err := OpenContext(WithMaxMemory(t.Context(), limit), filepath.Join(t.TempDir(), "absent", "db"))
		if err == nil {
			t.Fatal("missing directory unexpectedly opened")
		}
		if c != nil {
			t.Fatalf("clean failure retained owner at %d: %v", limit, err)
		}
	}
	c, err := OpenContext(WithMaxMemory(t.Context(), 8<<20), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Exec("create table x(v); insert into x values(17)"); err != nil {
		t.Fatal(err)
	}
	if err = c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestV15SQLiteOOMClassificationAndRetirement(t *testing.T) {
	if !IsOutOfMemoryPanic(errutil.OOMErr) || IsOutOfMemoryPanic(errors.New(string(errutil.OOMErr))) || IsOutOfMemoryPanic([]int{1}) {
		t.Fatal("allocation sentinel classification")
	}
	c, err := OpenContext(WithMaxMemory(context.Background(), 8<<20), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	var exhausted bool
	func() {
		defer func() {
			if failure := recover(); failure != nil {
				if !IsOutOfMemoryPanic(failure) {
					panic(failure)
				}
				exhausted = true
			}
		}()
		err = c.Exec("create table x(v); insert into x values(zeroblob(16777216))")
		exhausted = errors.Is(err, NOMEM)
	}()
	if !exhausted {
		t.Fatalf("large SQL failed to reach actual engine exhaustion: %v", err)
	}
	if err = c.Retire(); err != nil {
		t.Fatal(err)
	}
	if c.wrp != nil {
		t.Fatal("retirement retained wrapper")
	}
}
