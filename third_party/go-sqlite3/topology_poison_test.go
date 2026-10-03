package sqlite3

import (
	"context"
	"errors"
	"testing"

	"github.com/ncruces/go-sqlite3/internal/sqlite3_wrap"
)

func TestV15SQLitePoisonedEntryBoundary(t *testing.T) {
	// Deliberately no engine Module: any attempted engine entry panics. This
	// checks entry admission independently of SQLite's own IOERR handling.
	c := &Conn{wrp: &sqlite3_wrap.Wrapper{Poisoned: true}, interrupt: context.Background(), topology: true}
	s := &Stmt{c: c, handle: 1}
	checks := map[string]func() error{
		"exec":     func() error { return c.Exec("select 1") },
		"prepare":  func() error { _, _, err := c.Prepare("select 1"); return err },
		"config":   func() error { _, err := c.Config(DBCONFIG_ENABLE_FKEY, false); return err },
		"finalize": s.Close, "reset": s.Reset, "clear": s.ClearBindings,
		"bind-text": func() error { return s.BindText(1, "value") },
		"bind-int":  func() error { return s.BindInt64(1, 7) },
		"step": func() error {
			if s.Step() {
				t.Fatal("poisoned step returned row")
			}
			return s.Err()
		},
		"statement-exec":   s.Exec,
		"error-diagnostic": func() error { return c.errorFor(1, res_t(IOERR_CLOSE), "select 1") },
		"hidden-ok":        func() error { return c.errorFor(1, 0) },
	}
	for name, check := range checks {
		t.Run(name, func(t *testing.T) {
			if err := check(); !errors.Is(err, IOERR_CLOSE) {
				t.Fatal("poison did not dominate result", err)
			}
		})
	}
	if s.BindCount() != 0 {
		t.Fatal("poisoned bind count entered engine")
	}
	if !c.CleanupFailed() {
		t.Fatal("adapter cannot see sticky cleanup state")
	}
}

func TestV15SQLitePoisonedStepSuccessResult(t *testing.T) {
	for _, code := range []res_t{_ROW, _DONE} {
		c := &Conn{wrp: &sqlite3_wrap.Wrapper{Poisoned: true}}
		s := &Stmt{c: c}
		if s.stepResult(code) || !errors.Is(s.Err(), IOERR_CLOSE) {
			t.Fatal("in-flight success erased cleanup failure", code, s.Err())
		}
	}
}
