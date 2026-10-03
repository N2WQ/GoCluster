package sqlite3_wrap

import (
	"errors"
	"testing"
)

type failCloser struct {
	fail  bool
	calls int
}

func (c *failCloser) Close() error {
	c.calls++
	if c.fail {
		return errors.New("injected close failure")
	}
	return nil
}

func TestV15SQLiteHandleReleaseOwnership(t *testing.T) {
	w := &Wrapper{}
	c := &failCloser{fail: true}
	id := w.AddHandle(c)
	if err := w.DelHandle(id); err == nil || w.GetHandle(id) != c || !w.Poisoned {
		t.Fatal("failed DelHandle forgot owner")
	}
	if err := w.Close(); err == nil || w.GetHandle(id) != c {
		t.Fatal("failed wrapper close forgot owner")
	}
	c.fail = false
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if c.calls != 3 {
		t.Fatalf("close attempts=%d", c.calls)
	}
}

func TestV15SQLiteMetadataHandleAdmission(t *testing.T) {
	w := &Wrapper{}
	for i := 0; i < 255; i++ {
		w.AddHandle(&failCloser{})
	}
	if !w.CanAcquireHandles(2) {
		t.Fatal("two free slots refused before acquisition")
	}
	w.AddHandle(&failCloser{})
	if w.CanAcquireHandles(2) || !w.CanAcquireHandles(1) {
		t.Fatal("two-owner operation would overflow fixed table")
	}
	w.Poisoned = true
	if w.CanAcquireHandles(1) {
		t.Fatal("poisoned owner admitted another handle")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestV15SQLiteTerminalEmergencyOwner(t *testing.T) {
	for _, occupied := range []int{254, 255, 256} {
		w := &Wrapper{}
		for i := 0; i < occupied; i++ {
			w.AddHandle(&failCloser{})
		}
		if w.CanAcquireHandles(2) != (occupied <= 255) {
			t.Fatal("incorrect pair admission", occupied)
		}
		if occupied <= 255 {
			main, metadata := &failCloser{}, &failCloser{}
			w.AddHandle(main)
			w.RetainFailedHandle(metadata)
			if !w.Poisoned || w.CanAcquireHandles(1) {
				t.Fatal("terminal owner permitted reuse")
			}
			if err := w.Close(); err != nil || main.calls != 1 || metadata.calls != 1 {
				t.Fatal(err, main.calls, metadata.calls)
			}
		} else {
			owner := &failCloser{}
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("emergency slot was successful registration")
					}
				}()
				w.AddHandle(owner)
			}()
			if !w.Poisoned || w.GetHandle(^Ptr_t(256)) != owner || w.CanAcquireHandles(1) {
				t.Fatal("emergency owner uncharged or reusable")
			}
			if err := w.Close(); err != nil || owner.calls != 1 {
				t.Fatal(err, owner.calls)
			}
		}
	}
}

func TestV15SQLitePoisonedArenaUnwind(t *testing.T) {
	w := &Wrapper{Poisoned: true} // no Module: engine free would panic
	a := Arena{sqlt: w, base: 16, next: 4096}
	a.ptrs = a.ptrStorage[:0]
	reset := a.Mark()
	a.ptrs = append(a.ptrs, 8192) // an overflow allocation from an active call
	reset()
	if len(a.ptrs) != 0 || a.next != 4096 {
		t.Fatal("arena scalar unwind changed")
	}
	a.Free()
	w.Free(8192)
	// Suballocation pointers can be forgotten only because the same poisoned
	// wrapper still owns its complete fixed engine extent until retirement.
	if !w.Poisoned {
		t.Fatal("unwind cleared engine ownership state")
	}
}
