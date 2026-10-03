package sqlite3_wrap

import (
	"io"

	sqlite3_wasm "github.com/ncruces/go-sqlite3-wasm/v6"
	"github.com/ncruces/go-sqlite3/internal/errutil"
)

type Wrapper struct {
	*sqlite3_wasm.Module
	*Memory
	DB       any
	SysError error
	Retiring bool
	Poisoned bool

	// Fixed slots include VFS files and every callback handle. A retired slot
	// is reusable only after its closer succeeds; failed owners remain charged.
	handles [257]any
}

func (w *Wrapper) Close() (err error) {
	w.Retiring = true
	for i, h := range w.handles {
		if c, ok := h.(io.Closer); ok {
			if e := c.Close(); e != nil {
				w.Poisoned = true
				if err == nil {
					err = e
				}
				continue
			}
		}
		w.handles[i] = nil
	}
	if err != nil {
		return err
	}
	if w.Memory != nil {
		if err = w.Memory.Close(); err != nil {
			w.Poisoned = true
			return err
		}
	}
	*w = Wrapper{}
	return err
}

func (w *Wrapper) GetHandle(id Ptr_t) any {
	if id == 0 {
		return nil
	}
	return w.handles[^id]
}

func (w *Wrapper) DelHandle(id Ptr_t) error {
	if id == 0 {
		return nil
	}
	a := w.handles[^id]
	if c, ok := a.(io.Closer); ok {
		if err := c.Close(); err != nil {
			w.Poisoned = true
			return err
		}
	}
	w.handles[^id] = nil
	return nil
}

// Admission precedes native acquisition. The final slot can retain an error
// owner while another slot holds the partially opened main file.
func (w *Wrapper) CanAcquireHandles(count int) bool {
	if w.Poisoned || w.Retiring {
		return false
	}
	free := 0
	for _, handle := range w.handles {
		if handle == nil {
			free++
		}
	}
	return free >= count
}

func (w *Wrapper) RetainFailedHandle(owner io.Closer) {
	w.Poisoned = true
	for i, handle := range w.handles {
		if handle == nil {
			w.handles[i] = owner
			return
		}
	}
	// Callers must admit their maximum simultaneous owners before acquisition.
	panic(errutil.AssertErr())
}

func (w *Wrapper) AddHandle(a any) Ptr_t {
	if a == nil {
		panic(errutil.NilErr)
	}

	for id, h := range w.handles[:256] {
		if h == nil {
			w.handles[id] = a
			return ^Ptr_t(id)
		}
	}
	// VFS open transfers an already-created file. Keep that final owner even
	// when admission fails; the operation becomes poisoned and must retire.
	if w.handles[256] != nil {
		panic(errutil.AssertErr())
	}
	w.handles[256] = a
	w.Poisoned = true
	panic(errutil.OOMErr)
}

func (w *Wrapper) Xmemory() sqlite3_wasm.Memory { return w.Memory }

func (w *Wrapper) Xgo_destroy(pApp int32) { w.DelHandle(Ptr_t(pApp)) }
