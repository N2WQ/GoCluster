//go:build !unix && !windows

package sqlite3_wrap

import "math"

type Memory struct {
	Buf        []byte
	Max        int64
	Accounting *MemoryAccounting
}

func (m *Memory) Slice() *[]byte {
	return &m.Buf
}

func (m *Memory) Grow(delta, max int64) int64 {
	len := int64(len(m.Buf))
	old := len >> 16
	if delta == 0 {
		return old
	}
	new := old + delta
	max = min(max, m.Max, 128, int64(math.MaxInt)>>16)
	if delta < 0 || new > max || new < old {
		return -1
	}
	if m.Buf == nil {
		if m.Accounting == nil {
			m.Accounting = new(MemoryAccounting)
		}
		m.Buf = make([]byte, max<<16)
		m.Accounting.EngineBacking.Store(int64(cap(m.Buf)))
	}
	m.Buf = m.Buf[:new<<16]
	return old
}

func (m *Memory) Close() error {
	m.Buf = nil
	if m.Accounting != nil {
		m.Accounting.EngineBacking.Store(0)
	}
	return nil
}
