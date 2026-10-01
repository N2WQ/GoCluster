//go:build qualification

package peer

import "sync/atomic"

// Observers count owned backing, including allocation rounding. They never
// expose a concurrently mutated slice to the qualification sampler. A sample
// does not stand in for the separate worst-case transient allocation proof.
type readerAllocationState struct {
	buffer, readBuffer, raw atomic.Int64
}

func (s *readerAllocationState) setBuffer(size int) { s.buffer.Store(int64(allocationBytes(size))) }
func (s *readerAllocationState) setReadBuffer(size int) {
	s.readBuffer.Store(int64(allocationBytes(size)))
}
func (s *readerAllocationState) setRaw(size int) { s.raw.Store(int64(allocationBytes(size))) }
func (s *readerAllocationState) snapshot() (backing, raw int64) {
	return s.buffer.Load() + s.readBuffer.Load(), s.raw.Load()
}
