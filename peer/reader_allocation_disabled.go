//go:build !qualification

package peer

type readerAllocationState struct{}

func (*readerAllocationState) setBuffer(int)     {}
func (*readerAllocationState) setReadBuffer(int) {}
func (*readerAllocationState) setRaw(int)        {}
