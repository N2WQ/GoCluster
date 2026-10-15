// File role: Defines stable path keys and process-local shard placement.
// Crawler notes: Packed keys identify evidence; shard mixing changes only the
// owning lock/map. Update and capture must use the same selector.
package pathreliability

// shardForKey mixes the complete key before modulo selection. Fine keys have
// zero low bits; folding fields with XOR alone would also cancel same-cell
// paths. Fixed unsigned avalanche steps avoid both placement pathologies.
// No bucket identity, persisted value or retained lookup state is changed.
func (s *Store) shardForKey(key uint64) *shard {
	key ^= key >> 33
	key *= 0xff51afd7ed558ccd
	key ^= key >> 33
	key *= 0xc4ceb9fe1a85ec53
	key ^= key >> 33
	return &s.shards[key%uint64(len(s.shards))]
}

// packKey packs receiver cell, sender cell, and band into a uint64.
func packKey(receiver CellID, sender CellID, band uint16) uint64 {
	if receiver == InvalidCell || sender == InvalidCell {
		return 0
	}
	return (uint64(band) << 48) | (uint64(receiver) << 32) | (uint64(sender) << 16)
}

// packCoarseKey packs coarse (res-1) cells and band into a uint64.
func packCoarseKey(receiver CellID, sender CellID, band uint16) uint64 {
	if receiver == InvalidCell || sender == InvalidCell {
		return 0
	}
	return (uint64(band) << 32) | (uint64(receiver) << 16) | uint64(sender)
}
