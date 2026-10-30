// File role: Bridges an active immutable matcher to its own saved dialect bank.
// Callers synchronize capture, then may hash immutable generations outside locks.
// Detached captures own collections; fingerprinting exposes no mutable program data.
package filter

func (c Configuration) borrowedV7(banks V7FilterBanks) V7Configuration {
	if c.Filters.preparedCC != nil {
		banks.CC = c.Filters.nativeCC()
	} else {
		banks.GO = c.V6().Filters
	}
	return V7Configuration{Filters: banks, Settings: c.Settings}
}

// CaptureV7 merges by the active program's kind, never a possibly changed
// selection. The other bank remains intact and all returned collections detach.
func (c Configuration) CaptureV7(banks V7FilterBanks) V7Configuration {
	return c.borrowedV7(banks).Clone()
}

// FingerprintV7 reads admitted immutable bank definitions without cloning the
// expanded native data on every revision observation.
func (c Configuration) FingerprintV7(banks V7FilterBanks) [32]byte {
	return c.borrowedV7(banks).Fingerprint()
}
