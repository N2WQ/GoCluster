package peerdiag

import "unsafe"

// Reservations are deliberately conservative admission bounds, not observed
// heap use or an OOM claim. The source inventory and qualification boundaries
// are recorded in docs/pc92-v15-ownership-validation.md.
// Preserve the existing directory reservation. Windows now uses fixed native
// find data and one owned handle; this allowance is conservative headroom,
// not a claim that the removed ReadDir pool still belongs to the helper.
const (
	processMetadataReservation = 64 << 10
	directoryBufferReservation = 3 * (64 << 10)
)

func consumeBound(remaining *uint64, count, size uint64) bool {
	if size != 0 && count > *remaining/size {
		return false
	}
	*remaining -= count * size
	return true
}

func parentLaunchFits(executableBytes, siblingBytes, environmentBytes int) bool {
	if executableBytes < 0 || siblingBytes < 0 || environmentBytes < 0 {
		return false
	}
	remaining := uint64(parentReservation)
	fixed := uint64(unsafe.Sizeof(Service{}) + unsafe.Sizeof(Mailbox{}) + QueueSize*RecordBytes + unsafe.Sizeof(helperProcess{}) + 2*RecordBytes + ackBytes)
	// Executable acquisition, absolute sibling, native UTF-16 application name,
	// environment copy/native block, and fixed short argv can overlap.
	return consumeBound(&remaining, 1, fixed+processMetadataReservation+parentNativePathReservation()) &&
		consumeBound(&remaining, 2, uint64(executableBytes)) &&
		consumeBound(&remaining, 3, uint64(siblingBytes)) &&
		consumeBound(&remaining, 5, uint64(environmentBytes))
}

func helperOptionsFit(directoryBytes, overlongBytes, cwdBytes uint64) bool {
	remaining := uint64(helperReservation)
	fixed := uint64(unsafe.Sizeof(helperSink{}) + 2*RecordBytes + ackBytes + optionsHeaderBytes)
	// 64*path covers recursive MkdirAll errors, simultaneous active/archive
	// paths, native conversions and normalized copies. Windows'64*cwd also
	// covers mkdir recursion after OS-facing absolute preparation; Linux keeps
	// its prior16*cwd allowance. Received bytes and resulting strings coexist.
	return consumeBound(&remaining, 1, fixed+directoryBufferReservation+processMetadataReservation+nativePathExpansionReservation) &&
		consumeBound(&remaining, 7, uint64(helperEnvironmentBytes(helperEnvironmentRoot()))) &&
		consumeBound(&remaining, 64, max(directoryBytes, overlongBytes)) &&
		consumeBound(&remaining, cwdPathFactor, cwdBytes) &&
		consumeBound(&remaining, 2, directoryBytes) &&
		consumeBound(&remaining, 2, overlongBytes)
}

func validOptions(options Options) bool {
	return options.RetentionDays >= 0 && options.DedupeWindow >= 0 &&
		helperOptionsFit(uint64(len(options.Directory)), uint64(len(options.OverlongPath)), 0)
}
