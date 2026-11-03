package logfiles

import (
	"dxcluster/internal/asynclog"
	"time"
)

// ProcessDecision describes a category-specific operation after admission.
type ProcessDecision uint8

const (
	ProcessWrite ProcessDecision = iota
	ProcessSuppress
	ProcessDrop
)

// Processor belongs to one configured stream and runs only on its destination
// worker, never under a producer lock. It must not retain input or output.
// Prepare commits intentional suppression but stages candidate writes without
// clearing earlier suppression evidence. For ProcessWrite, output[:n] is the
// whole payload including suffix. Complete is called exactly once for each
// ProcessWrite: written is true only after confirmed complete physical write.
// False must preserve prior suppression across retry and owner replacement.
// A payload exceeding the output capacity returns ProcessDrop.
type Processor interface {
	Prepare(at time.Time, input []byte, output *[asynclog.MaxRecordBytes]byte) (n int, decision ProcessDecision)
	Complete(input []byte, written bool)
}

// ProcessorFactory is invoked after the stream's complete backing reservation
// succeeds; it must construct only the backing covered by that reservation.
type ProcessorFactory func() Processor
