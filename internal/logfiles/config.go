package logfiles

import "time"

// DestinationKind selects the existing on-disk format and rollover scheme.
type DestinationKind uint8

const (
	Daily DestinationKind = iota + 1
	Overlong
)

// DestinationConfig is supplied for the complete runtime logging generation.
// Path is the log directory for Daily and the full active path for Overlong.
// ProcessorFactory and RotateHook are owned by the destination worker.
type DestinationConfig struct {
	Stream           uint8
	Kind             DestinationKind
	Path             string
	RetentionDays    int
	ProcessorFactory ProcessorFactory
	RotateHook       func(time.Time, string, string)
}
