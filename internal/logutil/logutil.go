// Package logutil contains small shared logging helpers.
package logutil

// SafePrintf calls logger.Printf when logger is non-nil.
func SafePrintf(logger interface{ Printf(string, ...any) }, format string, args ...any) {
	if logger == nil {
		return
	}
	logger.Printf(format, args...)
}
