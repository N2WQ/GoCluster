//go:build !windows && !linux

package logidentity

import "os"

// Unsupported platforms fail closed rather than treating unknown identities as
// distinct. The rest of the application can still run without these logs.
func inspect(*os.File) (metadata, error) { return metadata{}, ErrUnsupported }
