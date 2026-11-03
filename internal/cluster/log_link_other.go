//go:build !windows && !linux

package cluster

import "os"

// When link identity is unavailable, the active log is disposable.
func logArchiveSingleLink(*os.File) (bool, error) { return false, nil }
