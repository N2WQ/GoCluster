//go:build !windows

package filter

import "os"

func replaceUserFile(source, target string) error {
	return os.Rename(source, target) // #nosec G703 -- Temporary sibling and validated user-record or preset target, under the operator's data directory.
}
