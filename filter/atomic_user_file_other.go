//go:build !windows

package filter

import "os"

func replaceUserFile(source, target string) error { return os.Rename(source, target) }
