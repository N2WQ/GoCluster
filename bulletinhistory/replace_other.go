//go:build !windows

package bulletinhistory

import "os"

func replaceFile(source, target string) error { return os.Rename(source, target) }
