//go:build !windows

package voacap

import "os"

func replaceFile(source, target string) error {
	return os.Rename(source, target)
}
