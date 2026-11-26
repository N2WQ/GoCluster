//go:build !windows

package main

import "os"

func reparsePoint(info os.FileInfo) bool { return info.Mode()&os.ModeSymlink != 0 }
