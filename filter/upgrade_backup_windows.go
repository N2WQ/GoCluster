package filter

import "golang.org/x/sys/windows"

func publishUserBackup(source, target string) error {
	return moveUserFile(source, target, windows.MOVEFILE_WRITE_THROUGH)
}

// The same-directory, write-through move owns publication on Windows.
func syncUserBackupDirectory(string) error { return nil }
