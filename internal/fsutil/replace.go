package fsutil

import "os"

// ReplaceFile publishes a closed, same-directory temporary file without first
// deleting the destination. The Go runtime also handles extended Windows paths.
func ReplaceFile(source, destination string) error {
	return os.Rename(source, destination)
}
