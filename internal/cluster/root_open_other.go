//go:build !linux

package cluster

import (
	"os"
	"path/filepath"
)

// openRootNoFollow confines operations to one entry under the borrowed root.
// The caller verifies the opened file identity before using it.
func openRootNoFollow(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
	return openRootNoFollowOwned(root, name, flags, mode, nil)
}

func openRootNoFollowOwned(root *os.Root, name string, flags int, mode os.FileMode, _ *logCloseOwner) (*os.File, error) {
	if root == nil || name == "" || name == "." || name == ".." || filepath.Base(name) != name || !filepath.IsLocal(name) {
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrInvalid}
	}
	return root.OpenFile(name, flags, mode)
}
