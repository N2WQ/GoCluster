//go:build !windows

package peer

import "os"

type topologyDirectoryOwner struct{}

func (*topologyDirectoryOwner) Close() error                         { return nil }
func (db *topologyDatabase) makeTopologyDirectory(path string) error { return os.MkdirAll(path, 0755) }
