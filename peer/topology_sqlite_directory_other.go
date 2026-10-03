//go:build !windows

package peer

func topologyDirectoryPath(path string) (string, error) { return path, nil }
