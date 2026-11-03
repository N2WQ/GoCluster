package config

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	"dxcluster/internal/logutil"
)

// validateLogFolderNames rejects obvious configured folder collisions without
// touching the filesystem during startup. Physical aliases need a separate
// worker-side identity check before logging is safe.
func validateLogFolderNames(cfg *Config) error {
	type destination struct{ name, dir string }
	var destinations []destination
	add := func(enabled bool, name, dir string) {
		if enabled {
			destinations = append(destinations, destination{name, filepath.Clean(dir)})
		}
	}
	l := cfg.Logging
	add(l.Enabled, "logging", l.Dir)
	add(l.Propagation.Enabled, "logging.propagation", l.Propagation.Dir)
	add(l.LoginAttempts.Enabled, "logging.login_attempts", l.LoginAttempts.Dir)
	add(l.ReputationDrops.Enabled, "logging.reputation_drops", l.ReputationDrops.Dir)
	add(l.TelnetConnections.Enabled, "logging.telnet_connections", l.TelnetConnections.Dir)
	add(l.IngestConnections.Enabled, "logging.ingest_connections", l.IngestConnections.Dir)
	add(l.PeerConnections.Enabled, "logging.peer_connections", l.PeerConnections.Dir)
	if l.DroppedCalls.Enabled {
		add(l.DroppedCalls.BadDEDX, "logging.dropped_calls.bad_de_dx", filepath.Join(l.DroppedCalls.Dir, "bad_de_dx"))
		add(l.DroppedCalls.NoLicense, "logging.dropped_calls.no_license", filepath.Join(l.DroppedCalls.Dir, "no_license"))
		add(l.DroppedCalls.Harmonics, "logging.dropped_calls.harmonics", filepath.Join(l.DroppedCalls.Dir, "harmonics"))
	}
	add(cfg.Peering.Enabled, "peer overlong log", filepath.Dir(logutil.PeerOverlongPath))
	for i, left := range destinations {
		for _, right := range destinations[i+1:] {
			same := left.dir == right.dir
			if runtime.GOOS == "windows" {
				same = strings.EqualFold(left.dir, right.dir)
			}
			if same {
				return fmt.Errorf("%s and %s must use separate log folders", left.name, right.name)
			}
		}
	}
	return nil
}
