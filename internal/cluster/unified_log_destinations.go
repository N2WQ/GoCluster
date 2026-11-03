package cluster

import (
	"path/filepath"
	"strings"
	"time"

	"dxcluster/config"
	"dxcluster/internal/logfiles"
	"dxcluster/internal/logutil"
)

const (
	peerOverlongPath          = logutil.PeerOverlongPath
	maxConfiguredLogPathBytes = 64 << 10
)

// fileDestinationConfigs supplies the complete generation at once, before
// starting the workers. cfg is already normalized by the config loader.
func fileDestinationConfigs(cfg config.LoggingConfig, rotate logRotateHook) []logfiles.DestinationConfig {
	result := make([]logfiles.DestinationConfig, 0, 11)
	retention := func(days int) int {
		if days > 0 {
			return days
		}
		if cfg.RetentionDays > 0 {
			return cfg.RetentionDays
		}
		return 7
	}
	dir := func(path string) string {
		if len(path) > maxConfiguredLogPathBytes {
			return ""
		}
		// TrimSpace returns a substring and can retain a much larger YAML or
		// caller allocation. The config lease must own bounded backing even
		// after the original runtime configuration has retired.
		return strings.Clone(strings.TrimSpace(path))
	}
	add := func(stream uint8, path string, days int, factory logfiles.ProcessorFactory) {
		result = append(result, logfiles.DestinationConfig{Stream: stream, Kind: logfiles.Daily, Path: dir(path), RetentionDays: retention(days), ProcessorFactory: factory})
	}
	if cfg.Enabled {
		add(streamSystemFile, cfg.Dir, cfg.RetentionDays, nil)
	}
	if cfg.Propagation.Enabled {
		add(streamPropagation, cfg.Propagation.Dir, cfg.Propagation.RetentionDays, nil)
		result[len(result)-1].RotateHook = rotate
	}
	dropped := cfg.DroppedCalls
	if dropped.Enabled {
		window := time.Duration(dropped.DedupeWindowSeconds) * time.Second
		var factory logfiles.ProcessorFactory
		if window > 0 {
			factory = func() logfiles.Processor { return newDroppedLogProcessor(window) }
		}
		addDropped := func(enabled bool, stream uint8, leaf string) {
			if !enabled {
				return
			}
			base := dir(dropped.Dir)
			path := ""
			if base != "" && len(base)+1+len(leaf) <= maxConfiguredLogPathBytes {
				path = filepath.Join(base, leaf)
			}
			add(stream, path, dropped.RetentionDays, factory)
		}
		addDropped(dropped.BadDEDX, streamDroppedBadCall, "bad_de_dx")
		addDropped(dropped.NoLicense, streamDroppedUnlicensed, "no_license")
		addDropped(dropped.Harmonics, streamDroppedHarmonic, "harmonics")
	}
	addEvent := func(stream uint8, event config.EventFileLoggingConfig) {
		if !event.Enabled {
			return
		}
		window := time.Duration(event.DedupeWindowSeconds) * time.Second
		var factory logfiles.ProcessorFactory
		if window > 0 {
			factory = func() logfiles.Processor { return newEventLogProcessor(window) }
		}
		add(stream, event.Dir, event.RetentionDays, factory)
	}
	addEvent(streamLoginAttempts, cfg.LoginAttempts)
	addEvent(streamReputationDrops, cfg.ReputationDrops)
	addEvent(streamTelnetConnections, cfg.TelnetConnections)
	addEvent(streamIngestConnections, cfg.IngestConnections)
	peer := cfg.PeerConnections
	if peer.Enabled {
		window := time.Duration(peer.DedupeWindowSeconds) * time.Second
		var factory logfiles.ProcessorFactory
		if window > 0 {
			factory = func() logfiles.Processor { return logfiles.NewPeerProcessor(window) }
		}
		add(streamPeerConnections, peer.Dir, peer.RetentionDays, factory)
	}
	result = append(result, logfiles.DestinationConfig{Stream: streamPeerOverlong, Kind: logfiles.Overlong, Path: peerOverlongPath})
	return result
}
