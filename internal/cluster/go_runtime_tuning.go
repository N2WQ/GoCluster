// File role: applies YAML-owned Go runtime controls at startup before the
// cluster constructs retained stores, sockets, and long-lived goroutines.
package cluster

import (
	"fmt"
	"log"
	"runtime"
	"runtime/debug"

	"dxcluster/config"
)

const bytesPerMiB = 1024 * 1024

func applyGoRuntimeTuning(cfg config.GoRuntimeConfig) {
	if cfg.MemoryLimitMiB > 0 {
		debug.SetMemoryLimit(int64(cfg.MemoryLimitMiB) * bytesPerMiB)
	}
	if cfg.GCPercent > 0 {
		debug.SetGCPercent(cfg.GCPercent)
	}
	if cfg.MaxProcs > 0 {
		runtime.GOMAXPROCS(cfg.MaxProcs)
	}
}

func logGoRuntimeTuning(cfg config.GoRuntimeConfig) {
	memoryLimit := "unchanged"
	if cfg.MemoryLimitMiB > 0 {
		memoryLimit = formatGoRuntimeMiB(cfg.MemoryLimitMiB)
	}
	gcPercent := "unchanged"
	if cfg.GCPercent > 0 {
		gcPercent = formatGoRuntimePercent(cfg.GCPercent)
	}
	maxProcs := "unchanged"
	if cfg.MaxProcs > 0 {
		maxProcs = formatGoRuntimeCount(cfg.MaxProcs)
	}
	log.Printf("Go runtime tuning: memory_limit=%s gc_percent=%s max_procs=%s", memoryLimit, gcPercent, maxProcs)
}

func formatGoRuntimeMiB(v int) string {
	return fmt.Sprintf("%dMiB", v)
}

func formatGoRuntimePercent(v int) string {
	return fmt.Sprintf("%d", v)
}

func formatGoRuntimeCount(v int) string {
	return fmt.Sprintf("%d", v)
}
