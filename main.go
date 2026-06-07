// Program gocluster delegates the live runtime to internal/cluster while keeping
// build/version resolution in the root package main.
package main

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"dxcluster/internal/cluster"
)

// Version will be set at build time.
var Version = "dev"
var Commit = "unknown"
var BuildTime = "unknown"

type binaryVersion struct {
	version     string
	commit      string
	buildTime   string
	vcsModified string
	goVersion   string
}

// Purpose: Resolve executable identity from linker flags or Go build metadata.
// Key aspects: Prefers explicit ldflags, then falls back to embedded VCS settings.
// Upstream: main startup/version output.
// Downstream: runtime/debug.ReadBuildInfo and startup logging.
func resolveBinaryVersion() binaryVersion {
	info := binaryVersion{
		version:   strings.TrimSpace(Version),
		commit:    strings.TrimSpace(Commit),
		buildTime: strings.TrimSpace(BuildTime),
	}
	if info.version == "" {
		info.version = "dev"
	}
	if info.commit == "" {
		info.commit = "unknown"
	}
	if info.buildTime == "" {
		info.buildTime = "unknown"
	}

	buildInfo, ok := debug.ReadBuildInfo()
	if !ok {
		return info
	}
	info.goVersion = strings.TrimSpace(buildInfo.GoVersion)
	if info.goVersion == "" {
		info.goVersion = runtime.Version()
	}

	vcsRevision := ""
	vcsTime := ""
	vcsModified := ""
	for _, setting := range buildInfo.Settings {
		switch setting.Key {
		case "vcs.revision":
			vcsRevision = strings.TrimSpace(setting.Value)
		case "vcs.time":
			vcsTime = strings.TrimSpace(setting.Value)
		case "vcs.modified":
			vcsModified = strings.TrimSpace(setting.Value)
		}
	}
	if info.commit == "unknown" && vcsRevision != "" {
		info.commit = shortRevision(vcsRevision)
	}
	if info.buildTime == "unknown" && vcsTime != "" {
		info.buildTime = vcsTime
	}
	if info.version == "dev" {
		switch generated := compileDateVersion(info.buildTime, vcsRevision, vcsModified); {
		case generated != "":
			info.version = generated
		case vcsRevision != "":
			info.version = "dev-" + shortRevision(vcsRevision)
		default:
			if mainVer := strings.TrimSpace(buildInfo.Main.Version); mainVer != "" && mainVer != "(devel)" {
				info.version = mainVer
			}
		}
	}
	if vcsModified != "" {
		info.vcsModified = vcsModified
	}
	return info
}

func compileDateVersion(buildTime, revision, vcsModified string) string {
	stamp, ok := compileDateStamp(buildTime)
	if !ok {
		return ""
	}
	commit := shortRevision(strings.TrimSpace(revision))
	if commit == "" {
		commit = "unknown"
	}
	if isVCSModified(vcsModified) {
		commit += "+dirty"
	}
	return stamp + "-" + commit
}

func compileDateStamp(buildTime string) (string, bool) {
	parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(buildTime))
	if err != nil {
		return "", false
	}
	utc := parsed.UTC()
	return fmt.Sprintf("v%02d.%02d.%02d", utc.Year()%100, utc.Day(), int(utc.Month())), true
}

func isVCSModified(vcsModified string) bool {
	switch strings.ToLower(strings.TrimSpace(vcsModified)) {
	case "true", "1", "yes", "dirty":
		return true
	default:
		return false
	}
}

func shortRevision(revision string) string {
	const maxLen = 12
	if len(revision) <= maxLen {
		return revision
	}
	return revision[:maxLen]
}

func shouldPrintVersion(args []string) bool {
	for _, arg := range args {
		switch strings.ToLower(strings.TrimSpace(arg)) {
		case "--version", "-version", "version":
			return true
		}
	}
	return false
}

func printVersion(info binaryVersion) {
	fmt.Printf("gocluster %s\n", info.version)
	fmt.Printf("commit: %s\n", info.commit)
	fmt.Printf("built:  %s\n", info.buildTime)
	if info.vcsModified != "" {
		fmt.Printf("dirty:  %s\n", info.vcsModified)
	}
	if info.goVersion != "" {
		fmt.Printf("go:     %s\n", info.goVersion)
	}
}

func (info binaryVersion) clusterBuildInfo() cluster.BuildInfo {
	return cluster.BuildInfo{
		Version:     info.version,
		Commit:      info.commit,
		BuildTime:   info.buildTime,
		VCSModified: info.vcsModified,
		GoVersion:   info.goVersion,
	}
}

func main() {
	versionInfo := resolveBinaryVersion()
	if shouldPrintVersion(os.Args[1:]) {
		printVersion(versionInfo)
		return
	}
	if err := cluster.Run(versionInfo.clusterBuildInfo()); err != nil {
		log.Fatalf("Startup failed: %v", err)
	}
}
