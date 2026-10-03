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
var ReleaseTag = ""
var Commit = "unknown"
var BuildTime = "unknown"

type binaryVersion struct {
	version    string
	releaseTag string
	commit     string
	buildTime  string
	goVersion  string
}

// Purpose: Resolve executable identity from linker flags or Go build metadata.
// Key aspects: Prefers explicit ldflags, then falls back to embedded VCS settings.
// Upstream: main startup/version output.
// Downstream: runtime/debug.ReadBuildInfo and startup logging.
func resolveBinaryVersion() binaryVersion {
	info := binaryVersion{
		version:    strings.TrimSpace(Version),
		releaseTag: strings.TrimSpace(ReleaseTag),
		commit:     strings.TrimSpace(Commit),
		buildTime:  strings.TrimSpace(BuildTime),
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
	for _, setting := range buildInfo.Settings {
		switch setting.Key {
		case "vcs.revision":
			vcsRevision = strings.TrimSpace(setting.Value)
		case "vcs.time":
			vcsTime = strings.TrimSpace(setting.Value)
		}
	}
	if info.commit == "unknown" && vcsRevision != "" {
		info.commit = shortRevision(vcsRevision)
	}
	if info.buildTime == "unknown" && vcsTime != "" {
		info.buildTime = vcsTime
	}
	if info.version == "dev" {
		switch generated := compileDateVersion(info.buildTime); {
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
	return info
}

// compileDateVersion uses UTC YYMMDD; source identity remains separate metadata.
func compileDateVersion(buildTime string) string {
	stamp, ok := compileDateStamp(buildTime)
	if !ok {
		return ""
	}
	return stamp
}

func compileDateStamp(buildTime string) (string, bool) {
	parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(buildTime))
	if err != nil {
		return "", false
	}
	utc := parsed.UTC()
	return utc.Format("060102"), true
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
	fmt.Printf("Product version: %s\n", info.version)
	if info.releaseTag != "" {
		fmt.Printf("Release tag:     %s\n", info.releaseTag)
	}
	fmt.Printf("Commit:          %s\n", info.commit)
	fmt.Printf("Built:           %s\n", info.buildTime)
	if info.goVersion != "" {
		fmt.Printf("Go:              %s\n", info.goVersion)
	}
}

func (info binaryVersion) clusterBuildInfo() cluster.BuildInfo {
	return cluster.BuildInfo{
		Version:    info.version,
		ReleaseTag: info.releaseTag,
		Commit:     info.commit,
		BuildTime:  info.buildTime,
		GoVersion:  info.goVersion,
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
