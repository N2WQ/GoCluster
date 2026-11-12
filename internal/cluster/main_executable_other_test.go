//go:build !windows

package cluster

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Non-Windows CI retains the original isolated, plain real-main build.
func mainExecutableForFixture(t *testing.T, repo, work string) string {
	t.Helper()
	image := filepath.Join(work, "gocluster")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", image, ".")
	build.Dir = repo
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build main executable: %v\n%s", err, output)
	}
	return image
}

func observeMainFixtureImage(t *testing.T, process *os.Process, expected string) {
	t.Helper()
	t.Logf("MAIN-CHILD launched image=%s pid=%d", expected, process.Pid)
}
