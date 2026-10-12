package testsupport

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"
)

// RunMain executes the current saved test binary's selected main helper in an
// isolated working directory. Main's deliberate exit must remain distinct from
// launch, timeout and join failures, so this child omits -test.paniconexit0.
func RunMain(t testing.TB, child, directory string, environment []string, arguments ...string) (int, string, string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	args := append([]string{"-test.run=^" + regexp.QuoteMeta(child) + "$", "--"}, arguments...)
	cmd := exec.CommandContext(ctx, exe, args...)
	cmd.Dir = directory
	cmd.Env = append(os.Environ(), environment...)
	cmd.WaitDelay = 2 * time.Second
	var output, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &stderr
	err = cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("actual main missed execution/join bound: %v", ctx.Err())
	}
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("child launch/join failure: %v", err)
		}
		code = exitErr.ExitCode()
	}
	return code, output.String(), stderr.String()
}
