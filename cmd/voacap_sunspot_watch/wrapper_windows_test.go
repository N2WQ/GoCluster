package main

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestWatcherNativePowerShellArgumentArray(t *testing.T) {
	script, err := filepath.Abs("../../scripts/watch-voacap-ssn.ps1")
	if err != nil {
		t.Fatal(err)
	}
	const probe = `function global:go {
  $record = @{ Args = @($args); Repo = (Get-Location).Path }
  [IO.File]::WriteAllText($env:DXC_WATCH_FIXTURE_CAPTURE, ($record | ConvertTo-Json -Depth 4 -Compress))
  $global:LASTEXITCODE = [int]$env:DXC_WATCH_FIXTURE_STATUS
}
& $env:DXC_WATCH_FIXTURE_SCRIPT -Interval '5m' -HalfLife '4h' -Threshold 0.2 -Timeout '3s' -StatePath $env:DXC_WATCH_FIXTURE_STATE -Once`
	for _, status := range []int{0} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			dir := t.TempDir()
			capture := filepath.Join(dir, "args.json")
			statePath := filepath.Join(dir, "state with spaces.json")
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "pwsh", "-NoProfile", "-NonInteractive", "-Command", probe)
			cmd.Dir = dir
			cmd.WaitDelay = 2 * time.Second
			cmd.Env = append(os.Environ(), "DXC_WATCH_FIXTURE_CAPTURE="+capture, "DXC_WATCH_FIXTURE_SCRIPT="+script,
				"DXC_WATCH_FIXTURE_STATE="+statePath, "DXC_WATCH_FIXTURE_STATUS="+strconv.Itoa(status),
				"GOFLAGS=-gocluster-fixture-invalid", "GOTOOLCHAIN=local")
			output, runErr := cmd.CombinedOutput()
			if ctx.Err() != nil || watcherExitCode(t, runErr) != status {
				t.Fatalf("wrapper status mismatch: %v / %v / %s", runErr, ctx.Err(), output)
			}
			var record struct {
				Args []any
				Repo string
			}
			body, err := os.ReadFile(capture)
			if err != nil || json.Unmarshal(body, &record) != nil {
				t.Fatalf("real wrapper did not invoke controlled Go command: %s / %v", body, err)
			}
			want := []any{"run", "./cmd/voacap_sunspot_watch", "-interval", "5m", "-half-life", "4h", "-threshold", 0.2,
				"-timeout", "3s", "-state", statePath, "-once"}
			if !reflect.DeepEqual(record.Args, want) || !strings.EqualFold(filepath.Clean(record.Repo), filepath.Dir(filepath.Dir(script))) {
				t.Fatalf("wrapper changed argument array or cwd: %+v", record)
			}
		})
	}
}

func TestWatcherNativePowerShellWrapperFailure(t *testing.T) {
	script, err := filepath.Abs("../../scripts/watch-voacap-ssn.ps1")
	if err != nil {
		t.Fatal(err)
	}
	repo := filepath.Dir(filepath.Dir(script))
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	// Force a real Go-command failure before compilation or NOAA access. These
	// variables belong only to the child, not installed tools or parent settings.
	env := append(os.Environ(), "GOFLAGS=-gocluster-fixture-invalid", "GOTOOLCHAIN=local")
	direct := exec.CommandContext(ctx, "go", "run", "./cmd/voacap_sunspot_watch", "-once", "-state", statePath)
	direct.Dir, direct.Env = repo, env
	direct.WaitDelay = 2 * time.Second
	goOutput, goErr := direct.CombinedOutput()
	goStatus := watcherExitCode(t, goErr)
	if ctx.Err() != nil || goStatus == 0 || !strings.Contains(string(goOutput), "gocluster-fixture-invalid") {
		t.Fatalf("real Go failure control was not reached: %v / %s", goErr, goOutput)
	}
	cmd := exec.CommandContext(ctx, "pwsh", "-NoProfile", "-NonInteractive", "-File", script, "-Once", "-StatePath", statePath)
	cmd.Dir, cmd.Env = dir, env
	cmd.WaitDelay = 2 * time.Second
	output, runErr := cmd.CombinedOutput()
	if ctx.Err() != nil || watcherExitCode(t, runErr) != goStatus || !strings.Contains(string(output), "gocluster-fixture-invalid") {
		t.Fatalf("native -File wrapper did not preserve Go status %d: %v / %s", goStatus, runErr, output)
	}
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Fatalf("preflight failure unexpectedly created state: %v", err)
	}
	// Falsify the checker: removing the wrapper's explicit exit must stop
	// preserving the native Go status under -File, despite the same Go failure.
	body, err := os.ReadFile(script)
	if err != nil {
		t.Fatal(err)
	}
	broken := strings.Replace(string(body), "    exit $LASTEXITCODE", "    # fixture: omit explicit exit", 1)
	if broken == string(body) {
		t.Fatal("wrapper exit mutation was not installed")
	}
	brokenDir := filepath.Join(dir, "negative-repo", "scripts")
	if err := os.MkdirAll(brokenDir, 0o755); err != nil {
		t.Fatal(err)
	}
	brokenScript := filepath.Join(brokenDir, "watch.ps1")
	if err := os.WriteFile(brokenScript, []byte(broken), 0o600); err != nil {
		t.Fatal(err)
	}
	negative := exec.CommandContext(ctx, "pwsh", "-NoProfile", "-NonInteractive", "-File", brokenScript, "-Once", "-StatePath", statePath)
	negative.Dir, negative.Env = dir, env
	negative.WaitDelay = 2 * time.Second
	negativeOutput, negativeErr := negative.CombinedOutput()
	if ctx.Err() != nil || watcherExitCode(t, negativeErr) != 0 || !strings.Contains(string(negativeOutput), "gocluster-fixture-invalid") {
		t.Fatalf("negative wrapper control did not falsify propagation: %v / %s", negativeErr, negativeOutput)
	}
}
