package pathreliability

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Isolated saved-path children prevent unrelated prior test owners from
// contaminating process-wide exact whole-batch allocation measurements.
func TestS6PopulationWarmedAllocations(t *testing.T) {
	output, err := runPopulationAllocationChild(t, false)
	if err != nil || strings.Count(output, "population-allocation-accepted controls=21") != 1 || strings.Contains(output, "SKIP") || !strings.Contains(output, "--- PASS: TestS6PopulationAllocationChild") {
		t.Fatalf("incomplete allocation child: %v\n%s", err, output)
	}
}

func TestS6PopulationAllocationWitness(t *testing.T) {
	output, err := runPopulationAllocationChild(t, true)
	var failed *exec.ExitError
	if !errors.As(err, &failed) || failed.ExitCode() != 1 || !strings.Contains(output, "allocations=1 operations=2000 usable=true") || !strings.Contains(output, "--- FAIL: TestS6PopulationAllocationChild") || strings.Contains(output, "population-allocation-accepted") || strings.Contains(output, "panic:") || strings.Contains(output, "timed out") || strings.Contains(output, "SKIP") {
		t.Fatalf("one-per-batch allocation escaped the real oracle: %v\n%s", err, output)
	}
}

func runPopulationAllocationChild(t *testing.T, fault bool) (string, error) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, exe, "-test.run=^TestS6PopulationAllocationChild$", "-test.v", "-test.timeout=25s")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "GOCLUSTER_S6_ALLOCATION=") {
			command.Env = append(command.Env, entry)
		}
	}
	command.Env = append(command.Env, fmt.Sprintf("GOCLUSTER_S6_ALLOCATION=%t", fault))
	command.WaitDelay = 2 * time.Second
	out, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("allocation child exceeded lifetime: %v\n%s", ctx.Err(), out)
	}
	return string(out), err
}

func TestS6PopulationAllocationChild(t *testing.T) {
	setting := os.Getenv("GOCLUSTER_S6_ALLOCATION")
	if setting == "" {
		return
	}
	if setting != "false" && setting != "true" {
		t.Fatal("unknown allocation selector")
	}
	controls := 0
	for _, control := range populationBenchControls() {
		for _, mode := range []string{ReceiverContributionOff, ReceiverContributionShadow, ReceiverContributionEnforce} {
			f := newPopulationBenchFixture(t, control, mode, "candidate")
			operations, usable := 0, true
			allocations := testing.AllocsPerRun(1, func() {
				for range 1000 {
					if setting == "true" && operations == 1000 {
						pathShardAllocationWitness = make([]byte, 64)
					}
					if got := f.predict(); got != f.want {
						usable = false
					}
					operations++
				}
			})
			if allocations != 0 || operations != 2000 || !usable {
				t.Fatalf("shape=%s mode=%s allocations=%g operations=%d usable=%t", control.name, mode, allocations, operations, usable)
			}
			f.assertState(t)
			controls++
		}
	}
	if controls != 21 {
		t.Fatal("missing populated controls")
	}
	fmt.Println("population-allocation-accepted controls=21")
}
