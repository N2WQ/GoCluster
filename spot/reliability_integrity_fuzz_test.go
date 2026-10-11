// File role: Fuzzes the public reliability loader's numerical integrity without
// changing its existing malformed-text or finite-clamping contract.
package spot

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func FuzzReliabilityLoaderNumericalIntegrity(f *testing.F) {
	const finiteControl = "K1ZERO -2\nK1ONE 2\nK1DUP 0.25\n k1dup\t0.75\r\nK1ZERO 0\nK1HALF 0.5\n"
	for _, body := range []string{
		finiteControl,
		"K1BAD NaN\n", "K1BAD +Inf\n", "K1BAD -Inf\n",
		"K1BAD 1e999\n", "K1BAD -1e999\n",
		"K1PREFIX 0.95\nK1BAD NaN\nK1LATER 0.9\n",
		"K1BAD not_a_number\n", "K1BAD 1e\n", "missing_weight\n",
		"# comment\n\nK1GOOD 0\n", "K1DUP 0.9\nK1DUP 1\n",
	} {
		f.Add(body)
	}
	f.Fuzz(func(t *testing.T, body string) {
		// Limit per-input file, scanner, and retained-map work. The public
		// loader owns parsing; the fuzzer supplies no alternative parser.
		if len(body) > 4096 {
			t.Skip("input exceeds bounded asset fixture")
		}
		path := filepath.Join(t.TempDir(), "reliability.txt")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		weights, applied, err := LoadSpotterReliability(path)
		if err != nil {
			if body == finiteControl {
				t.Fatalf("known finite control must load successfully: %v", err)
			}
			return
		}
		for receiver, weight := range weights {
			if math.IsNaN(weight) || math.IsInf(weight, 0) || weight < 0 || weight > 1 {
				t.Fatalf("successful asset contains invalid reliability for %q: %v", receiver, weight)
			}
		}
		// A reject-all implementation cannot satisfy this literal positive
		// seed. Its six applied rows retain four final receiver weights.
		if body == finiteControl {
			want := SpotterReliability{"K1ZERO": 0, "K1ONE": 1, "K1DUP": 0.75, "K1HALF": 0.5}
			if applied != 6 || len(weights) != len(want) {
				t.Fatalf("finite control lost successful rows: applied=%d weights=%v", applied, weights)
			}
			for receiver, expected := range want {
				actual, ok := weights[receiver]
				if !ok || actual != expected {
					t.Fatalf("finite control %s=%v present=%v, want %v", receiver, actual, ok, expected)
				}
			}
		}
	})
}
