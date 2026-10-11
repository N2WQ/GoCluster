# Custom SCP Storage and Delivery Regression

Run from the repository root on native Windows:

```powershell
pwsh -NoProfile -File scripts/test-customscp-regression.ps1
pwsh -NoProfile -File scripts/test-customscp-regression.ps1 -Race
pwsh -NoProfile -File scripts/test-customscp-regression.ps1 -Baseline
```

The fixed runs must succeed. The defective baseline run is an intentional
negative check and must exit nonzero after the consumer runs. It uses the
original `4d711a4` store through a temporary Go overlay with a scheduling-only
callback immediately after cleanup's iterator creation. It does not check out
or modify that revision. Fixture generation succeeds even if preservation
fails, allowing the actual defect-bearing database to reach the consumer.

The baseline overlay also restores that revision's store tests and benchmarks
to match its private API, and excludes incompatible newer store tests from the
producer package. The selected producer and its preservation checks remain
current; only its scheduling function uses the historical cleanup callback.
The consumer always builds without an overlay and retains its independent
delivered-output expectations. Unavailable historical sources fail before
building or executing saved binaries.

Each run owns a new directory under `.tmp/customscp-regression/`. Keep its logs
and `ready.json` when examining a failure. The manifest reports exact refreshed
observation/static value preservation and qualified support after reopening;
it does not choose the expected delivery result. Both cases successfully
persist the refresh before cleanup applies the historical candidates.

The consumer requires that run's readiness identity and existing database
before opening it. It reopens the generated database and exercises the existing
support floor and stabilizer gate, then verifies actual local telnet bytes,
the shared buffer snapshot and a decoded persisted archive row. An independent
one-observer control keeps the same static membership and must remain held
before release. Held output is a bounded-delay observation, not a claim of
permanent loss.

The runner builds and directly executes the canonical stable Windows test paths
under `.tmp/windows-tests/dxcluster/spot/` and
`.tmp/windows-tests/dxcluster/internal/cluster/`. It restores fixture environment
variables, propagates setup/consumer failures, and does not change firewall
rules. See [Windows test paths](../../scripts/README.md#windows-tests-with-stable-firewall-paths).

Run the storage regressions independently for exact timestamps/cells, stale
request preservation, genuine expiry/overflow removal across chunks, derived
state coupling, concurrent lookup/write/cleanup, and deterministic lifecycle
overlap. The full validation lane is in [dev-runbook](../dev-runbook.md).

The [push/manual CI workflow](../../.github/workflows/ci.yml) runs the connected
regression on Ubuntu after the unit suite. The
[nightly/manual race workflow](../../.github/workflows/nightly-race.yml) runs the
same handoff with both stages compiled using `-race`. Each creates a fresh
runner-temporary directory, exports its absolute path and unique run identity,
checks that both exact test names exist, then runs the producer before the
consumer with `-count=1` and a two-minute test timeout. Setup or delivery failure
fails the job; verbose logs include preservation, reopen and actual TCP output.
These CI steps exercise the repaired code. The intentional defective-baseline
check remains available through the Windows runner's `-Baseline` mode.

Plain `go test ./...` still runs both tests with independent fixtures; its pass
does not establish the producer's database reached the consumer. Native Windows
connected validation continues to use the dedicated stable-path runner above.

Passing these checks establishes this storage repair. It does not establish
field call accuracy, independent receiver errors, probability calibration,
live peer wire delivery, power-loss durability or 24/7 production performance.

Decision: [ADR-0279](../decisions/ADR-0279-custom-scp-cleanup-and-database-ownership.md).
