# Per-Mode Minimum SNR Validation

The implementation follows the user's `Approved v1` scope and
[ADR-0258](decisions/ADR-0258-per-mode-minimum-snr-filter.md). Numeric PASS and
REJECT set identical inclusive minima. Human and missing-report spots bypass
this category, and removed exact modes remain saved but inactive.

| Approved contract | Implementation | Falsifiable evidence |
| --- | --- | --- |
| Signed inclusive minima, exemptions, ordinary filter composition and dormant exact-mode activity | `filter/min_snr.go`, `filter/filter.go` | `TestMinSNRMatcherBoundaries`, `TestMinSNRExemptionsAndComposition`, `TestMinSNRDormantTaxonomy`: below/equal/above negative and zero thresholds; present zero versus absent report; independent band/source rejection; removal, alias remapping and exact restoration |
| Atomic human list validation, selected/all resets and finite resulting maps | `telnet/minsnr_commands.go`, `telnet/filter_commands.go` | Command grammar, atomic rejection, union-bound and exact dormant-alias clear tests; invalid later names preserve full live/disk state; human persistence failure retains existing live-first behavior; command fuzzing |
| Disk version 3, old-format migration and limits before typed construction | `filter/min_snr_storage.go`, configuration/storage/user/preset files | Stored round trips, nested preset baselines, versions 0/1/2, literal preserved version 2 State values, malformed/future protection, effective legacy merges and unused anchors; exact/over entry and raw-key-byte limits with invalid-value sentinels |
| Explicit schema 3, frozen schemas 1/2 and atomic machine persistence | `telnet/machine_*`, `telnet/configuration_capabilities.go` | Literal old projection order, hidden-map/revision preservation, signed extrema, complete PUT and PATCH replacement, authoritative prior dormant eligibility, persistence-failure and VALIDATE no-mutation checks; framed sessions in both dialects; schema fuzzing |
| Complete bounded human/YAML readbacks, configured/inactive status and HELP | Human configuration files, `telnet/configuration_readback.go`, `commands/configuration_help.go`, `commands/processor.go` | Sorted signed values, inactive markers/counts, long exact names, final response rejection, schema3 repeated-status-key admission and old-status omission; topic routing, 78-byte HELP lines and README HELP synchronization |
| Detached configuration identity, presets, reconnect and history invalidation | `filter/configuration.go`, `filter/configuration_compare.go`, existing transaction/history integration | Order-independent fingerprints, zero versus absence and dormant edits, preset modification/restoration/reconnect, detached history predicates, cursor and pending-page invalidation, preserved self exception and concurrent mutation |
| Operator/support guidance and durable decision | README/operator/config/telnet docs, domain contract, support card/routes, ADR-0258/index and generated code maps | Direct final diff and cross-reference review; generated-map check and whitespace check |

Support-agent documentation impact is required because command behavior,
readbacks, machine vocabulary and downgrade guidance changed. The configuration
support card and source/developer routes were updated. No support-agent Worker
or action-schema implementation changes were needed. Source parsing, report
units, aggregation, archive layout and scientific/model behavior are unchanged.

The Go quality reviewer used inherited context, not an independent context.
The lead verified and fixed both findings: missing schema3 activity status and
an unsupported explicit schema1 HELP alternative. The final fresh review found
no additional material issue. The parser selector's lint finding was corrected
to an equivalent tagged switch; affected machine tests and fuzzing passed again.

## Observed Local Checks

The following checks completed successfully on Linux with Go 1.27.1:

```text
go test ./...
go vet ./...
staticcheck ./...
golangci-lint run ./... --config=.golangci.yaml
go test -race ./...
go test ./telnet -run '^$' -fuzz '^FuzzMinSNRCommands$' -fuzztime=30s -parallel=4
go test ./telnet -run '^$' -fuzz '^FuzzMachineSchema$' -fuzztime=30s -parallel=4
go test ./filter -run '^$' -bench '^BenchmarkMinSNRMatches$' -benchmem -benchtime=100ms
```

Lint finished with `0 issues`. The full race suite exited successfully, including
`dxcluster/filter` (13.929s) and `dxcluster/telnet` (12.787s). Its MINSNR stimuli
include 500 threshold updates alongside captured-snapshot and live matching,
plus a held archive scan whose old predicate remains detached while publication
is invalidated. This is test/race evidence, not deployed-runtime leak evidence.

Command fuzzing passed 151,423 executions; the final schema fuzz run passed
9,933 executions. Both ran for 30 seconds with preservation/bounds oracles.
These bounded runs supplement literal boundary fixtures and do not prove that
all possible parser inputs are covered.

Local enabled-threshold benchmarks reported:

| Case | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Negative report below minimum | 391.5 | 0 | 0 |
| Report equal to minimum | 746.9 | 0 | 0 |
| Report above minimum | 689.0 | 0 | 0 |

These are single local microbenchmarks during other validation work. There is
no before/after latency, production p99 or live-profile claim. Dormant and active
rules share the 128-entry and 65,536 raw-key-byte limits; complete readback and
preset encoding budgets remain separate.

Generated maps are refreshed with `go run ./cmd/codemap generate -all`, followed
by `go run ./cmd/codemap check -all` and `git diff --check` for closeout.
