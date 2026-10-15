# ADR-0307: PSKReporter Report Presence

- Status: Accepted
- Date: 2026-10-15
- Decision Origin: Design, Approved v2

## Context

Both existing PSKReporter decoders distinguish a present numeric zero report
from missing/null input. The handler nevertheless rejected report value zero
after parsing, discarding usable ordinary and path-only reports. Spot.HasReport
already carries explicit presence through normalization and downstream storage.

## Decision

Use parsed report presence for admission. Admit a present numeric zero through
the existing taxonomy route. Preserve missing/null and malformed rejection,
both parser implementations, source timestamps, mode offsets, frequency
normalization, subscriptions, queues, workers and overload policies.

Normal reports continue through normal ingestion and its existing output gates.
Path-only reports continue through learning and remain excluded from normal
archive, telnet and peer output. Ordinary PSKReporter source exclusion from
peering also remains in force.

## Alternatives considered

1. Continue treating zero as absence: loses explicit source evidence.
2. Admit absent reports as numeric zero: invents signal evidence.
3. Expand parser compatibility or change transport policy: separate scope,
   unnecessary for the existing zero-preserving input routes.

## Consequences

### Benefits

- Existing fast and compatibility input routes preserve real zero reports.
- Presence has one meaning from decoded input through learning and output.

### Risks

- Newly admitted reports add legitimate processing volume within existing
  resource bounds. Subsequent whole-cluster comparisons must account for it.
- Additional evidence does not establish RF accuracy, independent samples,
  calibrated confidence or QSO probability.

### Operational impact

- No operator configuration, saved setting, threshold or firewall change.
- Normal output remains subject to existing downstream gates. Path-only
  admission does not make those reports normal DX-cluster output.

## Links

- Related code: `pskreporter/client.go`.
- Related tests: `pskreporter/report_presence_test.go`,
  `internal/cluster/pskr_zero_connected_test.go`,
  `internal/cluster/pskr_zero_protocol_test.go`, `telnet/pskr_zero_wire_test.go`.
- Related docs: `pskreporter/README.md`,
  `docs/path-reliability-first-wave-execution.md`.
- Related TSRs: [TSR-0098](../troubleshooting/TSR-0098-benchmark-completion-evidence.md)
  records a separate benchmark-checker repair; it changes no admission policy.
- Supersedes / superseded by: none; consistent with the explicit report
  presence used by ADR-0291's separate correction-evidence contract.
