# ADR-0062: Shared Ingest Flood Control With Required YAML Policy

- Status: Accepted
- Date: 2026-04-08
- Decision Origin: Design

Native automated-source DECALL applicability is superseded by
[ADR-0352](ADR-0352-native-reporter-quota-exemption.md). Other rails and the
required startup policy remain in force.

## Context

The cluster already had primary dedupe and telnet-facing secondary dedupe, but it had no shared-ingest flood stage that could react to actor burst behavior before spots entered primary dedupe state. That left a gap for near-unique floods from one spotter, node, IP, or hot DX target. The requested implementation also required that operator-tunable flood behavior live entirely in YAML rather than code defaults.

This change affects:

- startup config contract
- shared-ingest ordering
- shared drop/suppress semantics
- operator observability

Under the repository decision-memory rules, that requires an ADR rather than `No decision change`.

## Decision

Add a new required config file, `data/config/floodcontrol.yaml`, merged into the main config directory as a top-level `flood_control` block.

Implement flood control as a dedicated shared-ingest stage placed between `ingestValidator` and primary dedupe.

Use these actor rails only:

1. `DECALL`
2. `SourceNode`
3. `SpotterIP`
4. `DXCALL`

Partition all flood state by exact `SourceType`.

Make the following operator-tunable only through YAML:

- top-level enablement
- log interval
- per-rail enablement
- per-rail action (`observe|suppress|drop`)
- per-rail window
- per-rail per-partition state cap
- per-source thresholds
- `DXCALL` active mode and mode tables

Startup fails fast if `floodcontrol.yaml` is missing or the `flood_control` schema is incomplete or invalid.

Runtime state remains bounded by explicit per-partition caps. When a rail partition is full and a new actor key arrives, the controller fails open for that unseen key instead of growing memory unbounded or blocking ingest.

## Alternatives considered

1. Put flood control inside primary dedupe.
   Rejected because dedupe and flood policy solve different problems and need different keys and operator knobs.
2. Apply flood control after primary dedupe or in telnet fan-out.
   Rejected because shared-ingest suppression must happen before dedupe state is consumed, and telnet-only placement would miss peer/archive implications.
3. Keep code-bound thresholds and windows.
   Rejected because the requested policy was explicit: no hardcoded operator config parameters.

## Consequences

### Benefits

- Shared actor floods can be observed or suppressed before they contaminate primary dedupe history.
- Operator policy is explicit and reviewable in one dedicated YAML file.
- Memory stays bounded by config instead of assuming benign actor diversity.
- Flood decisions are reason-coded in runtime stats and logs.

### Risks

- Startup is stricter because a missing or partial `floodcontrol.yaml` now fails the process.
- Misconfigured `suppress` or `drop` rails can remove spots cluster-wide, not just for one telnet user.
- More config surface area means more operator tuning responsibility.

### Operational impact

- The shipped defaults are traffic-neutral because all rails start in `observe`.
- Operators can promote individual rails to `suppress` or `drop` without code changes.
- Overflow on actor-state caps fails open for unseen keys and is logged/counted so operators can raise caps deliberately if needed.

## Links

- Related issues/PRs/commits:
- Related tests: `config/flood_control_config_test.go`, `floodcontrol/controller_test.go`, `stats/tracker_flood_test.go`
- Related docs: `README.md`, `data/config/README.md`, `data/config/floodcontrol.yaml`
- Related TSRs: none
- Supersedes / superseded by:
