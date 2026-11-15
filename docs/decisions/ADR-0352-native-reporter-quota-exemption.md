# ADR-0352: Native Reporter Quota Exemption

- Status: Accepted
- Date: 2026-11-15
- Decision Origin: Design

## Context

Reporting-station flood allowances protect human posting. Native automated
receiver feeds can produce legitimate bursts that are not a human posting
rate. The owner selected a reporting-station-only exemption, separate human
source allowances and automatic recovery under configurable fixed limits.

## Decision

Native RBN, FT4, FT8 and PSKReporter source types bypass the DECALL rail only.
Use the existing source classifier after canonical source normalization. Mode,
comment, reported callsign suffix and mutable human flags cannot grant the
exemption. PEER and human UPSTREAM reports remain human for this purpose.

Retain configured SourceNode, SpotterIP and DXCALL behavior for native sources.
Keep MANUAL, PEER and UPSTREAM reporting-station partitions separate. Retain
the existing rolling window, inclusive cutoff, per-arrival charging and
automatic recovery. Do not change shipped rates, actions or required tables.
Native DECALL values remain required/validated for configuration compatibility
but do not create reporting-station state. The startup cardinality guard may
conservatively include these inactive configured partitions.

## Alternatives considered

1. Set shipped native DECALL thresholds to zero. This does not make the
   exemption explicit in private policies with nonzero values.
2. Bypass every flood rail for automated sources. Rejected by the owner;
   node, reported-IP and spotted-target controls remain distinct.
3. Infer automation from mode, comment or a relayed skimmer marker. Rejected;
   these fields do not establish trusted native source provenance.

## Consequences

### Benefits

- Native receiver activity cannot consume a human reporting-station allowance.
- External human sources cannot consume local reporting-station allowance.
- Other configured actor protections remain available.

### Risks

- A private native DECALL suppress/drop setting becomes inactive on restart.
- Rolling windows and current shipped values are not calibrated proof of
  human-safe contest behavior. Operational selection remains evidence-dependent.

### Operational impact

Review `data/config/README.md#flood-configuration` before restart. Numeric values,
actions and zero/false contracts remain unchanged. Native per-reporter limits
cannot be reactivated merely by increasing or decreasing their DECALL values.
No live deployment, algorithm winner or duplicate-accounting change follows
from this decision.

## Links

- Related tests: `floodcontrol/human_quota_test.go`,
  `internal/cluster/human_flood_isolation_test.go`.
- Related docs: `data/config/README.md`, `docs/flood-control-v2-execution.md`.
- Related TSRs: TSR-0133 (startup configuration repair).
- Supersedes: ADR-0062's DECALL applicability to native automated sources only.
