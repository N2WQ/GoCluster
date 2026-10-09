# ADR-0115: DX Numeric SSID Canonicalization

- Status: Accepted
- Date: 2026-05-05
- Decision Origin: Design

## Context
Numeric SSIDs on the station being spotted should not create separate DX
identities. Earlier designs handled only manual spots, `WHOSPOTSME`, and
own-call display as separate use cases. That missed the higher-level contract:
all new DX spots should use the baseline DX call regardless of ingest source.

Telnet login identity is a separate concern. A user logged in as `N2WQ-7`
should still have that session identity, but own-call features should use
`N2WQ` as the baseline own call.

## Decision
Spot construction and shared DX fallback paths canonicalize the spotted station
with a DX-specific normalizer. The normalizer keeps generic callsign
normalization behavior intact, then removes only a trailing numeric hyphen SSID
from the DX side.

Resolver evidence ingress also uses the DX-specific normalizer. After a spot or
resolver evidence item crosses its ingestion/materialization boundary, stored DX
identity fields are expected to be baseline calls; resolver support lookups
compare stored candidates directly instead of repeatedly normalizing candidates
on read.

The contract is:

- `CALL-2` and `CALL-15` become `CALL` when they are DX calls.
- Non-numeric suffixes such as `CALL-ABC` are preserved.
- Portable intent remains governed by existing callsign normalization.
- DE/spotter calls keep source-specific behavior unless the local telnet
  sender is being converted to a baseline own call for manual spots.
- Login records keep the full login callsign; `SHOW OWN`, self matching, manual
  telnet sender identity, and `WHOSPOTSME` use the baseline own call.
- Existing archives are not migrated on disk. Materialized spot rows pass
  through model normalization, so old rows can display canonically after read,
  but storage migration is a separate decision.

## Alternatives considered
1. Patch only manual DX, `SHOW OWN`, and `WHOSPOTSME`.
   Rejected because non-manual ingest sources would continue to create suffixed
   DX identities.
2. Change global `NormalizeCallsign`.
   Rejected because login, DE, CTY, ULS, and source identity paths have
   different semantics and should not silently lose numeric SSIDs.
3. Strip all hyphen suffixes from DX calls.
   Rejected because non-numeric suffixes can carry source or operator meaning.

## Consequences
### Benefits
- New spots from manual telnet, RBN, PSKReporter, DXSummit, peers, and replay
  tools share one DX identity for numeric-SSID variants.
- `WHOSPOTSME` and self-spot delivery behave consistently for users logged in
  with numeric SSIDs.
- The login/session identity contract remains stable.

### Risks
- Operators may still find older on-disk archive records that were persisted
  before this ADR with numeric DX SSIDs if they inspect raw storage.
- Shared constructor and resolver-ingress behavior has broad reach, so
  regressions must be covered at constructors, source parsers,
  correction/replay assignment, resolver evidence, and telnet self-delivery
  boundaries.

### Operational impact
- No config changes are required.
- `SHOW OWN` lets telnet users confirm login call versus baseline own call.
- Support answers should explain that numeric DX SSIDs are stripped for spots,
  while login calls remain distinct session identities.

## Links

- Reputation refinement: [ADR-0271](ADR-0271-shared-station-reputation.md)
- Related issues/PRs/commits: current working tree
- Related tests: `spot/callsign_test.go`, `spot/spot_test.go`,
  `spot/who_spots_me_test.go`, `commands/processor_test.go`,
  `telnet/server_filter_test.go`, `telnet/server_spot_snapshot_test.go`,
  `rbn/parse_spot_test.go`, `dxsummit/client_test.go`,
  `pskreporter/client_test.go`, `peer/parse_test.go`,
  `internal/cluster/main_test.go`, `spot/signal_resolver_test.go`,
  `cmd/rbn_replay/pipeline_test.go`,
  `cmd/rbn_replay/rbn_history_test.go`,
  `cmd/callcorr_reveng_rebuilt/main_test.go`
- Related docs: `README.md`, `commands/README.md`,
  `docs/OPERATOR_GUIDE.md`, `customgpt/source-map.md`,
  `customgpt/common-questions.md`, `customgpt/operator-guide-index.md`
- Related TSRs: -
- Supersedes / superseded by: -
