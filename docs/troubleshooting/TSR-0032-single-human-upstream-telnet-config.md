# TSR-0032 - Single Human Upstream Telnet Config

Status: Resolved
Date Opened: 2026-07-10
Date Resolved: 2026-07-10
Owner: GoCluster maintainers
Technical Area: config, rbn, internal/cluster, ui
Trigger Source: Operator report
Led To ADR(s): ADR-0218
Tags: human_telnet, upstream, lifecycle, dashboard

## RCA Summary

- What happened: Adding more than one `human_telnet` server did not create and
  display multiple upstream connections.
- Why: The loader decoded `human_telnet` into one `RBNConfig`, and the runtime,
  health monitor, dashboard, and shutdown paths each owned one client.
- What fixed it: ADR-0218 replaced the singleton with a bounded ordered
  registry, independent supervised clients, joined shared-channel shutdown,
  and one console/health identity per enabled server.
- How we know: Source inspection traced the singleton through config load,
  runtime startup, health, dashboard, and shutdown. Focused config, client,
  cluster, and UI tests pass normally and with the Go race detector, including
  two-feed failure isolation and 64-row console coverage.
- Operator/support answer: Put complete entries in the ordered `human_telnet`
  list in effective `ingest.yaml`. Each enabled entry should appear as
  `HUMAN/<name>`; red means disconnected/retrying and green means its TCP
  connection is established. Scroll the source pane for entries beyond ten
  visible rows.

## Triggering Request

- Request date: 2026-07-10
- Request summary: Review why more than one human/upstream telnet server did not
  work and add support for any practical number, with peer-style YAML reuse and
  correct per-server console counts.
- Request reference (chat/issue/link): operator report relayed in chat

## Symptoms and Impact

- What failed or looked wrong? Only one typed config value and one runtime
  client existed, so additional upstream definitions could not become
  independent connections or status rows.
- User/operator impact: A node could not obtain resilience or coverage from
  multiple human upstreams and could not tell which named server was live.
- Scope and affected components: YAML load/validation, RBN client lifecycle,
  cluster ingest fan-in, connection health, dashboard formatting/UI, shutdown,
  operator/support docs.

## Timeline

1. 2026-07-10 - Operator report identified that adding upstream definitions did
   not work.
2. 2026-07-10 - Current-state tracing confirmed singleton config and runtime
   ownership and identified reusable RBN and peer-registry patterns.
3. 2026-07-10 - Approved Scope Ledger v7 implemented and focused normal/race
   integration tests passed.

## Hypotheses and Tests

1. Hypothesis A - YAML already supported a sequence, but startup accidentally
   selected only the first entry.
   - Evidence/commands: Inspected `Config.HumanTelnet`, loader presence rules,
     and runtime construction at the pre-change revision.
   - Outcome: Rejected
2. Hypothesis B - The whole path was structurally singleton.
   - Evidence/commands: Traced config, `humanTelnetClient`, health source,
     dashboard call, startup log, and shutdown ownership.
   - Outcome: Supported
3. Hypothesis C - The peer manager could be reused without semantic changes.
   - Evidence/commands: Compared peer registry/manager contracts with RBN
     minimal-parser human ingest and downstream provenance.
   - Outcome: Rejected

## Findings

- Root cause (or best current explanation): The implementation used one
  `RBNConfig` and one client across every material layer; multiple upstreams
  were not a supported runtime shape.
- Contributing factors: The single-map YAML looked like an extensible named
  source, while status formatting grouped source families and the older client
  lifecycle had mutable connection-wide state.
- Why this did or did not require a durable decision: It required ADR-0218
  because the fix changes config compatibility, connection/retry ownership,
  resource bounds, shutdown ordering, and operator-visible liveness.

## Decision Linkage

- ADR created/updated: ADR-0218
- Decision delta summary: Replace one human upstream with a bounded ordered
  registry of independently supervised clients and per-entry health/UI state.
- Contract/behavior changes (or `No contract changes`): New canonical list
  shape, 64-entry and queue bounds, legacy one-map compatibility, independent
  retry, and `HUMAN/<name>` console rows.

## Verification and Monitoring

- Validation steps run: focused `go test` and `go test -race` across `config`,
  `rbn`, `internal/cluster`, and `ui`; final repository validation is recorded
  in the implementation closeout.
- Signals to monitor (metrics/logs): Per-entry `HUMAN/<name>` rows, Ingest
  connected/enabled totals, ingest-connection event logs, retry logs, raw/spot
  drop counters, shutdown duration.
- Rollback triggers: reconnect spin, stale-generation activity, shutdown over
  two seconds, send-after-close/race findings, missing or miscounted entries,
  or parser/dedupe/provenance regression.

## References

- Issue(s): none recorded
- PR(s): none recorded
- Commit(s): none recorded
- Related ADR(s): ADR-0218, ADR-0181, ADR-0156
- Related docs: `README.md`, `data/config/README.md`,
  `docs/OPERATOR_GUIDE.md`, `docs/domain-contract.md`, `rbn/README.md`
