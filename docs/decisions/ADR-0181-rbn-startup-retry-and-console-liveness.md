# ADR-0181: RBN Startup Retry And Console Liveness

- Status: Accepted
- Date: 2026-06-16
- Decision Origin: Troubleshooting chat

## Context

The console overview showed the top-level RBN label as offline even while RBN
digital FT spots were being ingested. The aggregate label required both RBN
clients to be connected, so a CW/RTTY outage made the whole RBN family red.

The same incident exposed a lifecycle gap: when an enabled RBN client failed
the first dial during startup, `Connect` returned before the reconnect
supervisor was started. Mid-stream reconnects were bounded and automatic, but
launch-time failures were sticky until process restart.

Operators need two separate signals:

- the RBN source family is contributing when at least one enabled RBN feed is
  connected
- each RBN subfeed remains individually visible so partial outage is still
  obvious

## Decision

Add an explicit `ConnectWithInitialRetry` path to the RBN client. It starts the
reconnect supervisor before the first dial, returns the first dial error to the
caller for logging, and schedules bounded background retry until the client
connects or shutdown is signaled. Retry uses the existing exponential backoff
range of 5 seconds to 60 seconds.

Use that startup path for the production RBN CW/RTTY and RBN digital feed
callers. Even when the first dial fails, the runtime keeps the enabled client,
starts the spot forwarder, includes the source in ingest health monitoring, and
lets later reconnect success turn the source live.

Keep `Client.Connect` unchanged for existing callers that still want first-dial
failure to abort that caller's setup. Human/relay telnet startup behavior is
therefore unchanged by this decision.

Change the console RBN family liveness rule from "both RBN clients connected"
to "either RBN client connected." Detailed ingest source rows remain per-feed:
`RBN` and `RBN-FT` can independently show green or red.

PSKReporter, DXSummit, and peer liveness rules remain unchanged in this
decision. They already have source-specific health semantics in the dashboard.

## Alternatives considered

1. Keep the aggregate RBN label red unless both feeds are connected.
   - Rejected because it reports a total source-family outage when one RBN feed
     is still connected and ingesting spots.
2. Change `Client.Connect` globally to retry after first-dial failure.
   - Rejected for this slice because the minimal-parser human/relay telnet
     caller has a broader operational contract and should not be changed as an
     incidental side effect.
3. Block startup until enabled RBN feeds connect.
   - Rejected because DNS or upstream outage would hold the whole cluster
     hostage even when other sources, telnet users, and peers can run.
4. Add YAML knobs for retry delays.
   - Rejected because the existing bounded 5s to 60s reconnect policy is
     already adequate and adding operator knobs would widen the config surface
     without fixing the bug more directly.

## Consequences

### Benefits

- A startup DNS or dial failure no longer makes an enabled RBN feed permanently
  offline for the process lifetime.
- The top-level RBN label now matches source-family contribution: green if
  either RBN subfeed is connected.
- Operators still see partial outage because detailed `RBN` and `RBN-FT` rows
  are independent.
- Startup keeps the first failure visible through logs and ingest connection
  events instead of hiding it behind silent retry.

### Risks

- A process with bad RBN DNS or host configuration will continue retrying for
  the process lifetime. The existing bounded backoff prevents tight retry
  storms, and the source stays red until a connection succeeds.
- The RBN family label can be green while one subfeed is red. Operators must use
  the detailed ingest source rows when they need per-feed status.

### Operational impact

- No YAML/config schema, parser, source-admission, dedupe, telnet command, or
  protocol behavior changes.
- Enabled RBN clients now remain health-monitored even after a first-dial
  startup failure.
- Existing RBN mid-stream reconnect semantics are preserved.

## Links

- Related issues/PRs/commits: none
- Related code: `rbn/client.go`, `internal/cluster/main_runtime.go`,
  `internal/cluster/bootstrap.go`
- Related tests: `rbn/client_test.go`, `internal/cluster/main_stats_test.go`
- Related docs: `rbn/README.md`, `README.md`
- Related TSRs: TSR-0031
- Supersedes / superseded by: none
