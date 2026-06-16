# TSR-0031: RBN Startup Retry And Console Liveness

- Status: Resolved
- Date opened: 2026-06-16
- Status date: 2026-06-16

## Trigger

The operator reported that the UI console showed RBN red while RBN CW and FT
spots were being ingested, and then asked whether a launch-time RBN failure
could stay red permanently.

## Symptoms and impact

One RBN feed could fail the first startup connection attempt with a DNS or dial
error while the other RBN feed connected and ingested spots. The top-level RBN
dashboard label still showed red because the aggregate required both RBN
clients to be connected.

For the feed that failed during startup, the runtime returned before starting
the reconnect supervisor or the spot forwarder. That made a first-dial failure
sticky until the process was restarted. Mid-stream failures after a successful
first connection already used bounded reconnect.

## Hypotheses tested

1. RBN digital ingest was actually down.
   - Disproved by live behavior and code paths showing the digital client could
     connect and ingest while the aggregate label stayed red.
2. The RBN aggregate label required both RBN feeds.
   - Confirmed by the old `rbnFeedsLive` rule, which returned false unless both
     clients were non-nil and connected.
3. RBN launch-time failures automatically retried like mid-stream failures.
   - Disproved by `Client.Connect`, which returned before starting
     `connectionSupervisor` when `establishConnection` failed.

## Evidence

- Runtime startup wiring returned immediately after a failed RBN CW/RTTY or RBN
  digital `Connect` call, so no forwarder was started for that enabled client.
- `Client.Connect` started the reconnect supervisor only after a successful
  first dial.
- The reconnect supervisor already used shutdown-aware exponential backoff for
  later reconnect signals.
- The dashboard kept per-feed source rows, but the top-level RBN label required
  both RBN clients to be connected.

## Root cause or best current explanation

Two independent behaviors combined into a misleading console state:

- the RBN source-family label used AND semantics across CW/RTTY and digital RBN
  feeds
- a first-dial startup failure never entered the normal supervised reconnect
  path

That made a partial RBN outage look like a total RBN outage, and could keep the
failed startup feed offline until restart.

## Fix or mitigation

- Add `ConnectWithInitialRetry` to start RBN reconnect supervision before the
  first dial, return the first error for logging, and keep bounded retrying
  until connect or shutdown.
- Use that path for production RBN CW/RTTY and RBN digital startup.
- Always start the RBN spot forwarder for enabled production RBN clients so a
  later reconnect can feed the unified ingest pipeline.
- Change the top-level RBN source-family label to green when either RBN feed is
  connected.
- Preserve per-feed rows so `RBN` and `RBN-FT` independently show red or green.

No config change is required.

## Why an ADR was or was not required

ADR required because the fix changes the operator-visible dashboard liveness
contract and the startup lifecycle contract for production RBN clients.

## Links

- Related ADRs: ADR-0181
- Related issues/PRs/commits: none
- Related tests: `rbn/client_test.go`, `internal/cluster/main_stats_test.go`
- Related docs: `README.md`, `rbn/README.md`
