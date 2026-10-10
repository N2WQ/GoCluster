# ADR-0114: Human Comment Toxicity Classifier

- Status: Accepted
- Date: 2026-05-05
- Decision Origin: Design

## Context

Human-entered spot comments can contain abusive or unsafe text that operators
may want to hide from telnet sessions. The cluster cannot send every spot to an
AI model: skimmer and automated feeds dominate volume, output fan-out is latency
sensitive, and external classifier outages must not stall unrelated traffic.

The privacy boundary is also narrow. Toxicity classification only needs the
comment text. Mode, band, callsigns, source, IP, session identifiers, raw spot
lines, and archive records are not required and would increase unnecessary data
sharing with the external Worker.

## Decision

GoCluster adds an optional human-comment toxicity classifier stage after primary
spot validation/dedupe and before archive, peer, and telnet fan-out visibility.

The stage uses these rules:

- Only human-class spots are eligible for AI classification. Skimmer and other
  non-human spots bypass the classifier.
- The Cloudflare Worker contract is `POST /classify` with bearer auth, request
  body `{ "comment": "..." }`, and a safe/toxic JSON response.
- The Worker uses Cloudflare Workers AI with Llama Guard and receives only the
  cleaned complete comment.
- A conservative local safe gate marks routine ham-radio shorthand as
  `SAFE_LOCAL`; the whole comment must match the gate grammar.
- Non-routine comments, including common Western-language comments, are routed
  to AI without language-specific goroutines, detectors, queues, or classifiers.
- A memory-only normalized-comment cache stores bounded decisions by hash plus
  the normalized comment text for collision protection. The cache has TTL and
  max-entry eviction.
- AI work runs behind bounded worker and queue limits. Queue full, timeout,
  malformed response, auth/rate/server failures, disabled config, and Worker
  unavailability all fail open as `UNAVAILABLE`.
- Toxicity statuses are `UNKNOWN`, `SAFE`, `SAFE_LOCAL`, `TOXIC`, and
  `UNAVAILABLE`. Only `TOXIC` is hidden by `REJECT TOXIC`.
- `PASS TOXIC` / `REJECT TOXIC` are per-user telnet filter toggles and apply to
  live spots and archive-backed history queries.
- Archive records carry toxicity status, categories, and model for classified
  records. Legacy records decode as `UNKNOWN`.
- The feature is disabled by default through optional `toxicity.yaml`.

## Alternatives considered

1. Classify every spot comment.
   This would spend external calls on high-volume automated traffic and put
   Cloudflare latency on paths that do not need human-comment moderation.
2. Use only a local blocklist or keyword rules.
   This is deterministic and cheap, but too brittle for multilingual abuse,
   evasive phrasing, and operator trust.
3. Send callsigns, mode, band, source, and other spot metadata to the Worker.
   More context could help explain some comments, but it expands the privacy
   boundary without being necessary for comment toxicity classification.
4. Fail closed when the Worker is unavailable.
   This would hide legitimate spots during API outages, auth mistakes, rate
   limiting, or network failures. The cluster should keep operating and let
   operators troubleshoot classifier health separately.
5. Persist the cache across restarts.
   This would reduce calls after restart but creates retention and migration
   concerns. The first implementation keeps retention memory-only and bounded.

## Consequences

### Benefits

- Operators can hide AI-classified toxic human comments without affecting
  skimmer traffic.
- The hot path remains bounded: finite queue, finite workers, finite cache,
  timeout-bound external calls, and fail-open behavior.
- The external data boundary is small and testable: only cleaned comment text is
  sent to the Worker.
- Common Western-language fixtures exercise the routing contract while AI owns
  the actual language understanding.

### Risks

- `REJECT TOXIC` does not hide `UNKNOWN` or `UNAVAILABLE` spots, so toxic text
  can pass during startup, Worker outage, timeout, queue saturation, or model
  misclassification.
- `SAFE_LOCAL` depends on a conservative checked-in safe gate. Overbroad edits
  could bypass AI incorrectly, so the gate is intentionally narrow.
- Archive records now have a newer record version. Legacy records remain
  readable, but rollback must keep binary and archive expectations aligned.

### Operational impact

- Operators must deploy a Cloudflare Worker and set a bearer token environment
  variable before enabling `toxicity.yaml`.
- Worker and cluster bearer token values must remain out of checked-in config.
- Startup with missing optional `toxicity.yaml` disables the feature. Startup
  with enabled but incomplete classifier config fails with a config error.
- Shutdown drains pending classifier results for a bounded interval before
  stopping the classifier.

## Links

- Related issues/PRs/commits: none
- Related tests: `internal/toxicity/*_test.go`, `internal/cluster/output_pipeline_toxicity_test.go`, `filter/filter_test.go`, `telnet/server_filter_test.go`, `archive/record_test.go`, `cloudflare/toxicity-worker/test/worker.test.js`
- Related docs: `README.md`, `telnet/README.md`, `data/config/README.md`, `cloudflare/toxicity-worker/README.md`
- Lifecycle refinement: [ADR-0274](ADR-0274-terminal-classifier-retirement.md)
  defines terminal admission, cancellation, queue retirement and result ownership.
- Related TSRs: [TSR-0068](../troubleshooting/TSR-0068-classifier-terminal-retirement.md)
- Supersedes / superseded by: none
