# TSR-0004 - Resolver Primary Switchover Mode and Rollback Contract

Status: Resolved
Date Opened: 2026-02-25
Date Resolved: 2026-02-25
Owner: Cluster maintainers
Technical Area: main output pipeline, spot/correction, config
Trigger Source: Chat request
Led To ADR(s): ADR-0026
Tags: resolver, cutover, rollback, call-correction

## Triggering Request

- Request date: 2026-02-25
- Request summary: convert the approved switchover ledger into implementation-ready cutover behavior with explicit rollback handling.
- Request reference (chat/issue/link): repository chat thread.

## Symptoms and Impact

- What failed or looked wrong?
  - Existing runtime was resolver-shadow-only, so production cutover could not be exercised safely.
- User/operator impact:
  - No in-process switch to make resolver authoritative while retaining comparison visibility.
- Scope and affected components:
  - `main.go`, `config/config.go`, call-correction pipeline integration, operator config/docs.

## Timeline

1. 2026-02-25 - Switchover ledger approved in chat.
2. 2026-02-25 - Resolver primary mode wiring implemented with default `shadow` behavior preserved.
3. 2026-02-25 - Validation suite executed and documentation/decision records updated.

## Hypotheses and Tests

1. Hypothesis A - Resolver primary cutover can be added without changing default production behavior.
   - Evidence/commands: config default normalization and tests (`resolver_mode` defaults to `shadow`).
   - Outcome: Supported.
2. Hypothesis B - Resolver primary can preserve bounded/non-blocking contracts.
   - Evidence/commands: existing non-blocking resolver enqueue path retained; full test/vet/staticcheck/race suite passed.
   - Outcome: Supported.
3. Hypothesis C - Legacy comparison visibility can be retained during primary mode.
   - Evidence/commands: legacy correction runs in shadow-only path for disagreement observation when `resolver_mode=primary`.
   - Outcome: Supported.

## Findings

- Root cause (or best current explanation):
  - Phase 2 architecture intentionally blocked correction cutover, leaving no production-mode switchover control.
- Contributing factors:
  - Need to protect bounded-resource and rollback requirements while changing correction authority.
- Why this did or did not require a durable decision:
  - Required a durable decision due config contract and runtime correction-authority semantics change.

## Decision Linkage

- ADR created/updated:
  - `docs/decisions/ADR-0026-resolver-primary-switchover-mode.md`
- Decision delta summary:
  - Added `call_correction.resolver_mode` (`shadow`/`primary`) and primary-mode correction behavior.
- Contract/behavior changes (or `No contract changes`):
  - New config contract for correction authority selection.

## Verification and Monitoring

- Validation steps run:
  - `go test ./...`
  - `go vet ./...`
  - `staticcheck ./...`
  - `go test -race ./...`
- Signals to monitor (metrics/logs):
  - `Resolver:` summary line (agreement/disagreement and drop/pressure counters),
  - `CorrGate:` and `Stabilizer:` lines during cutover trials.
- Rollback triggers:
  - Any sustained disagreement or pressure threshold breach during primary trials,
  - any operator-observed correction regression requiring immediate revert to `resolver_mode=shadow`.

## References

- Issue(s): n/a
- PR(s): n/a
- Commit(s): n/a
- Related ADR(s):
  - `docs/decisions/ADR-0022-phase2-signal-resolver-shadow-mode.md`
  - `docs/decisions/ADR-0026-resolver-primary-switchover-mode.md`
- Related docs:
  - `docs/call-correction-phase3-switchover-ledger-v2.md`
