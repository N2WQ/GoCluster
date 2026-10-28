# TSR-0112 - Full Telnet Server Stripe Wait

Status: Monitoring
Date Opened: 2026-10-28
Date Resolved: n/a
Owner: GoCluster maintainers
Technical Area: Telnet login admission and profile handoff
Trigger Source: Chat request
Led To ADR(s): ADR-0325
Tags: telnet, session capacity, reconnect, locks, deadline

## RCA Summary

- What happened: Review of commit `edab913` identified an admission
  interleaving that can delay a full-server rejection behind an unrelated
  profile transaction.
- Why: When the initial nonblocking registry check fails, login can wait for
  the configuration stripe without checking capacity again. An unrelated
  owner can hold that stripe even after the registry lock becomes available.
- What addresses it: The approved follow-up retries a nonblocking full-capacity
  check while waiting for the login stripe. It preserves the full normalized
  identity's replacement and profile-handoff rules.
- How we know: Source inspection established the original lock ordering.
  Controlled ordinary and race tests force that ordering and both departing-owner
  outcomes. This review did not observe a production incident.
- Operator/support answer: Check the executable revision, registered-session
  count, exact callsign and SSID, separate pre-login limits and storage health.
  A full-server notice is best effort.

## Triggering Request

- Request date: 2026-10-28.
- Request summary: Review the committed ceiling and correct the full-registry
  rejection path and departing-owner handoff test gaps.
- Request reference: Review of commit `edab9131275b861b9ed37d223d5bf6c71d153015`.

## Symptoms and Impact

When the server is full and a different identity's initial registry check
encounters contention, its login may wait behind a stripe shared with an
unrelated save. If that save stays blocked, the login can expire instead of
receiving the full-server response. The wait retains a pre-login slot until
cleanup. Source review did not establish an exceeded session ceiling or a
capacity-rejected profile write from this interleaving.

## Timeline

1. 2026-10-28 - Commit `edab913` introduced the strict session ceiling.
2. 2026-10-28 - Review identified the registry-check and stripe-wait ordering.
3. 2026-10-28 - The owner approved a follow-up with bounded rechecks and
   controlled departing-owner cases.

## Hypotheses and Tests

1. A briefly busy registry can hide a full server from a waiting login.
   - Evidence: In `edab913`, the failed `TryRLock` path enters stripe
     acquisition without a subsequent check until that acquisition completes.
   - Outcome: Supported by source inspection; controlled ordinary and race
     tests passed with the registry released while the stripe remained held.
2. A reconnect is necessarily still replacing an owner after its stripe wait.
   - Evidence: The previous owner's final save and unregister hold that same
     stripe; the owner can depart before the reconnect obtains it.
   - Outcome: Rejected as an assumption. Controlled ordinary and race tests
     passed for both the reconnect taking the freed slot and another identity
     taking it first.

## Findings

- Root cause: A single nonblocking capacity check was not retried while
  waiting for configuration ownership.
- Contributing factor: The previous collision test held the stripe but did
  not force the initial registry check to fail.
- Durable decision: ADR-0325 records the session ceiling and bounded recheck
  behavior. No identity-authentication policy changes.

## Decision Linkage

- ADR updated: [ADR-0325](../decisions/ADR-0325-strict-telnet-session-ceiling.md).
- Decision delta: Retry full-capacity checks during login stripe waits without
  holding that stripe or introducing an unbounded waiter pool.
- Contract/behavior changes: A full-server rejection can be detected while an
  unrelated stripe remains occupied; exact-identity replacement remains allowed.

## Verification and Monitoring

- Validation: The targeted Windows Telnet run selected the session-ceiling and
  existing configuration-handoff tests: 36 cases passed, none skipped, both
  ordinarily and with the race detector. The full native Windows ordinary and
  race lanes each passed 56 test packages and build-checked 16 more; each reported
  93 skipped cases. Local vet, Staticcheck and golangci-lint passed.
- Signals to monitor: Admission rejection reason `max_connections`, login
  timeout and configuration-handoff diagnostics, pre-login active capacity,
  and storage errors.
- Boundaries: The approximately 50 ms retry interval is not a guaranteed
  response time. Existing registry-lock and profile-I/O waits do not establish
  an absolute end-to-end login deadline; client-side disconnect may not be
  noticed immediately during a wait. Monitor live operation separately.

## References

- Original commit: `edab9131275b861b9ed37d223d5bf6c71d153015`.
- Related ADR: [ADR-0325](../decisions/ADR-0325-strict-telnet-session-ceiling.md).
- Related code and tests: [login handoff](../../telnet/configuration_session.go),
  [capacity rechecks](../../telnet/session_ceiling_recheck_test.go),
  [departing owner](../../telnet/session_ceiling_departure_test.go),
  [capacity lifecycle](../../telnet/session_ceiling_lifecycle_test.go).
