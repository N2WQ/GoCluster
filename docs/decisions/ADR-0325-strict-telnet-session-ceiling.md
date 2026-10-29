# ADR-0325: Strict Telnet Session Ceiling

- Status: Accepted
- Date: 2026-10-28
- Decision Origin: Design

## Context

The accept-time count of registered clients could admit several connections
before any finished logging in. Their later registrations could exceed the
configured ceiling. Rejecting at accept time also blocked reconnects while the
server was full, before the server could identify a matching existing session.
Profile restoration can persist login metadata, upgrade a profile, or clean a
shared preset library, so capacity rejection must precede those effects.

## Decision

Enforce `telnet.max_connections` as a strict limit on registered sessions.
Admission and registration must preserve that limit under concurrent logins.
Permit replacement of an existing session with the same full normalized
callsign and SSID at capacity. A different SSID, including a distinct leading
zero spelling, is a different session and needs capacity of its own. Preserve
the existing duplicate notice and configuration handoff, including the fence
against a retired session overwriting its successor.

Order shutdown signaling with the final ownership change. A shutdown signal
that wins this ordering prevents registration and replacement. If a replacement
is already published, the session writer also observes shutdown so a displaced
idle connection need not wait for replacement notifications to return. A write
already in progress remains subject to its existing deadline and synchronous
work can delay cleanup. Duplicate notice delivery is not guaranteed on shutdown.

Continue to apply the separate pre-login gates to all attempts, including
replacements. A new identity rejected for capacity must not write its profile,
upgrade backups or shared preset library. The server attempts a full-server
notice and closes the connection; notice delivery is not guaranteed. Other
login failures retain their prior persistence behavior.

When an in-progress login occupies otherwise available capacity, another new
identity waits for the outcome and rechecks admission. Its existing login
deadline, cancellation and shutdown bound the wait. No fairness or ordering
between waiters is guaranteed.

While a login waits for its configuration stripe, it also retries a
nonblocking full-capacity check approximately every 50 milliseconds. A busy
registry does not block the retry. If registered sessions fill the ceiling and
the full normalized identity is absent, the login can be rejected without
waiting for an unrelated stripe owner to finish. A still-registered matching
identity retains the existing replacement path. The retry is subject to the
original login deadline, cancellation and shutdown; scheduling and a busy
registry can delay a check. It does not establish a hard deadline for blocked
registry-lock acquisition or profile storage operations elsewhere in login.

Runtime YAML requires a positive `telnet.max_connections`; a direct server
caller can disable the registered-session ceiling with `MaxConnections` zero
or less. This does not disable separate pre-login limits.

## Alternatives considered

1. Keep the accept-time limit: it can exceed the ceiling and rejects a
   same-identity reconnect at capacity.
2. Reject every login at capacity: it maintains the limit but leaves a user
   unable to replace a stale registered session.
3. Reject whenever capacity is temporarily occupied by an in-progress login:
   this can refuse another user even if that login does not finish.

## Consequences

### Benefits

- Concurrent registrations respect the configured ceiling.
- A matching reconnect can replace a stale session without increasing the
  registered count, while rejected new identities do not change saved data.

### Risks

- Callsign validation does not establish identity ownership. A duplicate
  login can displace the current session at capacity, consistent with the
  existing duplicate-login policy.
- In-progress logins can delay other attempts until they complete or the
  waiting attempt's deadline expires. Pre-login limits can reject a reconnect
  before its callsign is evaluated.
- Stripe waiters use bounded periodic capacity checks while the ceiling is
  enabled. Scheduling and registry contention mean 50 milliseconds is a retry
  interval, not a guaranteed response time. Pre-login admission bounds the
  number of waiting client attempts.

### Operational impact

Deploy the updated executable to enforce the ceiling; no YAML value change is
required. The limit counts registered sessions, not all open sockets. Use the
separate pre-login settings and diagnostics to investigate earlier refusals.

## Links

- Related tests: [session ceiling](../../telnet/session_ceiling_test.go),
  [capacity lifecycle](../../telnet/session_ceiling_lifecycle_test.go),
  [configuration handoff](../../telnet/configuration_session_test.go).
- Related docs: [telnet login](../../telnet/README.md#login-and-session-behavior),
  [domain contract](../domain-contract.md#telnet-session-capacity).
- Related decisions: [ADR-0041](ADR-0041-telnet-tier-a-prelogin-admission-gate.md),
  [ADR-0244](ADR-0244-exact-configuration-persistence.md),
  [ADR-0322](ADR-0322-local-user-login-ssids.md).
- Related TSRs: [TSR-0112](../troubleshooting/TSR-0112-full-session-stripe-wait.md),
  [TSR-0114](../troubleshooting/TSR-0114-telnet-replacement-during-shutdown.md).
- Supersedes / superseded by: none.
