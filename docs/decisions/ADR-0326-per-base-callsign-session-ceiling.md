# ADR-0326: Per-Base Callsign Session Ceiling

- Status: Accepted
- Date: 2026-10-28
- Decision Origin: Design

## Context

The strict global session ceiling prevents the registered Telnet population
from exceeding its configured maximum, but one station can still occupy many
slots by logging in with different SSIDs. Profile restoration can write login
metadata, upgrade backups, and shared preset data, so capacity rejection must
precede that work.

## Decision

Add `telnet.max_sessions_per_base_call` as a required, nonnegative runtime YAML
setting. The shipped value is 10; zero disables this ceiling. Ten is an operator
policy choice, not a calibrated threshold. The existing global ceiling remains
in force independently, as do the existing pre-login gates.

Group sessions using `spot.NormalizeOwnCallsign` on the normalized login: remove
the numeric SSID while preserving portable location forms. For example,
`K1ABC-1` and `K1ABC-2` share the `K1ABC` quota; `VE3/K1ABC-1` uses the
separate `VE3/K1ABC` quota. Keep the full normalized callsign and SSID as the
session replacement and profile identity. A matching registered session may
be replaced at either ceiling without consuming an additional slot.

Make the global and station admission decisions and reservations atomic. Count
in-progress new identities as well as registered sessions, even when the global
limit is disabled. If pending admissions occupy otherwise available capacity,
wait for their outcome and recheck under the existing login deadline,
cancellation, and shutdown behavior; do not promise fairness. Release the
configuration stripe while waiting for capacity. A new identity rejected at a
ceiling must not change its profile, upgrade backups, or shared preset library.
Other login failures keep their existing persistence behavior.

At a station ceiling, attempt a station-specific notice and close the
connection. If both ceilings reject the attempt, use the global full-server
notice. Delivery is best effort. The pre-login ticket remains held until the
rejected connection closes. Existing pre-login limits may prevent a reconnect
before its identity is evaluated.

Keep station accounting tied to registered sessions and active reservations;
remove entries when no longer owned so historical callsigns do not accumulate.
Expose its current size in the admission metrics snapshot and sampled station
rejection logs. Aggregate rejection logs retain the station-specific reason.
Publish the tracked-station count as an observational atomic gauge while the
registry lock protects map updates. Reading that count in the snapshot or
sampled rejection log must not acquire the registry lock. The registry map,
not the gauge, remains authoritative for admission; the overall metrics
snapshot is not a single atomic transaction.
Preserve per-SSID configuration handoff and the retired-owner save fence.

## Alternatives considered

1. Rely on the global ceiling alone: a single station can occupy many global
   slots with different SSIDs.
2. Reject same-identity reconnects at capacity: a stale session could prevent
   its own replacement and existing duplicate-login behavior would change.
3. Reject when a pending login temporarily occupies a station slot: a user can
   be refused even when that pending login never registers.
4. Add a per-IP ceiling in the same change: address-based policy has separate
   tradeoffs and remains a later decision.

## Consequences

### Benefits

- A station cannot occupy more registered slots than its enabled quota under
  concurrent logins, while matching reconnects retain their handoff behavior.
- Capacity-rejected logins do not change saved profile or shared preset data.

### Risks

- Callsign validation does not authenticate identity. A matching login can
  displace a current session under the existing duplicate-login policy.
- Pre-login limits can still reject a reconnect, and storage or lock delays
  can outlast the intended login deadline. Waiters have no guaranteed order.
- A quota of 10 can reject legitimate multi-session use; operators can select
  another nonnegative value or disable this ceiling independently.

### Operational impact

Add the required key to the active private `runtime.yaml` and deploy the updated
executable. Missing or invalid values prevent startup. Older executables ignore
the new key and do not enforce this ceiling. The setting limits registered
sessions, not all open sockets or disk usage; pre-login and storage limits stay
separate. No saved-profile migration is required.

## Links

- Related tests: [global session ceiling](../../telnet/session_ceiling_test.go),
  [station session ceiling](../../telnet/base_session_quota_test.go),
  [capacity lifecycle](../../telnet/session_ceiling_lifecycle_test.go),
  [configuration handoff](../../telnet/configuration_session_test.go).
- Related docs: [telnet login](../../telnet/README.md#login-and-session-behavior),
  [domain contract](../domain-contract.md#telnet-session-capacity),
  [runtime config](../../data/config/README.md).
- Related decisions: [ADR-0325](ADR-0325-strict-telnet-session-ceiling.md),
  [ADR-0244](ADR-0244-exact-configuration-persistence.md),
  [ADR-0322](ADR-0322-local-user-login-ssids.md).
- Related TSRs: [TSR-0112](../troubleshooting/TSR-0112-full-session-stripe-wait.md),
  [TSR-0113](../troubleshooting/TSR-0113-telnet-station-diagnostics-contention.md).
- Supersedes / superseded by: none.
