# ADR-0327: Per-IP Telnet Session Ceiling

- Status: Accepted
- Date: 2026-10-28
- Decision Origin: Design

## Context

The global session ceiling limits the whole cluster, and the per-base callsign
ceiling limits a station using different SSIDs. One observed network address
can still occupy many slots by using different callsigns. Profile restoration
can write login metadata, upgrade backups, and shared preset data, so a
capacity-rejected login must be refused before that work.

The observed address can be shared by legitimate users. An exact-IP quota is an
operator policy, not a measurement of people, authentication, or abuse.

## Decision

Add `telnet.max_sessions_per_ip` as a required, nonnegative runtime YAML setting.
The shipped value is 10; zero disables this ceiling. Ten is an operator choice,
not a calibrated threshold. Existing global and per-base ceilings and separate
pre-login gates remain in force.

Use the observed connection peer IP, without its port, as the quota identity.
Normalize equivalent IP text, and group IPv4-mapped IPv6 with its IPv4 address.
Retain IPv6 scope zones: a scoped address on different zones uses separate
allowances. Do not use forwarded addresses or subnet grouping. When this ceiling
is enabled, reject a missing or malformed observed IP before profile writes;
when disabled, retain existing handling for non-nil peer addresses. A nil peer
is handled defensively rather than panicking. The separate pre-login address
policy remains unchanged.

Decide and reserve IP, station, and global admission capacity atomically. Count
registered sessions and relevant admissions in progress. A matching full
normalized callsign and SSID reconnecting from the same IP reuses its existing
IP slot and can replace its current owner at capacity, subject to pre-login
gates. A matching reconnect from a different IP needs capacity at the
destination IP, even though it does not add a global or station session. If
registered sessions fill the destination IP ceiling, reject the reconnect
before profile writes and preserve the existing owner and saved data.

If pending admissions occupy otherwise available capacity, wait for their
outcome and re-evaluate the current owner and all applicable ceilings. Release
the configuration stripe while waiting. An owner can depart or another login
can take capacity during that wait. Existing login deadline, cancellation, and
shutdown behavior apply; no fairness or arrival ordering is promised. Blocked
storage or registry acquisition can outlast the intended deadline.

For a new identity, when multiple registered-session ceilings refuse the
login, report the global ceiling first, then station, then IP. Attempt an
IP-specific notice for an IP-limit refusal and close the connection; delivery
is best effort. Keep the pre-login ticket until the rejected connection closes.
The no-profile-write guarantee applies to capacity and invalid-peer-IP
rejections. Other login failures retain their existing persistence behavior.

Keep IP accounting coupled to registered owners and active reservations, and
remove entries when they have neither. Publish tracked-IP cardinality for
diagnostics without acquiring the registry lock on snapshot or rejection-log
reads. The accounting under the registry lock remains authoritative for
admission; diagnostics are observational and a multi-field snapshot is not a
single atomic transaction. Preserve per-SSID profile handoff and the retired
owner's save and unregister fences.

## Alternatives considered

1. Rely only on the existing global and station ceilings: one address can use
   many distinct callsigns and occupy a large share of global capacity.
2. Permit a cross-IP replacement at a full destination: a successful reconnect
   could exceed the configured IP ceiling.
3. Reject when an in-progress admission temporarily fills the destination: a
   connection can be refused even when the pending admission later fails.
4. Limit by subnet or a forwarded address: this changes the identity and trust
   policy beyond the observed peer-IP limit selected here.

## Consequences

### Benefits

- One observed IP cannot exceed its enabled registered-session limit through
  concurrent logins or cross-IP replacements.
- Capacity-rejected attempts do not change profiles, backups, or shared presets.

### Risks

- Users sharing a router, club, contest network, or proxy share one allowance;
  a limit of 10 can reject legitimate concurrent users.
- Exact-IP limits do not group a station's different IPv6 addresses or account
  for many clients behind a proxy separately.
- Callsign validation does not authenticate identity. An accepted matching
  reconnect retains the existing ability to displace a current session.
- Pre-login gates can block a reconnect, pending logins can delay it, and
  storage or locks can delay operations beyond the intended deadline.

### Operational impact

Add `telnet.max_sessions_per_ip` to the active private `runtime.yaml` and deploy
the updated executable together. Missing or invalid values prevent startup.
An older executable ignores the new key and does not enforce this ceiling.
Operators may change the nonnegative value or disable it with zero. The limit
covers registered sessions and admission reservations, not every open socket;
separate pre-login controls remain in force. No profile migration is required.

## Links

- Related docs: [telnet login](../../telnet/README.md#login-and-session-behavior),
  [domain contract](../domain-contract.md#telnet-session-capacity),
  [runtime config](../../data/config/README.md).
- Related decisions: [ADR-0325](ADR-0325-strict-telnet-session-ceiling.md),
  [ADR-0326](ADR-0326-per-base-callsign-session-ceiling.md),
  [ADR-0244](ADR-0244-exact-configuration-persistence.md).
- Related troubleshooting: [TSR-0112](../troubleshooting/TSR-0112-full-session-stripe-wait.md),
  [TSR-0113](../troubleshooting/TSR-0113-telnet-station-diagnostics-contention.md).
- Supersedes / superseded by: none.
