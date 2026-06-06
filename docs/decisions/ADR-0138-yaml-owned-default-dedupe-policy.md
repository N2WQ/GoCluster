# ADR-0138: YAML-Owned Default Dedupe Policy

- Status: Accepted
- Date: 2026-05-25
- Decision Origin: Design

## Context
New telnet user records previously fell back to a hard-coded `MED` policy.
Operators need that default to be an explicit YAML-owned setting, with the
shipped config defaulting new users to the more conservative `SLOW` policy.

## Decision
Add `dedup.default_policy` to the merged runtime config loaded from
`data/config/dedupe.yaml`.

The setting is required, accepts only `FAST`, `MED`, or `SLOW`, and is threaded
into telnet login so new records use the configured default. Existing saved
valid per-callsign policies remain authoritative. If the configured default
names a secondary policy that is disabled by its window, the telnet server uses
the existing enabled-policy fallback path and reports the effective choice
through the normal dedupe status surfaces.

Archive and peer secondary dedupe continue to use their existing MED-oriented
runtime path. This decision changes the telnet/user default only.

## Alternatives considered
1. Keep the hard-coded `MED` default. Rejected because default user policy is an
   operator-owned behavior and should be visible in YAML.
2. Move the default under `runtime.yaml`. Rejected because secondary dedupe
   policy windows already live under `dedup` in `dedupe.yaml`.
3. Reject startup when `default_policy` names a disabled secondary policy.
   Rejected because the telnet server already has an operator-visible fallback
   path for disabled policy selections.

## Consequences
### Benefits
- Operators can change the default user dedupe policy without a code change.
- The shipped public config defaults new users to `SLOW`.
- Existing saved user preferences remain stable.

### Risks
- New users receive fewer repeated spots by default than they did under `MED`.
- If an operator disables the configured default policy window, the effective
  user default depends on the existing enabled-policy fallback order.

### Operational impact
- User-visible default behavior changes for new users.
- No change to primary dedupe, secondary window semantics, archive/peer MED
  policy behavior, slow-client handling, queues, shutdown, or `SET DEDUPE`
  syntax.

## Links
- Related issues/PRs/commits: current working tree
- Related tests: `config/dedup_config_test.go`, `filter/user_record_test.go`, `telnet/server_options_test.go`
- Related docs: `README.md`, `telnet/README.md`, `data/config/README.md`, `data/config/dedupe.yaml`
- Related TSRs:
- Supersedes / superseded by: ADR-0148 supersedes the archive/peer
  MED-oriented side rail; telnet default-policy behavior remains accepted.
