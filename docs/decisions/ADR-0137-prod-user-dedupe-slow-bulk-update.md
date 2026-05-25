# ADR-0137: Prod User Dedupe Slow Bulk Update

- Status: Accepted
- Date: 2026-05-25
- Decision Origin: Design

## Context
Prod-derived user records under `tmp/users/` needed their persisted
`dedupe_policy` values aligned to `SLOW`.

## Decision
No durable decision change.

This task only updates existing per-user persisted state in `tmp/users/*.yaml`
so every record carries `dedupe_policy: SLOW`. It does not change telnet
commands, runtime defaults, dedupe windows, fan-out behavior, schema handling,
or operator documentation.

## Alternatives considered
1. Change the runtime default to `SLOW`. Rejected because the request targeted
   prod user files, not new-user defaults.
2. Change only records that were already `MED`. Rejected after discovery found
   existing `FAST`, existing `SLOW`, and missing-key records in the target set.
3. Leave missing-key records untouched. Rejected because the objective was for
   all target user files to use `SLOW`.

## Consequences
### Benefits
- All target prod-derived user records now use the same persisted dedupe policy.
- Runtime code and global defaults remain unchanged.

### Risks
- Users previously persisted as `FAST` will receive the more conservative
  `SLOW` policy from these records.
- Records that omitted `dedupe_policy` now have an explicit persisted value.

### Operational impact
- Affected users load `SLOW` as their telnet broadcast dedupe policy.
- No change to slow-client handling, queue capacity, shutdown behavior,
  protocol parsing, archive schema, or dedupe implementation.

## Links
- Related issues/PRs/commits: current working tree
- Related tests: mechanical verification of `tmp/users/*.yaml`
- Related docs: `filter/user_record.go`, `telnet/README.md`
- Related TSRs:
- Supersedes / superseded by:
