# TSR-0021: Command-Like Login CTY Admission

- Status: Resolved
- Date opened: 2026-05-04
- Status date: 2026-05-04

## RCA Summary

- What happened: A telnet connection could log in as command-like text such as
  `SET/NOFT8`.
- Why: Login admission combined a loose slash-token callsign syntax check with
  CTY longest-prefix lookup; prefixes like `SE` and `N` could make command-like
  segments look country-resolvable even though no segment was a real station
  callsign.
- What fixed it: ADR-0110 strengthened shared callsign validation so CTY-backed
  admission rejects command-like tokens before portable prefix lookup can
  validate them.
- How we know: Source tracing isolated login input before command dispatch and
  the CTY prefix behavior; callsign, telnet handshake, command, and ingest tests
  covered the shared validation contract.
- Operator/support answer: If a command-like callsign appears in login state,
  check the login prompt transcript first; post-login command handling is not
  the primary explanation.

## Trigger
A telnet connection appeared as logged in with the callsign `SET/NOFT8`.

## Symptoms and impact
Command-like text entered at the login prompt could create a telnet session and
persist user state when it also matched the loose callsign syntax predicate.
This was a local admission bug; post-login `SET/NOFT8` command handling was not
the cause.

## Hypotheses tested
1. The client was already logged in and ran `SET/NOFT8`.
2. The login loop treated the first line as a command.
3. CTY validation accepted the token through portable prefix lookup.

## Evidence
- `telnet.Server.handleClient` reads login input before command dispatch.
- `spot.IsValidNormalizedCallsign` allowed slash-separated alphanumeric tokens
  with any digit in the whole string.
- `cty.LookupCallsignPortable` split `SET/NOFT8` into segments and accepted a
  segment when CTY longest-prefix lookup matched a country prefix.
- `SET` resolved through the `SE` prefix and `NOFT8` through the `N` prefix;
  neither segment was a valid station callsign.

## Root cause or best current explanation
CTY longest-prefix lookup is correct for metadata, but it was being used as an
admission proof after a syntax predicate that did not require a concrete
call-like identity segment.

## Fix or mitigation
Strengthen shared callsign validation so CTY-backed admission paths reject
mode/command-like tokens before prefix lookup can validate them.

## Why an ADR was or was not required
- ADR required because the fix changes a shared validation contract used by
  telnet login, manual DX posting, ingest validation, peer parsing, and replay
  parity.

## Links
- Related ADRs: ADR-0110
- Related issues/PRs/commits: none
- Related tests: `spot/callsign_test.go`, `telnet/handshake_transcript_test.go`,
  `commands/processor_test.go`, `internal/cluster/ingest_validation_test.go`
- Related docs: `telnet/README.md`
