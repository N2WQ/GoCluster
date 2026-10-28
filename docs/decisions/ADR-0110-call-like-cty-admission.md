# ADR-0110: Call-Like CTY Admission

- Status: Accepted
- Date: 2026-05-04
- Decision Origin: Incident

Local user-login SSID syntax is refined by [ADR-0322](ADR-0322-local-user-login-ssids.md).

## Context
ADR-0090 added CTY-backed local telnet login validation, but reused the shared
callsign syntax predicate and CTY portable lookup as an admission proof. That
allowed command-like tokens such as `SET/NOFT8` to pass when a slash segment
began with a known CTY prefix.

CTY longest-prefix lookup remains necessary for metadata and explicit operator
prefix queries. The admission bug is not that CTY can resolve prefixes; it is
that admission paths trusted prefix resolution without first proving that the
input contained a concrete station callsign shape.

## Decision
Shared callsign validation now requires at least one call-like identity segment.
For each slash or hyphen segment, a qualifying segment must have at most two
letters before the first digit, at least one digit, and at least one ASCII
letter after the first digit.

CTY lookup remains a metadata resolver. Admission paths must use the stricter
callsign predicate before CTY prefix validation can accept local login,
manually entered DX calls, ingested spots, peer frames, or replayed correction
winners.

## Alternatives considered
1. Change CTY longest-prefix lookup globally
   - Rejected because operator prefix queries and metadata enrichment still
     require prefix lookup semantics.
2. Fix only telnet login
   - Rejected because manual DX, ingest, peer, and replay paths used the same
     loose callsign predicate or mirrored CTY prefilters.
3. Add a YAML-configurable validation mode
   - Rejected because this is a correctness fix, not an operator preference.

## Consequences
### Benefits
- Command and mode tokens such as `SET/NOFT8`, `SET/FT8`, `NOFT8`, and `PSK31`
  are rejected before CTY prefix lookup.
- Runtime and replay CTY admission use the same malformed-call policy.
- Raw CTY prefix lookup remains available where prefix lookup is the intended
  operator behavior.

### Risks
- Some historical test fixtures or synthetic calls without a concrete
  call-like segment must be renamed to valid fake calls.
- A rare real-world special call that lacks a digit or has no letter after the
  first digit would be rejected by admission.

### Operational impact
- Invalid login attempts still show the configured invalid-login message.
- File-only login and dropped-call logs continue to record structured rejection
  events.
- No YAML settings or stored user-record migration are introduced.

## Links
- Related issues/PRs/commits: none
- Related tests: `spot/callsign_test.go`, `telnet/handshake_transcript_test.go`,
  `commands/processor_test.go`, `internal/cluster/ingest_validation_test.go`,
  `peer/parse_test.go`
- Related docs: `telnet/README.md`, `customgpt/troubleshooting-index.md`
- Related TSRs: TSR-0021
- Supersedes / superseded by: supersedes ADR-0090
