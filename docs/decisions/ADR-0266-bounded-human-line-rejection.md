# ADR-0266: Bounded Human Line Rejection

- Status: Accepted
- Date: 2026-10-09
- Decision Origin: Troubleshooting

## Context

The command reader returned immediately on a forbidden byte or length violation.
Its caller reported the error and retried, leaving the rejected physical line's
tail available for dispatch. `DX 14074 K1ABC CW !REJECT BAND ALL` could therefore
reject the DX input and subsequently change the band's filter state.

The optional ziutek codec strips negotiation bytes, owns a private raw buffer,
and can block writing a negotiation reply before returning decoded data. A
decoded-character counter and socket read deadline alone cannot bound recovery.

## Decision

Reject a whole invalid or overlong human physical line with zero command effects.
Retain the session only after recovering a safe data line boundary. Discard
without editing or echo, spending at most 64 KiB of wire data and five seconds;
any earlier installed read deadline takes priority. Disconnect on unsafe,
incomplete, over-budget or timed-out recovery.

Install raw accounting before either transport codec. Reserve buffered raw data
and conservatively account for hidden/decoded codec buffers; the at-most bound
does not promise recovery at every exact-limit boundary. Expose one decoded byte
at a time during human/header reception, so a later codec reply cannot hide an
already offending byte. Machine body reception retains bulk reads.

The reader owns progress, raw accounting and the installed read-deadline value.
A recovery-owned timer interrupts the raw connection, including blocked codec
writes. Boundary acceptance and expiry are serialized; reception cleanup joins
any started expiry callback before returning. Both caller loops handle terminal
human errors before retryable timeout or validation errors.

Physical progress survives editing. Partial EOF/timeout/transport errors are
terminal. Empty logged-in idle timeouts remain retryable, including when a
previous CR's LF/NUL arrives separately. Machine upload framing remains governed
by ADR-0245 and its later refinements.

## Alternatives Considered

- Disconnect on every rejected line: safe, but loses recoverable sessions.
- Drain to an unbounded newline or use the normal 24-hour idle timeout: recovery
  retains resources for too long and can process unbounded wire negotiation.
- Bound decoded bytes and read deadlines alone: codec-hidden work and blocked
  reply writes bypass those limits.

## Consequences

Malformed-line recovery has explicit resource and lifecycle limits. Oversized
negotiation streams, uncertain boundaries and partial reads can disconnect users.
Conservative buffer accounting can disconnect near the wire limit earlier than
an exact byte-position decoder would. No dependency fork or new public protocol
option is introduced.

## Links

- [Approved Scope Ledger v3](../command-fix-scope-ledger-v3.md), checkpoint 01
- [Execution evidence](../command-fix-execution.md#checkpoint-01-physical-line-rejection)
- [Human input contract](../../telnet/README.md#rejected-input-and-recovery)
- [Machine configuration ADR](ADR-0245-machine-yaml-configuration.md)
