# ADR-0367 — CW call retention on the experimental branch

- Status: Accepted for the experimental server branch
- Date: 2026-11-21
- Decision origin: Owner's selection of live RBN arrival timing for v17

[ADR-0368](ADR-0368-opt-in-cw-call-retention.md) defines the opt-in defaults on
`main`. This record describes the original experimental checkout.

## Context

Existing RBN replay can distinguish some ordinary wrong renames using separate frequency groups or corroborated bursts of different keying speeds. RBN wire reports expose minutes, while the burst experiment uses seconds. Interpreting every report in the same minute as simultaneous would manufacture support.

## Decision

Give each safeguard its own optional switch. Enable both in the supplied branch configuration and preserve the old behavior for an operator who omits both keys. The frequency safeguard uses separate receiver-supported groups. The burst safeguard uses local arrivals and requires an independent 60-second decay decision that would retain the original at the same release. A safeguard can keep an incoming spelling after an ordinary rename qualifies. Existing validation still governs delivery.

## Alternatives and consequences

Using arrival times makes the burst rule usable on the live wire and exposes it to network batching. Ignoring WPM, globally decaying ordinary evidence or applying the burst rule to every ordinary rename changed valid decisions in development replay. The independent comparison and burst-history requirements narrow the affected population and add runtime work. Reserve claims about live accuracy and capacity for measured deployments.

Review the [branch inventory and rollback procedure](../cw-call-correction-branch.md), [five-day audit](../../scripts/research/call-correction-v16/README.md) and [existing selective rescue decision](ADR-0361-selective-current-signal-call-rescue.md). The optional selective rescue retains its own switch.
