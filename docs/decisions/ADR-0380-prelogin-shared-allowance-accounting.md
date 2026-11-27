# ADR-0380 — Shared pre-login allowance accounting

- Status: Accepted
- Date: 2026-11-27
- Decision Origin: Security review follow-up, approved scope v2

## Context

Telnet and browser admission use one shared rate allowance. The original
order charged it before IP and subnet checks, so those refusals could spend
capacity otherwise available to other connections.

## Decision

Check the hard concurrent pre-login cap first. An exhausted shared allowance
then returns `global_rate` without charging a source, creating a ticket, or
changing the admission trackers. Due maintenance can run on a later attempt
with shared capacity or on ticket release.

When shared capacity is available, reject known IP concurrency, IP rate and
subnet rate failures before taking a shared token. A subnet refusal may still
charge an existing IP tracker. A previously untracked IP refused by its subnet
creates no IP tracker. Allocate new trackers and perform capacity evictions
only after taking the shared token. Keep ASN and country checks after that charge.

## Alternatives considered

- Moving the charge below all existing source checks would allow source refusals
  to drive new tracker allocation and capacity scans ahead of the shared bound.
- A second shared intake limit would add another place where both transports
  could be refused at once.

## Consequences

IP and subnet refusals preserve shared capacity. Concurrent and rate exhaustion
can still block new logins. Operators can use existing admission reject reasons
to distinguish shared rate pressure from source limits. A compatible previous
cluster binary can restore the former accounting with the same configuration.

## Links

- Refines the order in [ADR-0048](ADR-0048-multi-dimensional-prelogin-admission-and-logging-guardrails.md).
- [Admission tests](../../telnet/prelogin_gate_test.go) and [shared transport test](../../telnet/web_listener_test.go).
- [Operator settings](../ENVIRONMENT.md#telnet-session-timeouts) and [runtime contract](../domain-contract.md#telnet-session-capacity).
