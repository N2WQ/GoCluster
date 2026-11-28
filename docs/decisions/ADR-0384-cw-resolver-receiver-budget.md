# ADR-0384: CW Resolver Receiver Budget

- Status: Accepted
- Date: 2026-11-27
- Decision Origin: Scope Ledger v2

## Context

A contest profile recorded receiver evictions at the live 64-receiver cap.
Once an eligible report has been lost, the resolver withholds current
confidence and its support share under ADR-0292. A uniform increase to the
map size hint wastes memory on quiet calls. A single total row limit at the old
1,024 maximum would let heavily reported calls consume the capacity currently
available to new callsigns.

## Decision

Each raw callsign in a live CW frequency bucket retains its base capacity of
64 receivers. The bucket shares 256 additional receiver memberships, with a
hard maximum of 256 per raw callsign and 16 raw callsigns per bucket. The
single owner accounts for a receiver separately for each raw callsign it
supports. Refreshing an existing receiver consumes no additional capacity.
With 6,000 active keys the configured maximum is 7,680,000 current memberships
when all keys are CW and saturated. This bound also limits the count of receiver
references per key and the raw rows of its current publication.

Pruning and whole-candidate eviction return excess memberships. If an incoming
receiver reaches either limit, replace the oldest receiver within the arriving
candidate using the existing deterministic tie break and record the actual
loss under ADR-0292. The existing whole-candidate and key limits still apply.

Allocate candidate and key receiver maps on their first observation. For
expanded CW keys, rebuild a receiver map when its population reached beyond the
base and fell to half its recorded peak. Refresh the peak when rebuilding.
The owner replaces backing maps during pruning or key-reference cleanup, and
only immutable row copies reach readers. With stable maps, the recorded peak for
each map is bounded by the greater of the base and twice its current row count.
The Go allocator adds capacity class rounding and transient old and new storage
during growth or rebuild. The membership cap does not set a process heap or RSS
ceiling. Validate retained memory after alternating dense and sparse keys.

Use the same capacity profile for live CW and ordinary replay. Frozen studies
keep their original limits and import bounds. Reuse resolver pressure and loss
metrics to verify benefit under comparable contest traffic.

## Consequences

CW signals can retain clean current support above 64 receivers. Closely spaced
signals can still exhaust their shared allowance, and real loss still withholds
confidence. Larger current populations cost more to publish and evaluate.
No persistent data migration is required.

## Links

- [ADR-0292](ADR-0292-resolver-cap-loss-authority.md)
- [Resolver evidence contracts](../../spot/README.md)
