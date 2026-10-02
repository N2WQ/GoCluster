# ADR-0233: PC92 Controlled Retries and Configured Peer Cap

- Status: Accepted
- Date: 2026-10-02
- Decision Origin: Troubleshooting chat

## Context

V12's exact refused-record feasibility checks exhausted service capacity with
63 blocked identities and one healthy ingress. The zero-blocker control passed.
The owner selected controlled retries and approved v14, including an explicit
YAML cap within the existing resource envelope. Approval is not qualification.

## Decision

Use a fixed manager-owned identity coordinator, protected by Manager.mu, while
the existing controller retains graph/freshness authority and the sole writer
retains socket ownership. Remove retained refused wires and repeated graph
simulation from recovery. A refusal closes the owner and fences replacement
startup until controller acknowledgement of ingress invalidation.

Overload retries share configured exponential backoff across both directions,
one candidate per identity, and at least one second between global startup
grants. Stable circular ordering bounds overtaking by N−1 for continuously
eligible ready identities. Authentication precedes inbound ownership; outbound
dial failure consumes no startup grant. Original handshake deadlines remain
absolute, including a64-identity wave that cannot all start within60 seconds.

Only successful current PC9x establishment/replay and the matching recovery C/A
local Flush can start the60-second healthy interval that resets retry history.
Three scalar FIFO counters capture completion without widening control channels.
The healthy interval uses the later event's occurrence time, not callback lock
arrival. Pure global gates interrupt that interval without advancing cooldown;
genuine recorded failures still count once. Recovery of local publication does
not make remote membership complete.

Require integer `peering.max_peers` in1–64, shipped64, with no fallback or
unlimited sentinel. Raw YAML validation rejects floats before integer coercion.
Presence/type/range apply even while disabled; active enabled identity count
must fit N. Dormant over-cap rows remain loadable while globally disabled.
Direct construction validates active semantics before resources. Bound direct
owners/replays/retry identities by N, pending candidates by128, and transport
owners by N+128. Reserve actual configured identities in complete publication.

Preserve all existing wire, authentication, cache, memory and deadline contracts,
including one-second membership queue admission during recovery and five-second
complete C/A. Incremental fixed bookkeeping must fit32KiB after allocator-class
rounding across every retained owner. No new goroutine, persistent retry history,
wire dialect, allocation partition or limit is introduced.

## Alternatives considered

1. Keep exact witness checks: falsified by the retained sustained experiment.
2. Add graph certificates or another actor: expands retained state and authority
   complexity without current evidence requiring it.
3. Unlimited or cap-above64 configuration: outside the approved resource envelope.
4. Reset on handshake/enqueue: does not prove matching recovery output flushed.

## Consequences

### Benefits

Retry service examines bounded scalars instead of replaying graph transactions.
Smaller valid fresh work can be admitted even when an older large refusal would
still fail. Operators explicitly configure their supported direct peer count.

### Risks

Retries can fail repeatedly while pressure persists; backoff reaches the
configured cap. Fair pacing may consume a candidate's original deadline. Actual
service, lifecycle, allocation and qualification evidence remains mandatory.
The enabled-SQLite, context backing and outer-retirement aggregate proofs remain
open; this decision cannot establish the480MiB claim by itself.

### Operational impact

Add `max_peers` to existing peering YAML before starting this version. Restart
is required for changes. Distinguish cooldown, waiting for startup, active
attempt, healthy-reset interval, global gates and incomplete remote topology.
No local-user preference command, database migration or deployment is included.

## Links

- Related issues/PRs/commits: Approved v14, branch p92, baseline2c06079.
- Related tests: v14 config/retry/writer/capacity and qualification families.
- Related docs: [ledger](../pc18-pc92-scope-ledger-v14.md),
  [validation matrix and evidence](../pc92-v14-validation.md).
- Related TSRs: [TSR-0035](../troubleshooting/TSR-0035-pc92-qualification-accounting.md).
- Supersedes / superseded by: only admission-recovery clauses of
  ADR-0230/0231/0232; other clauses and historical failed evidence remain valid.
