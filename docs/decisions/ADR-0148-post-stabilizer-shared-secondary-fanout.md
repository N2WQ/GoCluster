# ADR-0148: Post-Stabilizer Shared Secondary Fanout

- Status: Accepted
- Date: 2026-06-06
- Decision Origin: Design

## Context
The output pipeline previously kept a separate archive/peer secondary dedupe
rail when the stabilizer was enabled. That side rail was introduced to preserve
archive/peer timing while making the stabilizer telnet-only, but it meant
archive and peer output could observe spots before stabilizer resolution while
telnet waited.

The desired architecture is now that archive, peer, ring-buffer history, and
telnet represent the same final stabilized cluster output. Secondary dedupe
should be the final output-thinning decision after correction, temporal/FT
holds, and stabilizer processing.

## Decision
Make stabilizer processing a pre-final-fanout gate:

- held stabilizer spots do not enter ring buffer, archive, peer, or telnet
  output while pending;
- delayed stabilizer release rechecks the required gates and then uses the same
  final fanout helper as immediate output;
- archive and peer consume the shared final MED secondary decision instead of a
  separate MED-oriented cache;
- telnet FAST/MED/SLOW policy behavior remains unchanged;
- stabilizer retry limits, timeout action, overflow fail-open behavior, and
  bounded pending queue semantics remain unchanged.

This supersedes only the archive/peer side-rail portions of ADR-0013, ADR-0020,
ADR-0029, and ADR-0138. The stabilizer itself and the telnet policy controls
remain accepted.

## Alternatives considered
1. Keep the separate archive/peer secondary rail.
   - Rejected because it preserves a pre-stabilizer output path that no longer
     matches the intended cluster-output history contract.
2. Feed delayed stabilizer releases back through the entire output pipeline.
   - Rejected because it would repeat resolver, mode, harmonic, FT confidence,
     path, and toxicity work that already happened before the spot was held.
3. Make archive/peer use per-user SLOW policy or `dedup.default_policy`.
   - Rejected because `dedup.default_policy` is a telnet new-user default, not
     an archive/peer output contract.

## Consequences
### Benefits
- Archive, peer, ring-buffer, and telnet output now share one final stabilized
  output boundary.
- One secondary dedupe cache, cleanup goroutine, and per-spot archive/peer
  `ShouldForward` call are removed.
- Stabilizer-suppressed spots no longer appear in archive or ring-buffer
  history.

### Risks
- Archive and peer output timing changes for stabilizer-held spots; they now
  wait until release or overflow fail-open.
- Peer publish and archive enqueue work can shift from ingest time to
  stabilizer release bursts.
- Operators comparing old and new archive history may see fewer risky spots
  when `stabilizer_timeout_action: suppress` is used.

### Operational impact
- No YAML key, archive schema, protocol wire-format, or telnet command syntax
  changes.
- Existing secondary FAST/MED/SLOW windows continue to define output thinning.
- Existing stabilizer bounds and counters remain the operational rollback and
  monitoring surface.

## Links
- Related issues/PRs/commits: current working tree
- Related tests: `internal/cluster/output_pipeline_delivery_test.go`,
  `internal/cluster/output_pipeline_ownership_test.go`
- Related docs: `docs/decision-log.md`,
  `docs/code-maps/runtime-ingest-fanout.md`
- Related TSRs:
- Supersedes / superseded by: partially supersedes archive/peer side-rail
  clauses in ADR-0013, ADR-0020, ADR-0029, and ADR-0138
