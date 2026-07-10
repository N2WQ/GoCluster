---
name: scope-ledger-adversarial-review
description: "Use for independent read-only adversarial review of a gocluster Proposed Scope Ledger before approval. Trigger for Non-trivial Scope Ledgers when the environment supports independent agents, tool/user authorization permits spawning, and the user has not explicitly prohibited independent-agent use."
---

# Scope Ledger Adversarial Review

## Overview

Use this skill to challenge a `Proposed Scope Ledger vN` before the lead agent
presents the approval token. The review is independent evidence from a separate
agent context; it does not transfer gate ownership away from the lead agent.

## Workflow

1. Confirm the phase boundary.
   - This skill is pre-approval only.
   - Apply the independent-review contract in `AGENTS.md` `Subagent Use`.

2. Inspect the proposed scope and its evidence.
   - Read the proposed Scope Ledger, Current-State Discovery, relevant
     workflow markers, and cited code/docs/tests/ADRs when needed.
   - Treat generated maps, prior summaries, and other agents' outputs as
     evidence to verify, not proof by themselves.

3. Ask the required adversarial question:
   `What edge case would make this scope unsafe or incomplete?`

4. Check applicable edge areas.
   - lifecycle, shutdown, cancellation, goroutines, timers, sockets
   - backpressure, queues, drops, disconnect behavior, overload
   - bounded memory, retained state, caches, maps, indexes, cleanup
   - zero, nil, empty, malformed, and boundary inputs
   - YAML, config, schema, defaults, sentinel values
   - parser, protocol, compatibility, user-visible behavior
   - metrics, logs, latency, p99, performance claims
   - tests, race checks, fuzzing, benchmarks, profiling
   - docs, support-agent routing, ADR/TSR obligations
   - hidden shared interfaces, semantic callers, dependency impact

5. Classify every material issue.
   - Covered by the ledger.
   - Explicitly out of scope and safe to defer.
   - Requires revised Scope Ledger before approval.
   - Validation, docs, support, or ADR follow-up.

6. Report only findings and evidence.
   - Name inspected files, commands, and unknowns.
   - Report the canonical agent status and separate status detail, role
     outcome, and waiver disposition.
   - The lead agent must disposition findings and owns the official
     `SCOPE ADVERSARIAL REVIEW`.

## Output Expectations

- the canonical four-field independent-result envelope from `AGENTS.md`;
- Include a compact `Scope adversarial review` result.
- Lead disposition must be explicit for every material finding.
- If the independent review is unsupported, not authorized/not requested,
  failed, timed out, or explicitly prohibited, report that as an evidence
  status, gap, or waiver instead of silently substituting ordinary self-review.
