---
name: scope-ledger-adversarial-review
description: "Use for independent read-only adversarial review of a gocluster Proposed Scope Ledger before approval. Trigger for Non-trivial Scope Ledgers when the environment supports independent agents unless the user explicitly prohibits independent-agent use."
---

# Scope Ledger Adversarial Review

## Overview

Use this skill to challenge a `Proposed Scope Ledger vN` before the lead agent
presents the approval token. The review is independent evidence from a separate
agent context; it does not transfer gate ownership away from the lead agent.

## Workflow

1. Confirm the phase boundary.
   - This skill is pre-approval only.
   - Do not edit files, propose diffs, run formatters, create generated
     artifacts, or run full checker suites.
   - If independent agents are supported and not explicitly prohibited, use an
     independent explorer for this review.

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
   - State whether the review used an independent agent with a separate context
     window, was prohibited, was unsupported, failed, or timed out.
   - The lead agent must disposition findings and owns the official
     `SCOPE ADVERSARIAL REVIEW`.

## Output Expectations

- Include a compact `Scope adversarial review` result.
- Lead disposition must be explicit for every material finding.
- If the independent review is unavailable, failed, timed out, or explicitly
  prohibited, report that as an evidence gap or waiver instead of silently
  substituting ordinary self-review.
