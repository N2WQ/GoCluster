---
name: scope-ledger-adversarial-review
description: "Use for read-only adversarial review of a Proposed Scope Ledger when scope is High-risk, uncertain, disputed, difficult to reverse, or leaves material residual uncertainty. Do not trigger for every Non-trivial ledger."
---

# Scope Ledger Adversarial Review

## Overview

Use this skill to challenge a `Proposed Scope Ledger vN` before the lead agent
presents the approval token. Use a separate non-steered context when credible
adversarial evidence depends on reasoning outside the lead's accumulated
assumptions. Otherwise the method may remain lead-owned. Separation never
transfers gate ownership.

## Workflow

1. Confirm the phase boundary.
   - This skill is pre-approval only.
   - If a separate reviewer is used, keep it read-only and findings-only.

2. Inspect the proposed scope and its evidence.
   - Read the proposed Scope Ledger and relevant approval, discovery, scope,
     validation, code, documentation, test, and decision evidence.
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
   - The lead agent must disposition every material finding.

## Output Expectations

- Report a compact result without a mandatory heading or envelope.
- Lead disposition must be explicit for every material finding.
- If triggered independence is unavailable or fails, report the material gap
  directly instead of calling lead review independent.
