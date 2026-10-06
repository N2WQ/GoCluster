---
name: "go-hotpath-design"
description: "Use when designing or reviewing Go runtime-performance patches on hot paths. Inspect the actual caller pattern and data shape first, choose the algorithm to match the runtime shape, define allocation targets up front, and require targeted benchmarks plus live-profile acceptance criteria."
---

# Go Hot-Path Design

Use this skill when the task is not merely to optimize code, but to design the right optimization for a Go hot path without cargo-cult abstraction.

## Required workflow

1. Inspect the runtime shape before proposing code.
   - Identify the real caller pattern:
     - single-item update
     - single-item overflow correction
     - batch repair
     - periodic maintenance
     - scan-heavy read
   - Confirm the actual hot path from code and profile evidence, not naming alone.

2. Choose the algorithm to match the shape.
   - If the path is dominated by single-item overflow or single-item correction, default to in-place single-victim logic first.
   - Prefer deterministic linear scans over more abstract helpers when caps are small and correctness is easier to preserve.
   - Reject generic reuse when it adds scratch allocation or multi-pass work to a hot path.

3. Define the performance contract before implementation.
   - State the target for:
     - allocations
     - CPU
     - mutex/lock behavior when relevant
   - Name which measurements will prove success.

4. Lock correctness and invariants.
   - Specify tie-break rules, recency semantics, ownership/lifetime rules, and any persistence or cleanup convergence requirements.
   - Separate normal hot-path behavior from repair/cleanup/load behavior when they need different algorithms.

5. Require validation evidence.
   - Add targeted benchmarks for the exact hot path.
   - Define the live-profile acceptance check needed after the patch.
   - Do not call the change complete from microbenchmarks alone.

## Output expectations

- Ground every recommendation in both inspected code and measured hotspot behavior.
- Call out rejected alternatives when they are plausible but wrong for the actual runtime shape.
- If a prior optimization created a larger hotspot, make that the first-class problem immediately.
