---
name: explain-code
description: Explain existing code grounded in inspected repository source in plain English, focused on runtime behavior and observable effects without implementation changes.
---

# skills/explain-code/skill.md — Explain Existing Code (Grounded, Plain English)

## PURPOSE
Provide code explanations that are strictly grounded in the repository source, written in plain English, focusing on runtime behavior and observable effects. This skill is for understanding code, not changing it.

## WHEN TO USE (TRIGGERS)
Use this skill when the user asks any of the following and does not explicitly request implementation changes:
- “Explain what this code does”
- “How does this method work?”
- “Walk me through this file/package”
- “Help me understand the flow”
- “Review this code to understand it”

If the user asks to modify, optimize, refactor, or implement features, do not use this skill; use the full delivery workflow in AGENTS.md.

## NON-NEGOTIABLE RULES (GROUNDING)
1) Read the code first.
- Open and read the exact file(s) referenced by the user.
- If the user referenced only a symbol (function/method/type), locate it and open the defining file.
- Follow the call chain at least one level: the target function/method and its primary callees that determine behavior (not every helper).
- If runtime behavior depends on interfaces, configuration, or injected dependencies, open the defining interface/type and the primary implementation used in the relevant wiring path.

2) No speculation.
- If something cannot be concluded from inspected code, state: `Unknown from inspected code`.
- Then name exactly what to inspect next (specific files/functions) to resolve it.

3) Evidence is required.
- Reference concrete identifiers (functions, methods, structs, fields, constants).
- Cite code locations using file paths and, when available, line ranges.

4) If code is not accessible, stop and request it.
- If the assistant cannot access the repository view or the user has not provided the code, do not explain from memory or typical patterns.
- Ask for the specific file(s) or snippet(s) needed.

## OUTPUT FORMAT (DEFAULT)
First response should be concise unless the user asks for a deep dive.

Start with a one-line evidence note:
- `Read:` <file:lines or file + identifiers actually inspected>

Then produce sections in this order:

1) Purpose (1–2 sentences)
- What this code is for in the system.

2) Step-by-step runtime behavior
- Explain in execution order.
- Name key branches/conditions and what triggers them.
- For loops, call out what advances the loop and what terminates it.
- For error paths, state the observable behavior (return value, log, disconnect, retry, drop).

3) Side effects and interactions
- Network I/O: reads/writes, deadlines/timeouts, disconnect triggers.
- Concurrency: goroutines started, ownership, channels/queues, locks.
- State: shared state mutated, caches, globals, metrics/logs emitted, timers/tickers.

4) Inputs, assumptions, invariants
- Expected input shape and validation.
- Invariants the code relies on (explicit or implicit).
- Config flags/limits that change behavior.

5) Unknowns (only if needed)
- Bulleted list of `Unknown from inspected code:` items with exact next-read pointers.

6) Clarifying questions (only if needed)
- Up to 3 short questions as plain sentences (not bullets).
- Only ask questions required to determine runtime behavior or intended semantics.

## BREVITY RULES
- Default target: <= 250 words for the first explanation.
- Expand only if the user asks for a deeper dive (“deep dive”, “full trace”, “edge cases”, “all branches”, “full analysis”).
- If more detail is required to be correct, say so and propose a split: “I can explain X now, then Y next.”

## DEPTH LEVELS
### Level 1 (Default)
- Concise purpose + key flow + major side effects + key assumptions.
- One level of call chain.

### Level 2 (On Request: “deep dive” / “full trace”)
- Enumerate main branches and error paths.
- Two levels of call chain where needed for correctness.
- Include edge-case behavior that is visible to clients/operators (drops, disconnect reasons, retries, ordering).

### Level 3 (On Request: “audit” / “correctness” / “concurrency”)
- Concurrency and lifecycle: goroutine ownership, cancellation propagation, shutdown sequencing.
- Backpressure/queue semantics where applicable.
- Determinism and ordering guarantees.
- List explicit invariants and how they are maintained.

## WHAT NOT TO DO
- Do not propose refactors, optimizations, or “where to edit” unless the user explicitly asks for changes.
- Do not generalize based on typical designs (“probably”, “usually”, “likely”) without code evidence.
- Do not claim tests were run or behavior was validated unless it was actually performed and observed.
- Do not summarize a package by skimming filenames only; read the code.

## EXAMPLES (STYLE, NOT CONTENT)

### Example: function explanation (Level 1)
Read: `pkg/conn/writer.go: WriterLoop(), enqueueControl(), enqueueSpot()`

Purpose: This implements the per-connection write loop and bounded enqueue logic for control vs spot messages.

Step-by-step runtime behavior: WriterLoop() blocks on the control queue first, then drains spots when control is empty. enqueueControl() pushes into the control queue and returns an error if full; enqueueSpot() drops the incoming spot when the spot queue is full.

Side effects and interactions: WriterLoop() performs net.Conn writes and applies SetWriteDeadline() before each flush. It logs disconnect reasons on write timeout and closes the connection.

Inputs, assumptions, invariants: Assumes a single writer goroutine owns the net.Conn. Queue capacities are fixed at construction and never grow.

### Example: unknown handling
Unknown from inspected code: whether the connection uses an idle timeout on reads. Next inspect: `pkg/conn/reader.go: readLoop()` and connection setup in `pkg/server/accept.go: handleConn()`.

## COMPLETION CHECK (INTERNAL)
Before responding, confirm:
- I actually read the code I’m describing.
- I cited identifiers and file locations.
- I did not guess; unknowns are labeled with next-read pointers.
- I did not propose changes unless asked.
- I stayed within the default brevity target unless the user requested depth.