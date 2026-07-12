---
name: explain-code
description: Explain, trace, or deeply review existing code using inspected repository evidence without implementing changes. Use for code-understanding requests unless the user explicitly asks only for a concise, brief, quick, high-level, or initial overview. This skill explicitly owns thorough reviews, comprehensive explanations, audits, diagnosis, correctness analysis, architecture assessment, risk analysis, execution tracing, concurrency analysis, lifecycle analysis, and full subsystem understanding.
---

# Explain Existing Code

## Purpose

Explain, trace, or deeply review existing code using inspected repository
evidence, focusing on runtime behavior, ownership, state, side effects,
invariants, failure paths, and observable effects.

This skill is for understanding and evaluating current code. It does not
authorize implementation changes.

## Routing Boundary

Use this skill when the user asks to understand, explain, trace, review, audit,
or diagnose existing code and does not request implementation.

This skill explicitly applies to requests such as:

* “Explain how this method works.”
* “Walk me through this file or package.”
* “Help me understand the flow.”
* “Review this code to understand it.”
* “Thoroughly review this subsystem.”
* “Give me a comprehensive explanation.”
* “Audit this code for correctness.”
* “Assess the architecture and risks.”
* “Trace all important execution paths.”
* “Diagnose why this behavior occurs.”
* “Review the concurrency and shutdown behavior.”
* “Identify weaknesses or unknowns in the current design.”

Do not use `initial-review` when the user requests:

* a thorough, comprehensive, detailed, or deep review;
* an audit or diagnosis;
* correctness, architecture, security, performance, concurrency, lifecycle, or
  risk analysis;
* a full trace, all branches, edge cases, failure paths, or subsystem
  understanding.

Use `initial-review` only when the user explicitly requests a concise, brief,
quick, high-level, or initial orientation.

If the user requests modification, optimization, refactoring, or
implementation, enter the applicable change workflow in `AGENTS.md` instead.

## Evidence and Grounding

### Read the code first

* Open and read the exact files referenced by the user.
* If the user references a symbol, locate and inspect its definition.
* Follow the execution and ownership path as far as necessary to answer the
  requested depth correctly.
* Inspect primary callers, callees, interfaces, configuration, injected
  dependencies, state owners, tests, and wiring when they materially determine
  behavior.
* Do not stop at one call-chain level when deeper inspection is necessary for
  correctness or when the user requested a thorough review.

### Separate evidence from uncertainty

* Present behavior directly established by inspected code as observed current
  behavior.
* If something cannot be established from inspected evidence, label it
  `Unknown from inspected code`.
* Name the specific files, functions, tests, configuration, runtime evidence,
  or external authority needed to resolve the unknown.
* Do not convert assumptions or typical implementation patterns into facts.

### Cite evidence

* Reference concrete functions, methods, types, fields, constants, tests, and
  configuration.
* Cite file paths and line ranges when available.
* Use the nearest identifiable function, type, or block when exact line ranges
  are unavailable.

### Stop when evidence is unavailable

If the repository or required code is inaccessible, request the specific file,
symbol, or snippet needed. Do not explain from memory or generic patterns.

## Depth Selection

Infer the required depth from the user’s request. Do not require the user to use
a specific phrase or request a second pass when the original request already
requires deeper analysis.

### Concise explanation

Use when the user asks a straightforward explanation without requesting a
brief initial overview or a deep review.

Cover:

* purpose;
* primary execution flow;
* major side effects;
* principal inputs and invariants;
* material unknowns.

### Detailed explanation

Use when the request asks for detail, execution tracing, branches, error paths,
edge cases, or full flow.

Cover:

* entry points and material callers;
* execution order;
* important branches and termination conditions;
* validation and normalization;
* error, retry, timeout, drop, disconnect, and recovery behavior;
* state transitions and observable effects;
* material tests and configuration;
* unresolved evidence.

### Thorough review or audit

Use automatically when the user asks for a thorough, comprehensive, deep,
architectural, diagnostic, correctness, risk, concurrency, lifecycle,
performance, or security review.

Inspect and report all applicable areas:

* entry points and ownership boundaries;
* complete material execution paths;
* state mutation and persistence;
* goroutine, channel, queue, lock, cancellation, and shutdown ownership;
* bounded-resource and backpressure behavior;
* validation, defaults, sentinels, malformed inputs, and boundary conditions;
* normal, overload, failure, recovery, and shutdown paths;
* protocol, compatibility, operator, configuration, and persistence contracts;
* assumptions and invariants;
* relevant tests and gaps in test coverage;
* concrete risks, contradictory evidence, and unresolved unknowns;
* confidence appropriate to the inspected evidence.

Do not impose a five-bullet structure, 250-word ceiling, three-risk limit, or
mandatory clarifying question on a thorough review.

## Recommended Output Structure

Adapt the structure to the request rather than forcing irrelevant sections.

For a normal explanation, prefer:

1. Evidence inspected.
2. Purpose.
3. Runtime behavior.
4. State and side effects.
5. Inputs and invariants.
6. Failure paths and edge cases, when material.
7. Unknowns and next evidence, when material.

For a thorough review, organize findings by the system’s natural execution,
ownership, risk, or architectural structure. Include enough evidence and
explanation to support material conclusions.

Do not use a fixed heading count, bullet count, word ceiling, risk limit, or
mandatory final question.

## Inference Rules

* Do not generalize from typical designs as though the behavior were observed.
* Clearly distinguish direct evidence from inference.
* A conclusion supported by multiple inspected facts may be labeled as an
  inference, with the supporting evidence identified.
* Competing explanations may be presented when current evidence does not
  distinguish them.
* State what additional evidence would confirm or disprove a material
  inference.
* Do not use unsupported terms such as “probably,” “usually,” or “likely”
  without identifying the evidence and uncertainty supporting them.

## Change Boundary

* Do not propose refactors, optimizations, patches, or edit locations unless the
  user explicitly asks for recommendations or changes.
* A request to identify current risks or weaknesses does not itself authorize
  implementation.
* Do not modify repository files.
* Do not claim tests were run, runtime behavior was observed, or performance was
  measured unless that work was actually performed.

## Clarifying Questions

Ask a question only when missing information prevents a correct or materially
useful analysis.

Do not ask a question merely because a question is customary, because another
code path could be explored, or because the response could be split into later
turns.

When the repository provides enough evidence, complete the requested
explanation or review in the current response.

## Completion Check

Before responding, confirm:

* The relevant current code was actually inspected.
* The depth matches the user’s original request.
* Material execution and ownership paths were followed far enough for the
  conclusions made.
* Facts, inferences, assumptions, and unknowns are distinguishable.
* Material claims cite repository evidence.
* Tests or runtime behavior are not claimed without observation.
* No implementation change was proposed or performed without user authority.
* A thorough request was not compressed into the `initial-review` format.
