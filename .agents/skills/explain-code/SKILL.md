---
name: explain-code
description: "Explain, trace, review or diagnose existing code from inspected repository evidence without implementation changes. Covers brief orientation through thorough subsystem investigation; use a more specific triggered specialist for its engineering method."
---

# Explain Existing Code

Read the requested current source before explaining behavior. Locate referenced
symbols, then follow material callers, callees, interfaces, wiring, configuration,
state owners and tests far enough to answer the actual question.

## Depth And Specialist Routing

- For a requested brief, quick, high-level or initial overview, cover entry
  points, primary flow, state and side effects, inputs/invariants and material
  risks or unknowns concisely. Do not impose five bullets, a word ceiling, a risk
  limit or a question unless the user requests that format or scope is unclear.
- For a detailed explanation, trace material branches, normalization,
  termination, error/retry/timeout/drop/disconnect/recovery paths, state changes,
  tests and configuration.
- For a thorough review, audit or diagnosis, inspect complete material execution
  and ownership paths, persistence, concurrency, resource bounds, backpressure,
  malformed/boundary inputs and normal, overload, failure, recovery and shutdown
  behavior. Assess applicable compatibility, operator, config and model contracts,
  test gaps, conflicting evidence and uncertainty. Do not compress a thorough
  request into an initial orientation or require the user to ask again.

Use a more specific repo-managed specialist when its positive trigger applies.
Its method governs the analysis; this skill does not constrain specialist depth
or output. Use `go-code-walk` for unfamiliar or cross-package Go navigation and
other triggered domain skills for their distinct methods.

## Evidence And Reporting

Explain purpose, observable behavior, state, side effects and material invariants
in plain language. Organize findings around the system or question rather than
a fixed report structure. Cite inspected files and relevant symbols or lines.

Separate facts from assumptions and inference. Label an unestablished fact
`Unknown from inspected code` and identify the next source, test, configuration,
runtime evidence or external authority needed. Explain competing inferences and
what would distinguish them. Never present a typical design as inspected fact.

Ask only when missing information prevents useful, correct analysis and cannot
be resolved from the repository. If required code is inaccessible, request that
specific evidence; do not substitute memory or generic implementation patterns.
Complete the requested depth when sufficient evidence is available.

## Authority

This skill is read-only. Do not modify repository files or generate artifacts.
Recommendations require a user request for recommendations or changes; finding
a risk does not authorize its repair. A later implementation request enters the
applicable `AGENTS.md` change route before mutation. Do not claim tests, runtime
behavior or performance measurements that were not actually observed.
