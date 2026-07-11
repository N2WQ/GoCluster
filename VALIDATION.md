# Codex Non-trivial Closeout Compliance

This file applies to Codex Non-trivial changes. It evaluates outcomes, not
response formatting. Read-only work and Small changes use their applicable
evidence and validation without this closeout review. Fable uses its own
validation contract.

## Pass Conditions

A Codex Non-trivial change passes only when current evidence establishes that:

- exact approval preceded mutation and the final work stayed within the
  approved scope;
- material discovery, compatibility, resource, and operational risks were
  resolved or reported;
- the smallest correct change was implemented without speculative additions;
- the final diff was reviewed against current source and approved scope;
- validation was selected from the touched surface and actual risk, with
  affected checks rerun after review fixes;
- material claims match observed commands, measurements, runtime evidence, or
  inspected source;
- affected documentation and durable decision records are current; and
- each approved item maps compactly to implementation and validation.

Any unresolved material condition fails closeout. Report the failure, gap,
waiver, or residual risk directly; do not convert missing evidence into a pass.

## Risk-triggered Evidence

Use race checks for concurrency or shared-state changes, fuzzing for parser or
protocol changes, and benchmarks plus profiles for performance claims. These
are triggered by the engineering surface, not by task size or reporting
category.

High-risk work requires a fresh final verification pass. Independent review is
conditional; a genuinely fresh lead pass is acceptable. Independent findings
remain evidence and never replace lead ownership.

## Reporting

State the overall validation result and material gaps or waivers in plain
language. Report substantive command results once. No numeric score, exact
block, audit taxonomy, fixed heading, or visible list of irrelevant categories
is required.

Static workflow checkers cannot prove conversational approval, correct risk
classification, adequate discovery, sufficient validation, genuine reviewer
independence, or engineering quality.
