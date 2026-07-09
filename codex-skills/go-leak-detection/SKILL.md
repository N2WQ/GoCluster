---
name: go-leak-detection
description: "Use when Go work involves or investigates goroutine, timer, ticker, channel, socket, file-handle, process, shutdown, lifecycle, queue, retained-heap, map/cache/index, pprof, trace, or long-running leak concerns. Distinguish static/local evidence from runtime confirmation."
---

# Go Leak Detection

## Overview

Use this skill to investigate and validate leak risks in long-lived Go cluster
paths. It covers goroutine/lifecycle leaks, OS resource leaks, and retained
heap growth.

## Workflow

1. Classify the leak class.
   - Goroutine blocked after cancellation or shutdown
   - Orphan timer/ticker or retry loop
   - Channel send/receive blocked by lifecycle mismatch
   - Socket, listener, file, process, or HTTP body not closed
   - Queue or backpressure path retaining work after disconnect
   - Retained heap growth in maps, caches, heaps, indexes, interners, or pools

2. Inspect ownership and shutdown.
   - Identify goroutine owners, cancellation sources, close ordering,
     WaitGroups, deadlines, drain/drop policy, and cleanup coupling.
   - Use `go-retained-state-audit` as well when server-lifetime retained state
     is added or modified.
   - Use `go-hotpath-design` as well when leak fixes touch hot paths or
     allocation-sensitive queues.

3. Define local tests.
   - Add targeted lifecycle tests for start/stop, reconnect, cancellation,
     queue overflow, slow clients, and repeated churn when the code changes.
   - Use `go test -race ./...` when concurrency, lifecycle, queues, timers,
     cancellation, long-lived connections, or shared state are touched.
   - Consider `go.uber.org/goleak` for targeted packages with goroutine-heavy
     tests. Do not add it repo-wide without an approved scope item.

4. Use profiling and runtime evidence when needed.
   - Use `go test` profiles: `-memprofile`, `-blockprofile`, `-mutexprofile`,
     `-trace`, and `go tool pprof`/`go tool trace` for focused tests.
   - Use `scripts/run-with-profiling.ps1` for local runtime captures; it
     collects CPU, heap, allocs, block, mutex, goroutine, trace,
     retained-state, and OS process samples.
   - Optional Sysinternals tools such as `handle.exe`, TCPView, or Process
     Explorer can improve Windows handle/socket/process evidence when
     installed. Missing optional tools are reported as evidence gaps only for
     investigations that need OS-handle proof.

5. Separate evidence levels.
   - Static reasoning: ownership and close paths inspected.
   - Local test evidence: targeted tests and race checks passed.
   - Profile evidence: pprof/trace/goroutine/OS samples reviewed.
   - Runtime confirmation: comparable long-running or load/churn capture shows
     stable goroutine, heap, handle, socket, and queue trends.
   - For command-backed local test/race, profile, trace, or runtime evidence,
     require a short captured transcript excerpt in
     `docs/review-checklist.md` `Verification command reporting`. The excerpt
     must show the command or evidence source, target scope, status, and key
     pass/fail/profile line. Do not paste full logs, secrets, tokens,
     credentials, environment dumps, private hostnames, unnecessary user data,
     or large traces; redact and say so.

## Output Expectations

- Include a `Leak-detection audit` section when this skill triggers.
- Name the leak class, owner, cancellation/close path, tests, profiles, and
  remaining evidence gaps.
- For command-backed evidence, reference the captured `Verification command
  reporting` excerpt. For static-only evidence, label it as static reasoning and
  name the inspected files.
- Do not claim "no leak" unless the relevant evidence level was actually
  performed.
- Say "locally validated, not runtime-confirmed" when pprof or long-running
  runtime evidence is missing.
