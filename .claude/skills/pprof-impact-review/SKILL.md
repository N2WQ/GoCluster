---
name: "pprof-impact-review"
description: "Optional. Use when the user asks to review, compare, or interpret multiple local Go pprof bundles across optimization rounds. Classify bundles as cold, warm, or transition, extract exact deltas for target symbols, and produce a net-win or net-regression judgment with explicit confidence and caveats."
---

# Pprof Impact Review

Use this skill for repo-local performance evidence review when multiple
profile bundles exist and the user wants to know whether a change helped,
regressed, or only partially succeeded. Fable-native counterpart to
`codex-skills/pprof-impact-review/SKILL.md`. Unlike the 7 skills required
by `docs/fable-workflow.md`'s Dependency rigor and audit sections, this one
is optional on both sides of the contract — it is not in `AGENTS.md`'s
required Skill Check trigger list either, so treat it as available
performance-evidence tooling, not a gating audit.

## Required workflow

1. Build the bundle ledger first.
   - List available bundles chronologically.
   - Identify capture timestamps, binary build timestamps when present, and
     likely restart boundaries.
   - Classify each bundle as `cold`, `warm`, or `transition`.

2. Inspect the standard profile set.
   - Run `go tool pprof -top` for:
     - CPU
     - mutex
     - heap (`inuse_space`)
     - allocs (`alloc_space`)
     - block when blocking behavior is part of the question
   - Prefer the actual bundle files in `logs/` over assumptions.

3. Compare the target symbols explicitly.
   - Extract exact before/after numbers for the functions the user cares
     about.
   - Separate:
     - cold-start vs warm-runtime
     - startup/load costs vs steady-state costs
     - `alloc_space` vs `inuse_space`
     - repo-owned costs vs dependency/platform/runtime noise

4. Produce the judgment.
   - State whether the round was:
     - net positive
     - net negative
     - mixed / partially successful
   - If one hotspot was removed and a larger one was introduced, say that
     directly and re-rank the next target accordingly.
   - Include a confidence level and the reason for any downgrade.

## Output expectations

- Use absolute timestamps.
- Mention process age or restart context when it affects interpretation.
- Do not claim runtime confirmation from microbenchmarks alone.
- If live comparability is weak, say so plainly.

## Default command pattern

Typical commands:

- `go tool pprof -top logs/cpu-<stamp>.pprof`
- `go tool pprof -top logs/mutex-<stamp>.pprof`
- `go tool pprof -top logs/heap-<stamp>.pprof`
- `go tool pprof -top -sample_index=alloc_space logs/allocs-<stamp>.pprof`

Use focused filtering when comparing specific symbols across bundles.
