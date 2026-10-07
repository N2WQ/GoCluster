# Support-Agent Quality Contract

This contract defines how the GoCluster support agent should behave when it is
the primary support mechanism for operators, telnet users, and developers.

## Goal

The support agent must produce answers that are:

- evidence-grounded: every GoCluster claim is supported by action-retrieved
  repository content from the current conversation
- specific: platform, feature, command, config, and symptom details narrow the
  route before the answer is synthesized
- pragmatic: troubleshooting answers give the smallest safe next step and say
  what each result means
- bounded: the agent asks only for missing evidence that changes the next
  decision and refuses when required source evidence is unavailable

This is a quality contract, not a second source of GoCluster behavior. Runtime,
operator, protocol, config, and workflow behavior still belong to the existing
repo docs, package READMEs, source, tests, ADRs, and TSRs.

## End-To-End Flow

1. Classify the user question.
   - Quick fact: one focused source can be enough.
   - Troubleshooting: use the troubleshooting index plus the underlying
     operator/config/source route.
   - Config-sensitive: include `data/config/README.md` and state that effective
     YAML controls the node.
   - Developer/debug: include package README or code-map routing, then focused
     source/tests when exact behavior matters.
   - External tooling: use `customgpt/external-authorities.md` only for
     Go/GitHub/Linux/systemd/PowerShell mechanics, never for GoCluster behavior.
2. Retrieve support-route evidence.
   - Support, symptom, config, telnet/user, operator, developer/debug,
     ambiguous, retrieval-resilience, and safety-boundary questions start with
     `getSupportRoute`.
   - A support route returns persona, domain, confidence, ambiguity, a support
     card, required sources, required answer facts, and forbidden unsafe claims.
3. Retrieve fallback routing evidence when needed.
   - Symptoms, failures, surprising output, startup problems, and "how do I
     troubleshoot" questions use `getTroubleshootingIndex` when no support card
     is decisive.
   - Normal topic questions use `getSourceMap` when no support card is decisive.
4. Choose the narrowest matching route.
   - A platform-specific, command-specific, source-specific, or feature-specific
     route beats a broad route.
   - If a later user message narrows the platform or symptom, retrieve the
     newly specific route before answering.
5. Retrieve authoritative content.
   - Route rows, snippets, `related_paths`, `routes`, and `symptom_routes` are
     hints. The agent must call `getDoc` or use a concrete `getBundle` file
     before making a GoCluster claim.
   - `getSupportRoute` and `searchSupportCorpus` may return concrete `files[]`
     evidence. Those files can satisfy retrieval when they are authoritative
     for the claim.
   - Use `searchSupportCorpus` for exact diagnostic strings, config keys,
     command names, glyphs, or log names that are hard to locate through broad
     docs.
   - If a file is truncated, use the header, related paths, directory listing,
     filename discovery, or a bounded line window before refusing.
6. Synthesize a support answer.
   - Start with the direct answer or most likely cause when evidence supports
     it.
   - Give an ordered next-step checklist.
   - Explain what each check proves or rules out.
   - Ask at most one focused follow-up unless several fields are inseparable
     from the next action.
   - End with `Source: <primary retrieved path>`.

## Troubleshooting Answer Shape

Troubleshooting answers should normally use this structure:

```text
Most likely first check: <safe check>.

Run:
<copy/paste command or exact UI/config location, when documented>

If <result A>, then <meaning and next step>.
If <result B>, then <meaning and next step>.

Please paste: <one focused missing artifact>.

Source: <retrieved path>
```

For startup and config failures, the first checks are usually the launch
command, active config directory, complete startup output, config loader
diagnostics, H3 table validation, and gridstore open/recovery messages. Platform
details determine whether Windows console commands, Linux service commands, or
manual run commands should be shown.

## Quality Gates

An answer is not good enough when:

- it cites a route document but never retrieves the authoritative underlying
  source
- it gives a generic checklist after the user supplied a platform, command,
  feature, or symptom detail
- it asks for logs without first giving a safe way to capture the relevant logs
- it lists possible causes without ordering the first likely check
- it recommends changing config, services, firewall rules, permissions,
  persistent data, or Git state before reading the relevant evidence
- it treats extra YAML key warnings as fatal, hides uncertainty, or invents
  commands/defaults/ports/config fields
- it refuses while action-returned content, related paths, directory listing,
  filename discovery, or bounded line windows could still retrieve usable
  evidence
- it ignores a support route's `must_include`, `must_avoid`, ambiguity, or
  security-sensitive flags

## Evaluation Expectations

Support-agent changes should be checked with four layers:

1. Contract checks: instruction size, schema shape, Worker route behavior, auth
   behavior, safe path denial, route extraction, line windows, and deployed
   endpoint health.
2. Route checks: representative prompts name the expected support route, route
   documents, and minimum authoritative files that must be retrieved.
3. Answer checks: representative prompts are judged against the quality gates in
   this document, not against exact wording alone.
4. Local harness checks: `scripts/evaluate-support-agent.ps1` runs the
   machine-readable cases from `docs/support-agent-eval-cases.json` against the
   checked-in Worker and current workspace. It verifies retrieval/source
   coverage by default, and can score pasted Preview/browser/app answers or
   optional live-model answers when configured.

The persona-domain coverage ledger lives in
`docs/support-agent-coverage-ledger.md`. The regression prompt set lives in
`docs/support-agent-evals.md`; the executable case catalog lives in
`docs/support-agent-eval-cases.json`. Deployment, smoke-check, and local eval
instructions live in `docs/support-agent-runbook.md`.

## Curated Search Contract

Search scans a fixed 46-file public corpus, not the entire repository. It returns
at most 25 merged overlapping context regions. Exact case-insensitive phrases
rank before same-line all-word substring matches. Within each quality tier,
files receive one region before receiving their next; ties use path then line.
An optional safe file/directory `path` restricts corpus membership. Empty,
unsafe and non-corpus scopes return 400; omission searches the entire corpus.

`corpus_count` identifies the configured corpus; `eligible_file_count` describes
the selected scope and `searched_count` counts successful source reads.
`failed_paths` names unsuccessful reads; `source_truncated_paths` names files
whose searchable prefix was capped at 140,000 characters. `coverage_complete`
is true only when all eligible sources were read without that cap.
The complete serialized response is capped at 99,000 characters, including
JSON escaping, formatting, metadata, and both `matches` and `files` copies.
`response_budget_truncated` reports shortening or omission to meet that budget.
Oversized regions are shortened around their strongest source match where
possible, using a shared snippet ceiling to preserve ranked file diversity.
`results_truncated` is true when any result evidence is shortened or omitted,
whether by the response budget or the 25-region limit. The existing `truncated`
flag also includes incomplete source coverage. Exactly 25 complete regions
with complete coverage and no budget truncation do not indicate overflow.

Each match and corresponding file has `snippet_truncated`, `column_start`, and
`column_end`. The snippet is a literal contiguous source slice with LF line
endings; line ranges describe the returned slice. Columns are one-based UTF-16
code units, with inclusive start and exclusive end. Shortening preserves
surrogate pairs. `matched_line` and `match_type` describe the strongest original source
match, which may not fit entirely in the slice. `matched_lines` lists only
matching lines whose phrase or all-word match is fully visible; it can be empty
for a shortened long line. No synthetic ellipsis is inserted into evidence.

Partial reads return HTTP 200 when at least one eligible file was read, even
when no matches were found; all-source failure returns HTTP 502 with the same
coverage metadata. Zero matches establish absence only within the completed
selected corpus search. Follow source-map routes and `getDoc` for evidence
outside that corpus. The configuration support card is now searchable; its
automatic support-route selection remains unchanged.

See [ADR-0255](decisions/ADR-0255-support-search-evidence-and-coverage.md) and
[ADR-0256](decisions/ADR-0256-support-search-response-budget.md).

## Public retrieval access

The support-agent API is public and requires no Worker token or Action
authentication. Successful retrieval responses report `auth: "none"`. Public
access does not broaden allowed paths or expose the deployment bundle; it
changes caller admission only. Read-only methods, search budgets, source
coverage and retrieval evidence requirements remain unchanged.

See [ADR-0257](decisions/ADR-0257-public-support-agent-retrieval.md).
