# Support Card: Truncated Retrieval Recovery

## Match

Use when action metadata says a file is truncated, source-truncated, too large,
or when the agent has partial evidence but not the exact lines needed.

## First Safe Check

Treat truncation as partial evidence, not as retrieval failure.

## Must Include

- Say `partial evidence` when the action returned truncated content or source
  metadata.
- Use returned header, `related_paths`, `listDir`, `findFiles`, `/search`, or a
  bounded `getDoc` line window before refusing.
- If the needed material is in a large file, retrieve a focused window with
  `start_line` and `line_count`.
- Refuse only when no action call returns usable content, path, and source URL,
  or when safety policy blocks the request.

## Must Avoid

- Do not say required documentation could not be retrieved when the action
  returned content and source metadata.
- Do not answer from partial evidence as if it were complete.

## Sources

- `docs/support-agent-quality-contract.md`
- `docs/support-agent-runbook.md`
- `customgpt/source-map.md`

## Search coverage

For `/search`, inspect `coverage_complete`, `failed_paths`,
`source_truncated_paths`, and `results_truncated`. Source coverage and result
truncation are separate. `response_budget_truncated` identifies shortening or
omission to fit the 99,000-character serialized-response budget. A snippet
with `snippet_truncated: true` is a literal partial source slice, with
one-based UTF-16 columns (inclusive start, exclusive end). `matched_line`
anchors the original match; `matched_lines` lists only matches fully visible
in the slice. Retrieve the source before interpreting missing context.
A partial HTTP 200 response, including zero matches,
does not establish absence. Narrow the corpus `path` or follow the discovered
source with `getDoc` and line windows. HTTP 502 means no eligible source could
be read; preserve that uncertainty rather than inventing an answer.
