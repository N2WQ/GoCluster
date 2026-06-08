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
