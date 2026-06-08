# Support Card: Confidence Glyph Meaning

## Match

Use when a telnet user asks what `?`, `S`, `P`, `V`, `C`, or `B` means in a spot
line, or whether a confidence field is a probability.

## First Safe Check

Route to GoCluster confidence documentation and current command/help docs. Do
not infer meanings from other cluster software.

## Must Include

- Explain only the documented GoCluster confidence meaning.
- For `P`, say it is a confidence glyph, not an external-cluster field or a
  probability.
- If the user asks about current HELP output, retrieve `commands/README.md`.

## Must Avoid

- Do not claim compatibility with DXSpider or other cluster software unless
  GoCluster docs explicitly say so.
- Do not invent numeric probability semantics.

## Sources

- `customgpt/troubleshooting-index.md`
- `README.md`
- `spot/README.md`
- `commands/README.md`
