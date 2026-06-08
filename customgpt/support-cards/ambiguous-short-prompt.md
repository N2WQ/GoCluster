# Support Card: Ambiguous Short Prompt

## Match

Use for bare tokens or under-specified prompts such as `P93`, `MED?`, `bad
path`, `filter weird`, `cluster broken`, or `what is this glyph?`.

## First Safe Check

Preserve uncertainty. Do not infer a command, callsign, country, glyph, config
key, or error condition from a bare token unless retrieved docs explicitly
support that meaning.

## Must Include

- State that the prompt is ambiguous or does not have enough context.
- Offer the nearest documented lookup path when one is safe, such as `SHOW DXCC
  <token>` for possible DXCC/prefix context or `HELP <command>` for command
  context.
- Ask for one focused context detail, such as the full command, full spot line,
  exact error, platform, or log snippet.

## Must Avoid

- Do not claim a bare token is a valid login callsign or command.
- Do not invent a specialized meaning for an undocumented token.

## Sources

- `customgpt/source-map.md`
- `commands/README.md`
- `telnet/README.md`
