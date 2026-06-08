# Support Card: Toxicity Filter Status

## Match

Use when a telnet user asks why `REJECT TOXIC` did not hide a comment or why
toxicity filtering appears inconsistent.

## First Safe Check

Check the user's filter state and the classifier status before assuming the
comment was classified toxic.

## Must Include

- Run `SHOW FILTER` to confirm `REJECT TOXIC` is active.
- Distinguish `TOXIC` from `UNKNOWN`, `SAFE_LOCAL`, and `UNAVAILABLE`.
- Confirm toxicity classifier configuration and Worker auth/timeout/429/5xx
  health before guessing the classification.
- Skimmer or automated-source spots may bypass human comment toxicity
  classification.

## Must Avoid

- Do not guess the AI classification.
- Do not claim `REJECT TOXIC` blocks unknown or unavailable status.

## Sources

- `customgpt/troubleshooting-index.md`
- `README.md`
- `telnet/README.md`
- `data/config/README.md`
- `cloudflare/toxicity-worker/README.md`
