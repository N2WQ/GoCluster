# Support Card: Startup And Runtime Log Destination

## Match

Use when a node operator asks where startup errors, missing config diagnostics,
path prediction aggregates, or file-only events are logged.

## First Safe Check

Classify the log type before naming a file:

- startup before configured logging opens: console/stderr or service manager
  capture
- configured system runtime log: `system.log` in the configured system log dir
- propagation/path aggregates: `propagation.log` in `logging.propagation.dir`
- file-only events: dedicated files controlled by `logging.*` keys in
  `app.yaml`

## Must Include

- Use exact terms when relevant: `stderr`, `Config diagnostics`, `Config
  warning`, `required config file`, `required YAML setting`, `system log`, and
  `propagation.log`.
- Early config diagnostics can appear before the configured system log opens.
- Startup config failures should be checked for `Config diagnostics` and
  `Config warning` lines.
- Path prediction aggregate lines are not lost when absent from `system.log`;
  they moved to the propagation log.

## Must Avoid

- Do not conflate console/UI output with file-only event logs.
- Do not claim logging was lost unless the relevant dedicated log is also
  missing.
- Do not send Windows users first to Linux service logs.

## Sources

- `customgpt/troubleshooting-index.md`
- `docs/OPERATOR_GUIDE.md`
- `data/config/README.md`
- `docs/decisions/ADR-0123-dedicated-propagation-log.md`
- `docs/decisions/ADR-0093-file-only-connection-and-gate-event-logs.md`
- `internal/cluster/bootstrap.go`
