# ADR-0188: Local Console Mode Retirement

- Status: Accepted
- Date: 2026-06-17
- Decision Origin: Scope Ledger v1

## Context

The local console UI had accumulated four runtime-mode concepts: headless,
ANSI, legacy tview, and tview-v2. The useful operator surfaces are now
headless service operation and the bounded page-based `tview-v2` renderer.

Keeping ANSI and legacy tview code added startup branches, configuration
fields, tests, docs, and support routes for modes that no longer represent the
maintained console experience. Silent aliasing of old values would also hide
private config drift and leave operators with unclear runtime behavior.

## Decision

Retire the legacy local console modes and keep only:

- `headless`
- `tview-v2`

Config load now rejects `ansi`, `tview`, `auto`, `ansi_poc`, `none`, and any
other unsupported `ui.mode` value with an explicit migration error. The ANSI
renderer, legacy tview renderer, ANSI console doc, and the console-size helper
script are removed.

The top-level UI fields that only existed for the retired renderers are removed
from typed config and public YAML. Stale private keys such as `ui.refresh_ms`,
`ui.color`, `ui.clear_screen`, and `ui.pane_lines` follow the existing extra-key
policy: they are logged as config warnings and ignored. The `ui.Surface`
contract and tview-v2 buffer/page configuration are unchanged.

## Alternatives considered

1. Keep legacy modes but mark them deprecated.
   - Rejected because it keeps unsupported code, tests, docs, and startup
     branches in the maintained surface.
2. Silently map old modes to `headless` or `tview-v2`.
   - Rejected because that hides private config drift and makes service
     startup behavior less explicit.
3. Keep ANSI as a lightweight terminal fallback.
   - Rejected because it duplicates local-console behavior without carrying the
     bounded page and buffer controls that make `tview-v2` the maintained
     interactive renderer.

## Consequences

### Benefits

- Local UI startup has a smaller mode surface and fewer dead branches.
- Operators have one service mode and one interactive console mode to reason
  about.
- Support routing points to current code instead of deleted renderer files.

### Risks

- Private configs that still set a retired `ui.mode` now fail startup until the
  operator changes the mode to `headless` or `tview-v2`.
- Private configs can still contain stale renderer-only keys; those keys warn
  under the existing extra-key policy instead of failing startup.

### Operational impact

Unattended services should use `ui.mode: headless`. Manual terminal inspection
should use `ui.mode: tview-v2`. Telnet protocol behavior, ingest processing,
fan-out, file logging, and tview-v2 layout/key behavior are unchanged.

## Links

- Related code: `config/config.go`, `internal/cluster/main_runtime.go`,
  `ui/dashboard_v2.go`
- Related tests: `config/ui_v2_test.go`, `config/config_dir_load_test.go`,
  `internal/cluster/main_runtime_test.go`, `ui/dashboard_v2_test.go`
- Related docs: `README.md`, `docs/OPERATOR_GUIDE.md`,
  `data/config/app.yaml`, `data/config/README.md`,
  `customgpt/developer-guide-index.md`,
  `docs/code-maps/runtime-ingest-fanout.md`
- Related ADRs: ADR-0002, ADR-0040, ADR-0142
- Related TSRs: none
