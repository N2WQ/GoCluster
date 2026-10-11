# Dashboard keyboard controls

The Pipeline page's **Call decisions** pane shows corrected, rejected and retained
evaluations. A share describes support for its named current call; static/history
recognition is separate. Events precede delivery and do not claim a final glyph
or client receipt. **Corrections** counts accepted identity changes since process
start. See [diagnostic meanings](../docs/decisions/ADR-0293-correction-decision-diagnostics.md).

The V2 console dashboard accepts uppercase and lowercase navigation letters.

| Key | Action |
|---|---|
| H, h or ? | Open or close keyboard help |
| Esc | Close keyboard help |
| O or o | Overview page |
| I or i | Ingest page |
| P or p | Pipeline page |
| E or e | Events page |
| Tab / Shift+Tab | Next / previous pane on the current page |
| Up / Down or k / j | Scroll the focused pane |
| PageUp / PageDown | Scroll the focused pane by a page |
| Home / End | Go to the top / bottom of the focused pane |
| Q, q or Ctrl+C | Quit the dashboard |

Closing help returns to the underlying page. Page navigation selects its first
pane; the Events page has one pane, so Tab stays on it.

`dashboard_v2_keybindings_test.go` exercises the actual tcell input capture,
tview pages and pane consumers without starting a terminal or scheduler. These
checks establish key behavior, not OS-terminal rendering or production shutdown.
