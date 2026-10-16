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

## Stabilizer matrix

The Pipeline Quality pane replaces the old final-glyph average line with a
matrix of average turns per delayed spot. Rows show the glyph at first
stabilizer admission (`?`, `S`, `P`). Columns show the glyph at the terminal
decision (`?`, `S`, `P`, `V`, `C`, `B`). For example, an `S` spot released as `V`
after two checks contributes one observation of two turns to `S → V`.

`B` means a proposed correction was rejected and the original call was retained.

Each cell is total turns divided by completed spots in that cell. Values round
to the nearest tenth, with halfway values rounded up (`2.25` displays as
`2.3`). An em dash means there are no observations. Turns are checks, not
seconds. Each completed spot counts once, including timeout suppression and
completion followed by secondary output filtering. Retries do not add samples.
Immediate passes, initial queue overflow, earlier correction/license rejection,
and work dropped during shutdown do not contribute. A failed retry enqueue
ends an already admitted path and counts its completed checks once.

The matrix refreshes on the same configured statistics cycle as the other
console numbers, on the Overview, Pipeline, and Events pages. It accumulates
process-local observations until statistics reset or restart. It starts empty;
old final-glyph totals cannot reconstruct entry glyphs. Storage is fixed at
18 cells and retains no callsign history. The matrix occupies more vertical
space than the old line; its table is 56 columns wide and its heading is 52.

The existing headless `Stabilizer Glyph` diagnostics retain their final-glyph
definition and fanout boundary. Their averages can differ from the console
matrix, which counts terminal paths before output filtering. This display
change does not alter stabilizer timing, retry limits, eligibility, or delivery.
