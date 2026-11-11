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
Terminal ordinary letters retain the strongest level reached for an unchanged
held call (`? < S < P < V`); `C`/`B` are separate outcomes. Thus `S → ?`, `P → ?`
and `P → S` stay empty by construction. This does not mean evidence never weakens.

Each cell is total turns divided by completed spots in that cell. Values round
to the nearest tenth, with halfway values rounded up (`2.25` displays as
`2.3`). An em dash means there are no observations. Turns are checks, not
seconds. Each completed spot counts once, including timeout suppression and
completion followed by secondary output filtering. Retries do not add samples.
Immediate passes, initial queue overflow, earlier correction/license rejection,
pre-core age expiry, and work dropped during shutdown do not contribute. A failed retry enqueue
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
uses the retained letter, while holding and learning still use fresh evidence.
Completed checks never reset, and current reason-based budgets remain in effect.

The `Stabilizer: age drops=…` and `Stabilizer: retained letter=…` rows appear in all
three Pipeline Quality panes and headless stats. `age drops` counts once for an
actually held path discarded for age, before acceptance or final offers. It
excludes unheld work and initial admission overflow. `retained letter` counts
once when a terminal ordinary letter exceeds the fresh letter, including timeout
suppression, retry overflow and later thinning. It excludes retries, pre-core
aborts, C/B and percentage-only decreases. A later final age drop does not undo
terminal accounting or accepted learning, but cannot create an output/family
witness. Both counters reset with statistics reset/restart.

`Corrected output: no grid=…` counts locally accepted literal call corrections
admitted to at least one ordinary output lane with an empty final DX grid, after
the final age check and sealing. It counts once per spot, not per sink or client.
Immediate and delayed paths share this boundary. Later family hiding, recipient
filters or queue refusal still count. Unchanged calls (including B), holding,
timeout suppression, age drops, all-ordinary-lanes thinning and SELF-only output
do not count. It accumulates until statistics reset/restart, with no daily reset.
Watch its increase over time to measure this output condition; it is not a count
of lookup attempts/failures or a claim that any recipient received those spots.
Cold delayed lookups and timed-out immediate lookups can leave location unknown;
see [grid precedence](../spot/README.md#live-stabilizer-output).

The default Pipeline Quality pane is now 14 rows, including borders. It expands
or scrolls with content. Legacy Held counts admission/retry events, and legacy
release/suppression counters retain their existing reset behavior; they are not
a conservation equation for unique spots. A retained letter can accompany a
lower or unavailable current percentage. Confidence filters see that letter.
