# Personal Filter Banks

The filter package owns bounded immutable GO rule and native CC preparation and
matching. Current profiles keep independent banks and six controls per bank.
See [native CC and generation seven](../docs/cc-dialect.md). The GO
expr child package owns syntax and canonical formatting; it does not evaluate
spots or resolve user location. Telnet owns configuration publication, GRID,
client delivery policy and history continuation.

Each numbered slot holds one PASS or REJECT expression. A matching REJECT vetoes;
otherwise any PASS qualifies. No PASS means unrestricted personal selection
except matching REJECTs. New-user slot 0 is an ordinary editable configured rule;
user-issued reset clears rules without reinstalling it.

NEARBY is an operand-free predicate with ordinary NOT/AND/OR semantics. It
preserves the existing H3 helper and never strips geography or selects dedupe.
The caller supplies captured fine/coarse user cells and path class by value.
Prepared rules retain no user location, callback or cached proximity truth.
Evaluation-local lazy state caches a reached NEARBY result, including false,
across all rules. Missing proximity is false; NOT NEARBY includes unknown data.
Nil spots and invalid ordinary matching owners fail before negation.

Each rule has one canonical program. Ten slots, 1,024 bytes per expression,
32 atoms, 128 nodes and depth 8 per rule, 128 total atoms and 512 total values
bound ownership. A NEARBY term consumes an atom/node and no values. Replacements
share only unchanged immutable programs; snapshots release them normally.
No global program cache or historical user registry is introduced.

Current configuration uses generation seven. Frozen v6 DTOs remain the legacy
GO machine/carrier boundary and cannot represent native CC. The login coordinator
recognizes safe pre-seven structure without compiling discarded rules, clears
old filters, six switches and old named presets, and initializes both banks from
current operator mode/source defaults. It preserves selected dialect, unrelated
settings and metadata. Backups, independent reset markers and strict notice
acknowledgement keep per-file ownership and retry semantics explicit.

Start with [domain contracts](../docs/domain-contract.md#boolean-personal-rules),
[telnet rules](../telnet/README.md#filters),
[native storage](../data/config/README.md#user-configuration-storage) and
[ADR-0330](../docs/decisions/ADR-0330-native-cc-banks-and-generation-seven.md).
Relevant source owners are rule_set.go, rule_compile.go, rule_evaluate.go,
rule_view.go, match_snapshot.go, cc_prepare.go, cc_match.go, profile_v7.go,
storage_v7.go, preset_v7.go and upgrade_v7.go. Tests distinguish direct semantics from transport,
publication, history, persistence and actual delivery wiring.
