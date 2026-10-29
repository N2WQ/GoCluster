# Boolean Personal Filters

The filter package owns bounded immutable rule preparation and matching. The
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

Active configuration and machine boundaries are explicit generation 6. Ordinary
codecs remain strict. Only the login-owned coordinator recognizes safe retired
unversioned/version 1-5 definitions to discard after historical validation;
it never activates or translates their rules. Settings, six separate controls,
metadata, backup conflicts and per-file atomicity retain their owners.

Start with [domain contracts](../docs/domain-contract.md#boolean-personal-rules),
[telnet rules](../telnet/README.md#filters),
[native storage](../data/config/README.md#user-configuration-storage) and
[ADR-0329](../docs/decisions/ADR-0329-nearby-boolean-predicate-and-generation-six.md).
Relevant source owners are rule_set.go, rule_compile.go, rule_evaluate.go,
rule_view.go, match_snapshot.go, configuration_v6.go, storage_v6.go and
legacy_upgrade.go. Tests must distinguish direct semantics from transport,
publication, history, persistence and actual delivery wiring.
