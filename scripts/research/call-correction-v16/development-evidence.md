# Findings from exposed RBN archives

The 533 selected decisions include 105 missed reference-supported corrections in 100 episodes. A different set contains 105 already-applied reference-supported fixes. The two groups answer different questions. The complete-stream denominators below are reported separately. The labels describe agreement with RBN references.

## What stopped the 105 missed corrections

| Final ordinary outcome | Count |
|---|---:|
| Maximum edit distance | 25 |
| Split resolver state | 22 |
| No snapshot | 12 |
| Recent corroboration bonus rejected on distance or call family | 11 |
| Recent corroboration bonus rejected on inadequate winner history | 10 |
| Minimum report requirement | 10 |
| Same call | 5 |
| Unvalidated truncation candidate | 3 |
| Proposed winner failed licensing | 2 |
| Applied another correction | 2 |
| Subject was not weaker for the recent-report bonus | 2 |
| Contested edit neighbor | 1 |

GoCluster proposed the intended call in 40 cases. It saw it without proposing it in another 55. Ten had no record of the intended call in the consulted traces. Only one intended correction exceeded the configured maximum distance of three. The resolver saw the intended call exclusively in neighboring 400 Hz slots in six cases. Of these six it already proposed the call once. These counts overlap the final gate categories.

**Time decay.** At 60 seconds, rescoring fixed evidence moved the intended call into a strict lead in 24 cases and out in two. At 90 seconds those counts were 22 and two. Among 85 selected wrong renames, the original call gained a strict lead in 53 and 43 cases respectively. All 105 selected valid fixes retained a strict lead at both half-lives. A ranking change can still fail report, margin, licensing, holding or frequency gates. The native arm uses event-time projection and an age-based weight. A matched unit-weight diagnostic isolates the change in freshness.

**Lower-reliability receivers.** The actual reference target was proposed and rejected because of too few votes in 34 cases, including more specific recent-report bonus outcomes above. Twenty-four were one raw report short. Fifteen of those had one additional trained 0.850–0.949 family in the right slot and within 100 Hz. Three were one-to-two opportunities and twelve were two-to-three. The frozen trained maximum was below 0.990. A literal trained 0.990 anchor would admit no examples.

On all four November development dates, 3,222 immediate proposals had the relevant one-short recent-report refusal. Of those, a distinct trusted and weaker family reported the target within 3 seconds during the preceding 15 seconds in 118 cases. Both frozen references supported the target in 4. A single reference supported it in 24, supported the subject in 3, or supported a third identity in 1. The references conflicted in 1. The remaining 85 lacked strong labels. Pair-level WPM agreement within 2 changed none of the 118. These are upstream opportunities. The native arm also requires actual selected evidence for the trusted witness, observes stored weights, and reruns downstream gates.

**SNR and temporal smoothing.** The sole 1-vs-1 subject/target pairing in the 105 misses was K2JVD at +15 dB versus the proposed K2JVB at +9 dB. This is the opposite ordering from the suggested rescue. Current SCP state cannot establish past validation. The completed September 26/27 native smoothing comparison changed 305 internal actions across 1,360,742 inputs with no change to delivered calls or decision times.

## Burst timing and WPM

The November 7/14 archived Method B and C study records were paired on immutable IDs. For each extra actual output where ordinary emitted the incoming spelling, predictor-side raw CQ/CW observations supplied two distinct nonfocal receiver families per spelling, over the preceding 300 seconds within 300 Hz. Both report pairs had to span at most 3 seconds and be separated by more than 3 seconds. Neither the focal report nor a later ordinal entered the evidence.

| Three-second latest-original veto on extra receipts | Method B | Method C |
|---|---:|---:|
| Extra receipts considered, including ordinary suppressions | 367 | 446 |
| Ordinary also emitted the subject and the added receipt was CW | 354 | 432 |
| Two corroborated spelling bursts | 170 | 220 |
| Original burst most recent, added rename vetoed | 160 | 210 |
| Vetoes where either reference supported the subject | 107 | 116 |
| Vetoes where either reference supported the new call | 0 | 3 |
| New R1 disagreements remaining among all additions | 6 | 7 |
| New R2 disagreements remaining among all additions | 4 | 4 |

Holding subsequent decisions fixed, the veto yields 16,744 stable out of 22,283 applied for B, and 16,765 out of 22,311 for C. The immutable v8 ordinary comparator is 16,587 out of 22,076. Both marginally exceed that stability fraction, but both retain strong contradictions. A 2-second or 5-second window also leaves contradictions. The historical v8 and later native ordinary controls differ on one November 14 receipt. Use matched controls for every comparison.

Across the four exposed May and September ordinary output streams, the three-second latest-original overlay retained 16,310 incoming calls. At least one reference supported that choice in 4,454 rows, while at least one supported the original correction in 70. Adding incoming WPM agreement reduced the veto to 4,677, with 1,396 gains and 23 losses. When the bursts were less than 30 seconds apart, allowing WPM to override recency added 318 vetoes, including 14 gains and two losses. At 30–59 seconds it added 178, with 16 gains and one loss. A physical turnover or station departure is not uniquely identifiable from these observations.

The synthetic controls include separate and overlapping emissions, offsets of 0, 80 and 200 Hz across scenarios, missing WPM, equal-speed stations and a speed-changing source. At duration 3 seconds and simultaneous onset, WPM plus frequency split 277/300 cochannel different-speed mixtures and 3/300 equal-speed mixtures. Frequency alone split 259/300 equal-speed mixtures offset by 200 Hz. On isolated one-source speed-change pairs separated by 30–60 seconds, the speed rule split 250/300. The samples have known construction provenance. Source calls, enrichment, artificial shifts and repeated use of the same reports constrain generalization.

## Guarded rescue comparator

The narrow frequency rescue runs after an ordinary maximum-distance rejection, requires the ordinary winner to be isolated in frequency, then vetoes an alternative when at least two receivers in the associated group support the incoming call. Across eight exposed dates it added 832 receipts, of which 751 were stable. Ordinary had 120,889 stable out of 149,251 applied and the guarded method had 121,640 out of 150,083. At least one strong reference supported 84 additions. Both supported 3. Neither had a strong label for 748. The native v16 parity comparison and the single-finalist qualification determine what further claims these data support.
