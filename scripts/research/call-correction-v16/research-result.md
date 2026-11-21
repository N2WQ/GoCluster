# GoCluster RBN call correction v16

**Keep ordinary correction live. Guarded frequency rescue won on the eight development dates and failed the frozen requirements on the two previously unused confirmation dates.**

The isolated research build used revision 6e11e5d763d6. Both the ordinary control and the existing guarded rescue reproduced their eight historical decision streams byte for byte. These eight dates span May, September and November 2026. The frozen training assets, receiver family map and ordinary licensing gates were identical across paired arms. Every scored stream reconciled archive identities, suppression and final release timing.

## Development results

| Method | Dates | Paired inputs | Net applied | Stable / applied | Ordinary stable / applied | New R1 disagreements | New R2 disagreements |
|---|---|---|---|---|---|---|---|
| Guarded frequency rescue | 8 | 8,404,719 | +832 | 81.048% | 80.997% | 0 | 0 |
| 60-second decay | 5 | 3,135,733 | -3,825 | 77.139% | 74.029% | 44 | 52 |
| 90-second decay | 5 | 3,135,733 | -2,760 | 76.550% | 74.029% | 42 | 49 |
| One additional trained receiver | 2 | 1,229,298 | +56 | 73.109% | 73.138% | 0 | 1 |
| Decision clock alone | 5 | 3,135,733 | +0 | 74.029% | 74.029% | 0 | 0 |
| Method B + latest burst | 1 | 458,495 | +63 | 74.012% | 74.035% | 2 | 2 |
| Method B + latest burst and WPM | 1 | 458,495 | +108 | 73.995% | 74.035% | 20 | 6 |
| Guarded rescue + latest burst | 8 | 8,404,719 | +827 | 81.050% | 80.997% | 0 | 0 |
| Guarded rescue + latest burst and WPM | 8 | 8,404,719 | +831 | 81.049% | 80.997% | 0 | 0 |
| Guarded rescue + one additional receiver | 1 | 458,495 | +43 | 74.050% | 74.035% | 0 | 1 |
| Guarded rescue + 90-second decay | 1 | 770,803 | -385 | 74.317% | 72.637% | 15 | 8 |

Each row compares the method with ordinary on the dates listed for that arm in development-verdict.json. Selection stopped an arm after a strong contradiction. For decay, four additional November dates answered the specific turnover question. The clock-only arm is an attribution control. R1 and R2 can label the same input. Their counts must be read separately. Stability uses three follow-on rows within 500 Hz and 15 minutes.

## Selection and qualification

The development selector chose Guarded frequency rescue after testing the declared alternatives. Eligible methods were ranked by correction count, exact stability fraction, disruptions to baseline outcomes, then candidate ID.

Development included 8,404,719 terminal IDs. The selected method applied 150,083 corrections, 121,640 stable, against 149,251 and 120,889 for ordinary. The baseline loss/retarget/retime union was 0.

| Mode | Paired inputs | Ordinary stable / applied | Finalist stable / applied |
|---|---|---|---|
| CW | 7,327,276 | 108,614 / 131,801 | 109,365 / 132,633 |
| RTTY | 1,077,443 | 12,275 / 17,450 | 12,275 / 17,450 |

The single frozen finalist failed the November 17/18 RBN confirmation. Its complete paired population contained 483,066 inputs.

| Role | Applied | Stable | Stable / applied |
|---|---|---|---|
| control | 7,110 | 5,130 | 72.152% |
| variant | 7,124 | 5,141 | 72.165% |

New strong disagreements under R1 and R2 were 6 and 7. The number of distinct disrupted baseline applied inputs was 0.

| CW distance | Incremental episodes | Jointly assessable | R1 covered | R2 covered | Pass |
|---|---|---|---|---|---|
| CW/1 | 3 | 0 | 0 | 0 | False |
| CW/2 | 1 | 0 | 0 | 0 | False |
| CW/3 | 1 | 0 | 1 | 0 | False |

The episode score counts newly delivered targets after accounting for what ordinary already delivered in the same episode. Receipt-level contradictions still count on each affected input. Each newly active cell requires 200 jointly assessable incremental episodes, including 50 on each of two dates. Each reference must cover at least 80%, show net supported gains on both dates, and introduce zero strong contradictions. The complete confirmation population must also improve applied volume with nondecreasing stability. See the date-level receipts for which gates failed.

Seven of the 14 additions renamed RT4M to RA3G during one November 18 episode. Both withheld references supported RT4M on six inputs. R2 supported RT4M and R1 was unavailable on the seventh. One different addition, GM4T to GM4OAS, had R1 support, and the other six additions had neither strong label.

| Mode | Paired inputs | Ordinary stable / applied | Finalist stable / applied |
|---|---|---|---|
| CW | 482,005 | 5,130 / 7,103 | 5,141 / 7,117 |
| RTTY | 1,061 | 0 / 7 | 0 / 7 |

## The turnover hypothesis

| Arm | Original restored among 68 two-burst wrong renames | Restored among all 85 wrong renames | 105 valid fixes retained | 105 misses recovered |
|---|---|---|---|---|
| clock-only | 0 | 0 | 105 | 0 |
| decay60 | 45 | 53 | 105 | 18 |
| decay90 | 40 | 45 | 105 | 17 |
| ordinary | 0 | 0 | 105 | 0 |

Decay repairs a real class of frequency-turnover-like errors, but alters unrelated votes, rankings and gate decisions elsewhere. RBN reports alone cannot prove that a particular station left the frequency or establish absolute RF accuracy. WPM can distinguish controlled same-frequency stations with different speeds and can also split one station after it changes speed. The controlled experiment, full miss audit and marginal veto counts are supplied separately.

No live server was changed. Both reserved November 21/22 v12 dates were excluded from this study.

## Reproduction and checks

The source kit recreated the tested executable byte for byte, SHA-256 6d83da5c76af8afbef2b3b24aa82dded2052b14be37d321008299af76aa01ac3. The three affected packages passed 2,812 ordinary regression cases. The final weak-witness, both decay, matched-clock and burst tests passed targeted Windows race checks. An initial clock-only race invocation also selected a half-life assertion that the no-decay control is designed to fail. The matching clock-only expectations passed on rerun. Experimental receiver and WPM paths allocate additional memory. These checks establish replay correctness, with live capacity and production latency still unmeasured.

An independent reviewer checked the scope of the scoring evidence, the callable research paths and the saved research receipts. Detailed inputs and limits are in development-evidence.md and the native per-date results. The two confirmation ZIPs are the November 17 and 18 files in the [official RBN daily archive](https://www.reversebeacon.net/raw_data/).
