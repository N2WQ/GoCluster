# CW call-retention safeguards

GoCluster enables two safeguards in its supplied configuration. They can keep the incoming RBN CW callsign when ordinary correction would rename it. The archived experiments examine false overwrites. Live correction accuracy and capacity still need measurement.

## Build and start from VS Code

Open the repository folder at the published `main` commit. From its PowerShell terminal, with the repository's Go 1.27 toolchain installed, run

```powershell
go build -buildvcs=true -o .\gocluster-cw-trial.exe .
.\gocluster-cw-trial.exe --version
go version -m .\gocluster-cw-trial.exe
```

Use your node's complete private config directory. For a new node, copy the example directory once and set your own node and RBN login callsigns before connecting. The [node setup guide](OPERATOR_GUIDE.md#configure-a-real-node) covers feed configuration and runtime assets.

The supplied `pipeline.yaml` already has these settings in its `call_correction` block. To enable the safeguards in an existing private configuration, set each desired switch to `true` in the active `pipeline.yaml` and restart. A missing switch leaves its safeguard disabled.

```yaml
call_correction:
  enabled: true
  cw_frequency_separation_enabled: true
  cw_burst_wpm_enabled: true
  selective_rescue_enabled: false
```

Start from the repository root so relative asset paths resolve. For a private directory at `data/config.local`, run

```powershell
$env:DXC_CONFIG_PATH = (Resolve-Path .\data\config.local).Path
.\gocluster-cw-trial.exe
```

**Rollback.** Set both `cw_frequency_separation_enabled` and `cw_burst_wpm_enabled` to `false` in the active configuration and restart. Each switch also works separately.

Keep `selective_rescue_enabled` false to use the burst safeguard. Method C can introduce a subsequent rename, so enabling it disables the burst comparison and its history. Frequency separation remains independently available.

When burst history is active, the resolver pressure summary shows the number of occupied rows and cumulative evictions. Rows can remain stored beyond the eligible lookback. An eviction can make the burst comparison abstain when the overwritten evidence overlaps that lookback.

## What the safeguards do

**Frequency separation.** The incoming and proposed spellings occupy distinct groups of frequencies. Multiple receiver families have heard both calls close in time, and at least two families other than the focal receiver recently heard the incoming spelling. The safeguard keeps the incoming call only with sufficient consistent evidence.

**Burst timing and WPM.** Each spelling needs a distinct burst corroborated by at least two receiver families other than the focal receiver. Within a burst the reports are at most three seconds apart. The later burst starts more than three seconds after the earlier one ends. The incoming spelling has the latest burst and its keying speed fits that burst. A separate decision using a 60-second half-life must also retain the incoming spelling and identity at the same release. Missing or ambiguous evidence leaves the ordinary decision in force.

RBN live spots carry a minute-level station timestamp. The live rule measures when GoCluster receives the reports. Network buffering can group reports that were distributed over time at the source. The five-day study measured archive timestamps with second resolution. It provides evidence about the historical rule and cannot establish the live error rate.

The decay comparison ages the exact-slot resolver publication captured before the focal report is queued. The ordinary resolver can read a newer publication if the worker has published more evidence in the meantime.

All 658 historical WPM retentions had immediate decisions. In the decayed arm, 624 kept the same call, 30 declined a replacement on edit distance and four declined it on confidence. The live burst rule requires positive support for the incoming call in a settled exact slot or an eligible settled neighborhood. It declines a pending, held, delayed or otherwise uncertain comparison. With the live stabilizer and its support store active, only a verified (`V`) original is eligible. A probable (`P`) original can enter a delay if the exact-slot evidence changes before delivery. Frequency separation can also be checked on delayed delivery. When both safeguards would qualify, frequency separation is recorded first. The decision reasons are `cw_frequency_separation_retained` and `cw_burst_wpm_decay_retained`.

## Work since Thursday, November 19

| Opportunity | Server status | Evidence |
| --- | --- | --- |
| Ordinary resolver repairs | Included in Thursday's integration | Report parsing and missing harmonic reports, neighboring-frequency admission, receiver-specific SNR, slash-call receiver counts and observed display, and consistent decision-local history reads. [Implementation and validation](call-correction-v2-execution.md#unit-accounting) |
| Separate CW frequency groups | Enabled in supplied configuration | In four development dates, 365 proposed renames were retained. Frozen references supported 23 across four episodes, supported the ordinary rename in zero, and left 342 unclassified. |
| Distinct short bursts with incoming WPM and decay confirmation | Enabled in supplied configuration | Five archive dates produced 658 retentions. RBN references supported 403 across 86 episodes and the ordinary rename in zero. Another 254 lacked a strong label, and one reference supported a third call. [Portable replay and per-rule counts](../scripts/research/call-correction-v16/README.md) |
| Method C selective rescue and Method B receiver compatibility | C is implemented and ships off. B is replay-only. | Both added corrections and reduced the stable fraction on the matched two-date comparison. Scalar margins and the later burst exclusions failed to remove the reference disagreements. [v8 comparison](call-correction-v2-v8-results.md) · [Follow-up](../scripts/research/call-correction-v16/development-evidence.md) |
| Guarded frequency rescue | Replay-only | It added 832 corrections across eight development dates. On fresh dates, seven of its 14 added corrections renamed RT4M to RA3G against reference evidence in one episode. [Confirmation](../scripts/research/call-correction-v16/research-result.md#selection-and-qualification) |
| Global 60/90-second decay and weaker receivers | Replay-only | Global 60-second decay reduced applied corrections by 3,825 across five dates and introduced reference contradictions. The weaker-receiver arm added 56 corrections on two dates and also introduced a contradiction. The 60-second comparison on this branch can only veto an ordinary rename. [Paired measurements](../scripts/research/call-correction-v16/research-result.md#development-results) |
| Temporal smoothing, unequal-SNR tie abstention and CW reliability fallback | Investigated, ordinary decisions retained | Smoothing changed 305 internal actions across 1,360,742 inputs. Delivered calls and times matched. SNR abstention retimed ten deliveries over those two days without a demonstrated identity gain. The fallback experiment changed zero delivered calls. [Smoothing audit](../scripts/research/call-correction-v16/development-evidence.md) · [Fallback comparison](call-correction-v2-v10-results.md#cw-fallback-comparison) |
| One-strong-receiver SNR rescue, edit distance and adjacent buckets | Investigation only | The one-against-one case among 105 misses had the stronger SNR on the incoming call. The resolver had already seen the intended call in 95 of the misses. Only one intended correction exceeded the distance limit. [Miss audit](../scripts/research/call-correction-v16/development-evidence.md#what-stopped-the-105-missed-corrections) |
| Tighter frequency partitions, missing frequency rows and small Method B score margins | Existing rules retained | The 180 Hz diameter cap removed existing protections. Missing or inconsistent evidence about the two competing identities still declines the safeguard. The proposed small margins left nearly all Method B additions intact. |
| General one-versus-two transmitter model, skimmer geography, Morse-element alignment and SNR edge-bin changes | Research proposals | Synthetic mixtures showed that WPM can separate stations keying at different speeds. The same approach split 250 of 300 single-source speed changes in one control. No general transmitter classifier or new Morse/SNR model qualified for the server. [Controlled tracks](../scripts/research/call-correction-v16/development-evidence.md#burst-timing-and-wpm) |

Reference agreement is measured on RBN data. Overlap between the two safeguards and the effect of their combination on fresh live traffic remain unqualified.
