# Review the CW call-correction experiments

## Run the five-day burst and WPM check

From the repository root in the VS Code terminal, run the following command with Python 3.11 or later. The branch includes the replay inputs and uses only the Python standard library.

```powershell
python -B scripts/research/call-correction-v16/burst_review.py --check
```

The test replays 4,270 eligible decisions and compares every count for all four rules against the saved audit. The eligible population consists of ordinary CW CQ renames where the native 60-second half-life replay kept the original spelling and identity at the same decision time. The fixture includes eligible cases where neither spelling formed a burst.

| Rule | Retained original | Reference supports original | Reference supports ordinary rename |
| --- | ---: | ---: | ---: |
| Latest corroborated burst favors original | 2,830 | 1,563 | 21 |
| Latest plus a receiver family shared by both bursts | 288 | 159 | 1 |
| Latest plus incoming WPM agreement | 658 | 403 | 0 |
| Latest plus shared family and incoming WPM agreement | 80 | 43 | 0 |

The WPM rule's 403 supported retentions span 86 episodes. Another 254 retentions lacked a strong reference, and one had a reference supporting a third identity. References are imperfect and can disagree. Development dates were September 26 and November 7, 14, 15 and 16, 2026. This retrospective study uses archive timestamps. The [live trial](../../../docs/cw-call-correction-branch.md) measures when each report arrives. Qualification on untouched dates remains open.

### Inspect a single decision

This example shows an incoming IN3ZYO at 24 WPM that ordinary correction renamed to IN3GYO. Set a VS Code breakpoint in `matching_arms` to examine how timing and speed affect the decision.

```powershell
python -B scripts/research/call-correction-v16/burst_review.py --case 2026-09-26:276885
```

Each report has five fields in order: UTC seconds since midnight, one-based RBN CSV ordinal, frequency in Hz, receiver family and WPM. Each spelling needs two distinct families within three seconds. History covers the previous 300 seconds and ±300 Hz on the same band, excludes the focal receiver family, and uses earlier archive ordinals from the predictor cohort. The later pair must start more than three seconds after the earlier pair ends. Chronology selects the pairs before WPM is scored. Both pairs must have positive speeds within 2 WPM internally. Their mean speeds must differ by at least 4 WPM, and the focal speed must be within 2 WPM of the original and at least 2 WPM closer to it.

### Evidence and reproduction

`burst-review.jsonl.gz` contains the two histories, focal observation, ordinary decision identities and withheld strong reference labels for each eligible case. The records are derived from the RBN daily archives and the saved paired native replays. The default check works offline in a fresh checkout. To regenerate the fixture from those original captures, supply their parent directory. The extractor uses archive and receiver metadata paths in the plans, with overrides available through `--archives` and `--metadata`. Choose a new output filename.

```powershell
python -B scripts/research/call-correction-v16/extract_burst_review.py --captures PATH_TO_CAPTURES --out extracted.jsonl.gz
python -B scripts/research/call-correction-v16/burst_review.py --fixture extracted.jsonl.gz --check
```

The earlier guarded frequency rescue was a separate experiment. Its fresh-date qualification failed. See [the research report](research-result.md) and [development evidence](development-evidence.md).

## Rebuild the v16 guarded frequency rescue

For all historical Go overlay builders, use the checkpoint that contains the
study kit and its original Go source. Later server versions changed some
of the exact source lines those builders replace. From a Windows checkout run

```powershell
git fetch origin tag research/call-correction-v16-source
git -c core.autocrlf=true worktree add --detach ../gocluster-v16-rebuild 499ba0a33585a72163020d70e842fdb5c1b8dfd5
cd ../gocluster-v16-rebuild
```

The experimental binary runs the GoCluster RBN replay command. The ordinary resolver runs first. When its proposed winner fails maximum edit distance, the rescue checks whether the frequency evidence separates that winner from the incoming spot. It requires support for the alternative in the ordinary snapshot, applies the selective method's existing gates and declines a rename when at least two receivers in the associated group reported the incoming spelling. Literal receiver labels follow the existing resolver convention.

The checkpoint adds the replay kit to the original Go source at `6e11e5d763d68d819d6b539f4d8abf05aa6474c2`. The Windows checkout uses CRLF for the five frozen base files. The tracked replacement templates keep their original bytes. The builder checks both against SHA-256 before creating the overlay.

```powershell
python -B scripts/research/call-correction-v16/build_guarded.py --repo . --out .tmp/v16-rebuild
```

On the research host the source kit reproduced the tested native binary byte for byte. Its SHA-256 is `6d83da5c76af8afbef2b3b24aa82dded2052b14be37d321008299af76aa01ac3`.

For the guarded method, use `GOCLUSTER_CROWDING_REPLAY_ARM=winner-separated` and the frozen enabled method candidate `c_f150_t60_d3` in the replay driver. An empty `GOCLUSTER_V16_BURST` selects the guarded parent. Values `clock` and `speed` select the matched retrospective burst experiments. Ordinary runs use the disabled method. The executable also enforces the study's date and phase restrictions. Use the frozen input assets and manifests for a historical reproduction.

The sibling decay builders and templates reproduce the separate age-decay and matched-clock experiments. The study report records which method was selected and its independent-date qualification result.
