# Rebuild the v16 guarded frequency rescue

The experimental binary runs the GoCluster RBN replay command. The ordinary resolver runs first. When its proposed winner fails maximum edit distance, the rescue checks whether the frequency evidence separates that winner from the incoming spot. It requires support for the alternative in the ordinary snapshot, applies the selective method's existing gates and declines a rename when at least two receivers in the associated group reported the incoming spelling. Literal receiver labels follow the existing resolver convention.

Build from the GoCluster checkout used for the experiment, revision `6e11e5d763d68d819d6b539f4d8abf05aa6474c2`. The builder checks each original and replacement file against its SHA-256 and creates the Go overlay in the output directory.

```powershell
python -B scripts/research/call-correction-v16/build_guarded.py --repo . --out .tmp/v16-rebuild
```

On the research host the source kit reproduced the tested native binary byte for byte. Its SHA-256 is `6d83da5c76af8afbef2b3b24aa82dded2052b14be37d321008299af76aa01ac3`.

For the guarded method, use `GOCLUSTER_CROWDING_REPLAY_ARM=winner-separated` and the frozen enabled method candidate `c_f150_t60_d3` in the replay driver. An empty `GOCLUSTER_V16_BURST` selects the guarded parent. Values `clock` and `speed` select the matched retrospective burst experiments. Ordinary runs use the disabled method. The executable also enforces the study's date and phase restrictions. Use the frozen input assets and manifests for a historical reproduction.

The sibling decay builders and templates reproduce the separate age-decay and matched-clock experiments. The study report records which method was selected and its independent-date qualification result.
