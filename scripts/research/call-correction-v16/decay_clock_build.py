"""Build the identity-weight control from the frozen, decision-clock 60-second overlay."""
import argparse
import hashlib
import json
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
FROZEN_OVERLAY = "4e75daba1dcb14c42a59556db6671a19a9cf514fe057f32f8cd243880b64fd27"
DECAY_LOOP = """\t\tfor j, row := range candidate.receivers {
\t\t\tage := max(time.Duration(0), now.Sub(row.ObservedAt))
\t\t\trow.WeightMilli = int(math.Round(float64(row.WeightMilli) * math.Exp2(-float64(age)/float64(v16HalfLife))))
\t\t\trows[j] = row
\t\t}"""


def build(out):
    matched = json.loads((out / "decay60-overlay" / "overlay.json").read_text())["Replace"]
    source_files = {Path(key).relative_to(REPO).as_posix(): Path(path).read_bytes()
                    for key, path in matched.items()}
    receipt = hashlib.sha256()
    for key, data in sorted(source_files.items()):
        receipt.update(f"{key}\n{hashlib.sha256(data).hexdigest()}\n".encode())
    if receipt.hexdigest() != FROZEN_OVERLAY:
        raise ValueError("matched decision-clock overlay content changed")

    model_name = "spot/v16_experiment_decay.go"
    model = source_files[model_name].decode().replace("\r\n", "\n")
    if model.count(DECAY_LOOP) != 1 or model.count('\t"math"\n') != 1:
        raise ValueError("unexpected decay model source")
    source_files[model_name] = model.replace(DECAY_LOOP, "\t\tcopy(rows, candidate.receivers)").replace('\t"math"\n', "").encode()
    for pkg, template in [("spot", "decay_clock_model_test.go.tmpl"),
                          ("cmd/rbn_replay", "decay_clock_replay_test.go.tmpl")]:
        source_files[f"{pkg}/v16_clock_only_test.go"] = (HERE / template).read_bytes()
    dest = out / "clock-only-overlay"
    dest.mkdir(exist_ok=True)
    replacements = {}
    for key, content in source_files.items():
        path = dest / key.replace("/", "_")
        path.write_bytes(content)
        replacements[(REPO / key).as_posix()] = path.as_posix()
    overlay = dest / "overlay.json"
    overlay.write_text(json.dumps({"Replace": replacements}, indent=2), encoding="utf8")
    flags = ["-overlay", str(overlay)]
    tests = "^Test(V16ClockOnly|V16DecayImmediateAndHoldEntryUseEventClock|V16DecayLatestVotePerNativeReceiver)"
    subprocess.run(["go","test",*flags,"./spot","./cmd/rbn_replay","-run",tests,"-count=1"],cwd=REPO,check=True)
    subprocess.run(["go","build",*flags,"-o",str(out/"clock-only.exe"),"./cmd/rbn_replay"],cwd=REPO,check=True)


if __name__ == "__main__":
    args = argparse.ArgumentParser(description=__doc__)
    args.add_argument("--out",type=Path,required=True,help="Directory containing the frozen decision-clock decay60-overlay")
    build(args.parse_args().out.resolve())
