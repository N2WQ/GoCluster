"""Build a chronological CW decay experiment without modifying the shipped Go sources."""
import argparse
import json
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]


def replace_once(source, old, new):
    if source.count(old) != 1:
        raise ValueError(f"expected one source anchor: {old!r}")
    return source.replace(old, new)


def build(out, half_life):
    out.mkdir(parents=True, exist_ok=True)
    generated = out / f"decay{half_life}-overlay"
    generated.mkdir(exist_ok=True)
    mapping = {}

    def overlay(name, text):
        destination = generated / name.replace("/", "_")
        destination.write_text(text, encoding="utf-8")
        mapping[(REPO / name).as_posix()] = destination.as_posix()

    def patch(name, edits):
        source = (REPO / name).read_text(encoding="utf-8")
        for old, new in edits:
            source = replace_once(source, old, new)
        overlay(name, source)

    owner_view = "view := publishedResolverEvidence(st)"
    decayed_view = "view := v16DecayView(publishedResolverEvidence(st), now, st.key.Mode)"
    patch("spot/resolver_evidence.go", [("type ResolverEvidenceView struct {",
        "type ResolverEvidenceView struct {\n\tv16Original *ResolverEvidenceView\n\tv16At time.Time")])
    patch("spot/signal_resolver.go", [
        (owner_view, decayed_view),
        ("\tprovisionalState := classifyResolverState", """\tif st.key.Mode == "CW" {
        totalReporters, totalWeightedSupportMilli, _ = view.ReceiverTotals()
    }
    provisionalState := classifyResolverState"""),
    ])
    patch("spot/resolver_loss.go", [(owner_view, decayed_view),
        ("\tpreviousOrder := make(map[string]int", """\tif st.key.Mode == "CW" {
        snapshot.TotalReporters, snapshot.TotalWeightedSupportMilli, _ = view.ReceiverTotals()
    }
    previousOrder := make(map[string]int""")])
    patch("spot/resolver_fresh.go", [
        ("view := snap.receiverEvidence", "view := v16DecayView(snap.receiverEvidence, now, snap.Key.Mode)"),
        ("if count == view.count {", "if count == view.count && view == snap.receiverEvidence {"),
    ])

    patch("cmd/rbn_replay/runner.go", [
        ("r.selectTemporalInitial(spotEntry, resolverEvidence.Key, subject)",
         "r.selectTemporalInitial(spotEntry, resolverEvidence.Key, subject, now)"),
    ])
    patch("cmd/rbn_replay/study_diagnostics_calls.go", [
        ("func (r *replayRunner) selectTemporalInitial(s *spot.Spot, key spot.ResolverSignalKey, subject string) correctionflow.ResolverPrimarySelection {",
         """// All inputs to primary and hold entry use a common event clock in the CW decay arm.
func v16VoteClock(key spot.ResolverSignalKey, now time.Time) time.Time {
    if key.Mode == "CW" {
        return now
    }
    return time.Time{}
}

func (r *replayRunner) selectTemporalInitial(s *spot.Spot, key spot.ResolverSignalKey, subject string, now time.Time) correctionflow.ResolverPrimarySelection {"""),
        ("SelectResolverPrimarySnapshotForCall(r.resolver, key, r.cfg.CallCorrection, subject)",
         "SelectResolverPrimarySnapshotForCallAt(r.resolver, key, r.cfg.CallCorrection, subject, v16VoteClock(key, now))"),
        ("subject, time.Time{}, &capture)", "subject, v16VoteClock(key, now), &capture)"),
    ])
    patch("cmd/rbn_replay/pipeline.go", [
        ("SelectResolverPrimarySnapshotForCall(resolver, evidence.Key, cfg, preCorrectionCall)",
         "SelectResolverPrimarySnapshotForCallAt(resolver, evidence.Key, cfg, preCorrectionCall, v16VoteClock(evidence.Key, now))"),
        ("preCorrectionCall, time.Time{}, &capture.Data)",
         "preCorrectionCall, v16VoteClock(evidence.Key, now), &capture.Data)"),
    ])
    overlay("spot/v16_experiment_decay.go",
            (HERE / "decay_model.go.tmpl").read_text(encoding="utf-8").replace("HALF_LIFE", str(half_life)))
    overlay("spot/v16_experiment_decay_test.go", (HERE / "decay_model_test.go.tmpl").read_text(encoding="utf-8"))
    overlay("cmd/rbn_replay/v16_experiment_decay_test.go", (HERE / "decay_replay_test.go.tmpl").read_text(encoding="utf-8"))
    overlay_file = generated / "overlay.json"
    overlay_file.write_text(json.dumps({"Replace": mapping}, indent=2), encoding="utf-8")
    for file in mapping.values():
        subprocess.run(["gofmt", "-w", file], cwd=REPO, check=True)
    flags = ["-overlay", str(overlay_file)]
    subprocess.run(["go", "test", *flags, "./spot", "./cmd/rbn_replay", "-run", "^TestV16Decay", "-count=1"], cwd=REPO, check=True)
    binary = out / f"decay{half_life}.exe"
    subprocess.run(["go", "build", *flags, "-o", str(binary), "./cmd/rbn_replay"], cwd=REPO, check=True)
    print(binary, flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--half-life", required=True, choices=(60,90), type=int)
    args = parser.parse_args()
    build(args.out.resolve(), args.half_life)
