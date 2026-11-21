"""Replay the five-day burst and WPM audit against the packaged RBN evidence."""
import argparse
from collections import Counter, defaultdict
import gzip
import json
from pathlib import Path
from typing import NamedTuple


HERE = Path(__file__).resolve().parent
FIXTURE = HERE / 'burst-review.jsonl.gz'


class Report(NamedTuple):
    sec: int
    ordinal: int
    hz: int
    family: str
    speed: int


def burst_pairs(rows):
    """Make all <=3-second corroborations of one spelling by two receiver families."""
    pairs = []
    for end, last in enumerate(rows):
        for index in range(end - 1, -1, -1):
            first = rows[index]
            if last.sec - first.sec > 3:
                break
            if first.family != last.family:
                pairs.append((first, last))
    return pairs


def rank(pair):
    first, last = pair
    return last.sec, last.ordinal, first.sec, first.ordinal


def latest_sequence(early_pairs, late_pairs):
    """Prefer the latest ending pair, then the latest eligible preceding pair."""
    if early_pairs:
        earliest_end = min(last.sec for _, last in early_pairs)
        late = max((p for p in late_pairs if p[0].sec > earliest_end + 3),
                   key=rank, default=None)
        if late:
            early = max((p for p in early_pairs if p[1].sec + 3 < late[0].sec), key=rank)
            return rank(late) + rank(early), early, late
    return None


def mean_speed(pair):
    a, b = pair[0].speed, pair[1].speed
    if min(a, b) > 0 and abs(a - b) <= 2:
        return (a + b) / 2
    return None


def matching_arms(case):
    original, replacement = (burst_pairs([Report(*r) for r in case[key]])
                             for key in ('original_history', 'replacement_history'))
    latest_original = latest_sequence(replacement, original)
    latest_replacement = latest_sequence(original, replacement)
    if not latest_original or (latest_replacement and latest_replacement[0] > latest_original[0]):
        return []
    _, early, late = latest_original
    arms = ['latest']
    shared = bool({r.family for r in early} & {r.family for r in late})
    if shared:
        arms.append('shared')
    earlier_speed, later_speed = mean_speed(early), mean_speed(late)
    focal_speed = Report(*case['focal']).speed
    if earlier_speed is None or later_speed is None or focal_speed <= 0:
        return arms
    error = abs(focal_speed - later_speed)
    if abs(later_speed - earlier_speed) >= 4 and error <= 2 and error + 2 <= abs(focal_speed - earlier_speed):
        arms.append('wpm')
        if shared:
            arms.append('shared_wpm')
    return arms


def summarize(fixture, inspect_id=None):
    totals = defaultdict(Counter)
    gain_episodes = defaultdict(set)
    dates = Counter()
    found = False
    with gzip.open(fixture, 'rt', encoding='utf-8') as stream:
        for line in stream:
            case = json.loads(line)
            dates[case['date']] += 1
            arms = matching_arms(case)
            refs = {r for r in (case['r1'], case['r2_frozen']) if r}
            if inspect_id == f"{case['date']}:{case['focal'][1]}":
                print(json.dumps(dict(case, matching_arms=arms), indent=2))
                found = True
            for arm in arms:
                counts = totals[arm]
                counts['retained'] += 1
                if case['subject_identity'] in refs:
                    counts['supported'] += 1
                    gain_episodes[arm].add(case['episode'])
                if case['replacement_identity'] in refs:
                    counts['blocked_valid'] += 1
                if not refs:
                    counts['unlabeled'] += 1
                if refs - {case['subject_identity'], case['replacement_identity']}:
                    counts['third_identity'] += 1
    if inspect_id and not found:
        raise ValueError(f'Case absent from eligible population: {inspect_id}')
    for arm, counts in totals.items():
        counts.setdefault('blocked_valid', 0)
        counts['supported_episodes'] = len(gain_episodes[arm])
    return {'eligible_by_date': dict(dates), 'arms': {k: dict(v) for k, v in totals.items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', type=Path, default=FIXTURE)
    parser.add_argument('--case', help='Print an eligible date:CSV-ordinal and its matching arms')
    parser.add_argument('--check', action='store_true', help='Check all counts against the saved audit')
    args = parser.parse_args()
    result = summarize(args.fixture, args.case)
    print(json.dumps(result, indent=2, sort_keys=True))
    if args.check:
        expected = json.loads((HERE / 'burst-review-expected.json').read_bytes())
        if result != expected:
            raise SystemExit('FAIL: replay differs from the five-day audit')
        print('PASS: all five dates and four decision rules matched.')


if __name__ == '__main__':
    main()
