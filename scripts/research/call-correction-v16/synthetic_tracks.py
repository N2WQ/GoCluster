"""Offline CW burst/WPM controls. Fixed exposed inputs, no candidate or holdout search."""
import csv
import hashlib
import io
import json
import re
import statistics
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parent
PRIOR = Path('C:/Users/r_bak/AppData/Local/Temp/gocluster-winner-separated-20261119/subject-protected')
DAYS = ('2026-05-30', '2026-09-26', '2026-11-07')
WIDTHS = (2, 3, 5)
END = 3 * 3600
CAP = 100
MED = statistics.median


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


@dataclass(frozen=True)
class Obs:
    t: int
    hz: int
    speed: int
    family: str


@dataclass(frozen=True)
class Group:
    band: str
    call: str
    slot: int
    obs: tuple


def family_map(path):
    groups = json.loads(path.read_bytes())
    aliases = {}
    for group in groups:
        # Only simple base receiver calls are eligible; map known aliases together.
        names = {s.strip().upper().split('-')[0] for s in group['aliases']}
        key = min(names)
        for name in names:
            if name in aliases and aliases[name] != key:
                raise ValueError('overlapping receiver groups need transitive closure')
            aliases[name] = key
    return aliases


def load(date, archive, expected, aliases):
    if digest(archive) != expected:
        raise ValueError(f'changed archive for {date}')
    counts = Counter()
    bins = defaultdict(list)
    last_time = -1
    with ZipFile(archive) as source, source.open(date.replace('-', '') + '.csv') as raw:
        for row in csv.DictReader(io.TextIOWrapper(raw, encoding='utf-8-sig')):
            h, m, s = map(int, row['date'][11:19].split(':'))
            t = h * 3600 + m * 60 + s
            if row['date'][:10] != date or t < last_time:
                raise ValueError('unexpected study clock')
            last_time = t
            if t >= END:
                break
            counts['raw_rows'] += 1
            if row['tx_mode'] != 'CW' or row['mode'] != 'CQ':
                continue
            counts['cw_cq'] += 1
            rx = row['callsign'].upper().split('-')[0]
            call = row['dx'].upper()
            if not re.fullmatch('[A-Z0-9]+', rx) or not re.fullmatch('[A-Z0-9]+', call):
                continue
            hz = round(float(row['freq']) * 1000)
            speed = int(row['speed']) if row['speed'].isdigit() else 0
            bins[(t // 10, row['band'], call)].append(Obs(t, hz, speed, aliases.get(rx, rx)))
            counts['simple_calls'] += 1
            counts['missing_speed'] += int(speed <= 0)
    groups = []
    for (slot, band, call), obs in sorted(bins.items()):
        first = {}
        for o in obs:
            first.setdefault(o.family, o)
        values = tuple(first.values())
        if len(values) < 2 or max(o.hz for o in values) - min(o.hz for o in values) > 100:
            continue
        groups.append(Group(band, call, slot, values))
    counts['groups'] = len(groups)
    return groups, counts


def extract(group, width):
    # First two-or-more-family burst, closed at the end of the 10-second bin.
    # Ties are deterministic. Later reports are never consulted.
    for i, start in enumerate(group):
        burst = [o for o in group[i:] if o.t <= start.t + width]
        if len(burst) >= 2:
            measured = [o.speed for o in burst if o.speed > 0]
            speed = None
            if len(measured) >= 2 and max(measured) - min(measured) <= 2:
                speed = MED(measured)
            return MED([o.hz for o in burst]), speed
    return None


def score(book, key, left, right, truth):
    for width in WIDTHS:
        row = book[key + '/' + str(width) + 's']
        row['pairs'] += 1
        a, b = extract(left, width), extract(right, width)
        if a is None or b is None:
            row['clock_unknown'] += 1
            continue
        geo_split = abs(a[0] - b[0]) > 100
        known = a[1] is not None and b[1] is not None
        veto = known and abs(a[1] - b[1]) >= 4
        row['wpm_known' if known else 'wpm_unavailable'] += 1
        row['geometry_split' if geo_split else 'geometry_link'] += 1
        split = geo_split or veto
        row['combined_split' if split else 'combined_link'] += 1
        if veto and not geo_split:
            row['extra_true_split' if truth == 'two' else 'extra_false_split'] += 1


def natural(groups, date, book):
    history = defaultdict(list)
    matches = []
    for g in groups:
        center = MED([o.hz for o in g.obs])
        fbin = int(center // 250)
        for nearby in (fbin - 1, fbin, fbin + 1):
            for previous in reversed(history[g.band, nearby]):
                if previous.slot < g.slot - 7:
                    break
                # At decision time (end of g's ten-second acquisition bin) both
                # group ends are in the past. Use actual acquisition end-to-end lag.
                lag = g.obs[-1].t - previous.obs[-1].t
                if not 30 <= lag <= 60:
                    continue
                distance = abs(center - MED([o.hz for o in previous.obs]))
                if distance > 250:
                    continue
                same = g.call == previous.call
                label = 'same_reported_call' if same else 'different_reported_call'
                score(book, date + '/' + label, previous.obs, g.obs, 'one' if same else 'two')
                if same and distance <= 50 and len(previous.obs) >= 3 and len(g.obs) >= 3:
                    matches.append((previous, g))
        history[g.band, fbin].append(g)
    # Each source group appears in at most one selected pair. Stable SHA order
    # avoids silently selecting the first period, callsign or highest vote count.
    def key(pair):
        a, b = pair
        return hashlib.sha256(f'{date}/{a.band}/{a.call}/{a.slot}/{b.slot}'.encode()).digest()
    selected, used = [], set()
    for a, b in sorted(matches, key=key):
        ids = {(g.band, g.call, g.slot) for g in (a, b)}
        if not ids & used:
            selected.append((a, b))
            used.update(ids)
            if len(selected) == CAP:
                break
    return selected


def transform(obs, first_t, center_hz, speed_center=None, missing=False):
    hz = MED([o.hz for o in obs])
    speeds = [o.speed for o in obs if o.speed > 0]
    speed = MED(speeds) if speeds else None
    def shifted(o):
        if missing or o.speed == 0 or speed is None:
            return 0
        return o.speed if speed_center is None else round(o.speed - speed + speed_center)
    return tuple(Obs(o.t - obs[0].t + first_t, round(o.hz - hz + center_hz), shifted(o), o.family) for o in obs)


def split_supported(observations, field, minimum_gap, local_spread):
    # Each side needs two distinct receiver families. Membership uses only the
    # observed property. The function never receives the injected source IDs.
    ordered = sorted((getattr(o, field), o.family) for o in observations
                     if field != 'speed' or o.speed > 0)
    for i in range(1, len(ordered)):
        low, high = ordered[i - 1][0], ordered[i][0]
        if high - low < minimum_gap:
            continue
        left = {family for value, family in ordered[:i] if low - value <= local_spread}
        right = {family for value, family in ordered[i:] if value - high <= local_spread}
        if len(left) >= 2 and len(right) >= 2:
            return True
    return False


def blind_score(book, key, left, right, truth):
    # A single series of timestamped anonymous observations. At each timestamp
    # decide using only its inclusive trailing burst. Capture any split warning
    # within this short event, so an early false split cannot disappear.
    obs = sorted((*left, *right), key=lambda o: o.t)
    for width in WIDTHS:
        row = book[key + '/' + str(width) + 's']
        row['events'] += 1
        assessable = has_speed = frequency_split = speed_split = False
        for now in sorted({o.t for o in obs}):
            recent = [o for o in obs if now - width <= o.t <= now]
            if len(recent) < 4 or len({o.family for o in recent}) < 2:
                continue
            assessable = True
            with_speed = [o for o in recent if o.speed > 0]
            has_speed |= len(with_speed) >= 4 and len({o.family for o in with_speed}) >= 2
            frequency_split |= split_supported(recent, 'hz', 101, 100)
            speed_split |= split_supported(recent, 'speed', 4, 2)
        if not assessable:
            row['clock_unknown'] += 1
            continue
        row['wpm_measurable' if has_speed else 'wpm_unavailable'] += 1
        row['geometry_alert' if frequency_split else 'geometry_no_alert'] += 1
        row['combined_alert' if frequency_split or speed_split else 'combined_no_alert'] += 1
        if not frequency_split and speed_split:
            row['extra_true_alert' if truth == 'two' else 'extra_false_alert'] += 1


def constructed(samples, date, book, blind_book, counts):
    # Triples use actual receiver multiplicities, timing, frequency and speed
    # residuals. Labels are known only by the explicit injected source assignment.
    for i, (a, b) in enumerate(samples):
        competitor = next((other for other, _ in samples[i + 1:] + samples[:i] if other.call != a.call), None)
        if competitor is None:
            counts['missing_second_source'] += 1
            continue
        lag = (30, 45, 60)[i % 3]
        scenarios = (
            ('one_repeat', b, 0, 28, False, 'one'),
            ('one_speed_change', b, 0, 36, False, 'one'),
            ('one_missing_speed', b, 0, None, True, 'one'),
            ('two_equal_speed_cochannel', competitor, 0, 28, False, 'two'),
            ('two_different_speed_cochannel', competitor, 0, 36, False, 'two'),
            ('two_nearby_different_speed', competitor, 80, 36, False, 'two'),
            ('two_separated_equal_speed', competitor, 200, 28, False, 'two'),
            ('two_missing_speed', competitor, 0, None, True, 'two'),
        )
        for name, source, delta, speed, missing, truth in scenarios:
            left = transform(a.obs, 0, 7000000, 28)
            right = transform(source.obs, lag, 7000000 + delta, speed, missing)
            assert max(o.t for o in left) < min(o.t for o in right)
            score(book, date + '/' + name, left, right, truth)
        overlap_cases = (
            ('one_identical_repeat', b, 0, 28, False, 'one'),
            ('one_changing_speed', b, 0, 36, False, 'one'),
            ('two_cochannel_equal_speed', competitor, 0, 28, False, 'two'),
            ('two_cochannel_different_speed', competitor, 0, 36, False, 'two'),
            ('two_separated_equal_speed', competitor, 200, 28, False, 'two'),
            ('two_cochannel_missing_speed', competitor, 0, None, True, 'two'),
        )
        for offset in (0, 2, 5):
            for name, source, delta, speed, missing, truth in overlap_cases:
                left = transform(a.obs, 0, 7000000, 28)
                right = transform(source.obs, offset, 7000000 + delta, speed, missing)
                blind_score(blind_book, date + '/' + name + '_dt' + str(offset), left, right, truth)
        counts['constructed_source_pairs'] += 1


def checks():
    a = (Obs(1, 0, 28, 'a'), Obs(3, 0, 28, 'b'))
    assert extract(a, 2) == (0, 28)
    assert extract(a, 1) is None
    b = (Obs(33, 100, 32, 'c'), Obs(35, 100, 32, 'd'))
    book = defaultdict(Counter)
    score(book, 'test', a, b, 'one')
    assert book['test/2s']['extra_false_split'] == 1
    assert extract((Obs(0, 0, 0, 'x'), Obs(1, 0, 32, 'y')), 2) == (0, None)
    assert extract((Obs(0, 0, 20, 'x'), Obs(1, 0, 30, 'y')), 2) == (0, None)


def main():
    checks()
    book, constructed_book, blind_book = defaultdict(Counter), defaultdict(Counter), defaultdict(Counter)
    facts = {}
    for day in DAYS:
        plan = json.loads((PRIOR / ('nov-capture-plan.json' if day[5:7] == '11' else 'older-capture-plan.json')).read_bytes())
        entry = next(i for i in plan['inputs'] if i['date_utc'] == day)['archive']
        metadata = plan['receiver_metadata']
        if digest(Path(metadata['path'])) != metadata['sha256']:
            raise ValueError('receiver metadata drift')
        groups, counts = load(day, Path(entry['path']), entry['sha256'], family_map(Path(metadata['path'])))
        samples = natural(groups, day, book)
        constructed(samples, day, constructed_book, blind_book, counts)
        facts[day] = {'archive_sha256': entry['sha256'], 'receiver_metadata_sha256': metadata['sha256'], 'counts': counts}
    for row in list(book.values()) + list(constructed_book.values()):
        assert row['pairs'] == row['clock_unknown'] + row['geometry_split'] + row['geometry_link']
        assert row['pairs'] == row['clock_unknown'] + row['combined_split'] + row['combined_link']
        assert row['wpm_known'] + row['wpm_unavailable'] == row['geometry_split'] + row['geometry_link']
    for row in blind_book.values():
        assert row['events'] == row['clock_unknown'] + row['geometry_alert'] + row['geometry_no_alert']
        assert row['events'] == row['clock_unknown'] + row['combined_alert'] + row['combined_no_alert']
    result = {'script_sha256': digest(Path(__file__)), 'dates': facts, 'constructs': constructed_book,
              'anonymous_overlap_controls': blind_book,
              'natural_30_60s': book, 'parameters': {'window_utc': '[00:00:00,03:00:00)', 'burst_width_s': WIDTHS,
              'min_burst_families': 2, 'comparison_lag_s_inclusive': [30, 60], 'natural_candidate_radius_hz': 250,
              'geometry_split_above_hz': 100, 'wpm_veto_absolute_difference_at_least': 4,
              'max_within_burst_wpm_range': 2, 'grouping': '10s UTC bins by band and reported simple call, first receipt per normalized family; within-bin 100Hz spread max; groups matched in same band',
              'clock': 'evaluate at second bin close; all constituent reports already available',
              'selection': 'up to 100 disjoint same-reported-call pairs/day, <=50Hz and >=3 families per group, 30-60s apart; source counts retained',
              'constructed_offsets': '30/45/60s first report separation; 0/80/200Hz median offsets; WPM median centers 28/36 preserving sample dispersion',
              'overlap_controls': 'Interleave and anonymize the two samples; no calls or manual source partition supplied to detector. 0/2/5s onset offsets. Any trailing 2/3/5s interval with >=4 observations and >=2 families is assessable. A gap >=101Hz (<=100Hz within-side spread) or >=4WPM (<=2WPM within-side spread) must separate two families per side. Any alert before event end counts. No alert does not assert a common transmitter.',
              'natural_labels': 'same vs different reported calls conditional on burst eligibility; split categories descriptive only; no RF identities',
              'limits': 'constructed source labels by assignment; natural repeated pairs may share families/groups, source sample is exposed and enriched; no production parity or effect inferred'}}
    encoded = (json.dumps(result, sort_keys=True, indent=2) + '\n').encode()
    path = ROOT / 'synthetic_tracks_results.json'
    if path.exists():
        if path.read_bytes() != encoded:
            if sys.argv[1:] != ['--replace-sha256', digest(path)]:
                raise ValueError('existing results differ; use --replace-sha256 with previous output digest')
            path.write_bytes(encoded)
    else:
        with path.open('xb') as output:
            output.write(encoded)
    print(path, hashlib.sha256(encoded).hexdigest())
    for name, data in [('constructed', constructed_book), ('natural', book)]:
        agg = defaultdict(Counter)
        for key, values in data.items():
            _, case, width = key.split('/')
            if width == '3s':
                agg[case].update(values)
        print(name, json.dumps(agg, sort_keys=True))


if __name__ == '__main__':
    main()

