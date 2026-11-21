"""Re-extract the portable five-day review fixture from native replay captures and RBN ZIPs."""
import argparse
import bisect
import collections
import csv
import functools
import gzip
import hashlib
import io
import json
import re
import zipfile
from pathlib import Path

from burst_review import Report

COHORT = 'ccv2-reference-v1'


@functools.cache
def normalized(label):
    call = label.strip().upper().replace('.', '/').removesuffix('/').strip()
    for suffix in ('/QRP', '/MM', '/AM', '/M', '/P'):
        if call.endswith(suffix):
            call = call.removesuffix(suffix)
            break
    return re.sub(r'-[0-9]+$', '', call)


@functools.cache
def receiver_key(label):
    parts = [s.strip() for s in normalized(label).split('-', 1)[0].split('/') if s.strip()]
    if not parts or any(not re.fullmatch('[A-Z0-9]+', part) for part in parts):
        return ''

    def score(seg):
        station = (bool(re.fullmatch(r'[A-Z0-9]*[0-9][A-Z][A-Z0-9]*', seg))
                   and len(re.match(r'[A-Z]*', seg).group()) <= 2 and 3 <= len(seg) <= 32)
        mixed = any(c.isalpha() for c in seg) and any(c.isdigit() for c in seg)
        return 100 * station + 40 * mixed + 20 * (len(seg) >= 4) - 50 * seg.isdigit() + len(seg)

    return min(parts, key=lambda seg: (-score(seg), -len(seg), seg))


def receiver_families(metadata):
    parents = {}

    def find(x):
        parents.setdefault(x, x)
        while parents[x] != x:
            x = parents[x]
        return x

    for group in metadata:
        keys = [receiver_key(a) for a in group['aliases']]
        assert all(keys)
        for key in keys[1:]:
            a, b = sorted((find(keys[0]), find(key)))
            parents[b] = a
    return {key: find(key) for key in parents}


@functools.cache
def cohort_a(family):
    return bool(family) and hashlib.sha256((COHORT + '\0' + family).encode()).digest()[0] % 2 == 0


def day_rows(source, day, cases, families):
    wanted = {(r['band'], call) for r in cases for call in (r['original'], r['other'])}
    selected = {r['ordinal']: r for r in cases}
    assert len(selected) == len(cases)
    rows = collections.defaultdict(list)
    focal = {}
    with zipfile.ZipFile(source) as archive, archive.open(day + '.csv') as raw:
        reader = csv.reader(io.TextIOWrapper(raw, encoding='utf-8-sig'))
        assert next(reader) == 'callsign de_pfx de_cont freq band dx dx_pfx dx_cont mode db date speed tx_mode'.split()
        for n, r in enumerate(reader, 1):
            if len(r) != 13 or r[8] != 'CQ' or r[12] != 'CW':
                continue
            key = (r[4], normalized(r[5]))
            if key not in wanted and n not in selected:
                continue
            k = receiver_key(r[0])
            family = families.get(k, k)
            timestamp = r[10]
            t = 3600 * int(timestamp[11:13]) + 60 * int(timestamp[14:16]) + int(timestamp[17:19])
            hz = round(float(r[3]) * 1000)
            speed = int(r[11]) if r[11].isdigit() else 0
            obs = Report(t, n, hz, family, speed)
            if n in selected:
                case = selected[n]
                assert key == (case['band'], case['original']), (case['raw_id'], key, case['original'])
                assert hz == round(case['freq'] * 1000) and cohort_a(family)
                focal[n] = obs
            if cohort_a(family):
                rows[key].append(obs)
    assert len(focal) == len(cases)
    for track in rows.values():
        assert all(a <= b for a, b in zip(track, track[1:]))
    return rows, focal


DAYS = ('2026-09-26', '2026-11-07', '2026-11-14', '2026-11-15', '2026-11-16')
ORDINAL = re.compile(rb'"ordinal":(\d+),')


def verify_digest(path, expected):
    with path.open('rb') as stream:
        if hashlib.file_digest(stream, 'sha256').hexdigest() != expected:
            raise ValueError(f'Input checksum differs: {path}')


def candidates(captures, date):
    scores = json.loads((captures / 'decay60' / f'{date}-score.json').read_bytes())
    targets = {}
    for row in scores['changed']:
        ordinary, decay = row['Control'], row['WinnerSeparated']
        if (ordinary['IdentityCorrection'] and decay['Status'] == 'output'
                and decay['FinalIdentity'] == row['Subject'] and decay['At'] == ordinary['At']):
            targets[row['Ordinal']] = row
    cases = []
    with (captures / 'ordinary' / date / 'decisions.jsonl').open('rb') as stream:
        for line in stream:
            n = int(ORDINAL.search(line)[1])
            if n not in targets:
                continue
            row = targets.pop(n)
            d = json.loads(line)
            raw = d['raw']
            if raw['Mode'] != 'CW' or raw['SpotClass'] != 'CQ':
                continue
            assert d['final_identity'] == row['Control']['FinalIdentity']
            assert d['subject_identity'] == row['Subject']
            original = normalized(raw['DXCall'])
            if original != row['WinnerSeparated']['FinalObserved']:
                continue
            cases.append(dict(ordinal=n, band=raw['Band'], original=original,
                              other=normalized(d['final_observed']), freq=raw['FreqKHz'],
                              raw_id=d['raw_id'], decision=d))
    if targets:
        raise ValueError(f'Missing ordinary decision rows on {date}')
    return cases


def extract_day(captures, date, archives, metadata):
    plan_name = 'older-plan.json' if date.startswith('2026-09') else 'nov-plan.json'
    plan = json.loads((captures / 'decay60' / plan_name).read_bytes())
    meta = metadata or Path(plan['receiver_metadata']['path'])
    verify_digest(meta, plan['receiver_metadata']['sha256'])
    families = receiver_families(json.loads(meta.read_bytes()))
    spec = next(i['archive'] for i in plan['inputs'] if i['date_utc'] == date)
    day = date.replace('-', '')
    archive = archives / (day + '.zip') if archives else Path(spec['path'])
    verify_digest(archive, spec['sha256'])
    cases = candidates(captures, date)
    rows, focal = day_rows(archive, day, cases, families)
    for c in cases:
        anchor = focal[c['ordinal']]
        histories = []
        for key in ('original', 'other'):
            track = rows[c['band'], c[key]]
            start = bisect.bisect_left(track, (anchor.sec - 300,))
            end = bisect.bisect_left(track, (anchor.sec, anchor.ordinal))
            histories.append([r for r in track[start:end] if r.ordinal < anchor.ordinal
                              and r.family != anchor.family and abs(r.hz - anchor.hz) <= 300])
        d = c['decision']
        assert c['raw_id'] == f"{spec['sha256']}:{anchor.ordinal}"
        yield dict(date=date, band=c['band'], original=c['original'], replacement=c['other'],
                   focal=anchor, original_history=histories[0], replacement_history=histories[1],
                   episode=d['episode_id'], subject_identity=d['subject_identity'],
                   replacement_identity=d['final_identity'],
                   r1=d['r1'].get('identity') if d['r1']['strong'] else None,
                   r2_frozen=d['r2_frozen'].get('identity') if d['r2_frozen']['strong'] else None)


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--captures', type=Path, required=True, help='Parent of ordinary and decay60 captures')
    cli.add_argument('--out', type=Path, required=True)
    cli.add_argument('--archives', type=Path, help='Override ZIP directory from the historical plans')
    cli.add_argument('--metadata', type=Path, help='Override receiver metadata path from the plans')
    args = cli.parse_args()
    with args.out.open('xb') as output:
        with gzip.GzipFile(fileobj=output, filename='', mode='wb', mtime=0) as writer:
            for date in DAYS:
                for row in extract_day(args.captures, date, args.archives, args.metadata):
                    writer.write((json.dumps(row, separators=(',', ':')) + '\n').encode())
                print(date, flush=True)


if __name__ == '__main__':
    main()
