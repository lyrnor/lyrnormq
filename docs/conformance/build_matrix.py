#!/usr/bin/env python3
"""Deterministically merge coordinator-reviewed evidence; no network or green inference."""
import argparse
import csv
import io
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / 'docs/conformance'
FIELDS = ['id', 'section', 'source_occurrence', 'status', 'implementation', 'tests', 'reviewers', 'note']
VALID = {'verified', 'test-gap', 'gap', 'policy-exception', 'not-applicable', 'unreviewed'}
# A weaker observation cannot be hidden by a second feature-level assertion.
RANK = {'gap': 6, 'policy-exception': 5, 'test-gap': 4, 'unreviewed': 3,
        'verified': 2, 'not-applicable': 1}


def render():
    catalogue = json.loads((BASE / 'catalogue.json').read_text())
    sources = catalogue['numbered'] + [dict(item, source_occurrence='unnumbered-candidate')
                                       for item in catalogue['unnumbered_candidates']]
    rows = {item['id']: dict(id=item['id'], section=item['section'],
             source_occurrence=item['source_occurrence'], status='unreviewed',
             implementation=[], tests=[], reviewers=[], note=[]) for item in sources}
    seen = set()
    for path in sorted((BASE / 'reviews').glob('*.json')):
        review = json.loads(path.read_text())
        for item in review['clauses']:
            key = item['id']
            if key not in rows:
                raise ValueError(f'unknown clause {key} in {path.name}')
            status = item['status']
            if status not in VALID:
                raise ValueError(f'unknown status {status}')
            if status == 'verified' and not (item['implementation'] and item['tests'] and review['verification']):
                raise ValueError(f'missing verified evidence {key}')
            row = rows[key]
            if key not in seen or RANK[status] > RANK[row['status']]:
                row['status'] = status
            seen.add(key)
            row['implementation'] += item.get('implementation', [])
            row['tests'] += item.get('tests', [])
            row['reviewers'].append(review['reviewer'])
            row['note'].append(item.get('note', ''))
    out = io.StringIO(newline='')
    writer = csv.DictWriter(out, fieldnames=FIELDS, lineterminator='\n')
    writer.writeheader()
    for row in rows.values():
        for field in ('implementation', 'tests', 'reviewers', 'note'):
            row[field] = ' | '.join(dict.fromkeys(value for value in row[field] if value))
        writer.writerow(row)
    return out.getvalue()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    result = render()
    path = BASE / 'mqtt5-matrix.csv'
    if args.check:
        if not path.exists() or path.read_text() != result:
            raise SystemExit('matrix stale: run python3 docs/conformance/build_matrix.py')
    else:
        path.write_text(result)
