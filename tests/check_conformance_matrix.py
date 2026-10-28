#!/usr/bin/env python3
"""Offline integrity gate for the clause catalogue and checked-in audit evidence.

This checks evidence links and status hygiene, not protocol conformance itself.
An existing symbol never makes a clause verified without an explicit review.
"""
import argparse
import csv
import json
from pathlib import Path
import re
import subprocess
import sys


VALID = {'verified', 'test-gap', 'gap', 'policy-exception', 'not-applicable', 'unreviewed'}


def check(root):
    base = root / 'docs/conformance'
    catalogue = json.loads((base / 'catalogue.json').read_text())
    ids = [item['id'] for item in catalogue['numbered'] + catalogue['unnumbered_candidates']]
    assert len(ids) == len(set(ids)), 'duplicate source identifier'
    assert re.fullmatch(r'[0-9a-f]{64}', catalogue['source_html_sha256'])
    assert catalogue['source_url'].startswith('https://docs.oasis-open.org/')
    with (base / 'mqtt5-matrix.csv').open(newline='') as matrix:
        rows = list(csv.DictReader(matrix))
    assert len(rows) == len(ids), 'missing or extra matrix rows'
    assert {row['id'] for row in rows} == set(ids), 'matrix/source inventory mismatch'
    assert len({row['id'] for row in rows}) == len(rows), 'duplicate matrix identifier'
    reviews = {}
    links = 0

    def reference(value):
        nonlocal links
        path, separator, symbol = value.partition(':')
        assert separator and symbol, f'reference must include a symbol: {value}'
        target = (root / path).resolve()
        assert target.is_relative_to(root.resolve()), f'outside-workspace reference: {value}'
        assert target.is_file(), f'missing evidence file: {value}'
        text = target.read_text()
        # Rust qualified method names are declared as fn member; Python can use
        # Class.method. Index targets remain exact source symbols, not line guesses.
        leaf = symbol.rsplit('::', 1)[-1].rsplit('.', 1)[-1]
        assert re.search(r'\b' + re.escape(leaf) + r'\b', text), f'missing symbol: {value}'
        links += 1

    for path in sorted((base / 'reviews').glob('*.json')):
        review = json.loads(path.read_text())
        assert review['reviewer'], path
        assert review['verification'], f'missing actual command results: {path}'
        for item in review['clauses']:
            assert item['id'] in ids, f'unknown review clause: {item["id"]}'
            assert item['status'] in VALID
            assert item['note'], f'missing decision rationale: {item["id"]}'
            if item['status'] == 'verified':
                assert item['implementation'] and item['tests'], item['id']
            for value in item.get('implementation', []) + item.get('tests', []):
                reference(value)
            reviews.setdefault(item['id'], []).append(item)
    for row in rows:
        assert row['status'] in VALID, row['id']
        if row['status'] == 'verified':
            assert row['implementation'] and row['tests'] and row['reviewers'], row['id']
            assert any(item['status'] == 'verified' for item in reviews.get(row['id'], [])), row['id']
        if row['status'] == 'unreviewed':
            assert not row['reviewers'], f'unreviewed row carries a claimed review: {row["id"]}'
    subprocess.run([sys.executable, str(base / 'build_matrix.py'), '--check'], check=True, cwd=root)
    print(f'PASS {len(rows)} unique matrix rows; {len(reviews)} reviewed identifiers; {links} source/test links')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    check(parser.parse_args().root.resolve())
