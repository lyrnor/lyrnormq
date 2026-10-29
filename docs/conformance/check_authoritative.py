#!/usr/bin/env python3
"""Offline completeness/reference guard, not an automatic normative-meaning proof."""
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / 'docs/conformance'


def check():
    inventory = json.loads((BASE / 'authoritative-inventory.json').read_text())
    review = json.loads((BASE / 'authoritative-review.json').read_text())
    assert review['source_sha256'] == inventory['source_sha256']
    assert review['verification'], 'missing real verification results'
    source = {x['id']: x for x in inventory['candidates'] + inventory['tables']}
    decisions = review['entries']
    assert len(decisions) == len(source) == len({x['id'] for x in decisions})
    assert {x['id'] for x in decisions} == set(source)
    links = 0
    for item in decisions:
        assert item['text_sha256'] == source[item['id']]['text_sha256'], item['id']
        assert item['status'] in ('verified', 'not-applicable'), item['id']
        assert item['note'] and item['role'] and item['disposition'], item['id']
        if item['status'] == 'verified':
            assert item['implementation'] and item['tests'], item['id']
        for reference in item['implementation'] + item['tests']:
            path, sep, symbol = reference.partition(':')
            target = (ROOT / path).resolve()
            assert sep and target.is_relative_to(ROOT) and target.is_file(), reference
            leaf = symbol.rsplit('::', 1)[-1].rsplit('.', 1)[-1]
            assert re.search(r'\b' + re.escape(leaf) + r'\b', target.read_text()), reference
            links += 1
    print(f'PASS {len(source)} authoritative dispositions; {links} source/test links')


if __name__ == '__main__':
    check()
