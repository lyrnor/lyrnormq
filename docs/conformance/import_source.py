#!/usr/bin/env python3
"""Import identifiers/candidate fingerprints from a locally saved official OS HTML.

No network, no source text copied, and no inferred implementation status.
"""
import argparse
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import re

BASE = Path(__file__).resolve().parent
ID = re.compile(r'\[MQTT-([0-9]+(?:\.[0-9]+)*-[0-9]+)\]')
KEYWORDS = re.compile(r'\b(MUST|SHALL|SHOULD|REQUIRED|RECOMMENDED|MAY|OPTIONAL)\b')
ERROR = re.compile(r'\b(protocol error|malformed packet)\b', re.I)
BLOCKS = {'p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'}


class Source(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []
        self.tag = None
        self.current = []
        self.blocks = []

    def handle_starttag(self, tag, attrs):
        if tag in BLOCKS:
            self.tag, self.current = tag, []

    def handle_endtag(self, tag):
        if tag == self.tag:
            self.blocks.append((tag, ' '.join(' '.join(self.current).split())))
            self.tag = None

    def handle_data(self, value):
        self.parts.append(value)
        if self.tag:
            self.current.append(value)


def catalogue(data):
    source = Source()
    source.feed(data.decode('cp1252'))
    text = ' '.join(' '.join(source.parts).split())
    start = next(i for i, (tag, value) in enumerate(source.blocks)
                 if tag == 'h1' and value.startswith('1 '))
    end = next(i for i, (_, value) in enumerate(source.blocks)
               if i > start and value.startswith('Appendix B.'))
    body_ids = set(ID.findall(text[:text.rfind('Appendix B.')]))
    numbered = [{'id': 'MQTT-' + key, 'section': key.rsplit('-', 1)[0],
                 'source_occurrence': 'body' if key in body_ids else 'appendix-only'}
                for key in dict.fromkeys(ID.findall(text))]
    candidates = []
    section = ''
    for index, (tag, value) in enumerate(source.blocks[start:end], start):
        if tag.startswith('h'):
            match = re.match(r'^(\d+(?:\.\d+)*)\b', value)
            if match:
                section = match[1]
        if tag != 'p' or '[MQTT-' in value:
            continue
        kinds = []
        if KEYWORDS.search(value):
            kinds.append('rfc2119-keyword')
        if ERROR.search(value):
            kinds.append('error-constraint')
        if kinds:
            digest = hashlib.sha256(value.encode()).hexdigest()
            candidates.append(dict(id=f'U-{section}-{digest[:12]}', section=section,
                source_block=index, text_sha256=digest, candidate_kinds=kinds))
    return dict(source_url='https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html',
        source_date='2019-03-07', source_html_sha256=hashlib.sha256(data).hexdigest(),
        source_charset='windows-1252',
        scope='Literal numbered identifiers (including appendix index anomalies), plus heuristic unnumbered paragraph candidates. This is not a complete inventory of every normative sentence or table cell.',
        numbered=numbered, unnumbered_candidates=candidates, anomalies=[
          dict(id='MQTT-3.2.2-8', note='Appendix index identifier; body reason-code statement has malformed literal label T-3.2.2-8]. Requires body-context review, not an invented separate obligation.'),
          dict(id='MQTT-4.2.0-1', canonical_id='MQTT-4.2-1', note='Appendix index spelling differs from body identifier. Alias; do not double count.')])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('html', type=Path)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    result = catalogue(args.html.read_bytes())
    target = BASE / 'catalogue.json'
    if args.check:
        if result != json.loads(target.read_text()):
            raise SystemExit('source catalogue mismatch: review source/version/anomalies before importing')
        print('PASS official-source digest, numbered IDs and unnumbered candidate fingerprints')
    else:
        target.write_text(json.dumps(result, indent=2) + '\n')
