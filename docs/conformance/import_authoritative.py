#!/usr/bin/env python3
"""Fingerprint authoritative DOCX body requirements/tables without copying the source.

Structural inventory only. Semantic role/applicability review is a separate gate.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET
import zipfile

BASE = Path(__file__).resolve().parent
NS = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
W = '{' + NS['w'] + '}'
IDS = re.compile(r'\[MQTT-([0-9]+(?:\.[0-9]+)*-[0-9]+)\]')
MUST = re.compile(r'\b(?:MUST|SHALL|REQUIRED)\b')
ERROR = re.compile(r'\b(?:Protocol Error|Malformed Packet)\b', re.I)


def paragraph_text(paragraph):
    return ' '.join(''.join(node.text or '' for node in paragraph.findall('.//w:t', NS)).split())


def inventory(path):
    raw = path.read_bytes()
    with zipfile.ZipFile(path) as archive:
        info = archive.getinfo('word/document.xml')
        if info.file_size > 16 * 1024 * 1024:
            raise ValueError('document.xml exceeds source import bound')
        root = ET.fromstring(archive.read(info))
    paragraphs = root.findall('.//w:p', NS)
    texts = [paragraph_text(p) for p in paragraphs]
    start = next(i for i, p in enumerate(paragraphs)
                 if p.find('./w:pPr/w:pStyle', NS) is not None
                 and p.find('./w:pPr/w:pStyle', NS).get(W+'val') == 'Heading1'
                 and texts[i] == 'Introduction')
    end = next(i for i in range(start + 1, len(texts))
               if texts[i].startswith('Mandatory normative statement'))
    headings = []
    records = []
    positions = {p: i for i, p in enumerate(paragraphs)}
    paths = {}
    for index in range(start, end):
        p, text = paragraphs[index], texts[index]
        style = p.find('./w:pPr/w:pStyle', NS)
        name = '' if style is None else style.get(W+'val', '')
        if re.fullmatch(r'Heading[1-6]', name):
            level = int(name[-1])
            headings = headings[:level-1] + [text]
        paths[index] = list(headings)
        ids = list(dict.fromkeys('MQTT-'+key for key in IDS.findall(text)))
        if not (MUST.search(text) or ERROR.search(text)):
            continue
        records.append(dict(id=f'DOCX-P{index}', paragraph=index,
            text_sha256=hashlib.sha256(text.encode()).hexdigest(),
            heading_path=list(headings), numbered_ids=ids,
            must_keywords=bool(MUST.search(text)), error_constraint=bool(ERROR.search(text)),
            source_kind='numbered-context' if ids else 'unnumbered-candidate'))
    tables = []
    for table in root.findall('.//w:tbl', NS):
        table_paragraphs = table.findall('.//w:p', NS)
        indices = [positions[p] for p in table_paragraphs]
        if not indices or not start <= indices[0] < end:
            continue
        content = '\n'.join(texts[i] for i in indices)
        tables.append(dict(id=f'DOCX-T{len(tables)}', first_paragraph=indices[0],
            last_paragraph=indices[-1], text_sha256=hashlib.sha256(content.encode()).hexdigest(),
            heading_path=paths.get(indices[0], []), rows=len(table.findall('./w:tr', NS)),
            cells=len(table.findall('.//w:tc', NS))))
    return dict(schema_version=1,
        source_url='https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.docx',
        source_sha256=hashlib.sha256(raw).hexdigest(), paragraph_count=len(paragraphs),
        body_start=start, body_end_exclusive=end, normalization='w:t concatenation then Unicode whitespace collapse; no document text copied',
        candidates=records, tables=tables,
        limitation='Keyword/error and complete body table inventory require human semantic review; no coverage status is inferred.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('docx', type=Path)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    result = inventory(args.docx)
    target = BASE / 'authoritative-inventory.json'
    if args.check:
        if result != json.loads(target.read_text()):
            raise SystemExit('authoritative inventory mismatch; review source before replacing')
        print('PASS authoritative DOCX hash/paragraph/table inventory')
    else:
        target.write_text(json.dumps(result, indent=2) + '\n')
