#!/usr/bin/env python3
"""Inspect the produced amd64 ELF without executing it or invoking ldd."""
import hashlib
import json
from pathlib import Path
import struct
import sys


def main():
    if len(sys.argv) != 2:
        raise SystemExit('usage: check-static-elf.py EXECUTABLE')
    path = Path(sys.argv[1])
    data = path.read_bytes()
    if len(data) < 64 or data[:6] != b'\x7fELF\x02\x01':
        raise SystemExit('expected ELF64 little-endian executable')
    if struct.unpack_from('<H', data, 18)[0] != 62:
        raise SystemExit('expected x86-64 machine type')
    offset = struct.unpack_from('<Q', data, 32)[0]
    size, count = struct.unpack_from('<HH', data, 54)
    if not count or size < 56 or offset + size * count > len(data):
        raise SystemExit('invalid program headers')
    for index in range(count):
        kind, _, start, _, _, length, _, _ = struct.unpack_from(
            '<IIQQQQQQ', data, offset + index * size)
        if start + length > len(data):
            raise SystemExit('invalid program segment range')
        if kind == 3:
            raise SystemExit('unexpected dynamic interpreter')
        if kind == 2:
            if length % 16:
                raise SystemExit('invalid dynamic entries')
            for pos in range(start, start + length, 16):
                tag, _ = struct.unpack_from('<qQ', data, pos)
                if tag == 1:
                    raise SystemExit('unexpected shared-library dependency')
    print(json.dumps({'path': str(path), 'target': 'x86_64-unknown-linux-musl',
                      'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
                      'interpreter': False, 'needed_shared_libraries': [],
                      'note': 'ELF inspection only, not execution on Linux'}, indent=2))


if __name__ == '__main__':
    main()
