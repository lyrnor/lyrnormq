#!/usr/bin/env python3
"""Offline Phase7j SQLite checkpoint export; normal Controllers never load SQLite.

Stop the old Controller first. Raft WAL must already be independent (Phase7j).
Source files are copied while holding controller.lock; SQLite only opens the copy,
so even WAL shared-memory reader bookkeeping cannot mutate the original files.
"""
import argparse
from contextlib import closing
import fcntl
import json
import os
from pathlib import Path
import shutil
import sqlite3
import stat
import tempfile

MAX_EXPORT = 128 * 1024 * 1024
MAX_SOURCE_FILE = 512 * 1024 * 1024
NAME = 'controller-checkpoint.export.json'


def regular(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f'expected regular file: {path.name}')
    return info


def no_constant(_):
    raise ValueError('non-finite JSON value in checkpoint')


def export(directory):
    directory = Path(directory).resolve(strict=True)
    output = directory / NAME
    if output.exists():
        raise ValueError(f'{NAME} already exists; preserve or move it explicitly before exporting again')
    if (directory / 'controller.bbolt').exists() or (directory / 'controller.bbolt.required').exists():
        raise ValueError('bbolt store or required marker already exists; SQLite export is not authoritative')
    source = directory / 'metadata.sqlite'
    regular(source)
    lock_path = directory / 'controller.lock'
    regular(lock_path)
    if not (directory / 'raft-wal').is_dir():
        raise ValueError('independent raft-wal missing; first upgrade this directory with Phase7j')
    temporary = None
    with lock_path.open('r+b') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError('Controller directory is in use; stop it before exporting') from error
        with tempfile.TemporaryDirectory(prefix='lyrnormq-control-export-') as scratch:
            copy = Path(scratch) / 'metadata.sqlite'
            for suffix in ('', '-wal'):
                original = Path(str(source) + suffix)
                if not original.exists():
                    continue
                if regular(original).st_size > MAX_SOURCE_FILE:
                    raise ValueError(f'{original.name} exceeds the 512MiB source copy limit')
                shutil.copyfile(original, Path(str(copy) + suffix))
            with closing(sqlite3.connect(copy.as_uri() + '?mode=ro', uri=True)) as db:
                db.execute('BEGIN')
                def read(key):
                    row = db.execute('SELECT length(data) FROM meta WHERE key=?', (key,)).fetchone()
                    if row is None:
                        return None
                    if row[0] is None or row[0] > MAX_EXPORT:
                        raise ValueError(f'checkpoint field {key} exceeds export bounds')
                    data = db.execute('SELECT data FROM meta WHERE key=?', (key,)).fetchone()[0]
                    return json.loads(data, parse_constant=no_constant)
                marker = read('raft_wal_migration')
                if type(marker) is not int or marker != 1:
                    raise ValueError('Phase7j migration marker missing; first upgrade with Phase7j')
                identity = read('identity')
                if (not isinstance(identity, list) or len(identity) != 2
                        or not isinstance(identity[0], str) or not identity[0]
                        or len(identity[0].encode()) > 128 or '\0' in identity[0]
                        or type(identity[1]) is not int or not 0 < identity[1] <= 2**64 - 1):
                    raise ValueError('invalid Controller identity')
                result = dict(format=1, identity=identity, state=read('state'),
                              snapshot=read('snapshot'),
                              snapshot_record_version=read('snapshot_record_version'))
        payload = json.dumps(result, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
        if len(payload) > MAX_EXPORT:
            raise ValueError('combined checkpoint exceeds 128MiB export bounds')
        try:
            fd, temporary = tempfile.mkstemp(prefix=NAME + '.tmp-', dir=directory)
            with os.fdopen(fd, 'wb') as file:
                file.write(payload)
                file.flush()
                os.fsync(file.fileno())
            # The controller lock excludes competing exporters and Controllers.
            if output.exists():
                raise ValueError(f'{NAME} appeared concurrently')
            os.rename(temporary, output)
            temporary = None
            parent = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(parent)
            finally:
                os.close(parent)
        finally:
            if temporary is not None:
                os.unlink(temporary)
    return output, len(payload)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', required=True, type=Path)
    args = parser.parse_args()
    try:
        output, size = export(args.data_dir)
    except (OSError, ValueError, sqlite3.Error, RecursionError) as error:
        parser.exit(1, f'export failed: {error}\n')
    print(json.dumps(dict(export=str(output), bytes=size)))


if __name__ == '__main__':
    main()
