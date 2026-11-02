"""Portable version-1 journal fixtures, independent of the new storage library."""
import copy
import json
from pathlib import Path
import sqlite3
import struct
import zlib


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()


def descriptor(outbound):
    value = dict(id=outbound['id'], name='', enabled=True, type=outbound['type'],
                 config={key: outbound['config'][key] for key in sorted(outbound['config'])},
                 delivery={key: outbound['delivery'][key] for key in (
                     'concurrency', 'batch_size', 'batch_wait_ms', 'queue_capacity',
                     'timeout_ms', 'max_attempts', 'retry_base_ms')})
    value['config']['headers'] = dict(sorted(value['config']['headers'].items()))
    return encoded(value)


def frame(transaction, payload=b''):
    metadata = encoded(transaction)
    body = struct.pack('<I', len(metadata)) + metadata + payload
    prefix = b'REQJ' + b'\x01\x00\x00\x00' + struct.pack('<II', len(body), zlib.crc32(body))
    return prefix + struct.pack('<I', zlib.crc32(prefix)) + bytes(4) + body


def wire(outbound='send', rule='hot', seq=0, event_id='legacy-event-0', marker='legacy-shared-marker'):
    return dict(event_id=event_id, delivery_id=f'{event_id}:{rule}:{outbound}',
                rule_id=rule, topic='concurrency/legacy', client_id='legacy-client',
                qos=1, retain=False, timestamp_ms=123456789,
                data=dict(marker=marker, seq=seq, temperature=30))


def install(path, config, entries):
    """entries: wire plus optional failed_once/deadline/inflight/complete/meta_client."""
    path = Path(path)
    with sqlite3.connect(path) as database:
        database.execute("CREATE TABLE rule_configuration (id INTEGER PRIMARY KEY CHECK (id = 1), data BLOB NOT NULL CHECK (typeof(data) = 'blob'))")
        database.execute('PRAGMA application_id=1380795206')
        database.execute('PRAGMA user_version=1')
        database.execute('INSERT INTO rule_configuration VALUES(1, ?)', (encoded(config),))
    path.chmod(0o600)
    root = Path(str(path) + '.outbound-queue')
    root.mkdir(mode=0o700)
    owner = root / 'OWNERSHIP'
    owner.write_bytes(b'')
    owner.chmod(0o600)
    generation = root / 'generation-00000000000000000001'
    generation.mkdir(mode=0o700)
    snapshot = dict(Snapshot=dict(bindings=[], entries=[], next_id=1, next_revision=1))
    revisions = {outbound['id']: index + 1 for index, outbound in enumerate(config['outbounds'])}
    bindings = [dict(outbound_id=o['id'], revision=revisions[o['id']], descriptor=list(descriptor(o)))
                for o in config['outbounds']]
    data = bytearray(frame(snapshot))
    data += frame(dict(Bind=dict(bindings=bindings, next_revision=len(bindings) + 1)))
    for index, item in enumerate(entries, 1):
        value = item['wire']
        outbound = item.get('outbound', value['delivery_id'].rsplit(':', 1)[1] if isinstance(value, dict) else 'send')
        payload = encoded(value) if isinstance(value, dict) else value
        meta = dict(id=index, outbound_id=outbound, revision=revisions[outbound],
                    client_id=item.get('meta_client', value.get('client_id') if isinstance(value, dict) else 'legacy-client'),
                    failures=0, not_before_ms=0, inflight=False, payload_len=len(payload))
        data += frame(dict(Append=dict(records=[dict(meta=meta, checksum=zlib.crc32(payload))],
                                      next_id=index + 1)), payload)
        if item.get('failed_once'):
            data += frame(dict(Retry=dict(ids=[index], not_before_ms=item['deadline'])))
        if item.get('inflight'):
            data += frame(dict(Begin=dict(ids=[index])))
        if item.get('complete'):
            data += frame(dict(Complete=dict(ids=[index])))
    segment = generation / 'segment-00000000000000000001.log'
    segment.write_bytes(data)
    segment.chmod(0o600)
    manifest = b'RMQCUR01' + struct.pack('<Q', 1)
    current = root / 'CURRENT'
    current.write_bytes(manifest + struct.pack('<I', zlib.crc32(manifest)))
    current.chmod(0o600)
    return root


def frames(root):
    for path in sorted(Path(root).glob('generation-*/segment-*.log')):
        data = path.read_bytes()
        offset = 0
        while offset < len(data):
            header = data[offset:offset + 24]
            if len(header) != 24 or header[:4] != b'REQJ':
                raise AssertionError('invalid frame in independent inspection')
            size, checksum = struct.unpack_from('<II', header, 8)
            if zlib.crc32(header[:16]) != struct.unpack_from('<I', header, 16)[0]:
                raise AssertionError('invalid header checksum')
            body = data[offset + 24:offset + 24 + size]
            if len(body) != size or zlib.crc32(body) != checksum:
                raise AssertionError('invalid body checksum')
            metadata_len, = struct.unpack_from('<I', body)
            yield header[4], json.loads(body[4:4 + metadata_len]), body[4 + metadata_len:]
            offset += 24 + size
